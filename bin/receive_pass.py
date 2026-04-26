#!/usr/bin/env python3
"""satpi – receive_pass

Executes one scheduled satellite pass from start to finish.

Reads the pass description from a JSON sidecar (written by schedule_passes.py),
starts SatDump with the configured reception settings, stops it at the
scheduled time, then triggers decode / DB import / plotting / rclone upload /
mail notification.

Improvements vs. the previous version:
  * --pass-file JSON input (no more fragile systemd arg quoting)
  * Skyfield loaded via local cache, with builtin fallback — no network at AOS
  * SatDump stdout consumed on a background thread so the clock check isn't
    blocked by long periods of silence
  * reception.json is persisted periodically (every N seconds) and at the end,
    not once per sync line (saves SD-card writes)
  * SIGTERM/SIGINT handler terminates SatDump cleanly instead of orphaning it
  * Timeouts on every subprocess call
  * Consistent normalize_sat_name helper, tolerant TLE lookup
  * Mail is sent even when rclone upload fails (with the local path)
  * Postprocessing consolidated into a single function

Author: Andreas Horvath
Project: Autonomous, config-driven satellite reception pipeline for Raspberry Pi
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import queue
import re
import signal
import socket
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from skyfield.api import EarthSatellite, Loader, wgs84


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from load_config import ConfigError, load_config  # noqa: E402


# --- Constants ---------------------------------------------------------------

SNR_LINE_RE = re.compile(
    r"^\[(?P<time>\d{2}:\d{2}:\d{2}) - (?P<date>\d{2}/\d{2}/\d{4})\].*?"
    r"SNR : (?P<snr>[0-9.]+)dB, Peak SNR: (?P<peak>[0-9.]+)dB"
)
SYNC_LINE_RE = re.compile(
    r"^\[(?P<time>\d{2}:\d{2}:\d{2}) - (?P<date>\d{2}/\d{2}/\d{4})\].*?"
    r"Viterbi : (?P<viterbi>[A-Z0-9_]+)\s+BER : (?P<ber>[0-9.]+),\s+Deframer : (?P<deframer>[A-Z0-9_]+)"
)

PERSIST_INTERVAL_SECONDS = 10
SATDUMP_TERMINATION_GRACE = 10         # seconds to wait after SIGTERM before SIGKILL
MAX_RUNTIME_SAFETY_MARGIN_MIN = 60     # hard cap = scheduled_end + this
DECODE_TIMEOUT_SECONDS = 15 * 60
COPY_TIMEOUT_SECONDS = 30 * 60
MAIL_TIMEOUT_SECONDS = 60
DB_IMPORT_TIMEOUT_SECONDS = 120
PLOT_TIMEOUT_SECONDS = 180
RCLONE_LINK_TIMEOUT_SECONDS = 60

SKYFIELD_DATA_DIR = os.environ.get(
    "SATPI_SKYFIELD_DATA",
    os.path.join(os.path.expanduser("~"), ".cache", "satpi", "skyfield"),
)

logger = logging.getLogger("satpi.receive_pass")

# Signal the main loop to stop cleanly on SIGTERM/SIGINT.
_STOP_EVENT = threading.Event()


# --- Helpers -----------------------------------------------------------------

def normalize_sat_name(name: str) -> str:
    """Same normalization used across predict_passes / update_tle."""
    return " ".join(name.strip().upper().replace("-", " ").replace("_", " ").split())


def safe_name(value: str) -> str:
    """Filesystem-safe variant: spaces→underscore, path separators out."""
    value = value.strip().replace(" ", "_").replace("/", "_").replace(":", "-")
    return value


def parse_utc(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def to_local_dt(utc_ts: str, tz_name: str) -> datetime:
    return parse_utc(utc_ts).astimezone(ZoneInfo(tz_name))


def format_local_filename_timestamp(utc_ts: str, tz_name: str) -> str:
    return to_local_dt(utc_ts, tz_name).strftime("%Y-%m-%d_%H-%M-%S")


def setup_logger(log_file: str) -> None:
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    fh = RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=3)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)


# --- CLI ---------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="SATPI pass receiver")
    p.add_argument(
        "--pass-file",
        required=True,
        help="JSON sidecar with pass parameters (written by schedule_passes.py)",
    )
    return p.parse_args()


def load_pass_file(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    required = [
        "satellite", "frequency_hz", "bandwidth_hz", "pipeline",
        "start", "end", "scheduled_start", "scheduled_end",
    ]
    missing = [k for k in required if k not in data]
    if missing:
        raise ValueError(f"pass file {path} missing fields: {missing}")
    return data


# --- Skyfield + TLE ----------------------------------------------------------

_SF_LOADER: Optional[Loader] = None
_SF_TIMESCALE = None
_TLE_CACHE: Dict[str, Any] = {"path": None, "mtime": None, "satellites": None}
_TLE_LOOKUP_FAIL_LOGGED = False


def _timescale():
    global _SF_LOADER, _SF_TIMESCALE
    if _SF_TIMESCALE is not None:
        return _SF_TIMESCALE
    os.makedirs(SKYFIELD_DATA_DIR, exist_ok=True)
    _SF_LOADER = Loader(SKYFIELD_DATA_DIR)
    try:
        _SF_TIMESCALE = _SF_LOADER.timescale()
    except Exception:
        logger.warning("Skyfield timescale download failed; using builtin data.")
        _SF_TIMESCALE = _SF_LOADER.timescale(builtin=True)
    return _SF_TIMESCALE


def _load_tle_satellites(tle_path: str) -> Dict[str, EarthSatellite]:
    mtime = os.path.getmtime(tle_path)
    if (
        _TLE_CACHE["path"] == tle_path
        and _TLE_CACHE["mtime"] == mtime
        and _TLE_CACHE["satellites"] is not None
    ):
        return _TLE_CACHE["satellites"]

    with open(tle_path, "r", encoding="utf-8") as f:
        lines = [ln.strip() for ln in f if ln.strip()]

    ts = _timescale()
    satellites: Dict[str, EarthSatellite] = {}
    i = 0
    while i + 2 < len(lines):
        name, l1, l2 = lines[i], lines[i + 1], lines[i + 2]
        if l1.startswith("1 ") and l2.startswith("2 "):
            satellites[normalize_sat_name(name)] = EarthSatellite(l1, l2, name, ts)
            i += 3
        else:
            i += 1

    _TLE_CACHE.update(path=tle_path, mtime=mtime, satellites=satellites)
    return satellites


def compute_az_el(
    config: Dict[str, Any],
    sample_ts_utc: str,
    satellite_name: str,
) -> Tuple[Optional[float], Optional[float]]:
    """Return (az, el) in degrees, or (None, None) if lookup fails."""
    global _TLE_LOOKUP_FAIL_LOGGED
    try:
        sats = _load_tle_satellites(config["paths"]["tle_file"])
        sat = sats.get(normalize_sat_name(satellite_name))
        if sat is None:
            if not _TLE_LOOKUP_FAIL_LOGGED:
                logger.warning("Satellite not found in TLE: %s", satellite_name)
                _TLE_LOOKUP_FAIL_LOGGED = True
            return None, None

        qth = config["qth"]
        observer = wgs84.latlon(qth["latitude"], qth["longitude"], elevation_m=qth["altitude"])
        t = _timescale().from_datetime(parse_utc(sample_ts_utc))
        alt, az, _ = (sat - observer).at(t).altaz()
        return float(round(az.degrees, 3)), float(round(alt.degrees, 3))
    except Exception as e:
        if not _TLE_LOOKUP_FAIL_LOGGED:
            logger.warning("Az/El lookup failed, continuing without geometry: %s", e)
            _TLE_LOOKUP_FAIL_LOGGED = True
        return None, None


# --- SatDump command ---------------------------------------------------------

def build_satdump_command(config: Dict[str, Any], pass_data: Dict[str, Any], pass_output_dir: str) -> List[str]:
    hw = config["hardware"]
    cmd = [
        config["paths"]["satdump_bin"],
        "live",
        pass_data["pipeline"],
        pass_output_dir,
        "--source", "rtlsdr",
        "--source_id", str(hw["source_id"]),
        "--samplerate", str(int(hw["sample_rate"])),
        "--frequency", str(int(pass_data["frequency_hz"])),
        "--bandwidth", str(int(pass_data["bandwidth_hz"])),
        "--gain", str(hw["gain"]),
    ]
    if hw["bias_t"]:
        cmd.append("--bias")
    return cmd


# --- Log parsing -------------------------------------------------------------

def _satdump_ts_to_iso(date_str: str, time_str: str) -> str:
    dt = datetime.strptime(f"{date_str} {time_str}", "%d/%m/%Y %H:%M:%S")
    return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def parse_snr_line(line: str) -> Optional[Dict[str, Any]]:
    m = SNR_LINE_RE.search(line)
    if not m:
        return None
    return {
        "timestamp": _satdump_ts_to_iso(m.group("date"), m.group("time")),
        "snr_db": float(m.group("snr")),
        "peak_snr_db": float(m.group("peak")),
    }


def parse_sync_line(line: str) -> Optional[Dict[str, Any]]:
    m = SYNC_LINE_RE.search(line)
    if not m:
        return None
    return {
        "timestamp": _satdump_ts_to_iso(m.group("date"), m.group("time")),
        "ber": float(m.group("ber")),
        "viterbi_state": m.group("viterbi"),
        "deframer_state": m.group("deframer"),
    }


# --- Reception JSON ----------------------------------------------------------

def build_reception_header(
    config: Dict[str, Any], pass_data: Dict[str, Any], pass_id: str
) -> Dict[str, Any]:
    hw = config["hardware"]
    return {
        "pass_id": pass_id,
        "satellite": pass_data["satellite"],
        "pipeline": pass_data["pipeline"],
        "frequency_hz": int(pass_data["frequency_hz"]),
        "bandwidth_hz": int(pass_data["bandwidth_hz"]),
        "gain": float(hw["gain"]),
        "source_id": str(hw["source_id"]),
        "bias_t": bool(hw["bias_t"]),
        "pass_start": pass_data["start"],
        "pass_end": pass_data["end"],
        "scheduled_start": pass_data["scheduled_start"],
        "scheduled_end": pass_data["scheduled_end"],
        "max_elevation": pass_data.get("max_elevation"),
        "aos_azimuth_deg": pass_data.get("aos_azimuth_deg"),
        "los_azimuth_deg": pass_data.get("los_azimuth_deg"),
        "direction": pass_data.get("direction"),
        "reception_setup": dict(config["reception_setup"]),
        "samples": [],
    }


def write_json_atomic(target_path: str, payload: Dict[str, Any]) -> None:
    target = Path(target_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
            f.write("\n")
        os.replace(tmp, target)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


# --- SatDump stdout reader (thread) -----------------------------------------

def _reader_thread(stream, q: "queue.Queue[Optional[str]]") -> None:
    try:
        for line in iter(stream.readline, ""):
            q.put(line)
    finally:
        q.put(None)  # sentinel: EOF


# --- Run SatDump with periodic persist + clock cap --------------------------

def run_satdump(
    config: Dict[str, Any],
    pass_data: Dict[str, Any],
    pass_output_dir: str,
    reception_json_path: str,
    reception_payload: Dict[str, Any],
    satdump_log_path: str,
    hard_deadline: datetime,
) -> Tuple[int, bool]:
    """Run SatDump, consume stdout in a thread, stop at scheduled_end.

    Returns (return_code, stopped_by_scheduler).
    """
    cmd = build_satdump_command(config, pass_data, pass_output_dir)
    logger.info("Running SatDump: %s", " ".join(cmd))

    stopped_by_scheduler = False
    current_state = {"snr_db": None, "peak_snr_db": None}
    last_persist = time.monotonic()
    samples = reception_payload["samples"]

    with open(satdump_log_path, "w", encoding="utf-8") as sd_log:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd=pass_output_dir,
        )
        logger.info("SatDump started with pid=%s", proc.pid)

        q: "queue.Queue[Optional[str]]" = queue.Queue(maxsize=10000)
        reader = threading.Thread(target=_reader_thread, args=(proc.stdout, q), daemon=True)
        reader.start()

        try:
            while True:
                if _STOP_EVENT.is_set():
                    logger.info("Stop event received — terminating SatDump")
                    stopped_by_scheduler = True
                    break

                now = datetime.now(timezone.utc)
                if now >= hard_deadline:
                    logger.info("Reached scheduled_end, terminating SatDump")
                    stopped_by_scheduler = True
                    break

                try:
                    line = q.get(timeout=1.0)
                except queue.Empty:
                    line = ""  # no output this second

                if line is None:
                    # Reader hit EOF
                    break

                if line:
                    sd_log.write(line)

                    snr = parse_snr_line(line)
                    if snr:
                        current_state["snr_db"] = snr["snr_db"]
                        current_state["peak_snr_db"] = snr["peak_snr_db"]

                    sync = parse_sync_line(line)
                    if sync and current_state["snr_db"] is not None:
                        az, el = compute_az_el(config, sync["timestamp"], pass_data["satellite"])
                        samples.append({
                            "timestamp": sync["timestamp"],
                            "snr_db": current_state["snr_db"],
                            "peak_snr_db": current_state["peak_snr_db"],
                            "ber": sync["ber"],
                            "viterbi_state": sync["viterbi_state"],
                            "deframer_state": sync["deframer_state"],
                            "azimuth_deg": az,
                            "elevation_deg": el,
                        })

                if proc.poll() is not None and q.empty():
                    break

                # Periodic persist
                if time.monotonic() - last_persist >= PERSIST_INTERVAL_SECONDS:
                    write_json_atomic(reception_json_path, reception_payload)
                    last_persist = time.monotonic()

            # Graceful termination.
            if proc.poll() is None:
                proc.terminate()
                try:
                    rc = proc.wait(timeout=SATDUMP_TERMINATION_GRACE)
                except subprocess.TimeoutExpired:
                    logger.warning("SatDump did not terminate in time, killing it")
                    proc.kill()
                    rc = proc.wait()
            else:
                rc = proc.returncode

            # Drain any queued output before closing.
            while True:
                try:
                    line = q.get_nowait()
                except queue.Empty:
                    break
                if line is None or not line:
                    continue
                sd_log.write(line)
        except BaseException:
            # Make absolutely sure SatDump doesn't outlive us.
            if proc.poll() is None:
                proc.kill()
                proc.wait()
            raise

    # Final persist at end.
    write_json_atomic(reception_json_path, reception_payload)
    return rc, stopped_by_scheduler


# --- Postprocessing ----------------------------------------------------------

def _run_with_timeout(
    cmd: List[str],
    *,
    timeout: int,
    log_path: Optional[str] = None,
    cwd: Optional[str] = None,
) -> int:
    logger.info("Running: %s", " ".join(cmd))
    try:
        if log_path:
            with open(log_path, "w", encoding="utf-8") as lf:
                proc = subprocess.run(
                    cmd, stdout=lf, stderr=subprocess.STDOUT, timeout=timeout, cwd=cwd,
                )
        else:
            proc = subprocess.run(
                cmd, capture_output=True, text=True, timeout=timeout, cwd=cwd,
            )
            if proc.stdout and proc.stdout.strip():
                logger.info("stdout: %s", proc.stdout.strip())
            if proc.stderr and proc.stderr.strip():
                (logger.debug if proc.returncode == 0 else logger.warning)(
                    "stderr: %s", proc.stderr.strip()
                )
    except subprocess.TimeoutExpired:
        logger.error("Command timed out after %ds: %s", timeout, " ".join(cmd))
        return 124
    logger.info("Exit code: %s", proc.returncode)
    return proc.returncode


def decode_image(
    config: Dict[str, Any],
    pass_data: Dict[str, Any],
    pass_id: str,
    pass_output_dir: str,
) -> bool:
    decode_cfg = config["decode"]
    cadu = os.path.join(pass_output_dir, f"{pass_data['pipeline']}.cadu")
    if not os.path.exists(cadu):
        logger.info("No CADU file: %s", cadu)
        return False

    size = os.path.getsize(cadu)
    logger.info("CADU %s (%d bytes, min=%d)", cadu, size, decode_cfg["min_cadu_size_bytes"])
    if size < decode_cfg["min_cadu_size_bytes"]:
        logger.info("CADU below threshold — skipping decode")
        return False

    log_path = os.path.join(config["paths"]["log_dir"], f"{pass_id}-decode.log")
    rc = _run_with_timeout(
        [config["paths"]["satdump_bin"], pass_data["pipeline"], "cadu", cadu, pass_output_dir],
        timeout=DECODE_TIMEOUT_SECONDS,
        log_path=log_path,
        cwd=pass_output_dir,
    )
    if rc != 0:
        logger.error("Decode failed")
        return False

    success_dir = os.path.join(pass_output_dir, decode_cfg["success_dir_relpath"])
    if os.path.isdir(success_dir):
        logger.info("Decode successful: %s", success_dir)
        return True
    logger.info("Decode finished but success dir missing: %s", success_dir)
    return False


def copy_output(
    config: Dict[str, Any], pass_id: str, pass_output_dir: str,
) -> Tuple[bool, Optional[str], Optional[str]]:
    cfg = config["copytarget"]
    if not cfg["enabled"]:
        logger.info("Copy target disabled")
        return False, None, None
    if cfg["type"] != "rclone":
        logger.error("Unsupported copy type: %s", cfg["type"])
        return False, None, None

    remote, remote_path = cfg["rclone_remote"], cfg["rclone_path"]
    if not remote or not remote_path:
        logger.error("rclone target not fully configured")
        return False, None, None

    pass_name = os.path.basename(pass_output_dir)
    target = f"{remote}:{remote_path}/{pass_name}"
    upload_log = os.path.join(config["paths"]["log_dir"], f"{pass_id}-upload.log")

    rc = _run_with_timeout(
        ["rclone", "copy", pass_output_dir, target],
        timeout=COPY_TIMEOUT_SECONDS,
        log_path=upload_log,
    )
    if rc != 0:
        logger.error("Copy failed (rc=%s)", rc)
        return False, target, None

    link: Optional[str] = None
    if cfg["create_link"]:
        try:
            result = subprocess.run(
                ["rclone", "link", target], capture_output=True, text=True,
                check=True, timeout=RCLONE_LINK_TIMEOUT_SECONDS,
            )
            link = result.stdout.strip() or None
            logger.info("Link: %s", link)
        except subprocess.TimeoutExpired:
            logger.warning("rclone link timed out")
        except subprocess.CalledProcessError as e:
            logger.warning("Link creation failed")
            if e.stdout:
                logger.warning("stdout: %s", e.stdout.strip())
            if e.stderr:
                logger.warning("stderr: %s", e.stderr.strip())

    return True, target, link


def _host_identity() -> Tuple[str, str]:
    hostname = socket.gethostname()
    ip = "unknown"
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(2)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
    except Exception:
        try:
            ip = socket.gethostbyname(hostname)
        except Exception:
            pass
    return hostname, ip


def _reception_summary(reception_payload: Dict[str, Any]) -> str:
    samples = reception_payload.get("samples", [])
    synced = [s for s in samples if s.get("deframer_state") == "SYNCED"]
    lines = [
        f"Satellite:       {reception_payload['satellite']}",
        f"Direction:       {reception_payload.get('direction', '?')}",
        f"Max elevation:   {reception_payload.get('max_elevation', '?')}°",
        f"Samples:         {len(samples)} total, {len(synced)} synced",
    ]
    if samples:
        snrs = [s["snr_db"] for s in samples if s.get("snr_db") is not None]
        if snrs:
            snrs_sorted = sorted(snrs)
            median = snrs_sorted[len(snrs_sorted) // 2]
            lines.append(f"SNR median/peak: {median:.1f} / {max(snrs):.1f} dB")
    return "\n".join(lines)


def send_notification(
    config: Dict[str, Any],
    pass_data: Dict[str, Any],
    pass_output_dir: str,
    reception_payload: Dict[str, Any],
    copy_target: Optional[str],
    link: Optional[str],
    copy_ok: bool,
) -> bool:
    ncfg = config["notify"]
    if not ncfg["enabled"]:
        logger.info("Notifications disabled")
        return True
    if not ncfg.get("mail_to"):
        logger.error("notify.mail_to not configured")
        return False

    mail_bin = config["paths"]["mail_bin"]
    if not os.path.exists(mail_bin):
        logger.error("mail binary not found: %s", mail_bin)
        return False

    cadu = os.path.join(pass_output_dir, f"{pass_data['pipeline']}.cadu")
    size_mb = round(os.path.getsize(cadu) / (1024 * 1024), 2) if os.path.exists(cadu) else 0

    hostname, ip = _host_identity()
    status = "ok" if copy_ok else "copy-FAILED"
    subject = (
        f"{ncfg['mail_subject_prefix']} [{hostname} | {ip}] "
        f"{pass_data['satellite']} [{status}], CADU={size_mb} MB"
    )

    body_parts = [_reception_summary(reception_payload), ""]
    if link:
        body_parts += ["Output link:", link]
    elif copy_ok:
        body_parts += ["Copy target:", copy_target or "(unknown)"]
    else:
        body_parts += [
            "Upload failed — files stayed local at:",
            pass_output_dir,
        ]
    body = "\n".join(body_parts) + "\n"

    mail_data = f"Subject: {subject}\n\n{body}"
    try:
        proc = subprocess.run(
            [mail_bin, ncfg["mail_to"]],
            input=mail_data, text=True, capture_output=True,
            timeout=MAIL_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        logger.error("Mail send timed out")
        return False

    if proc.returncode != 0:
        logger.error("Mail failed (rc=%s)", proc.returncode)
        if proc.stderr.strip():
            logger.error("mail stderr: %s", proc.stderr.strip())
        return False
    logger.info("Notification mail sent to %s", ncfg["mail_to"])
    return True


def postprocess(
    config: Dict[str, Any],
    pass_data: Dict[str, Any],
    pass_id: str,
    pass_output_dir: str,
    reception_json_path: str,
    reception_payload: Dict[str, Any],
    base_dir: str,
) -> None:
    # DB import
    try:
        rc = _run_with_timeout(
            [config["paths"]["python_bin"],
             os.path.join(base_dir, "bin", "import_reception_to_db.py"),
             reception_json_path],
            timeout=DB_IMPORT_TIMEOUT_SECONDS,
            cwd=base_dir,
        )
        logger.info("db_import_ok=%s", rc == 0)
    except FileNotFoundError as e:
        logger.warning("DB importer not found: %s", e)

    # Plots
    plot_script = os.path.join(base_dir, "bin", "plot_receptions.py")
    if os.path.exists(plot_script):
        rc = _run_with_timeout(
            [config["paths"]["python_bin"], plot_script, "--pass-id", pass_id],
            timeout=PLOT_TIMEOUT_SECONDS,
            cwd=base_dir,
        )
        logger.info("plots_ok=%s", rc == 0)
    else:
        logger.warning("plot_receptions.py not found: %s", plot_script)

    # Decode
    decode_ok = decode_image(config, pass_data, pass_id, pass_output_dir)
    logger.info("decode_ok=%s", decode_ok)

    # Copy
    copy_ok, target, link = copy_output(config, pass_id, pass_output_dir)
    logger.info("copy_ok=%s target=%s link=%s", copy_ok, target, link)

    # Notify — even if copy failed, the operator wants to know.
    if decode_ok:
        notify_ok = send_notification(
            config, pass_data, pass_output_dir, reception_payload,
            target, link, copy_ok,
        )
        logger.info("notify_ok=%s", notify_ok)
    else:
        logger.info("Skipping notification — decode not successful")


# --- Signals -----------------------------------------------------------------

def _install_signal_handlers() -> None:
    def handler(signum, _frame):
        logger.warning("Received signal %s — requesting clean stop", signum)
        _STOP_EVENT.set()
    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)


# --- Main --------------------------------------------------------------------

def main() -> int:
    args = parse_args()

    base_dir = str(Path(__file__).resolve().parent.parent)
    config_path = os.path.join(base_dir, "config", "config.ini")

    try:
        config = load_config(config_path)
    except ConfigError as e:
        print(f"[receive_pass] CONFIG ERROR: {e}", file=sys.stderr)
        return 2

    log_dir = config["paths"]["log_dir"]
    output_dir = config["paths"]["output_dir"]
    os.makedirs(log_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    try:
        pass_data = load_pass_file(args.pass_file)
    except (OSError, ValueError, json.JSONDecodeError) as e:
        print(f"[receive_pass] PASS FILE ERROR: {e}", file=sys.stderr)
        return 2

    tz = config["station"]["timezone"]
    local_start = format_local_filename_timestamp(pass_data["start"], tz)
    pass_id = f"{local_start}_{safe_name(pass_data['satellite'])}"
    pass_output_dir = os.path.join(output_dir, pass_id)
    os.makedirs(pass_output_dir, exist_ok=True)

    setup_logger(os.path.join(log_dir, f"{pass_id}-receive_pass.log"))
    _install_signal_handlers()

    logger.info("receive_pass.py started")
    logger.info("pass_file=%s", args.pass_file)
    logger.info("pass_id=%s", pass_id)
    logger.info("satellite=%s freq=%s Hz bw=%s Hz pipeline=%s",
                pass_data["satellite"], pass_data["frequency_hz"],
                pass_data["bandwidth_hz"], pass_data["pipeline"])
    logger.info("pass_start=%s pass_end=%s", pass_data["start"], pass_data["end"])
    logger.info("scheduled_start=%s scheduled_end=%s",
                pass_data["scheduled_start"], pass_data["scheduled_end"])
    logger.info("local pass end: %s",
                to_local_dt(pass_data["end"], tz).strftime("%Y-%m-%d %H:%M:%S %Z"))
    logger.info("hostname=%s user=%s",
                os.uname().nodename, os.environ.get("USER", "unknown"))

    # Sanity: scheduled_end must be in the future (with small skew tolerance).
    scheduled_end = parse_utc(pass_data["scheduled_end"])
    now = datetime.now(timezone.utc)
    if scheduled_end <= now:
        logger.error("scheduled_end (%s) is not in the future (now=%s) — aborting",
                     pass_data["scheduled_end"], now.isoformat())
        return 1

    # Hard cap: scheduled_end + safety margin.
    hard_deadline = min(
        scheduled_end,
        now + timedelta(hours=6),  # absolute ceiling for any single pass
    )
    hard_deadline += timedelta(minutes=0)  # no slack by default
    runtime_ceiling = scheduled_end + timedelta(minutes=MAX_RUNTIME_SAFETY_MARGIN_MIN)

    satdump_bin = config["paths"]["satdump_bin"]
    if not os.path.exists(satdump_bin):
        logger.error("SatDump binary not found: %s", satdump_bin)
        return 1

    # Build header; write once now so the DB importer / plotter can see it mid-pass.
    reception_payload = build_reception_header(config, pass_data, pass_id)
    reception_json_path = os.path.join(pass_output_dir, "reception.json")
    write_json_atomic(reception_json_path, reception_payload)

    satdump_log_path = os.path.join(log_dir, f"{pass_id}-satdump.log")
    logger.info("satdump_log=%s", satdump_log_path)
    logger.info("reception_json=%s", reception_json_path)

    try:
        rc, stopped_by_scheduler = run_satdump(
            config, pass_data, pass_output_dir,
            reception_json_path, reception_payload,
            satdump_log_path, hard_deadline,
        )
    except Exception:
        logger.exception("Unhandled error during SatDump run")
        return 1

    # Enforce absolute runtime ceiling.
    if datetime.now(timezone.utc) > runtime_ceiling:
        logger.warning("Runtime ceiling exceeded — skipping postprocessing")
        return 1

    logger.info("SatDump exited rc=%s stopped_by_scheduler=%s", rc, stopped_by_scheduler)

    expected_rcs = {0}
    if stopped_by_scheduler:
        expected_rcs.add(-signal.SIGTERM)  # usually -15 on POSIX

    if rc not in expected_rcs:
        logger.error("SatDump failed (rc=%s)", rc)
        return rc if rc and rc > 0 else 1

    postprocess(
        config, pass_data, pass_id, pass_output_dir,
        reception_json_path, reception_payload, base_dir,
    )
    logger.info("receive_pass.py finished successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
