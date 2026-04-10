#!/python3
# satpi
# Executes one scheduled satellite pass from start to finish.
# This includes preparing the pass-specific output directory, starting SatDump
# with the configured reception settings, stopping it at the scheduled time,
# triggering decode and post-processing steps, copying the results and sending
# an optional notification email.
# Author: Andreas Horvath
# Project: Autonomous, Config-driven satellite reception pipeline for Raspberry Pi

import json
import argparse
import logging
import os
import re
import sys
import subprocess
from pathlib import Path
import socket
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from skyfield.api import load, wgs84, EarthSatellite
import shutil

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from load_config import load_config, ConfigError

SNR_LINE_RE = re.compile(
    r"^\[(?P<time>\d{2}:\d{2}:\d{2}) - (?P<date>\d{2}/\d{2}/\d{4})\].*?"
    r"SNR : (?P<snr>[0-9.]+)dB, Peak SNR: (?P<peak>[0-9.]+)dB"
)

SYNC_LINE_RE = re.compile(
    r"^\[(?P<time>\d{2}:\d{2}:\d{2}) - (?P<date>\d{2}/\d{2}/\d{4})\].*?"
    r"Viterbi : (?P<viterbi>[A-Z0-9_]+)\s+BER : (?P<ber>[0-9.]+),\s+Deframer : (?P<deframer>[A-Z0-9_]+)"
)

_TS = load.timescale()
_TLE_CACHE = {
    "path": None,
    "mtime": None,
    "satellites": None,
}

def parse_args():
    parser = argparse.ArgumentParser(description="SATPI pass receiver")
    parser.add_argument("satellite")
    parser.add_argument("frequency_hz", type=int)
    parser.add_argument("bandwidth_hz", type=int)
    parser.add_argument("pipeline")
    parser.add_argument("pass_start")
    parser.add_argument("pass_end")
    parser.add_argument("scheduled_start")
    parser.add_argument("scheduled_end")
    return parser.parse_args()

def parse_utc(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def to_local_dt(utc_timestamp: str, timezone_name: str) -> datetime:
    utc_dt = parse_utc(utc_timestamp)
    return utc_dt.astimezone(ZoneInfo(timezone_name))


def format_local_filename_timestamp(utc_timestamp: str, timezone_name: str) -> str:
    local_dt = to_local_dt(utc_timestamp, timezone_name)
    return local_dt.strftime("%Y-%m-%d_%H-%M-%S")


def safe_name(value: str) -> str:
    value = value.strip().replace(" ", "_").replace("/", "_")
    value = value.replace(":", "-")
    return value

def setup_logger(log_file: str) -> logging.Logger:
    logger = logging.getLogger("satpi.receive_pass")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger

def build_satdump_command(config, args, pass_output_dir):
    hardware = config["hardware"]
    satdump = config["satdump"]

    satdump_bin = satdump["binary_path"]
    sample_rate = str(int(hardware["sample_rate"]))
    frequency_hz = str(args.frequency_hz)
    bandwidth_hz = str(int(args.bandwidth_hz))
    gain_db = str(hardware["gain"])
    live_pipeline = args.pipeline
    source_id = str(hardware["source_id"])

    cmd = [
        satdump_bin,
        "live",
        live_pipeline,
        pass_output_dir,
        "--source",
        "rtlsdr",
        "--source_id",
        source_id,
        "--samplerate",
        sample_rate,
        "--frequency",
        frequency_hz,
        "--bandwidth",
        bandwidth_hz,
        "--gain",
        gain_db,
    ]

    if hardware["bias_t"]:
        cmd.append("--bias")

    return cmd, live_pipeline

def decode_image(config, logger, args, pass_id, pass_output_dir):
    decode_cfg = config["decode"]
    satdump = config["satdump"]

    if not decode_cfg["enabled"]:
        logger.info("Decode disabled in config")
        return False

    cadu_file = os.path.join(pass_output_dir, f"{args.pipeline}.cadu")

    if not os.path.exists(cadu_file):
        logger.info("No CADU file found: %s", cadu_file)
        return False

    cadu_size = os.path.getsize(cadu_file)
    min_size = decode_cfg["min_cadu_size_bytes"]

    logger.info("CADU file=%s", cadu_file)
    logger.info("CADU size=%d bytes", cadu_size)
    logger.info("Minimum CADU size=%d bytes", min_size)

    if cadu_size < min_size:
        logger.info("CADU file below threshold, skipping decode")
        return False

    decode_log_path = os.path.join(config["paths"]["log_dir"], f"{pass_id}-decode.log")
    decode_cmd = [
        satdump["binary_path"],
        args.pipeline,
        "cadu",
        cadu_file,
        pass_output_dir,
    ]

    logger.info("Running decode command: %s", " ".join(decode_cmd))
    logger.info("decode_log=%s", decode_log_path)

    with open(decode_log_path, "w") as decode_log:
        proc = subprocess.Popen(
            decode_cmd,
            stdout=decode_log,
            stderr=subprocess.STDOUT,
            cwd=pass_output_dir,
        )
        rc = proc.wait()

    logger.info("Decode exited with return code %s", rc)

    if rc != 0:
        logger.error("Decode failed")
        return False

    success_dir = os.path.join(
        pass_output_dir,
        decode_cfg["success_dir_relpath"],
    )

    if os.path.isdir(success_dir):
        logger.info("Decode successful, output directory found: %s", success_dir)
        return True

    logger.info("Decode finished but output directory not found: %s", success_dir)
    return False

def copy_output(config, logger, pass_id, pass_output_dir):
    copy_cfg = config["copytarget"]

    if not copy_cfg["enabled"]:
        logger.info("Copy target disabled in config")
        return False, None, None

    pass_name = os.path.basename(pass_output_dir)
    copy_type = copy_cfg["type"]

    if copy_type == "rclone":
        remote = copy_cfg["rclone_remote"]
        remote_path = copy_cfg["rclone_path"]

        if not remote or not remote_path:
            logger.error("rclone target not fully configured")
            return False, None, None

        target = f"{remote}:{remote_path}/{pass_name}"
        upload_log = os.path.join(config["paths"]["log_dir"], f"{os.path.basename(pass_output_dir)}-upload.log")
        cmd = ["rclone", "copy", pass_output_dir, target]
        logger.info("Running copy command: %s", " ".join(cmd))
        logger.info("upload_log=%s", upload_log)

        with open(upload_log, "w") as logf:
            rc = subprocess.call(cmd, stdout=logf, stderr=subprocess.STDOUT)

        logger.info("Copy exited with return code %s", rc)

        if rc != 0:
            logger.error("Copy failed")
            return False, target, None

        link = None
        if copy_cfg["create_link"]:
            link_cmd = ["rclone", "link", target]
            logger.info("Running link command: %s", " ".join(link_cmd))
            try:
                result = subprocess.run(link_cmd, capture_output=True, text=True, check=True)
                link = result.stdout.strip() or None
                logger.info("Link created: %s", link)
            except subprocess.CalledProcessError as e:
                logger.warning("Link creation failed")
                if e.stdout:
                    logger.warning("link stdout: %s", e.stdout.strip())
                if e.stderr:
                    logger.warning("link stderr: %s", e.stderr.strip())

        return True, target, link

    if copy_type == "local":
        local_path = copy_cfg["local_path"]
        if not local_path:
            logger.error("local_path not configured")
            return False, None, None

        target_dir = os.path.join(local_path, pass_name)
        logger.info("Copying locally to %s", target_dir)

        if os.path.exists(target_dir):
            shutil.rmtree(target_dir)

        os.makedirs(os.path.dirname(target_dir), exist_ok=True)
        shutil.copytree(pass_output_dir, target_dir)

        return True, target_dir, None

    logger.error("Unsupported copy target type: %s", copy_type)
    return False, None, None

def get_host_identity():
    hostname = socket.gethostname()
    ip_address = "unknown"

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("8.8.8.8", 80))
        ip_address = sock.getsockname()[0]
        sock.close()
    except Exception:
        try:
            ip_address = socket.gethostbyname(hostname)
        except Exception:
            pass

    return hostname, ip_address


def send_notification(config, logger, args, pass_output_dir, link):
    notify_cfg = config["notify"]

    if not notify_cfg["enabled"]:
        logger.info("Notifications disabled in config")
        return True

    mail_to = notify_cfg["mail_to"]
    mail_bin = notify_cfg["mail_bin"]
    subject_prefix = notify_cfg["mail_subject_prefix"]

    if not mail_to:
        logger.error("notify.mail_to not configured")
        return False

    if not os.path.exists(mail_bin):
        logger.error("mail binary not found: %s", mail_bin)
        return False

    cadu_file = os.path.join(pass_output_dir, f"{args.pipeline}.cadu")
    size_mb = 0
    if os.path.exists(cadu_file):
        size_mb = round(os.path.getsize(cadu_file) / (1024 * 1024), 2)

    hostname, ip_address = get_host_identity()

    subject = (
        f"{subject_prefix} [{hostname} | {ip_address}] "
        f"{args.satellite} images received, cadu size = {size_mb} MB"
    )

    if link:
        body = f"Output link:\n{link}\n"
    else:
        body = f"Output directory:\n{pass_output_dir}\n"

    logger.info("Sending notification mail to %s", mail_to)

    mail_data = f"Subject: {subject}\n\n{body}"
    proc = subprocess.run(
        [mail_bin, mail_to],
        input=mail_data,
        text=True,
        capture_output=True,
    )

    logger.info("Mail command exited with return code %s", proc.returncode)
    if proc.stdout.strip():
        logger.info("mail stdout: %s", proc.stdout.strip())
    if proc.stderr.strip():
        logger.info("mail stderr: %s", proc.stderr.strip())

    if proc.returncode != 0:
        logger.error("Sending notification mail failed")
        return False

    return True

def postprocess_output(config, logger, args, pass_id, pass_output_dir, decode_ok):
    copy_ok, target, link = copy_output(config, logger, pass_id, pass_output_dir)
    logger.info("copy_ok=%s", copy_ok)
    logger.info("copy_target=%s", target)
    logger.info("copy_link=%s", link)

    if not copy_ok:
        logger.info("Copy failed, skipping notification")
        return

    if not decode_ok:
        logger.info("Decode not successful, skipping notification only")
        return

    notify_ok = send_notification(config, logger, args, pass_output_dir, link)
    logger.info("notify_ok=%s", notify_ok)

def satdump_timestamp_to_utc_iso(date_str: str, time_str: str) -> str:
    dt = datetime.strptime(f"{date_str} {time_str}", "%d/%m/%Y %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def parse_satdump_snr_line(line: str):
    m = SNR_LINE_RE.search(line)
    if not m:
        return None

    return {
        "timestamp": satdump_timestamp_to_utc_iso(m.group("date"), m.group("time")),
        "snr_db": float(m.group("snr")),
        "peak_snr_db": float(m.group("peak")),
    }


def parse_satdump_sync_line(line: str):
    m = SYNC_LINE_RE.search(line)
    if not m:
        return None

    return {
        "timestamp": satdump_timestamp_to_utc_iso(m.group("date"), m.group("time")),
        "ber": float(m.group("ber")),
        "viterbi_state": m.group("viterbi"),
        "deframer_state": m.group("deframer"),
    }


def build_reception_json_header(config, args, pass_id):
    hardware = config["hardware"]
    reception_setup = config["reception_setup"]

    return {
        "pass_id": pass_id,
        "satellite": args.satellite,
        "pipeline": args.pipeline,
        "frequency_hz": int(args.frequency_hz),
        "bandwidth_hz": int(args.bandwidth_hz),
        "gain": float(hardware["gain"]),
        "source_id": str(hardware["source_id"]),
        "bias_t": bool(hardware["bias_t"]),
        "pass_start": args.pass_start,
        "pass_end": args.pass_end,
        "scheduled_start": args.scheduled_start,
        "scheduled_end": args.scheduled_end,
        "reception_setup": {
            "antenna_type": reception_setup["antenna_type"],
            "antenna_location": reception_setup["antenna_location"],
            "antenna_orientation": reception_setup["antenna_orientation"],
            "lna": reception_setup["lna"],
            "rf_filter": reception_setup["rf_filter"],
            "feedline": reception_setup["feedline"],
            "raspberry_pi": reception_setup["raspberry_pi"],
            "power_supply": reception_setup["power_supply"],
            "additional_info": reception_setup["additional_info"],
        },
        "samples": [],
    }

def write_json_atomic(target_path: str, payload: dict):
    target = Path(target_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target.with_suffix(target.suffix + ".tmp")

    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
        f.write("\n")

    os.replace(tmp_path, target)


def maybe_build_sample(config, current_radio_state: dict, sync_data: dict, satellite_name: str):
    if current_radio_state.get("snr_db") is None:
        return None
    if current_radio_state.get("peak_snr_db") is None:
        return None

    sample_timestamp = sync_data["timestamp"]

    azimuth_deg, elevation_deg = compute_az_el(config, sample_timestamp, satellite_name)

    return {
        "timestamp": sample_timestamp,
        "snr_db": current_radio_state["snr_db"],
        "peak_snr_db": current_radio_state["peak_snr_db"],
        "ber": sync_data["ber"],
        "viterbi_state": sync_data["viterbi_state"],
        "deframer_state": sync_data["deframer_state"],
        "azimuth_deg": azimuth_deg,
        "elevation_deg": elevation_deg,
    }

def _load_satellites_from_tle_file(tle_path: str):
    mtime = os.path.getmtime(tle_path)

    if (
        _TLE_CACHE["path"] == tle_path
        and _TLE_CACHE["mtime"] == mtime
        and _TLE_CACHE["satellites"] is not None
    ):
        return _TLE_CACHE["satellites"]

    with open(tle_path, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]

    satellites = {}
    i = 0
    while i + 2 < len(lines):
        name = lines[i]
        line1 = lines[i + 1]
        line2 = lines[i + 2]

        if line1.startswith("1 ") and line2.startswith("2 "):
            satellites[name] = EarthSatellite(line1, line2, name, _TS)
            i += 3
        else:
            i += 1

    _TLE_CACHE["path"] = tle_path
    _TLE_CACHE["mtime"] = mtime
    _TLE_CACHE["satellites"] = satellites
    return satellites


def _resolve_satellite_from_tle(satellites: dict, satellite_name: str):
    if satellite_name in satellites:
        return satellites[satellite_name]

    normalized_target = safe_name(satellite_name).upper()

    for name, sat in satellites.items():
        if safe_name(name).upper() == normalized_target:
            return sat

    raise ValueError(f"Satellite not found in TLE file: {satellite_name}")

def compute_az_el(config, sample_timestamp_utc: str, satellite_name: str):
    tle_file = config["network"]["tle_file"]

    satellites = _load_satellites_from_tle_file(tle_file)
    satellite = _resolve_satellite_from_tle(satellites, satellite_name)

    qth = config["qth"]
    observer = wgs84.latlon(
        qth["latitude"],
        qth["longitude"],
        elevation_m=qth["altitude"],
    )

    dt = parse_utc(sample_timestamp_utc)
    t = _TS.from_datetime(dt)

    difference = satellite - observer
    topocentric = difference.at(t)
    alt, az, _distance = topocentric.altaz()

    return float(round(az.degrees, 3)), float(round(alt.degrees, 3))

def render_reception_plots(base_dir, logger, reception_json_path):
    plot_script = os.path.join(base_dir, "bin", "plot_reception.py")

    if not os.path.exists(plot_script):
        logger.warning("plot_reception.py not found: %s", plot_script)
        return False

    if not os.path.exists(reception_json_path):
        logger.warning("reception JSON not found, skipping plots: %s", reception_json_path)
        return False

    cmd = [
        sys.executable,
        plot_script,
        reception_json_path,
    ]

    logger.info("Running plot command: %s", " ".join(cmd))

    result = subprocess.run(
        cmd,
        text=True,
        capture_output=True,
    )

    logger.info("plot_reception.py exited with return code %s", result.returncode)
    if result.stdout.strip():
        logger.info("plot stdout: %s", result.stdout.strip())
    if result.stderr.strip():
        logger.info("plot stderr: %s", result.stderr.strip())

    if result.returncode != 0:
        logger.warning("Plot generation failed")
        return False

    return True

def import_reception_to_db(config, logger, reception_json_path: str) -> bool:
    if not config.get("reception_db", {}).get("enabled", False):
        logger.info("reception_db disabled in config, skipping DB import")
        return False

    if not config.get("reception_db", {}).get("import_after_pass", True):
        logger.info("reception_db import_after_pass=false, skipping DB import")
        return False

    if not os.path.exists(reception_json_path):
        logger.warning("reception JSON not found, cannot import to DB: %s", reception_json_path)
        return False

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    importer_path = os.path.join(base_dir, "bin", "import_reception_to_db.py")
    config_path = os.path.join(base_dir, "config", "config.ini")

    if not os.path.exists(importer_path):
        logger.warning("DB importer script not found: %s", importer_path)
        return False

    cmd = [
        config["systemd"]["python_bin"],
        importer_path,
        "--config",
        config_path,
        reception_json_path,
    ]

    logger.info("Importing reception JSON into DB: %s", reception_json_path)
    logger.info("Running DB import command: %s", " ".join(cmd))

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=base_dir,
    )

    if result.stdout:
        logger.info("DB import stdout: %s", result.stdout.strip())
    if result.stderr:
        logger.warning("DB import stderr: %s", result.stderr.strip())

    if result.returncode != 0:
        logger.error("DB import failed with return code %s", result.returncode)
        return False

    logger.info("DB import completed successfully")
    return True

def main():
    args = parse_args()

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(base_dir, "config", "config.ini")

    try:
        config = load_config(config_path)
    except ConfigError as e:
        print(f"[receive_pass] CONFIG ERROR: {e}")
        return 1

    log_dir = config["paths"]["log_dir"]
    output_dir = config["paths"]["output_dir"]

    os.makedirs(log_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    timezone_name = config["station"]["timezone"]
    local_pass_start = format_local_filename_timestamp(args.pass_start, timezone_name)

    pass_id = f"{local_pass_start}_{safe_name(args.satellite)}"

    pass_output_dir = os.path.join(output_dir, pass_id)

    os.makedirs(pass_output_dir, exist_ok=True)

    log_file = os.path.join(log_dir, f"{pass_id}-receive_pass.log")
    logger = setup_logger(log_file)

    logger.info("receive_pass.py started")
    logger.info("satellite=%s", args.satellite)
    logger.info("frequency_hz=%s", args.frequency_hz)
    logger.info("bandwidth_hz=%s", args.bandwidth_hz)
    logger.info("pipeline=%s", args.pipeline)
    logger.info("pass_start=%s", args.pass_start)
    logger.info("pass_end=%s", args.pass_end)
    logger.info("scheduled_start=%s", args.scheduled_start)
    logger.info("scheduled_end=%s", args.scheduled_end)
    logger.info("local_pass_start=%s", to_local_dt(args.pass_start, timezone_name).strftime("%Y-%m-%d %H:%M:%S %Z"))
    logger.info("local_pass_end=%s", to_local_dt(args.pass_end, timezone_name).strftime("%Y-%m-%d %H:%M:%S %Z"))
    logger.info("local_scheduled_start=%s", to_local_dt(args.scheduled_start, timezone_name).strftime("%Y-%m-%d %H:%M:%S %Z"))
    logger.info("local_scheduled_end=%s", to_local_dt(args.scheduled_end, timezone_name).strftime("%Y-%m-%d %H:%M:%S %Z"))
    logger.info("pass_output_dir=%s", pass_output_dir)
    logger.info("hostname=%s", os.uname().nodename)
    logger.info("user=%s", os.environ.get("USER", "unknown"))
    logger.info("cwd=%s", os.getcwd())

    hardware = config["hardware"]
    satdump = config["satdump"]
    copytarget = config["copytarget"]

    logger.info("config.hardware.source_id=%s", hardware["source_id"])
    logger.info("config.hardware.gain=%s", hardware["gain"])
    logger.info("config.hardware.sample_rate=%s", hardware["sample_rate"])
    logger.info("config.hardware.bias_t=%s", hardware["bias_t"])
    logger.info("config.hardware.ppm_correction=%s", hardware["ppm_correction"])

    logger.info("config.satdump.enabled=%s", satdump["enabled"])
    logger.info("config.satdump.binary_path=%s", satdump["binary_path"])
    logger.info("config.satdump.threads=%s", satdump["threads"])
    logger.info("config.satdump.realtime=%s", satdump["realtime"])

    logger.info("config.copytarget.enabled=%s", copytarget["enabled"])
    logger.info("config.copytarget.type=%s", copytarget["type"])
    logger.info("config.copytarget.local_path=%s", copytarget["local_path"])

    if not satdump["enabled"]:
        logger.info("SatDump disabled in config, exiting without running SatDump")
        logger.info("receive_pass.py finished successfully")
        return 0

    if not os.path.exists(satdump["binary_path"]):
        logger.error("SatDump binary not found: %s", satdump["binary_path"])
        return 1

    cmd, live_pipeline = build_satdump_command(config, args, pass_output_dir)

    logger.info("using live pipeline=%s", live_pipeline)

    satdump_log_path = os.path.join(log_dir, f"{pass_id}-satdump.log")
    reception_json_path = os.path.join(pass_output_dir, "reception.json")

    logger.info("satdump_log=%s", satdump_log_path)
    logger.info("reception_json=%s", reception_json_path)
    logger.info("running SatDump command: %s", " ".join(cmd))

    reception_payload = build_reception_json_header(config, args, pass_id)
    write_json_atomic(reception_json_path, reception_payload)

    current_radio_state = {
        "snr_db": None,
        "peak_snr_db": None,
    }

    scheduled_end_dt = parse_utc(args.scheduled_end)
    stopped_by_scheduler = False

    with open(satdump_log_path, "w", encoding="utf-8") as satdump_log:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd=pass_output_dir,
        )

        logger.info("SatDump started with pid=%s", process.pid)

        while True:
            line = process.stdout.readline()

            if line:
                satdump_log.write(line)
                satdump_log.flush()

                snr_data = parse_satdump_snr_line(line)
                if snr_data:
                    current_radio_state["snr_db"] = snr_data["snr_db"]
                    current_radio_state["peak_snr_db"] = snr_data["peak_snr_db"]

                sync_data = parse_satdump_sync_line(line)
                if sync_data:
                    sample = maybe_build_sample(config, current_radio_state, sync_data, args.satellite)
                    if sample is not None:
                        reception_payload["samples"].append(sample)
                        write_json_atomic(reception_json_path, reception_payload)

            return_code = process.poll()
            if return_code is not None:
                break

            now = datetime.now(timezone.utc)
            if now >= scheduled_end_dt:
                logger.info("Reached scheduled_end, stopping SatDump")
                stopped_by_scheduler = True
                process.terminate()
                try:
                    return_code = process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    logger.warning("SatDump did not terminate in time, killing it")
                    process.kill()
                    return_code = process.wait()
                break

            time.sleep(1)

    logger.info("SatDump exited with return code %s", return_code)

    if stopped_by_scheduler and return_code in (-15, 0):
        logger.info("SatDump was stopped intentionally by scheduler")

        db_import_ok = import_reception_to_db(config, logger, reception_json_path)
        logger.info("db_import_ok=%s", db_import_ok)

        plots_ok = render_reception_plots(base_dir, logger, reception_json_path)
        logger.info("plots_ok=%s", plots_ok)

        decode_ok = decode_image(config, logger, args, pass_id, pass_output_dir)
        logger.info("decode_ok=%s", decode_ok)

        postprocess_output(config, logger, args, pass_id, pass_output_dir, decode_ok)
        logger.info("receive_pass.py finished successfully")
        return 0

    if return_code != 0:
        logger.error("SatDump failed")
        return return_code

    db_import_ok = import_reception_to_db(config, logger, reception_json_path)
    logger.info("db_import_ok=%s", db_import_ok)

    plots_ok = render_reception_plots(base_dir, logger, reception_json_path)
    logger.info("plots_ok=%s", plots_ok)

    decode_ok = decode_image(config, logger, args, pass_id, pass_output_dir)
    logger.info("decode_ok=%s", decode_ok)

    postprocess_output(config, logger, args, pass_id, pass_output_dir, decode_ok)
    logger.info("receive_pass.py finished successfully")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
