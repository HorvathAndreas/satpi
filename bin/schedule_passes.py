#!/usr/bin/env python3
"""satpi – schedule_passes

Generates systemd service and timer units for all relevant future passes.

Reads the predicted pass data, removes outdated generated units and creates
one service + one timer for every pass that should still be received.

Protocol change vs. previous version:
  The generated .service no longer passes the pass data as positional CLI
  arguments. Instead, each pass is written to a sidecar JSON file
  (<unit-basename>.pass.json) next to its unit file, and the service is
  invoked as:
      ExecStart=<python> <receiver_script> --pass-file <json-path>
  The receiver script must accept --pass-file.

Author: Andreas Horvath
Project: Autonomous, config-driven satellite reception pipeline for Raspberry Pi
"""

from __future__ import annotations

import glob
import json
import logging
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from load_config import load_config, ConfigError


# --- Constants ---------------------------------------------------------------

LOG_MAX_BYTES = 1_000_000
LOG_BACKUP_COUNT = 5
SYSTEMCTL_TIMEOUT = 30  # seconds
RECENT_PASS_GRACE_SECONDS = 60  # keep passes that just started within this window

logger = logging.getLogger("satpi.schedule")


# --- Logging -----------------------------------------------------------------

def setup_logging(log_dir: str) -> None:
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "schedule_passes.log")

    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    fh = RotatingFileHandler(log_file, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)


# --- Subprocess helpers ------------------------------------------------------

def run(cmd: Sequence[str], *, check: bool = True, timeout: int = SYSTEMCTL_TIMEOUT) -> subprocess.CompletedProcess:
    """Run *cmd* and log output. Raise if check and returncode != 0."""
    logger.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)

    stdout = result.stdout.strip()
    stderr = result.stderr.strip()
    ok = result.returncode == 0

    if stdout:
        logger.debug("stdout: %s", stdout)
    if stderr:
        (logger.debug if ok else logger.warning)("stderr: %s", stderr)

    if not ok and check:
        raise RuntimeError(f"Command failed ({result.returncode}): {' '.join(cmd)}")
    return result


def ensure_sudo_nopasswd() -> None:
    """Fail fast if sudo would prompt for a password."""
    r = subprocess.run(["sudo", "-n", "true"], capture_output=True, text=True, timeout=5)
    if r.returncode != 0:
        raise RuntimeError(
            "Passwordless sudo is required for systemctl operations. "
            "Configure a sudoers rule for this user (e.g. "
            "'<user> ALL=(root) NOPASSWD: /bin/systemctl') and try again."
        )


def systemctl_is_active(unit: str) -> bool:
    r = subprocess.run(
        ["systemctl", "is-active", "--quiet", unit],
        capture_output=True, text=True, timeout=10,
    )
    return r.returncode == 0


# --- Time / name helpers -----------------------------------------------------

def parse_utc(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def isoformat_utc(dt: datetime) -> str:
    return (
        dt.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )



def systemd_time(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def sanitize_name(value: str) -> str:
    value = value.upper().replace(" ", "-").replace("_", "-")
    value = re.sub(r"[^A-Z0-9\-]", "-", value)
    value = re.sub(r"-+", "-", value).strip("-")
    return value


# --- Direction filtering -----------------------------------------------------

def _normalize_direction(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip().lower().replace("-", "_").replace(" ", "_")
    return s or None


def _azimuth_to_cardinal(azimuth_deg: float) -> str:
    az = float(azimuth_deg) % 360.0
    if az >= 337.5 or az < 22.5:
        return "north"
    if az < 67.5:
        return "northeast"
    if az < 112.5:
        return "east"
    if az < 157.5:
        return "southeast"
    if az < 202.5:
        return "south"
    if az < 247.5:
        return "southwest"
    if az < 292.5:
        return "west"
    return "northwest"


def determine_pass_direction(pass_entry: Dict[str, Any]) -> str:
    """Return a normalized direction label, reconstructing it from azimuths if needed."""
    for key in ("direction", "pass_direction", "flight_direction"):
        if pass_entry.get(key) not in (None, ""):
            return _normalize_direction(pass_entry[key]) or "all"

    aos = _first_present(pass_entry, ("aos_azimuth_deg", "aos_azimuth", "start_azimuth_deg", "start_azimuth"))
    los = _first_present(pass_entry, ("los_azimuth_deg", "los_azimuth", "end_azimuth_deg", "end_azimuth"))
    if aos is None or los is None:
        return "all"
    return f"{_azimuth_to_cardinal(aos)}_to_{_azimuth_to_cardinal(los)}"


def _first_present(d: Dict[str, Any], keys: Iterable[str]) -> Optional[Any]:
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None


# --- Pass loading / preparation ---------------------------------------------

def load_passes(pass_file: str) -> List[Dict[str, Any]]:
    if not os.path.exists(pass_file):
        raise FileNotFoundError(f"Pass file not found: {pass_file}")
    with open(pass_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("passes", [])


def build_scheduled_passes(
    passes: Sequence[Dict[str, Any]],
    pre_start_seconds: int,
    post_stop_seconds: int,
) -> List[Dict[str, Any]]:
    scheduled: List[Dict[str, Any]] = []
    for p in passes:
        entry = dict(p)
        entry["scheduled_start_dt"] = parse_utc(p["start"]) - timedelta(seconds=pre_start_seconds)
        entry["scheduled_end_dt"] = parse_utc(p["end"]) + timedelta(seconds=post_stop_seconds)
        entry["direction"] = determine_pass_direction(entry)  # settle it once, up front
        scheduled.append(entry)
    return sorted(scheduled, key=lambda x: x["scheduled_start_dt"])


def filter_future(
    scheduled_passes: Sequence[Dict[str, Any]],
    now: datetime,
    grace_seconds: int = RECENT_PASS_GRACE_SECONDS,
) -> List[Dict[str, Any]]:
    """Keep passes whose scheduled start is still ahead, or started <grace> seconds ago."""
    cutoff = now - timedelta(seconds=grace_seconds)
    return [p for p in scheduled_passes if p["scheduled_start_dt"] > cutoff]


def filter_by_direction(
    scheduled_passes: Sequence[Dict[str, Any]],
    satellites_cfg: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    by_name = {s["name"]: s for s in satellites_cfg}
    kept: List[Dict[str, Any]] = []
    for p in scheduled_passes:
        sat_cfg = by_name.get(p["satellite"])
        if sat_cfg is None:
            logger.warning("No satellite config found for pass '%s' – skipping", p["satellite"])
            continue
        if not sat_cfg.get("enabled", True):
            continue

        allowed = _normalize_direction(sat_cfg.get("pass_direction", "all"))
        if allowed in (None, "all"):
            kept.append(p)
            continue

        if p["direction"] == allowed:
            kept.append(p)
        else:
            logger.info(
                "Skipping %s pass at %s: direction %s != required %s",
                p["satellite"], p.get("start", "?"), p["direction"], allowed,
            )
    return kept


def warn_on_overlaps(passes: Sequence[Dict[str, Any]]) -> None:
    """Log a warning for any pair of passes whose scheduled windows overlap."""
    ordered = sorted(passes, key=lambda p: p["scheduled_start_dt"])
    for a, b in zip(ordered, ordered[1:]):
        if b["scheduled_start_dt"] < a["scheduled_end_dt"]:
            logger.warning(
                "Overlap: %s ends %s, %s starts %s",
                a["satellite"], isoformat_utc(a["scheduled_end_dt"]),
                b["satellite"], isoformat_utc(b["scheduled_start_dt"]),
            )


# --- Unit file generation ----------------------------------------------------

def make_unit_base_name(p: Dict[str, Any]) -> str:
    sat = sanitize_name(p["satellite"])
    start = p["scheduled_start_dt"].strftime("%Y%m%dT%H%M%SZ")
    return f"satpi-pass-{start}-{sat}"


def _service_content(
    p: Dict[str, Any],
    receiver_script: str,
    python_bin: str,
    base_dir: str,
    service_user: Optional[str],
    pass_file_path: str,
) -> str:
    user_line = f"User={service_user}\n" if service_user else ""
    description = (
        f"SATPI pass receiver for {p['satellite']} "
        f"({p.get('direction', 'unknown')}) at {isoformat_utc(p['scheduled_start_dt'])}"
    )
    # ExecStart is a systemd-parsed command, not a shell command. Keeping the
    # arguments minimal and data-free avoids any quoting issues.
    return (
        "[Unit]\n"
        f"Description={description}\n"
        "After=network-online.target\n"
        "Wants=network-online.target\n"
        "\n"
        "[Service]\n"
        "Type=oneshot\n"
        f"{user_line}"
        f"WorkingDirectory={base_dir}\n"
        f"ExecStart={python_bin} {receiver_script} --pass-file {pass_file_path}\n"
    )


def _timer_content(service_name: str, p: Dict[str, Any]) -> str:
    description = (
        f"SATPI timer for {p['satellite']} "
        f"({p.get('direction', 'unknown')}) at {isoformat_utc(p['scheduled_start_dt'])}"
    )
    return (
        "[Unit]\n"
        f"Description={description}\n"
        "\n"
        "[Timer]\n"
        f"OnCalendar={systemd_time(p['scheduled_start_dt'])}\n"
        "Persistent=true\n"
        f"Unit={service_name}\n"
        "\n"
        "[Install]\n"
        "WantedBy=timers.target\n"
    )


def _pass_sidecar(p: Dict[str, Any]) -> Dict[str, Any]:
    """Serializable view of the pass for the receiver script."""
    return {
        "satellite": p["satellite"],
        "frequency_hz": p["frequency_hz"],
        "bandwidth_hz": p["bandwidth_hz"],
        "pipeline": p["pipeline"],
        "start": p["start"],
        "end": p["end"],
        "scheduled_start": isoformat_utc(p["scheduled_start_dt"]),
        "scheduled_end": isoformat_utc(p["scheduled_end_dt"]),
        "max_elevation": p.get("max_elevation"),
        "max_elevation_time": p.get("max_elevation_time"),
        "aos_azimuth_deg": p.get("aos_azimuth_deg"),
        "los_azimuth_deg": p.get("los_azimuth_deg"),
        "direction": p.get("direction"),
    }


def write_file_atomic(path: str, content: str) -> None:
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(content)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


# --- Cleanup / create / enable ----------------------------------------------

def cleanup_existing_units(generated_units_dir: str) -> None:
    # Find ALL satpi-pass-* units in /etc/systemd/system/, regardless of where their files are
    systemd_units = sorted(glob.glob("/etc/systemd/system/satpi-pass-*.service")) + \
                    sorted(glob.glob("/etc/systemd/system/satpi-pass-*.timer"))

    unit_names = [os.path.basename(p) for p in systemd_units]
    active_skipped: List[str] = []

    for unit in unit_names:
        # Never tear down a unit whose service is currently running.
        service_unit = unit.replace(".timer", ".service")
        if systemctl_is_active(service_unit):
            logger.info("Leaving active unit in place: %s", service_unit)
            active_skipped.append(service_unit)
            if unit.endswith(".timer"):
                active_skipped.append(unit)
            continue

        run(["sudo", "systemctl", "disable", "--now", unit], check=False)
        run(["sudo", "systemctl", "reset-failed", unit], check=False)

    # Unlink any stale symlinks from /etc/systemd/system/
    for unit_file in systemd_units:
        if os.path.islink(unit_file):
            unit_name = os.path.basename(unit_file)
            if unit_name not in active_skipped:
                try:
                    os.unlink(unit_file)
                    logger.info("Removed stale symlink: %s", unit_file)
                except OSError as e:
                    logger.warning("Failed to remove symlink %s: %s", unit_file, e)

    # Clean up sidecar files from generated_units_dir
    for pass_file in glob.glob(os.path.join(generated_units_dir, "satpi-pass-*.pass.json")):
        try:
            os.remove(pass_file)
            logger.info("Removed old pass sidecar: %s", pass_file)
        except FileNotFoundError:
            pass

def create_units(
    generated_units_dir: str,
    receiver_script: str,
    future_passes: Sequence[Dict[str, Any]],
    python_bin: str,
    base_dir: str,
    service_user: Optional[str],
) -> List[Tuple[str, str, str, str]]:
    created: List[Tuple[str, str, str, str]] = []
    for p in future_passes:
        base = make_unit_base_name(p)
        service_name = f"{base}.service"
        timer_name = f"{base}.timer"

        service_path = os.path.join(generated_units_dir, service_name)
        timer_path = os.path.join(generated_units_dir, timer_name)
        pass_path = os.path.join(generated_units_dir, f"{base}.pass.json")

        # Write the sidecar first – the service is only usable once it exists.
        write_file_atomic(pass_path, json.dumps(_pass_sidecar(p), indent=2) + "\n")

        write_file_atomic(
            service_path,
            _service_content(p, receiver_script, python_bin, base_dir, service_user, pass_path),
        )
        write_file_atomic(timer_path, _timer_content(service_name, p))

        created.append((service_name, timer_name, service_path, timer_path))
    return created

def link_and_enable_units(created_units: Sequence[Tuple[str, str, str, str]]) -> None:
    if not created_units:
        run(["sudo", "systemctl", "daemon-reload"])
        return

    # Link each unit individually to avoid the whole command failing if one already exists
    linked_count = 0
    for _, _, service_path, timer_path in created_units:
        for path in [service_path, timer_path]:
            # Use --force to overwrite existing symlinks from old path locations
            result = run(["sudo", "systemctl", "link", "--force", path], check=False)
            if result.returncode == 0:
                linked_count += 1
            else:
                logger.warning("Failed to link %s", path)

    if linked_count == 0:
        logger.error("No units were successfully linked")
        raise RuntimeError("Failed to link any systemd units")

    # Reload daemon
    run(["sudo", "systemctl", "daemon-reload"])

    # Enable and start timers
    for _, _, _, timer_path in created_units:
        timer_name = Path(timer_path).name
        run(["sudo", "systemctl", "enable", timer_name], check=False)
        run(["sudo", "systemctl", "start", timer_name], check=False)

    logger.info("Linked and started %d timer/service pairs", len(created_units))

# --- Main --------------------------------------------------------------------

def main() -> int:
    base_dir = Path(__file__).resolve().parent.parent
    config_path = base_dir / "config" / "config.ini"

    try:
        config = load_config(str(config_path))
    except ConfigError as e:
        print(f"[schedule] CONFIG ERROR: {e}", file=sys.stderr)
        return 2

    setup_logging(config["paths"]["log_dir"])

    try:
        paths = config["paths"]
        pass_file = paths["pass_file"]
        generated_units_dir = paths["generated_units_dir"]
        python_bin = paths["python_bin"]
        receiver_script = str(base_dir / "bin" / "receive_pass.py")
        service_user = config["systemd"].get("service_user") or None
        pre_start = int(config["scheduling"]["pre_start"])
        post_stop = int(config["scheduling"]["post_stop"])
    except (KeyError, ValueError, ConfigError) as e:
        logger.error("Config error: %s", e)
        return 2

    if not os.path.exists(receiver_script):
        logger.error("Receiver script not found: %s", receiver_script)
        return 1

    try:
        ensure_sudo_nopasswd()
    except RuntimeError as e:
        logger.error("%s", e)
        return 1

    os.makedirs(generated_units_dir, exist_ok=True)

    try:
        passes = load_passes(pass_file)
    except FileNotFoundError as e:
        logger.error("%s", e)
        return 1
    logger.info("Loaded %d passes from %s", len(passes), pass_file)

    scheduled = build_scheduled_passes(passes, pre_start, post_stop)
    scheduled = filter_by_direction(scheduled, config["satellites"])
    logger.info("Keeping %d passes after direction filtering", len(scheduled))

    now = datetime.now(timezone.utc)
    future = filter_future(scheduled, now)
    logger.info("Keeping %d future passes", len(future))

    warn_on_overlaps(future)

    try:
        cleanup_existing_units(generated_units_dir)
    except subprocess.TimeoutExpired as e:
        logger.error("systemctl timed out during cleanup: %s", e)
        return 1

    if not future:
        logger.info("No future passes to schedule")
        run(["sudo", "systemctl", "daemon-reload"], check=False)
        return 0

    created = create_units(
        generated_units_dir, receiver_script, future, python_bin, base_dir, service_user,
    )
    logger.info("Created %d timer/service pairs", len(created))

    try:
        link_and_enable_units(created)
    except (RuntimeError, subprocess.TimeoutExpired) as e:
        logger.error("Failed while enabling units: %s", e)
        return 1

    logger.info("Scheduling complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
