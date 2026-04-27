#!/usr/bin/env python3
"""satpi – measure_noise_floor

Measures the RF noise floor using rtl_power and stores results in SQLite.

Checks for conflicting satpi pass timers before measuring and can optionally
stop them. Designed to run locally on the Pi (satpi5 or satpi4).

Usage examples:
    # Single 60-second measurement
    python3 measure_noise_floor.py

    # Old-style: 5 measurements, one per hour
    python3 measure_noise_floor.py --count 5 --interval 3600

    # Start at a fixed time today
    python3 measure_noise_floor.py --start-at 21:30 --duration 3600

    # Multiple fixed times
    python3 measure_noise_floor.py --start-at 05:45 --start-at 10:00 \
        --start-at 20:30 --start-at 00:30 --duration 3600

    # Start at sunset, measure for 1 hour
    python3 measure_noise_floor.py --anchor sunset --offset-minutes -30 --duration 3600

    # Measure every 15 minutes from sunrise to sunset, all day
    python3 measure_noise_floor.py --anchor sunrise --every 15 minute \
        --until-anchor sunset --duration 300

    # One week of daily sunset measurements
    python3 measure_noise_floor.py --anchor sunset --every 1 day --count 7 \
        --duration 3600 --label "sunset_{date}_{n}"

    # Stop conflicting pass timers automatically (requires sudo)
    python3 measure_noise_floor.py --stop-timers --sudo-password YOUR_PW

Author: Andreas Horvath
Project: Autonomous, config-driven satellite reception pipeline for Raspberry Pi
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import signal
import socket
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone, timedelta, date as date_type
from logging.handlers import RotatingFileHandler
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore[no-redef]

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from load_config import load_config, ConfigError

LOG_MAX_BYTES = 1_000_000
LOG_BACKUP_COUNT = 3
DB_NAME = "noise_floor.db"

ANCHOR_EVENTS = {"sunrise", "sunset", "noon", "midnight"}
EVERY_UNITS   = {"minute", "hour", "day", "week"}

logger = logging.getLogger("satpi.noise_floor")
_STOP_REQUESTED = False


# ---------------------------------------------------------------------------
# Signal handling
# ---------------------------------------------------------------------------

def _install_signal_handlers() -> None:
    def _handler(signum, _frame):
        global _STOP_REQUESTED
        _STOP_REQUESTED = True
        logger.warning("Signal %s received; stopping after current measurement.", signum)
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handler)
        except (ValueError, OSError):
            pass


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging(log_dir: str, verbose: bool = False) -> None:
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "measure_noise_floor.log")
    level = logging.DEBUG if verbose else logging.INFO
    logger.setLevel(level)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh = RotatingFileHandler(log_file, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT)
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Measure RF noise floor and store in SQLite.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Scheduling modes
────────────────
NEW — one or more fixed start times (repeatable):
  --start-at 05:45 --start-at 20:30 --duration 3600

NEW — anchor to astronomical event + optional offset:
  --anchor sunrise --offset-minutes -30 --duration 3600
  --anchor sunrise --anchor sunset   (two measurements)

NEW — recurring (combine with --start-at or --anchor):
  --anchor sunrise --every 15 minute --until-anchor sunset
  --start-at 06:00 --every 1 hour --count 24

OLD — fixed count with interval (still supported):
  --count 5 --interval 3600

Label templates (used with --label):
  {time}    UTC time of measurement start  e.g. 20:15
  {date}    date of measurement start      e.g. 2026-04-27
  {anchor}  anchor event name              e.g. sunset
  {n}       sequential number (1-based)    e.g. 3
  Example:  --label "experiment_{anchor}_{date}_{n}"
""",
    )

    # ── Core ──────────────────────────────────────────────────────────────
    parser.add_argument("--config", default=None,
                        help="Path to config.ini (default: ../config/config.ini)")
    parser.add_argument("--duration", type=int, default=None,
                        help="Duration of each measurement in seconds "
                             "(default: measurement_duration from config.ini, fallback 600)")
    parser.add_argument("--gain", type=float, default=None,
                        help="SDR gain in dB (default: from config.ini)")
    parser.add_argument("--label", default=None,
                        help="Label stored in the database. Supports placeholders: "
                             "{time}, {date}, {anchor}, {n}")

    # ── Frequency ─────────────────────────────────────────────────────────
    freq = parser.add_argument_group("Frequency (override config.ini)")
    freq.add_argument("--freq-start", type=float, default=None,
                      help="Start frequency in MHz")
    freq.add_argument("--freq-end", type=float, default=None,
                      help="End frequency in MHz")
    freq.add_argument("--bin-size", type=float, default=None,
                      help="FFT bin size in kHz")

    # ── New scheduling ────────────────────────────────────────────────────
    sched = parser.add_argument_group("Scheduling (new)")
    sched.add_argument("--start-at", action="append", default=[], metavar="HH:MM",
                       help="Start measurement at this local time. "
                            "May be specified multiple times for a list of start times. "
                            "If the time has already passed, waits until tomorrow "
                            "unless --no-wait is set.")
    sched.add_argument("--anchor", action="append", default=[],
                       choices=list(ANCHOR_EVENTS), metavar="EVENT",
                       help="Start relative to an astronomical event: "
                            "sunrise | sunset | noon | midnight. "
                            "May be specified multiple times. "
                            "Combine with --offset-minutes to shift the start time.")
    sched.add_argument("--offset-minutes", type=int, default=0, metavar="N",
                       help="Offset in minutes relative to --anchor or --start-at. "
                            "Positive = later, negative = earlier (default: 0).")
    sched.add_argument("--date", default=None, metavar="YYYY-MM-DD",
                       help="Date for which --anchor times are computed "
                            "(default: today, or tomorrow if the event has already passed).")
    sched.add_argument("--every", nargs=2, metavar=("N", "UNIT"),
                       help="After the first measurement, repeat every N units. "
                            "UNIT: minute | hour | day | week. "
                            "Combine with --until, --until-anchor, or --count to stop.")
    sched.add_argument("--until", default=None, metavar="HH:MM",
                       help="Stop repeating at this local time (for use with --every).")
    sched.add_argument("--until-anchor", default=None,
                       choices=list(ANCHOR_EVENTS), metavar="EVENT",
                       help="Stop repeating at an astronomical event (for use with --every).")
    sched.add_argument("--no-wait", action="store_true",
                       help="If a --start-at or --anchor time has already passed, "
                            "start immediately instead of waiting until tomorrow.")

    # ── Old scheduling (still supported) ──────────────────────────────────
    old = parser.add_argument_group("Scheduling (legacy)")
    old.add_argument("--count", type=int, default=None,
                     help="Number of measurements. With --every: maximum repetitions. "
                          "Without --every and without --start-at/--anchor: "
                          "run N times back-to-back with --interval between them (default: 1).")
    old.add_argument("--interval", type=int, default=3600,
                     help="Seconds between measurements in legacy mode (default: 3600).")

    # ── Control ───────────────────────────────────────────────────────────
    ctrl = parser.add_argument_group("Control")
    ctrl.add_argument("--stop-timers", action="store_true",
                      help="Stop conflicting satpi pass timers before measuring")
    ctrl.add_argument("--sudo-password", default=None,
                      help="Sudo password for stopping system timers")
    ctrl.add_argument("--dry-run", action="store_true",
                      help="Show what would happen without actually measuring")
    ctrl.add_argument("--verbose", action="store_true",
                      help="Enable debug logging")
    ctrl.add_argument("--install-timer", nargs="?", const="", default=None,
                      metavar="ONCALENDAR",
                      help="Install a systemd timer that runs this script automatically. "
                           "Without a value: uses schedule_minute from config.ini. "
                           "Or pass any systemd OnCalendar expression. Requires sudo.")
    ctrl.add_argument("--remove-timer", action="store_true",
                      help="Remove the satpi-noise-floor systemd timer and service")

    return parser.parse_args()


def get_config_path(cli_value: str | None) -> str:
    if cli_value:
        return os.path.abspath(cli_value)
    return str(SCRIPT_DIR.parent / "config" / "config.ini")


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def open_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db(db_path: str) -> None:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = open_db(db_path)
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS noise_measurements (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_utc    TEXT    NOT NULL,
                host             TEXT    NOT NULL,
                sdr_device       TEXT,
                antenna          TEXT,
                gain             REAL    NOT NULL,
                freq_start_hz    INTEGER NOT NULL,
                freq_end_hz      INTEGER NOT NULL,
                bin_size_hz      REAL    NOT NULL,
                duration_seconds INTEGER NOT NULL,
                label            TEXT,
                timers_stopped   TEXT,
                created_at       TEXT    NOT NULL
            );
            CREATE TABLE IF NOT EXISTS noise_samples (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                measurement_id  INTEGER NOT NULL REFERENCES noise_measurements(id),
                sample_time_utc TEXT    NOT NULL,
                frequency_hz    INTEGER NOT NULL,
                power_dbm       REAL    NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_samples_measurement
                ON noise_samples(measurement_id);
            CREATE INDEX IF NOT EXISTS idx_samples_freq
                ON noise_samples(frequency_hz);
            CREATE INDEX IF NOT EXISTS idx_measurements_ts
                ON noise_measurements(timestamp_utc);
        """)
        conn.commit()
        logger.debug("Database initialised: %s", db_path)
    finally:
        conn.close()


def insert_measurement(db_path: str, meta: dict, samples: list[dict]) -> int:
    conn = open_db(db_path)
    try:
        cur = conn.execute("""
            INSERT INTO noise_measurements
                (timestamp_utc, host, sdr_device, antenna, gain,
                 freq_start_hz, freq_end_hz, bin_size_hz, duration_seconds,
                 label, timers_stopped, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            meta["timestamp_utc"], meta["host"], meta["sdr_device"],
            meta["antenna"], meta["gain"],
            meta["freq_start_hz"], meta["freq_end_hz"],
            meta["bin_size_hz"], meta["duration_seconds"],
            meta.get("label"), meta.get("timers_stopped"),
            datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        ))
        measurement_id = cur.lastrowid
        conn.executemany("""
            INSERT INTO noise_samples (measurement_id, sample_time_utc, frequency_hz, power_dbm)
            VALUES (?,?,?,?)
        """, [
            (measurement_id, s["sample_time_utc"], s["frequency_hz"], s["power_dbm"])
            for s in samples
        ])
        conn.commit()
        return measurement_id
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Timer conflict detection
# ---------------------------------------------------------------------------

def _parse_time_to_seconds(time_str: str) -> int | None:
    """Parse systemctl 'time left' strings like '2h 36min', '5min', '30s'."""
    total = 0
    found = False
    for val, unit in re.findall(r"(\d+)\s*(h|min|s)", time_str):
        found = True
        v = int(val)
        if unit == "h":
            total += v * 3600
        elif unit == "min":
            total += v * 60
        else:
            total += v
    return total if found else None


def get_conflicting_timers(within_seconds: int = 300) -> list[str]:
    """Return satpi pass timer names that fire within the next N seconds."""
    try:
        result = subprocess.run(
            ["systemctl", "list-timers", "--all", "--no-pager"],
            capture_output=True, text=True, timeout=10
        )
    except Exception as e:
        logger.warning("Could not list timers: %s", e)
        return []

    conflicting = []
    for line in result.stdout.splitlines():
        if "satpi-pass-" not in line:
            continue
        stripped = line.strip()
        if stripped.startswith("-"):
            continue
        timer_name = next((p for p in line.split() if p.endswith(".timer")), None)
        if not timer_name:
            continue
        time_left_match = re.search(
            r"(\d+h\s+\d+min|\d+\s*h|\d+\s*min|\d+\s*s)(?=\s)", line
        )
        if time_left_match:
            secs = _parse_time_to_seconds(time_left_match.group(1))
            if secs is not None and secs <= within_seconds:
                conflicting.append(timer_name)
    return list(set(conflicting))


def find_conflict_free_minute(duration_seconds: int) -> int | None:
    """Scan all scheduled satpi-pass timers and find a conflict-free minute (0-59)."""
    try:
        result = subprocess.run(
            ["systemctl", "list-timers", "satpi-pass-*", "--all", "--no-pager"],
            capture_output=True, text=True, timeout=10
        )
    except Exception as e:
        logger.warning("Could not list pass timers for suggestion: %s", e)
        return None

    duration_minutes = (duration_seconds + 59) // 60
    blocked: set[int] = set()

    for line in result.stdout.splitlines():
        if "satpi-pass-" not in line:
            continue
        stripped = line.strip()
        if stripped.startswith("-"):
            continue
        m = re.search(r"\b(\d{2}):(\d{2}):\d{2}\b", line)
        if m:
            pass_minute = int(m.group(2))
            for delta in range(-duration_minutes, duration_minutes + 1):
                blocked.add((pass_minute + delta) % 60)

    for candidate in range(60):
        if candidate not in blocked:
            return candidate
    return None


def stop_timer(timer_name: str, sudo_password: str | None) -> bool:
    cmd_stop    = ["sudo", "-S", "systemctl", "stop",    timer_name]
    cmd_disable = ["sudo", "-S", "systemctl", "disable", timer_name]
    pw_input = (sudo_password + "\n").encode() if sudo_password else None
    try:
        r1 = subprocess.run(cmd_stop,    input=pw_input, capture_output=True, timeout=10)
        subprocess.run(cmd_disable, input=pw_input, capture_output=True, timeout=10)
        if r1.returncode == 0:
            logger.info("Stopped timer: %s", timer_name)
            return True
        else:
            logger.warning("Could not stop timer %s: %s", timer_name,
                           r1.stderr.decode().strip()[:200])
            return False
    except Exception as e:
        logger.warning("Error stopping timer %s: %s", timer_name, e)
        return False


# ---------------------------------------------------------------------------
# systemd timer install / remove
# ---------------------------------------------------------------------------

TIMER_NAME   = "satpi-noise-floor.timer"
SERVICE_NAME = "satpi-noise-floor.service"
SYSTEMD_DIR  = "/etc/systemd/system"


def _sudo_run(args: list[str], sudo_password: str | None, timeout: int = 15) -> bool:
    cmd = ["sudo", "-S"] + args
    pw = (sudo_password + "\n").encode() if sudo_password else None
    try:
        r = subprocess.run(cmd, input=pw, capture_output=True, timeout=timeout)
        if r.returncode != 0:
            logger.warning("Command failed (%s): %s", " ".join(args),
                           r.stderr.decode().strip()[:300])
        return r.returncode == 0
    except Exception as e:
        logger.warning("Error running %s: %s", " ".join(args), e)
        return False


def install_systemd_timer(on_calendar: str, config_path: str,
                          sudo_password: str | None) -> int:
    script_path = os.path.abspath(__file__)
    work_dir = os.path.normpath(os.path.join(os.path.dirname(script_path), ".."))
    service_content = (
        "[Unit]\n"
        "Description=SATPI RF noise floor measurement\n"
        "After=network-online.target\n"
        "Wants=network-online.target\n"
        "\n"
        "[Service]\n"
        "Type=oneshot\n"
        "User=andreas\n"
        f"WorkingDirectory={work_dir}\n"
        f"ExecStart=/usr/bin/python3 {script_path}"
        f" --config {config_path}\n"
    )
    timer_content = (
        "[Unit]\n"
        f"Description=SATPI noise floor measurement ({on_calendar})\n"
        "\n"
        "[Timer]\n"
        f"OnCalendar={on_calendar}\n"
        "Persistent=true\n"
        f"Unit={SERVICE_NAME}\n"
        "\n"
        "[Install]\n"
        "WantedBy=timers.target\n"
    )

    service_path = os.path.join(SYSTEMD_DIR, SERVICE_NAME)
    timer_path   = os.path.join(SYSTEMD_DIR, TIMER_NAME)

    import tempfile
    ok = True
    for path, content in [(service_path, service_content), (timer_path, timer_content)]:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".tmp", delete=False) as tf:
            tf.write(content)
            tmp_path = tf.name
        ok = ok and _sudo_run(["cp", tmp_path, path], sudo_password)
        ok = ok and _sudo_run(["chmod", "644", path], sudo_password)
        os.unlink(tmp_path)

    if not ok:
        logger.error("Failed to write systemd unit files to %s", SYSTEMD_DIR)
        return 1

    logger.info("Written: %s", service_path)
    logger.info("Written: %s", timer_path)

    ok = ok and _sudo_run(["systemctl", "daemon-reload"], sudo_password)
    ok = ok and _sudo_run(["systemctl", "enable", TIMER_NAME], sudo_password)
    ok = ok and _sudo_run(["systemctl", "start",  TIMER_NAME], sudo_password)

    if ok:
        logger.info("Timer '%s' installed and started.", TIMER_NAME)
        logger.info("OnCalendar: %s", on_calendar)
        try:
            r = subprocess.run(
                ["systemctl", "list-timers", TIMER_NAME, "--no-pager"],
                capture_output=True, text=True, timeout=10
            )
            for line in r.stdout.splitlines():
                if TIMER_NAME in line or "NEXT" in line:
                    logger.info("  %s", line)
        except Exception:
            pass
        return 0
    else:
        logger.error("Failed to enable timer.")
        return 1


def remove_systemd_timer(sudo_password: str | None) -> int:
    _sudo_run(["systemctl", "stop",    TIMER_NAME],   sudo_password)
    _sudo_run(["systemctl", "disable", TIMER_NAME],   sudo_password)
    _sudo_run(["systemctl", "stop",    SERVICE_NAME], sudo_password)

    service_path = os.path.join(SYSTEMD_DIR, SERVICE_NAME)
    timer_path   = os.path.join(SYSTEMD_DIR, TIMER_NAME)
    ok = True
    for path in [timer_path, service_path]:
        if os.path.exists(path):
            ok = ok and _sudo_run(["rm", "-f", path], sudo_password)
            if ok:
                logger.info("Removed: %s", path)

    ok = ok and _sudo_run(["systemctl", "daemon-reload"], sudo_password)
    if ok:
        logger.info("Timer '%s' removed.", TIMER_NAME)
        return 0
    else:
        logger.error("Failed to fully remove timer.")
        return 1


# ---------------------------------------------------------------------------
# SDR helpers
# ---------------------------------------------------------------------------

def check_satdump_running() -> bool:
    try:
        result = subprocess.run(["pgrep", "-x", "satdump"],
                                capture_output=True, timeout=5)
        return result.returncode == 0
    except Exception:
        return False


def is_noise_floor_service_running() -> bool:
    try:
        r = subprocess.run(
            ["systemctl", "is-active", SERVICE_NAME],
            capture_output=True, text=True, timeout=5,
        )
        return r.stdout.strip() == "active"
    except Exception:
        return False


def detect_sdr_device() -> str:
    try:
        result = subprocess.run(["rtl_test", "-t"], capture_output=True,
                                text=True, timeout=10)
        m = re.search(r"0:\s+(.+)", result.stdout + result.stderr)
        return m.group(1).strip() if m else "unknown"
    except Exception:
        return "unknown"


# ---------------------------------------------------------------------------
# rtl_power runner
# ---------------------------------------------------------------------------

def run_rtl_power(
    freq_start_mhz: float,
    freq_end_mhz: float,
    bin_size_khz: float,
    gain: float,
    duration_seconds: int,
    output_path: str,
    dry_run: bool = False,
) -> bool:
    cmd = [
        "rtl_power",
        "-f", f"{freq_start_mhz:.3f}M:{freq_end_mhz:.3f}M:{bin_size_khz:.3f}k",
        "-g", str(gain),
        "-i", "10",
        "-e", str(duration_seconds),
        output_path,
    ]
    logger.info("rtl_power command: %s", " ".join(cmd))
    if dry_run:
        logger.info("[dry-run] Would run: %s", " ".join(cmd))
        return True
    try:
        result = subprocess.run(cmd, capture_output=True, text=True,
                                errors="replace", timeout=duration_seconds + 30)
        if result.returncode != 0:
            logger.error("rtl_power failed (rc=%d): %s",
                         result.returncode, (result.stderr or "").strip()[:300])
            return False
        return True
    except subprocess.TimeoutExpired:
        logger.error("rtl_power timed out")
        return False
    except Exception as e:
        logger.error("rtl_power error: %s", e)
        return False


# ---------------------------------------------------------------------------
# CSV parser
# ---------------------------------------------------------------------------

def parse_rtl_power_csv(csv_path: str) -> list[dict]:
    """Parse rtl_power CSV into a list of {sample_time_utc, frequency_hz, power_dbm}."""
    samples = []
    try:
        with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = [p.strip() for p in line.split(",")]
                if len(parts) < 7:
                    continue
                try:
                    date_str = parts[0]
                    time_str = parts[1]
                    freq_start = int(parts[2])
                    freq_end   = int(parts[3])
                    bin_size   = float(parts[4])
                    powers = [float(p) for p in parts[6:] if p]
                    dt_str = f"{date_str}T{time_str}"
                    for i, power in enumerate(powers):
                        freq_hz = int(freq_start + i * bin_size)
                        if freq_hz > freq_end:
                            break
                        samples.append({
                            "sample_time_utc": dt_str,
                            "frequency_hz": freq_hz,
                            "power_dbm": round(power, 2),
                        })
                except (ValueError, IndexError):
                    continue
    except OSError as e:
        logger.error("Cannot read CSV %s: %s", csv_path, e)
    return samples


# ---------------------------------------------------------------------------
# Astronomical event computation
# ---------------------------------------------------------------------------

def compute_anchor_datetime(
    event: str,
    qth: dict,
    timezone_str: str,
    for_date: date_type,
    offset_minutes: int = 0,
) -> datetime | None:
    """Return the local datetime of a solar event on the given date, or None on error."""
    try:
        from skyfield.api import Loader, wgs84
        from skyfield import almanac

        _loader = Loader("/tmp/skyfield")
        ts  = _loader.timescale()
        eph = _loader("de421.bsp")
        tz  = ZoneInfo(timezone_str)

        # Search window: midnight-to-midnight in local time
        t0_local = datetime(for_date.year, for_date.month, for_date.day, 0, 0, tzinfo=tz)
        t1_local = t0_local + timedelta(days=1)
        t0 = ts.from_datetime(t0_local)
        t1 = ts.from_datetime(t1_local)

        location = wgs84.latlon(
            qth.get("latitude", 0.0),
            qth.get("longitude", 0.0),
            qth.get("altitude_m", 0),
        )

        if event in ("sunrise", "sunset"):
            times, events = almanac.find_discrete(
                t0, t1, almanac.sunrise_sunset(eph, location)
            )
            for t, e in zip(times, events):
                if event == "sunrise" and e == 1:
                    return t.astimezone(tz) + timedelta(minutes=offset_minutes)
                if event == "sunset" and e == 0:
                    return t.astimezone(tz) + timedelta(minutes=offset_minutes)

        elif event in ("noon", "midnight"):
            times, events = almanac.find_discrete(
                t0, t1,
                almanac.meridian_transits(eph, eph["Sun"], location)
            )
            for t, e in zip(times, events):
                # e == 1 → upper transit (solar noon), e == 0 → lower transit (midnight)
                if event == "noon"     and e == 1:
                    return t.astimezone(tz) + timedelta(minutes=offset_minutes)
                if event == "midnight" and e == 0:
                    return t.astimezone(tz) + timedelta(minutes=offset_minutes)

    except Exception as exc:
        logger.error("Could not compute anchor '%s' for %s: %s", event, for_date, exc)
    return None


# ---------------------------------------------------------------------------
# Scheduling helpers
# ---------------------------------------------------------------------------

def _parse_hhmm(s: str, ref_date: date_type, tz: ZoneInfo) -> datetime:
    """Parse HH:MM string into a timezone-aware datetime on ref_date."""
    hh, mm = map(int, s.split(":"))
    return datetime(ref_date.year, ref_date.month, ref_date.day, hh, mm, tzinfo=tz)


def expand_label(template: str | None, anchor: str, n: int, dt: datetime) -> str | None:
    """Expand label template with {time}, {date}, {anchor}, {n} placeholders."""
    if template is None:
        return None
    try:
        return template.format(
            time=dt.strftime("%H:%M"),
            date=dt.strftime("%Y-%m-%d"),
            anchor=anchor,
            n=n,
        )
    except KeyError as e:
        logger.warning("Unknown placeholder in label template: %s", e)
        return template


def build_schedule(args: argparse.Namespace, config: dict) -> list[tuple[datetime, str]]:
    """
    Build a sorted list of (start_datetime, anchor_name) tuples from CLI args.

    Handles --start-at, --anchor, --offset-minutes, --every, --until,
    --until-anchor, --count, and --no-wait.
    """
    tz_str = config.get("station", {}).get("timezone", "UTC")
    tz     = ZoneInfo(tz_str)
    now    = datetime.now(tz)
    qth    = config.get("qth", {})

    # Target date for anchor computation
    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        target_date = now.date()

    seeds: list[tuple[datetime, str]] = []

    # ── --start-at ────────────────────────────────────────────────────────
    for t_str in args.start_at:
        try:
            dt = _parse_hhmm(t_str, target_date, tz)
            dt += timedelta(minutes=args.offset_minutes)
            if dt <= now and not args.no_wait:
                dt += timedelta(days=1)
            seeds.append((dt, t_str))
        except ValueError:
            logger.error("Invalid --start-at value: %r (expected HH:MM)", t_str)

    # ── --anchor ──────────────────────────────────────────────────────────
    for event in args.anchor:
        dt = compute_anchor_datetime(event, qth, tz_str, target_date, args.offset_minutes)
        if dt is None:
            logger.error("Could not compute anchor '%s' for %s", event, target_date)
            continue
        if dt <= now and not args.no_wait:
            # Try next day
            dt = compute_anchor_datetime(
                event, qth, tz_str, target_date + timedelta(days=1), args.offset_minutes
            )
        if dt:
            seeds.append((dt, event))

    seeds.sort(key=lambda x: x[0])

    if not seeds:
        return []

    # ── No --every: fixed list, one measurement per seed ─────────────────
    if not args.every:
        max_n = args.count if args.count else len(seeds)
        return seeds[:max_n]

    # ── --every: recurring series from the first seed ────────────────────
    every_n, every_unit = args.every
    try:
        every_n = int(every_n)
    except ValueError:
        logger.error("--every N must be an integer, got %r", every_n)
        return []
    if every_unit not in EVERY_UNITS:
        logger.error("--every UNIT must be one of %s, got %r", EVERY_UNITS, every_unit)
        return []

    unit_map = {
        "minute": timedelta(minutes=every_n),
        "hour":   timedelta(hours=every_n),
        "day":    timedelta(days=every_n),
        "week":   timedelta(weeks=every_n),
    }
    step = unit_map[every_unit]

    # Stop conditions
    stop_time: datetime | None = None

    if args.until:
        try:
            st = _parse_hhmm(args.until, seeds[0][0].date(), tz)
            if st <= seeds[0][0]:
                st += timedelta(days=1)
            stop_time = st
        except ValueError:
            logger.error("Invalid --until value: %r (expected HH:MM)", args.until)

    if args.until_anchor:
        anchor_date = seeds[0][0].date()
        ua_dt = compute_anchor_datetime(args.until_anchor, qth, tz_str, anchor_date, 0)
        if ua_dt and ua_dt <= seeds[0][0]:
            ua_dt = compute_anchor_datetime(
                args.until_anchor, qth, tz_str, anchor_date + timedelta(days=1), 0
            )
        if ua_dt and (stop_time is None or ua_dt < stop_time):
            stop_time = ua_dt

    max_count = args.count if args.count else 10_000  # safety limit

    schedule: list[tuple[datetime, str]] = []
    current = seeds[0][0]
    anchor_name = seeds[0][1]

    while len(schedule) < max_count:
        if stop_time and current > stop_time:
            break
        schedule.append((current, anchor_name))
        current += step

    return schedule


def wait_until(target: datetime, label: str) -> bool:
    """Sleep until target datetime. Returns False if stop was requested."""
    now = datetime.now(target.tzinfo)
    secs = (target - now).total_seconds()
    if secs > 0:
        logger.info("Waiting %.0f seconds until %s for '%s' …",
                    secs, target.strftime("%Y-%m-%d %H:%M %Z"), label)
        deadline = time.monotonic() + secs
        while time.monotonic() < deadline:
            if _STOP_REQUESTED:
                return False
            time.sleep(min(5.0, deadline - time.monotonic()))
    return not _STOP_REQUESTED


# ---------------------------------------------------------------------------
# Main measurement loop
# ---------------------------------------------------------------------------

def run_measurement(
    config: dict,
    db_path: str,
    args: argparse.Namespace,
    sdr_device: str,
    label_override: str | None = None,
) -> bool:
    timestamp_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    host     = config.get("station", {}).get("name") or socket.gethostname()
    gain     = args.gain if args.gain is not None else float(
        config.get("hardware", {}).get("gain", 38.6)
    )
    antenna  = config.get("reception_setup", {}).get("antenna_type", "")
    label    = label_override if label_override is not None else args.label

    # Check SatDump
    if check_satdump_running():
        logger.error("SatDump is currently running and holds the RTL-SDR. "
                     "Aborting measurement.")
        return False

    # Check conflicting timers
    margin = args.duration + 60
    conflicting = get_conflicting_timers(within_seconds=margin)
    stopped_timers = []

    if conflicting:
        if args.stop_timers:
            logger.warning("Conflicting pass timers (--stop-timers active): %s", conflicting)
            for t in conflicting:
                if stop_timer(t, args.sudo_password):
                    stopped_timers.append(t)
        else:
            logger.error("=" * 60)
            logger.error("CONFLICT: noise floor measurement skipped!")
            logger.error("The following satellite pass timer(s) overlap with")
            logger.error("this measurement window (next %ds):", margin)
            for t in conflicting:
                logger.error("  • %s", t)
            free_min = find_conflict_free_minute(args.duration)
            if free_min is not None:
                logger.error("SUGGESTION: set  schedule_minute = %d  in config.ini", free_min)
            else:
                logger.error("No conflict-free minute found. "
                             "Consider reducing measurement_duration in config.ini.")
            logger.error("=" * 60)
            if not args.dry_run:
                return False

    # Run rtl_power
    csv_path = f"/tmp/noise_floor_{timestamp_utc.replace(':', '-')}.csv"
    logger.info("Starting measurement: %s MHz – %s MHz, %s kHz bins, %ss, gain %.1f dB",
                args.freq_start, args.freq_end, args.bin_size, args.duration, gain)

    ok = run_rtl_power(
        freq_start_mhz=args.freq_start,
        freq_end_mhz=args.freq_end,
        bin_size_khz=args.bin_size,
        gain=gain,
        duration_seconds=args.duration,
        output_path=csv_path,
        dry_run=args.dry_run,
    )
    if not ok and not args.dry_run:
        return False

    # Parse CSV
    samples = [] if args.dry_run else parse_rtl_power_csv(csv_path)
    if not samples and not args.dry_run:
        logger.error("No samples parsed from %s", csv_path)
        return False
    logger.info("Parsed %d samples from CSV", len(samples))

    # Insert into DB
    meta = {
        "timestamp_utc":    timestamp_utc,
        "host":             host,
        "sdr_device":       sdr_device,
        "antenna":          antenna,
        "gain":             gain,
        "freq_start_hz":    int(args.freq_start * 1e6),
        "freq_end_hz":      int(args.freq_end   * 1e6),
        "bin_size_hz":      args.bin_size * 1e3,
        "duration_seconds": args.duration,
        "label":            label,
        "timers_stopped":   json.dumps(stopped_timers) if stopped_timers else None,
    }

    if not args.dry_run:
        mid = insert_measurement(db_path, meta, samples)
        logger.info("Saved measurement id=%d to database (%d samples)", mid, len(samples))
    else:
        logger.info("[dry-run] Would save measurement with %d samples (label=%s)",
                    len(samples), label)

    # Cleanup temp CSV
    if not args.dry_run:
        try:
            os.unlink(csv_path)
        except OSError:
            pass

    return True


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    args = parse_args()
    config_path = get_config_path(args.config)

    try:
        config = load_config(config_path)
    except ConfigError as e:
        print(f"[measure_noise_floor] CONFIG ERROR: {e}", file=sys.stderr)
        return 2

    paths    = config.get("paths", {})
    base_dir = paths.get("base_dir", str(SCRIPT_DIR.parent))
    log_dir  = os.path.join(base_dir, paths.get("log_dir", "logs"))
    db_dir   = os.path.join(base_dir, os.path.dirname(
        paths.get("reception_db_file", "results/database/reception.db")
    ))
    db_path  = os.path.join(db_dir, DB_NAME)

    setup_logging(log_dir, verbose=args.verbose)
    _install_signal_handlers()

    logger.info("measure_noise_floor.py started")

    # ── Timer install / remove (early exit) ───────────────────────────────
    if args.install_timer is not None:
        on_calendar = args.install_timer
        if not on_calendar:
            minute = config.get("noise_floor", {}).get("schedule_minute", 0)
            on_calendar = f"*-*-* *:{minute:02d}:00"
            logger.info("Using schedule_minute=%d from config → OnCalendar=%s",
                        minute, on_calendar)
        return install_systemd_timer(
            on_calendar=on_calendar,
            config_path=config_path,
            sudo_password=args.sudo_password,
        )
    if args.remove_timer:
        return remove_systemd_timer(sudo_password=args.sudo_password)

    # ── Resolve measurement parameters ────────────────────────────────────
    if args.duration is None:
        args.duration = config.get("noise_floor", {}).get("measurement_duration", 600)
    logger.info("Measurement duration: %ds (%.1f min)", args.duration, args.duration / 60)

    nf_cfg = config.get("noise_floor", {})
    if args.freq_start is None:
        if nf_cfg.get("freq_start_mhz") is not None:
            args.freq_start = nf_cfg["freq_start_mhz"]
        else:
            half = nf_cfg.get("bandwidth_mhz", 0.4) / 2
            args.freq_start = nf_cfg.get("center_freq_mhz", 137.9) - half
    if args.freq_end is None:
        if nf_cfg.get("freq_end_mhz") is not None:
            args.freq_end = nf_cfg["freq_end_mhz"]
        else:
            half = nf_cfg.get("bandwidth_mhz", 0.4) / 2
            args.freq_end = nf_cfg.get("center_freq_mhz", 137.9) + half
    if args.bin_size is None:
        args.bin_size = nf_cfg.get("bin_size_khz", 10.0)
    logger.info("Frequency range: %.3f – %.3f MHz, bin size: %.1f kHz",
                args.freq_start, args.freq_end, args.bin_size)

    # ── Check if noise floor service is already running ───────────────────
    if is_noise_floor_service_running():
        if sys.stdin.isatty():
            print(f"\n⚠  {SERVICE_NAME} läuft bereits (eine Messung ist aktiv).")
            try:
                answer = input("Laufende Messung stoppen und neu starten? [j/N]: ").strip().lower()
            except EOFError:
                answer = ""
            if answer in ("j", "ja", "y", "yes"):
                pw = args.sudo_password
                if not pw:
                    try:
                        import getpass
                        pw = getpass.getpass("Sudo-Passwort: ") or None
                    except Exception:
                        pw = None
                _sudo_run(["systemctl", "stop", SERVICE_NAME], pw)
                logger.info("Laufende Messung gestoppt.")
            else:
                logger.info("Abgebrochen — laufende Messung wird nicht unterbrochen.")
                return 0
        else:
            logger.warning("%s ist bereits aktiv. Abbruch (nicht-interaktiver Modus).",
                           SERVICE_NAME)
            return 1

    logger.info("Database: %s", db_path)
    if not args.dry_run:
        init_db(db_path)

    sdr_device = detect_sdr_device()
    logger.info("SDR device: %s", sdr_device)

    # ── Determine scheduling mode ─────────────────────────────────────────
    use_new_scheduling = bool(args.start_at or args.anchor or args.every)

    if use_new_scheduling:
        schedule = build_schedule(args, config)
        if not schedule:
            logger.error("No scheduled times could be determined. "
                         "Check --start-at / --anchor / --every arguments.")
            return 1

        total = len(schedule)
        logger.info("Scheduled %d measurement(s):", total)
        for i, (dt, anchor) in enumerate(schedule):
            lbl = expand_label(args.label, anchor, i + 1, dt)
            logger.info("  %d. %s  anchor=%s  label=%s",
                        i + 1, dt.strftime("%Y-%m-%d %H:%M %Z"), anchor, lbl)

        if args.dry_run:
            logger.info("[dry-run] No measurements taken.")
            return 0

        success_count = 0
        original_label = args.label
        for i, (target_dt, anchor) in enumerate(schedule):
            if _STOP_REQUESTED:
                logger.info("Stop requested; exiting after %d/%d measurements.", i, total)
                break
            lbl = expand_label(original_label, anchor, i + 1, target_dt)
            if not wait_until(target_dt, anchor):
                break
            logger.info("--- Measurement %d/%d (%s) ---", i + 1, total, anchor)
            if run_measurement(config, db_path, args, sdr_device, label_override=lbl):
                success_count += 1
            else:
                logger.warning("Measurement %d/%d failed.", i + 1, total)

        logger.info("Done. %d/%d measurements successful.", success_count, total)
        return 0 if success_count > 0 else 1

    else:
        # ── Legacy mode: --count + --interval ─────────────────────────────
        count = args.count if args.count else 1
        success_count = 0
        for i in range(count):
            if _STOP_REQUESTED:
                logger.info("Stop requested; exiting after %d/%d measurements.", i, count)
                break
            if i > 0:
                logger.info("Waiting %ds before next measurement (%d/%d)…",
                            args.interval, i + 1, count)
                for _ in range(args.interval):
                    if _STOP_REQUESTED:
                        break
                    time.sleep(1)
            logger.info("--- Measurement %d/%d ---", i + 1, count)
            if run_measurement(config, db_path, args, sdr_device):
                success_count += 1
            else:
                logger.warning("Measurement %d/%d failed.", i + 1, count)

        logger.info("Done. %d/%d measurements successful.", success_count, count)
        return 0 if success_count > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
