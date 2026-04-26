#!/usr/bin/env python3
"""satpi – measure_noise_floor

Measures the RF noise floor using rtl_power and stores results in SQLite.

Checks for conflicting satpi pass timers before measuring and can optionally
stop them. Designed to run locally on the Pi (satpi5 or satpi4).

Usage examples:
    # Single 60-second measurement
    python3 measure_noise_floor.py

    # Measure every hour, 5 times
    python3 measure_noise_floor.py --count 5 --interval 3600

    # Custom frequency range and label (overrides config.ini)
    python3 measure_noise_floor.py --freq-start 130 --freq-end 145 --label morning

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
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from load_config import load_config, ConfigError

LOG_MAX_BYTES = 1_000_000
LOG_BACKUP_COUNT = 3
DB_NAME = "noise_floor.db"

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
        description="Measure RF noise floor and store in SQLite."
    )
    parser.add_argument("--config", default=None,
                        help="Path to config.ini (default: ../config/config.ini)")
    parser.add_argument("--freq-start", type=float, default=None,
                        help="Start frequency in MHz (default: from config.ini center_freq_mhz/bandwidth_mhz)")
    parser.add_argument("--freq-end", type=float, default=None,
                        help="End frequency in MHz (default: from config.ini center_freq_mhz/bandwidth_mhz)")
    parser.add_argument("--bin-size", type=float, default=None,
                        help="FFT bin size in kHz (default: from config.ini bin_size_khz)")
    parser.add_argument("--duration", type=int, default=None,
                        help="Measurement duration in seconds (default: from config.ini, fallback 600)")
    parser.add_argument("--gain", type=float, default=None,
                        help="SDR gain in dB (default: from config.ini)")
    parser.add_argument("--label", default=None,
                        help="Optional label for this measurement (e.g. 'morning')")
    parser.add_argument("--count", type=int, default=1,
                        help="Number of measurements to take (default: 1)")
    parser.add_argument("--interval", type=int, default=3600,
                        help="Seconds between measurements if --count > 1 (default: 3600)")
    parser.add_argument("--stop-timers", action="store_true",
                        help="Stop conflicting satpi pass timers before measuring")
    parser.add_argument("--sudo-password", default=None,
                        help="Sudo password for stopping system timers")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would happen without actually measuring")
    parser.add_argument("--verbose", action="store_true",
                        help="Enable debug logging")
    parser.add_argument("--install-timer", nargs="?", const="", default=None,
                        metavar="ONCALENDAR",
                        help="Install a systemd timer that runs this script automatically. "
                             "Without a value: uses schedule_minute from config.ini (default: 0 = "
                             "top of every hour). Or pass any systemd OnCalendar expression. "
                             "Requires sudo. Example: --install-timer or "
                             "--install-timer '*-*-* *:05:00'")
    parser.add_argument("--remove-timer", action="store_true",
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
    """Return satpi pass timer names that fire within the next N seconds.

    Only considers timers that have a scheduled future trigger (next trigger
    column is not "-"). Timers that have already fired show "-" in the next
    trigger column and are ignored.
    """
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
        # Skip timers with no scheduled future trigger (already fired / inactive)
        # systemctl format: "NEXT  LEFT  LAST  PASSED  UNIT  ACTIVATES"
        # Lines starting with "-" have no next trigger scheduled.
        stripped = line.strip()
        if stripped.startswith("-"):
            continue
        # Find timer name
        timer_name = next((p for p in line.split() if p.endswith(".timer")), None)
        if not timer_name:
            continue
        # Parse the "left" column — appears after the next trigger datetime
        # Format examples: "2h 36min left", "5min left", "30s left"
        time_left_match = re.search(
            r"(\d+h\s+\d+min|\d+\s*h|\d+\s*min|\d+\s*s)(?=\s)", line
        )
        if time_left_match:
            secs = _parse_time_to_seconds(time_left_match.group(1))
            if secs is not None and secs <= within_seconds:
                conflicting.append(timer_name)
    return list(set(conflicting))


def find_conflict_free_minute(duration_seconds: int) -> int | None:
    """Scan all scheduled satpi-pass timers and find a minute (0-59) that
    doesn't overlap with any of them.

    A minute M is considered blocked if any pass timer fires within
    [M - duration_minutes, M + duration_minutes] (mod 60).

    Returns the first free minute, or None if every minute is blocked.
    """
    try:
        result = subprocess.run(
            ["systemctl", "list-timers", "satpi-pass-*", "--all", "--no-pager"],
            capture_output=True, text=True, timeout=10
        )
    except Exception as e:
        logger.warning("Could not list pass timers for suggestion: %s", e)
        return None

    duration_minutes = (duration_seconds + 59) // 60  # round up
    blocked: set[int] = set()

    for line in result.stdout.splitlines():
        if "satpi-pass-" not in line:
            continue
        stripped = line.strip()
        if stripped.startswith("-"):
            continue
        # Extract HH:MM:SS from the "NEXT" column (first datetime on the line)
        m = re.search(r"\b(\d{2}):(\d{2}):\d{2}\b", line)
        if m:
            pass_minute = int(m.group(2))
            # Block every minute within [pass_minute - margin, pass_minute + margin]
            for delta in range(-duration_minutes, duration_minutes + 1):
                blocked.add((pass_minute + delta) % 60)

    for candidate in range(60):
        if candidate not in blocked:
            return candidate
    return None


def stop_timer(timer_name: str, sudo_password: str | None) -> bool:
    cmd_stop = ["sudo", "-S", "systemctl", "stop", timer_name]
    cmd_disable = ["sudo", "-S", "systemctl", "disable", timer_name]
    pw_input = (sudo_password + "\n").encode() if sudo_password else None
    try:
        r1 = subprocess.run(cmd_stop, input=pw_input, capture_output=True, timeout=10)
        r2 = subprocess.run(cmd_disable, input=pw_input, capture_output=True, timeout=10)
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

TIMER_NAME    = "satpi-noise-floor.timer"
SERVICE_NAME  = "satpi-noise-floor.service"
SYSTEMD_DIR   = "/etc/systemd/system"


def _sudo_run(args: list[str], sudo_password: str | None, timeout: int = 15) -> bool:
    """Run a command with sudo, return True on success."""
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
    """Create and enable the satpi-noise-floor systemd timer + service.

    Returns 0 on success, 1 on error.
    """
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

    # Write files via a temp location then sudo mv
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
        # Show next trigger
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
    """Stop, disable and remove the satpi-noise-floor systemd timer + service.

    Returns 0 on success, 1 on error.
    """
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


def check_satdump_running() -> bool:
    try:
        result = subprocess.run(["pgrep", "-x", "satdump"],
                                capture_output=True, timeout=5)
        return result.returncode == 0
    except Exception:
        return False


def is_noise_floor_service_running() -> bool:
    """Return True if satpi-noise-floor.service is currently active."""
    try:
        r = subprocess.run(
            ["systemctl", "is-active", SERVICE_NAME],
            capture_output=True, text=True, timeout=5,
        )
        return r.stdout.strip() == "active"
    except Exception:
        return False


# ---------------------------------------------------------------------------
# rtl_power runner
# ---------------------------------------------------------------------------

def detect_sdr_device() -> str:
    try:
        result = subprocess.run(["rtl_test", "-t"], capture_output=True,
                                text=True, timeout=10)
        m = re.search(r"0:\s+(.+)", result.stdout + result.stderr)
        return m.group(1).strip() if m else "unknown"
    except Exception:
        return "unknown"


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
                                timeout=duration_seconds + 30)
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
        with open(csv_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = [p.strip() for p in line.split(",")]
                if len(parts) < 7:
                    continue
                try:
                    date_str = parts[0]   # e.g. 2026-04-25
                    time_str = parts[1]   # e.g. 07:43:01
                    freq_start = int(parts[2])
                    freq_end   = int(parts[3])
                    bin_size   = float(parts[4])
                    # parts[5] = samples count
                    powers = [float(p) for p in parts[6:] if p]
                    # Reconstruct timestamp (local time from Pi — convert via timezone-naive parse)
                    dt_str = f"{date_str}T{time_str}"
                    sample_time = dt_str  # stored as-is; analysis script handles tz
                    # Build per-bin entries
                    for i, power in enumerate(powers):
                        freq_hz = int(freq_start + i * bin_size)
                        if freq_hz > freq_end:
                            break
                        samples.append({
                            "sample_time_utc": sample_time,
                            "frequency_hz": freq_hz,
                            "power_dbm": round(power, 2),
                        })
                except (ValueError, IndexError):
                    continue
    except OSError as e:
        logger.error("Cannot read CSV %s: %s", csv_path, e)
    return samples


# ---------------------------------------------------------------------------
# Main measurement loop
# ---------------------------------------------------------------------------

def run_measurement(
    config: dict,
    db_path: str,
    args: argparse.Namespace,
    sdr_device: str,
) -> bool:
    timestamp_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    host = config.get("station", {}).get("name") or socket.gethostname()
    gain = args.gain if args.gain is not None else float(
        config.get("hardware", {}).get("gain", 38.6)
    )
    antenna = config.get("reception_setup", {}).get("antenna_type", "")

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
            # Manual/interactive mode: caller explicitly asked to stop timers
            logger.warning("Conflicting pass timers (--stop-timers active): %s", conflicting)
            for t in conflicting:
                if stop_timer(t, args.sudo_password):
                    stopped_timers.append(t)
        else:
            # Automated/service mode: never kill receive_pass jobs
            logger.error("=" * 60)
            logger.error("CONFLICT: noise floor measurement skipped!")
            logger.error("The following satellite pass timer(s) overlap with")
            logger.error("this measurement window (next %ds):", margin)
            for t in conflicting:
                logger.error("  • %s", t)
            logger.error("")
            free_min = find_conflict_free_minute(args.duration)
            if free_min is not None:
                logger.error(
                    "SUGGESTION: set  schedule_minute = %d  in config.ini",
                    free_min,
                )
                logger.error(
                    "  [noise_floor]  →  schedule_minute = %d", free_min
                )
                logger.error(
                    "Then re-run:  python3 bin/measure_noise_floor.py "
                    "--install-timer --sudo-password <password>"
                )
            else:
                logger.error(
                    "No conflict-free minute found. "
                    "Consider reducing measurement_duration in config.ini."
                )
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
        "timestamp_utc": timestamp_utc,
        "host": host,
        "sdr_device": sdr_device,
        "antenna": antenna,
        "gain": gain,
        "freq_start_hz": int(args.freq_start * 1e6),
        "freq_end_hz": int(args.freq_end * 1e6),
        "bin_size_hz": args.bin_size * 1e3,
        "duration_seconds": args.duration,
        "label": args.label,
        "timers_stopped": json.dumps(stopped_timers) if stopped_timers else None,
    }

    if not args.dry_run:
        mid = insert_measurement(db_path, meta, samples)
        logger.info("Saved measurement id=%d to database (%d samples)", mid, len(samples))
    else:
        logger.info("[dry-run] Would save measurement with %d samples", len(samples))

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

    paths = config.get("paths", {})
    base_dir = paths.get("base_dir", str(SCRIPT_DIR.parent))
    log_dir = os.path.join(base_dir, paths.get("log_dir", "logs"))
    db_dir = os.path.join(base_dir, os.path.dirname(
        paths.get("reception_db_file", "results/database/reception.db")
    ))
    db_path = os.path.join(db_dir, DB_NAME)

    setup_logging(log_dir, verbose=args.verbose)
    _install_signal_handlers()

    logger.info("measure_noise_floor.py started")

    # ── Timer install / remove (early exit) ───────────────────────────────
    if args.install_timer is not None:
        on_calendar = args.install_timer
        if not on_calendar:
            # No explicit expression → derive from config schedule_minute
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

    # Resolve measurement duration: CLI > config.ini > built-in default (600s)
    if args.duration is None:
        args.duration = config.get("noise_floor", {}).get("measurement_duration", 600)
    logger.info("Measurement duration: %ds (%.1f min)", args.duration, args.duration / 60)

    # Resolve frequency range: CLI > explicit freq_start/end in config > center+bandwidth > built-in defaults
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

    # Check if noise floor service is already running
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
            logger.warning(
                "%s ist bereits aktiv. Abbruch (nicht-interaktiver Modus).",
                SERVICE_NAME,
            )
            return 1

    logger.info("Database: %s", db_path)

    if not args.dry_run:
        init_db(db_path)

    sdr_device = detect_sdr_device()
    logger.info("SDR device: %s", sdr_device)

    success_count = 0
    for i in range(args.count):
        if _STOP_REQUESTED:
            logger.info("Stop requested; exiting after %d/%d measurements.", i, args.count)
            break

        if i > 0:
            logger.info("Waiting %ds before next measurement (%d/%d)…",
                        args.interval, i + 1, args.count)
            for _ in range(args.interval):
                if _STOP_REQUESTED:
                    break
                time.sleep(1)

        logger.info("--- Measurement %d/%d ---", i + 1, args.count)
        if run_measurement(config, db_path, args, sdr_device):
            success_count += 1
        else:
            logger.warning("Measurement %d/%d failed.", i + 1, args.count)

    logger.info("Done. %d/%d measurements successful.", success_count, args.count)
    return 0 if success_count > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
