#!/usr/bin/env python3
"""satpi – receive_orchestrator

Orchestrates the complete workflow for satellite passes:
  1. Monitors scheduled passes
  2. Calls receive_pass.py to record passes
  3. Calls post_processing.py for post-processing

Can run as a systemd service or be called manually.

Author: Andreas Horvath
Project: satpi
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent / "lib"))
from read_config import read_config, ConfigError


logger = logging.getLogger("satpi.receive_orchestrator")


# --- Constants ---------------------------------------------------------------

DEFAULT_MONITOR_INTERVAL = 60  # seconds
RECEIVE_PASS_TIMEOUT = 180 * 60  # 3 hours
POST_PROCESSING_TIMEOUT = 60 * 60  # 1 hour


# --- Helpers -----------------------------------------------------------------

def setup_logger(verbose: bool = False, log_file: Optional[str] = None) -> None:
    """Setup logging to stderr and optionally to file."""
    level = logging.DEBUG if verbose else logging.INFO
    logger.setLevel(level)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        fh = logging.FileHandler(log_file)
        fh.setFormatter(fmt)
        logger.addHandler(fh)


def utc_now() -> datetime:
    """Get current UTC time."""
    return datetime.now(timezone.utc)


def run_subprocess(
    cmd: List[str],
    *,
    timeout: int,
    description: str = "",
) -> Tuple[int, str, str]:
    """Run a subprocess and capture output.

    Returns: (returncode, stdout, stderr)
    """
    desc = f": {description}" if description else ""
    logger.info("Running%s: %s", desc, " ".join(cmd))

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return proc.returncode, proc.stdout, proc.stderr
    except subprocess.TimeoutExpired:
        logger.error("Command timed out after %ds: %s", timeout, " ".join(cmd))
        return 124, "", f"Command timed out after {timeout}s"
    except Exception as e:
        logger.error("Command failed: %s", e)
        return 1, "", str(e)


def get_pass_direction(aos_azimuth: float, los_azimuth: float) -> str:
    """Determine pass direction from AOS and LOS azimuths.

    Returns: direction string (e.g., "north_to_south", "all" if ambiguous)
    """
    if aos_azimuth is None or los_azimuth is None:
        return "all"

    # Normalize azimuths to 0-360 range
    aos = aos_azimuth % 360.0
    los = los_azimuth % 360.0

    # Cardinal directions: N=0°, E=90°, S=180°, W=270°
    # Define 45° sectors for each direction
    def in_sector(az: float, center: float, width: float = 45.0) -> bool:
        center = center % 360.0
        lower = (center - width / 2) % 360.0
        upper = (center + width / 2) % 360.0
        if lower < upper:
            return lower <= az <= upper
        else:  # wraps around 360°
            return az >= lower or az <= upper

    # Determine cardinal quadrants for AOS and LOS
    aos_quad = None
    los_quad = None

    # Quadrants: N(0), NE(45), E(90), SE(135), S(180), SW(225), W(270), NW(315)
    quadrants = {
        "N": 0, "NE": 45, "E": 90, "SE": 135,
        "S": 180, "SW": 225, "W": 270, "NW": 315
    }

    for name, center in quadrants.items():
        if in_sector(aos, center):
            aos_quad = name
        if in_sector(los, center):
            los_quad = name

    if not aos_quad or not los_quad:
        return "all"

    # Map quadrant transitions to directions
    direction_map = {
        ("N", "S"): "north_to_south",
        ("S", "N"): "south_to_north",
        ("NE", "SW"): "northeast_to_southwest",
        ("SW", "NE"): "southwest_to_northeast",
        ("E", "W"): "east_to_west",
        ("W", "E"): "west_to_east",
        ("SE", "NW"): "southeast_to_northwest",
        ("NW", "SE"): "northwest_to_southeast",
    }

    return direction_map.get((aos_quad, los_quad), "all")


def is_pass_direction_valid(pass_direction: str, aos_azimuth: float, los_azimuth: float) -> bool:
    """Check if pass direction matches the filter.

    Returns: True if pass matches direction filter, False otherwise
    """
    if pass_direction == "all":
        return True

    detected_direction = get_pass_direction(aos_azimuth, los_azimuth)
    return detected_direction == pass_direction


def is_pass_in_timeslot(
    pass_start_utc: datetime,
    pass_timeslot: str,
    latitude: float,
    longitude: float,
    altitude_m: float,
) -> bool:
    """Check if pass start time matches the timeslot filter.

    timeslot values:
      - "all": always pass
      - "day": pass starts during daylight (sunrise to sunset)
      - "night": pass starts during night (sunset to sunrise)
      - "0800-1830": pass starts within UTC time range (HHmm-HHmm)

    Returns: True if pass matches timeslot, False otherwise
    """
    if pass_timeslot == "all":
        return True

    if pass_timeslot in ("day", "night"):
        try:
            from skyfield import api, wgs84

            ts = api.load.timescale()
            eph = api.load('de421.bsp')
            earth = eph['earth']
            sun = eph['sun']

            location = earth + wgs84.latlong(latitude, longitude, altitude_m)
            t = ts.from_datetime(pass_start_utc)
            astrometric = location.at(t).observe(sun).apparent()

            # Get altitude angle of sun
            alt, az, d = astrometric.apparent_geocentric_position.subvector_xyz.length.radians
            alt_deg = alt.degrees if hasattr(alt, 'degrees') else alt

            is_daylight = alt_deg > 0
            return is_daylight if pass_timeslot == "day" else not is_daylight

        except Exception as e:
            logger.warning("Failed to compute sun altitude: %s, allowing pass", e)
            return True

    if "-" in pass_timeslot:
        # Parse HHmm-HHmm format (UTC)
        try:
            parts = pass_timeslot.split("-")
            if len(parts) == 2:
                start_time, end_time = parts
                start_hm = int(start_time[:2]) * 60 + int(start_time[2:])
                end_hm = int(end_time[:2]) * 60 + int(end_time[2:])
                pass_hm = pass_start_utc.hour * 60 + pass_start_utc.minute
                return start_hm <= pass_hm <= end_hm
        except (ValueError, IndexError):
            logger.warning("Invalid timeslot format: %s, allowing pass", pass_timeslot)
            return True

    return True


def load_passes_to_schedule(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Load passes scheduled in the near future from passes.json.

    Reads from the central passes.json file (generated by predict_passes.py),
    filters by time window, direction, and timeslot, and returns passes ready
    for reception.

    Returns: List of pass dicts with required fields
    """
    passes = []

    # Get path to passes.json
    pass_file = config.get("paths", {}).get("pass_file", "results/passes.json")
    if not pass_file:
        logger.warning("pass_file not configured")
        return passes

    pass_file = os.path.expanduser(pass_file)
    if not os.path.isabs(pass_file):
        base_dir = str(Path(__file__).resolve().parent.parent)
        pass_file = os.path.join(base_dir, pass_file)

    if not os.path.exists(pass_file):
        logger.debug("Passes file not found: %s", pass_file)
        return passes

    # Get QTH coordinates for timeslot calculations
    qth = config.get("qth", {})
    latitude = float(qth.get("latitude", 0.0))
    longitude = float(qth.get("longitude", 0.0))
    altitude_m = float(qth.get("altitude_m", 0.0))

    # Load passes.json and filter for the next 24 hours
    now = utc_now()
    deadline = now + timedelta(hours=24)

    try:
        with open(pass_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        all_passes = data.get("passes", [])
        logger.debug("Found %d total passes in passes.json", len(all_passes))

        for pass_data in all_passes:
            # Check if pass is in the near future
            pass_start = pass_data.get("start")
            if not pass_start:
                continue

            try:
                # Parse ISO format: "2026-05-11T03:11:56Z"
                pass_dt = datetime.fromisoformat(pass_start.replace("Z", "+00:00"))
                if not (now <= pass_dt <= deadline):
                    continue
            except (ValueError, AttributeError):
                logger.warning("Invalid pass start time: %s", pass_start)
                continue

            # Check direction and timeslot filters for this satellite
            satellite = pass_data.get("satellite", "UNKNOWN")
            satellites_config = config.get("satellites", [])
            sat_config = next((s for s in satellites_config if s.get("name") == satellite), {})

            # Check pass_direction filter
            pass_direction = sat_config.get("pass_direction", "all")
            aos_azimuth = pass_data.get("aos_azimuth_deg")
            los_azimuth = pass_data.get("los_azimuth_deg")

            if not is_pass_direction_valid(pass_direction, aos_azimuth, los_azimuth):
                logger.debug("Pass %s filtered by direction %s", satellite, pass_direction)
                continue

            # Check pass_timeslot filter
            pass_timeslot = sat_config.get("pass_timeslot", "all")

            if not is_pass_in_timeslot(pass_dt, pass_timeslot, latitude, longitude, altitude_m):
                logger.debug("Pass %s filtered by timeslot %s", satellite, pass_timeslot)
                continue

            passes.append(pass_data)

    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Failed to load passes file: %s", e)

    return passes


def receive_pass(
    config: Dict[str, Any],
    pass_data: Dict[str, Any],
    testmode: bool = False,
) -> Tuple[bool, Optional[str]]:
    """Run receive_pass.py for the given pass using pass-id.

    Returns: (success: bool, pass_output_dir: Optional[str])
    """
    base_dir = str(Path(__file__).resolve().parent.parent)
    script = os.path.join(base_dir, "bin", "receive_pass.py")

    if not os.path.exists(script):
        logger.error("receive_pass.py not found: %s", script)
        return False, None

    # Build receive_pass.py command with pass-id
    python_bin = config.get("paths", {}).get("python_bin", "python3")
    pass_id = pass_data.get("pass-id", "")

    if not pass_id:
        logger.error("Pass missing pass-id")
        return False, None

    cmd = [python_bin, script, "--pass-id", pass_id]

    if testmode:
        cmd.append("--testmode")
        logger.info("Running in test mode")

    rc, stdout, stderr = run_subprocess(
        cmd,
        timeout=RECEIVE_PASS_TIMEOUT,
        description=f"receive_pass for {pass_data.get('satellite', 'UNKNOWN')}",
    )

    if rc != 0:
        logger.error("receive_pass failed (rc=%s)", rc)
        if stderr.strip():
            logger.error("stderr: %s", stderr.strip())
        return False, None

    # Construct pass_output_dir from output_dir + pass-id
    # receive_pass.py creates: output_dir / pass-id / (files)
    output_dir = config.get("paths", {}).get("output_dir", "")
    if output_dir:
        output_dir = os.path.expanduser(output_dir)
        pass_output_dir = os.path.join(output_dir, pass_id)

        if os.path.isdir(pass_output_dir):
            logger.info("Pass output directory: %s", pass_output_dir)
            return True, pass_output_dir
        else:
            logger.warning("Pass output directory does not exist: %s", pass_output_dir)

    logger.warning("Could not determine pass_output_dir")
    return True, None  # Success but no output dir


def post_process_pass(
    config: Dict[str, Any],
    pass_output_dir: str,
) -> bool:
    """Run post_processing.py for the given pass.

    Returns: success: bool
    """
    base_dir = str(Path(__file__).resolve().parent.parent)
    script = os.path.join(base_dir, "bin", "post_processing.py")

    if not os.path.exists(script):
        logger.warning("post_processing.py not found: %s", script)
        return False

    python_bin = config.get("paths", {}).get("python_bin", "python3")

    cmd = [
        python_bin,
        script,
        "--pass-output-dir",
        pass_output_dir,
    ]

    rc, stdout, stderr = run_subprocess(
        cmd,
        timeout=POST_PROCESSING_TIMEOUT,
        description=f"post_processing for {os.path.basename(pass_output_dir)}",
    )

    if rc != 0:
        logger.error("post_processing failed (rc=%s)", rc)
        if stderr.strip():
            logger.error("stderr: %s", stderr.strip())
        return False

    return True


def process_scheduled_passes(config: Dict[str, Any], testmode: bool = False) -> int:
    """Find and process all scheduled passes in the near future.

    Args:
        config: Configuration dictionary
        testmode: If True, use test mode with configured duration for all passes

    Returns: exit code
    """
    logger.info("─" * 60)
    logger.info("Checking for scheduled passes")

    passes = load_passes_to_schedule(config)
    if not passes:
        logger.info("No scheduled passes found")
        return 0

    logger.info("Found %d scheduled pass(es)", len(passes))

    failed_count = 0
    for i, pass_data in enumerate(passes, 1):
        satellite = pass_data.get("satellite", "UNKNOWN")
        pass_start = pass_data.get("start", "?")

        logger.info("─" * 60)
        logger.info("Pass %d/%d: %s at %s", i, len(passes), satellite, pass_start)

        # Wait until pass start time (skip in test mode)
        if not testmode:
            now = utc_now()
            try:
                pass_dt = datetime.fromisoformat(pass_start.replace("Z", "+00:00"))
                wait_seconds = (pass_dt - now).total_seconds()

                if wait_seconds > 0:
                    logger.info("Waiting %.0f seconds until pass start", wait_seconds)
                    time.sleep(wait_seconds)
            except (ValueError, OverflowError):
                logger.warning("Could not parse start time, proceeding immediately")
        else:
            logger.info("TEST MODE: Skipping wait, starting reception immediately")

        # Step 1: Receive pass
        logger.info("Step 1: Receiving satellite pass")
        recv_ok, pass_output_dir = receive_pass(config, pass_data, testmode=testmode)

        if not recv_ok:
            logger.error("Pass reception failed")
            failed_count += 1
            continue

        logger.info("Pass reception completed successfully")

        # Step 2: Post-process pass
        if pass_output_dir:
            logger.info("Step 2: Post-processing pass")
            postproc_ok = post_process_pass(config, pass_output_dir)

            if postproc_ok:
                logger.info("Post-processing completed successfully")
            else:
                logger.error("Post-processing failed (but reception succeeded)")
        else:
            logger.warning("No output directory, skipping post-processing")

        logger.info("Pass %d completed", i)

    logger.info("─" * 60)
    if failed_count > 0:
        logger.error("Completed with %d failure(s)", failed_count)
        return 1

    logger.info("All passes processed successfully")
    return 0


# --- CLI & Main --------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Orchestrate satellite pass reception and post-processing",
        epilog="""
MODES:

  Once (default):
    python3 receive_orchestrator.py
    Process all scheduled passes in the next 24 hours, then exit.

  Monitor (watch for new passes):
    python3 receive_orchestrator.py --monitor
    Continuously watch for new passes and process them.

  Manual (specific pass):
    python3 receive_orchestrator.py --pass-file /path/to/pass.json
    Process a specific pass from file.
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    p.add_argument(
        "--monitor",
        action="store_true",
        help="Continuously monitor for new passes (otherwise exit after processing current passes)",
    )
    p.add_argument(
        "--pass-file",
        metavar="FILE",
        help="Path to specific pass JSON file to process",
    )
    p.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_MONITOR_INTERVAL,
        metavar="SECONDS",
        help=f"Monitoring interval in seconds (default: {DEFAULT_MONITOR_INTERVAL})",
    )
    p.add_argument(
        "--config",
        metavar="FILE",
        help="Path to config.ini (default: ~/satpi/config/config.ini)",
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        help="Debug output",
    )
    p.add_argument(
        "--log-file",
        metavar="FILE",
        help="Log file path (in addition to stderr)",
    )
    p.add_argument(
        "--testmode",
        action="store_true",
        help="Use test mode with configured duration (in seconds) instead of actual pass duration",
    )

    return p.parse_args()


def main() -> int:
    args = parse_args()

    # Load config
    if args.config:
        config_path = args.config
    else:
        base_dir = str(Path(__file__).resolve().parent.parent)
        config_path = os.path.join(base_dir, "config", "config.ini")

    try:
        config = read_config(config_path)
    except ConfigError as e:
        print(f"[receive_orchestrator] CONFIG ERROR: {e}", file=sys.stderr)
        return 2

    # Setup logging
    log_file = args.log_file
    if not log_file and args.monitor:
        log_dir = config.get("paths", {}).get("log_dir", "/tmp")
        log_file = os.path.join(log_dir, "receive_orchestrator.log")

    setup_logger(args.verbose, log_file)

    logger.info("receive_orchestrator started (monitor=%s)", args.monitor)

    # Process specific pass if provided
    if args.pass_file:
        logger.info("Processing specific pass file: %s", args.pass_file)
        try:
            with open(args.pass_file, "r", encoding="utf-8") as f:
                pass_data = json.load(f)

            logger.info("Receiving pass: %s", pass_data.get("satellite", "UNKNOWN"))
            recv_ok, pass_output_dir = receive_pass(config, pass_data, testmode=args.testmode)

            if recv_ok and pass_output_dir:
                post_process_pass(config, pass_output_dir)

            return 0 if recv_ok else 1
        except (json.JSONDecodeError, OSError) as e:
            logger.error("Failed to load pass file: %s", e)
            return 1

    # Continuous monitoring mode
    if args.monitor:
        logger.info("Entering monitor mode (interval: %ds)", args.interval)
        try:
            while True:
                rc = process_scheduled_passes(config, testmode=args.testmode)
                if rc != 0:
                    logger.warning("Pass processing cycle completed with errors")
                logger.info("Sleeping %d seconds before next check", args.interval)
                time.sleep(args.interval)
        except KeyboardInterrupt:
            logger.info("Received interrupt, exiting")
            return 0
        except Exception as e:
            logger.error("Unexpected error: %s", e)
            return 1

    # One-time processing mode (default)
    logger.info("Processing scheduled passes once")
    return process_scheduled_passes(config, testmode=args.testmode)


if __name__ == "__main__":
    raise SystemExit(main())
