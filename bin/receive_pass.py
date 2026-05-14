#!/usr/bin/env python3
"""satpi – receive_pass

Receive a single satellite pass and save raw data.

Receives the specified satellite pass (identified by pass-id) using SatDump
and saves the output to the configured output directory. Loads pass metadata
from passes.json. Creates a reception.json file with metadata.

Called by receive_orchestrator.py or systemd units.

Author: Andreas Horvath
Project: Autonomous, config-driven satellite reception pipeline for Raspberry Pi
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

sys.path.insert(0, str(Path(__file__).parent.parent / "lib"))
from read_config import read_config, ConfigError

logger = logging.getLogger("satpi.receive_pass")


def setup_logger() -> None:
    """Setup logging to stderr."""
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)


def find_pass_in_json(
    pass_id: str, pass_file: str
) -> Optional[Dict[str, Any]]:
    """Find a pass by pass-id in passes.json.

    Args:
        pass_id: The pass identifier (format: YYYY-MM-DD_HH-MM-SS_SATELLITE)
        pass_file: Path to passes.json

    Returns:
        Pass data dict if found, None otherwise
    """
    if not os.path.exists(pass_file):
        logger.error("Passes file not found: %s", pass_file)
        return None

    try:
        with open(pass_file, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.error("Failed to load passes file: %s", e)
        return None

    passes = data.get("passes", [])
    for pass_record in passes:
        if pass_record.get("pass-id") == pass_id:
            return pass_record

    logger.error("Pass not found in passes.json: %s", pass_id)
    return None


def calculate_duration_sec(start_iso: str, end_iso: str) -> int:
    """Calculate duration in seconds from ISO format timestamps.

    Args:
        start_iso: Start time in ISO format (e.g., "2026-05-13T12:00:00Z")
        end_iso: End time in ISO format (e.g., "2026-05-13T12:15:30Z")

    Returns:
        Duration in seconds
    """
    try:
        # Parse ISO timestamps (handle both with/without Z suffix)
        start_str = start_iso.replace("Z", "+00:00")
        end_str = end_iso.replace("Z", "+00:00")

        start_dt = datetime.fromisoformat(start_str)
        end_dt = datetime.fromisoformat(end_str)

        duration = int((end_dt - start_dt).total_seconds())
        return max(duration, 60)  # Minimum 60 seconds
    except (ValueError, TypeError) as e:
        logger.warning("Failed to parse timestamps: %s", e)
        return 900  # Default 15 minutes


def receive_satellite_pass(
    config: Dict[str, Any],
    pass_data: Dict[str, Any],
    pass_id: str,
    testmode: bool = False,
) -> tuple[bool, Optional[str]]:
    """Receive satellite pass using SatDump.

    Args:
        config: Configuration dictionary
        pass_data: Pass information from passes.json
        pass_id: The pass identifier for output directory naming
        testmode: If True, use configured test duration instead of actual pass duration

    Returns:
        (success: bool, output_dir: Optional[str])
    """
    satellite = pass_data.get("satellite", "UNKNOWN")
    frequency_hz = pass_data.get("frequency_hz")
    bandwidth_hz = pass_data.get("bandwidth_hz")
    pipeline = pass_data.get("pipeline", "lrpt")
    start_iso = pass_data.get("start")
    end_iso = pass_data.get("end")

    if not frequency_hz:
        logger.error("No frequency specified for %s", satellite)
        return False, None

    if testmode:
        duration_sec = config.get("testing", {}).get("duration_seconds", 60)
        logger.info("TEST MODE: Using configured duration of %d seconds", duration_sec)
    else:
        duration_sec = calculate_duration_sec(start_iso, end_iso)

    # Create output directory
    output_dir = config.get("paths", {}).get("output_dir", "/tmp/satpi_output")
    output_dir = os.path.expanduser(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    # Create pass-specific subdirectory using pass_id (e.g., 2026-05-13_12-00-00_METEOR-M2-4)
    pass_dir = os.path.join(output_dir, pass_id)
    os.makedirs(pass_dir, exist_ok=True)

    logger.info("Receiving %s (%s) for %d seconds", satellite, pass_id, duration_sec)
    logger.info("Output directory: %s", pass_dir)

    # Get RTL-SDR hardware parameters from config
    hardware = config.get("hardware", {})
    source_id = hardware.get("source_id", "00000001")
    gain = hardware.get("gain", 38.6)
    sample_rate = hardware.get("sample_rate", 2.4e6)
    bias_t = hardware.get("bias_t", True)

    # Build SatDump command
    cmd = [
        "satdump",
        "live",
        pipeline,
        pass_dir,
        "--source", "rtlsdr",
        "--device-id", str(source_id),
        "--samplerate", str(int(sample_rate)),
        "--frequency", str(int(frequency_hz)),
        "--gain", str(float(gain)),
        "--timeout", str(duration_sec),
    ]

    if bias_t:
        cmd.append("--bias-t")

    if bandwidth_hz:
        cmd.extend(["--bandwidth", str(int(bandwidth_hz))])

    logger.debug("Command: %s", " ".join(cmd))

    try:
        # Run SatDump with timeout = duration + 30 seconds (for startup/cleanup)
        timeout = duration_sec + 30
        proc = subprocess.run(
            cmd,
            timeout=timeout,
            capture_output=True,
            text=True,
        )

        if proc.returncode != 0:
            logger.error("satdump failed with return code %d", proc.returncode)
            if proc.stderr:
                logger.error("stderr: %s", proc.stderr.strip())
            return False, None

    except subprocess.TimeoutExpired:
        logger.error("Reception timed out after %d seconds", timeout)
        return False, None
    except FileNotFoundError:
        logger.error("satdump not found. Is it installed?")
        return False, None
    except Exception as e:
        logger.error("Failed to run satdump: %s", e)
        return False, None

    # Create reception.json with metadata
    reception_data = {
        "pass_id": pass_id,
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "satellite": satellite,
        "pipeline": pipeline,
        "frequency_hz": frequency_hz,
        "bandwidth_hz": bandwidth_hz,
        "duration_sec": duration_sec,
        "output_dir": pass_dir,
        "gain": gain,
        "bias_t": bias_t,
        "sample_rate": sample_rate,
        "source_id": source_id,
    }

    reception_json = os.path.join(pass_dir, "reception.json")
    try:
        with open(reception_json, "w", encoding="utf-8") as f:
            json.dump(reception_data, f, indent=2)
        logger.info("Reception metadata saved to: %s", reception_json)
    except (OSError, IOError) as e:
        logger.warning("Could not save reception.json: %s", e)

    # List output files
    if os.path.isdir(pass_dir):
        files = os.listdir(pass_dir)
        logger.info("Generated %d file(s) in pass directory", len(files))
        for f in sorted(files):
            fpath = os.path.join(pass_dir, f)
            if os.path.isfile(fpath):
                size_mb = os.path.getsize(fpath) / (1024 * 1024)
                if size_mb > 0.1:
                    logger.info("  %s: %.2f MB", f, size_mb)

    logger.info("Pass reception completed successfully")
    return True, pass_dir


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Receive a satellite pass and save raw data",
        epilog="""
USAGE:
  python3 receive_pass.py --pass-id 2026-05-13_12-00-00_METEOR-M2-4

CONFIGURATION:
  Reads config.ini from the default location (../config/config.ini).
  Loads pass metadata from passes.json (configured path).
  Output directory specified in config [paths] section.
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--pass-id",
        required=True,
        help="Pass identifier (format: YYYY-MM-DD_HH-MM-SS_SATELLITE)",
    )

    parser.add_argument(
        "--testmode",
        action="store_true",
        help="Use test mode with configured duration (in seconds) instead of actual pass duration",
    )

    return parser.parse_args()


def main() -> int:
    """Main entry point."""
    args = parse_args()

    # Load configuration (always use default location)
    base_dir = str(Path(__file__).resolve().parent.parent)
    config_path = os.path.join(base_dir, "config", "config.ini")

    try:
        config = read_config(config_path)
    except ConfigError as e:
        print(f"[receive_pass] CONFIG ERROR: {e}", file=sys.stderr)
        return 2

    setup_logger()
    logger.info("receive_pass started with pass-id=%s", args.pass_id)

    # Load passes.json and find the pass
    pass_file = config.get("paths", {}).get("pass_file", "results/passes.json")
    pass_file = os.path.expanduser(pass_file)
    if not os.path.isabs(pass_file):
        pass_file = os.path.join(base_dir, pass_file)

    pass_data = find_pass_in_json(args.pass_id, pass_file)
    if not pass_data:
        logger.error("Could not find pass: %s", args.pass_id)
        return 1

    # Receive the pass
    success, output_dir = receive_satellite_pass(config, pass_data, args.pass_id, testmode=args.testmode)

    if not success:
        logger.error("Pass reception failed")
        return 1

    # Print output directory to stdout (for parent process to capture)
    if output_dir:
        print(output_dir)

    logger.info("receive_pass completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
