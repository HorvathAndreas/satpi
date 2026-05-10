#!/usr/bin/env python3
"""satpi – test_reception

Test-Skript zur Diagnose von Empfangsproblemen.
Liest Empfangsparameter aus config.ini und führt eine Testaufnahme durch.

Verwendung:
  python3 test_reception.py [options]

Beispiele:
  python3 test_reception.py
  python3 test_reception.py --satellite "METEOR-M2 4" --duration 60 --verbose
  python3 test_reception.py --help

Author: Andreas Horvath
Project: satpi
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Optional

sys.path.insert(0, str(Path(__file__).parent.parent / "lib"))
from read_config import read_config, ConfigError


logger = logging.getLogger("satpi.test_reception")


def setup_logger(verbose: bool = False) -> None:
    """Setup logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logger.setLevel(level)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        prog="test_reception.py",
        description="Test satellite reception with configurable parameters",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES:
  python3 test_reception.py
  python3 test_reception.py --satellite "METEOR-M2 4" --duration 60
  python3 test_reception.py --verbose --output-dir /tmp/my_test
        """
    )

    parser.add_argument(
        "--satellite",
        help="Satellite name (default: first from config)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=15,
        help="Duration in seconds (default: 15)",
    )
    parser.add_argument(
        "--frequency",
        type=float,
        help="Frequency in Hz (default: from config)",
    )
    parser.add_argument(
        "--samplerate",
        type=float,
        help="Sample rate in Hz (default: from config)",
    )
    parser.add_argument(
        "--gain",
        type=float,
        help="Gain in dB (default: from config)",
    )
    parser.add_argument(
        "--source-id",
        help="RTL-SDR device ID (default: from config)",
    )
    parser.add_argument(
        "--bias-t",
        action="store_true",
        help="Enable Bias-T",
    )
    parser.add_argument(
        "--output-dir",
        default="/tmp/test_reception",
        help="Output directory (default: /tmp/test_reception)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Debug output",
    )

    return parser.parse_args()


def main() -> int:
    """Main test reception function."""
    args = parse_args()
    setup_logger(args.verbose)

    # Load config
    base_dir = str(Path(__file__).resolve().parent.parent)
    config_path = os.path.join(base_dir, "config", "config.ini")

    try:
        config = read_config(config_path)
    except ConfigError as e:
        logger.error("CONFIG ERROR: %s", e)
        return 2

    # Get satellite
    satellites = config.get("satellites", [])
    if not satellites:
        logger.error("No satellites configured in config.ini")
        return 2

    if args.satellite:
        # Find matching satellite by name
        sat = None
        for s in satellites:
            if s["name"].lower() == args.satellite.lower():
                sat = s
                break
        if not sat:
            logger.error("Satellite not found: %s", args.satellite)
            logger.info("Available satellites: %s", [s["name"] for s in satellites])
            return 2
    else:
        # Use first satellite from config
        sat = satellites[0]

    logger.info("Using satellite: %s", sat["name"])
    logger.info("Duration: %d seconds", args.duration)

    # Get parameters (user-provided or from config)
    frequency_hz = args.frequency if args.frequency else sat.get("frequency")
    samplerate_hz = args.samplerate if args.samplerate else config.get("hardware", {}).get("sample_rate", 2.4e6)
    gain_db = args.gain if args.gain is not None else config.get("hardware", {}).get("gain", 0.0)
    source_id = args.source_id if args.source_id else config.get("hardware", {}).get("source_id")
    bias_t = args.bias_t if args.bias_t else config.get("hardware", {}).get("bias_t", False)
    bandwidth_hz = sat.get("bandwidth_hz", 120000)  # Default 120 kHz for LRPT

    logger.info("Frequency: %.1f Hz (%.3f MHz)", frequency_hz, frequency_hz / 1e6)
    logger.info("Sample rate: %.0f Hz", samplerate_hz)
    logger.info("Bandwidth: %.0f Hz", bandwidth_hz)
    logger.info("Gain: %.1f dB", gain_db)
    logger.info("Source ID: %s", source_id or "default")
    logger.info("Bias-T: %s", "enabled" if bias_t else "disabled")

    # Prepare output directory
    output_dir = os.path.expanduser(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)

    logger.info("Output directory: %s", output_dir)
    logger.info("")
    logger.info("Starting test reception...")
    logger.info("─" * 60)

    # Build SatDump command
    # Format: satdump live PIPELINE output_dir --source rtlsdr --source_id X [options]
    pipeline = sat.get("pipeline", f"METEOR_M2_{sat['norad_id']}")

    cmd = [
        "satdump",
        "live",
        pipeline,
        output_dir,
        "--source", "rtlsdr",
        "--source_id", str(source_id),
        "--samplerate", str(int(samplerate_hz)),
        "--frequency", str(int(frequency_hz)),
        "--bandwidth", str(int(bandwidth_hz)),
        "--gain", str(int(gain_db)),
    ]

    if bias_t:
        cmd.append("--bias")

    if args.verbose:
        logger.debug("Command: %s", " ".join(cmd))

    try:
        # Run SatDump with timeout = duration + 10 seconds (for startup/cleanup)
        timeout = args.duration + 10
        proc = subprocess.run(
            cmd,
            timeout=timeout,
            text=True,
        )
        rc = proc.returncode
    except subprocess.TimeoutExpired:
        logger.error("Test reception timed out after %d seconds", timeout)
        return 1
    except FileNotFoundError:
        logger.error("satdump not found. Is it installed?")
        return 1
    except Exception as e:
        logger.error("Failed to run satdump: %s", e)
        return 1

    logger.info("─" * 60)

    if rc != 0:
        logger.error("SatDump exited with code %d", rc)
        return 1

    logger.info("Test reception completed successfully!")
    logger.info("Output saved to: %s", output_dir)

    # List output files
    if os.path.isdir(output_dir):
        files = os.listdir(output_dir)
        if files:
            logger.info("Generated files:")
            for f in sorted(files):
                fpath = os.path.join(output_dir, f)
                if os.path.isfile(fpath):
                    size_mb = os.path.getsize(fpath) / (1024 * 1024)
                    logger.info("  - %s (%.2f MB)", f, size_mb)
        else:
            logger.warning("No files generated in output directory")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
