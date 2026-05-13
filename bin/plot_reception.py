#!/usr/bin/env python3
"""satpi – plot_reception

Plot reception data from SQLite database:
  - Skyplot (polar azimuth/elevation)
  - SNR timeline
  - Signal state timeline
  - Viterbi/Deframer states

Can be called with either --pass-id (composite key) or individual --date/--start-time/--satellite parameters.

Author: Andreas Horvath
Project: satpi
"""

from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent / "lib"))
from read_config import read_config, ConfigError


logger = logging.getLogger("satpi.plot_reception")


# --- Helpers -----------------------------------------------------------------

def setup_logger(log_level: str = "INFO") -> None:
    """Setup logging to stderr."""
    logger.setLevel(getattr(logging, log_level, logging.INFO))
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)


def db_connect(db_path: str) -> sqlite3.Connection:
    """Open SQLite connection with row factory."""
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database not found: {db_path}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def load_reception_samples(
    conn: sqlite3.Connection,
    pass_id: str,
) -> List[Dict[str, Any]]:
    """Load all samples for a pass from pass_detail table.

    Returns: List of sample dicts with keys: timestamp, snr_db, peak_snr_db, ber,
             viterbi_state, deframer_state, azimuth_deg, elevation_deg
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            timestamp, snr_db, peak_snr_db, ber,
            viterbi_state, deframer_state, azimuth_deg, elevation_deg
        FROM pass_detail
        WHERE pass_id = ?
        ORDER BY timestamp ASC
    """, (pass_id,))

    samples = []
    for row in cursor.fetchall():
        samples.append({
            "timestamp": row["timestamp"],
            "snr_db": row["snr_db"],
            "peak_snr_db": row["peak_snr_db"],
            "ber": row["ber"],
            "viterbi_state": row["viterbi_state"],
            "deframer_state": row["deframer_state"],
            "azimuth_deg": row["azimuth_deg"],
            "elevation_deg": row["elevation_deg"],
        })

    return samples


def angular_delta_deg(az1: float, az2: float) -> float:
    """Calculate shortest angular distance between two azimuths (0-360°).

    Returns: Delta in degrees (0-180)
    """
    delta = abs(az2 - az1)
    if delta > 180:
        delta = 360 - delta
    return delta


def plot_skyplot(
    samples: List[Dict[str, Any]],
    pass_label: str,
    output_path: str,
) -> None:
    """Plot azimuth/elevation as polar skyplot with single continuous line.

    Option 2: Render as one line from start to end, with visual gaps where
    elevation is below horizon (elevation < 0).
    """
    if not samples:
        logger.warning("No samples to plot")
        return

    # Extract visibility as binary indicator: 1 if elevation >= 0, 0 if elevation < 0
    azimuths = []
    elevations = []
    visibility = []

    for sample in samples:
        az = sample["azimuth_deg"]
        el = sample["elevation_deg"]

        azimuths.append(az)
        elevations.append(max(el, 0))  # Clamp negative elevations to 0 for polar plot
        visibility.append(1 if el >= 0 else 0)

    # Create figure with polar projection
    fig = plt.figure(figsize=(10, 10))
    ax = fig.add_subplot(111, projection="polar")

    # Convert azimuth to radians (0° = North, 90° = East, measured clockwise)
    azimuths_rad = np.deg2rad(azimuths)
    elevations_rad = np.deg2rad(elevations)

    # Plot with visibility-based styling:
    # - Solid line for above-horizon (visible) segments
    # - Use different coloring/alpha for below-horizon segments for clarity
    segments_visible = []
    segments_hidden = []

    for i in range(len(samples) - 1):
        if visibility[i] == 1 and visibility[i + 1] == 1:
            # Both above horizon: solid line segment
            segments_visible.append((azimuths_rad[i], elevations_rad[i],
                                    azimuths_rad[i + 1], elevations_rad[i + 1]))
        elif visibility[i] == 0 and visibility[i + 1] == 0:
            # Both below horizon: dashed line segment (for visual distinction)
            segments_hidden.append((azimuths_rad[i], elevations_rad[i],
                                   azimuths_rad[i + 1], elevations_rad[i + 1]))
        else:
            # Crossing horizon: solid line (connect from visible side)
            if visibility[i] == 1:
                segments_visible.append((azimuths_rad[i], elevations_rad[i],
                                        azimuths_rad[i + 1], elevations_rad[i + 1]))
            else:
                segments_hidden.append((azimuths_rad[i], elevations_rad[i],
                                       azimuths_rad[i + 1], elevations_rad[i + 1]))

    # Plot visible segments (above horizon)
    for az1, el1, az2, el2 in segments_visible:
        ax.plot([az1, az2], [el1, el2], "b-", linewidth=2, label="Above horizon" if az1 == azimuths_rad[0] else "")

    # Plot hidden segments (below horizon) with different style
    for az1, el1, az2, el2 in segments_hidden:
        ax.plot([az1, az2], [el1, el2], "b--", linewidth=1, alpha=0.3, label="Below horizon" if az1 == azimuths_rad[0] else "")

    # Mark start and end points
    ax.plot(azimuths_rad[0], elevations_rad[0], "go", markersize=10, label="Start")
    ax.plot(azimuths_rad[-1], elevations_rad[-1], "r^", markersize=10, label="End")

    # Configure polar plot
    ax.set_theta_zero_location("N")  # 0° at top (North)
    ax.set_theta_direction(-1)  # Clockwise
    ax.set_ylim(0, 90)
    ax.set_yticks([0, 30, 60, 90])
    ax.set_yticklabels(["0°", "30°", "60°", "90°"])
    ax.set_title(f"Skyplot - {pass_label}", pad=20, fontsize=14)
    ax.grid(True)
    ax.legend(loc="upper right", bbox_to_anchor=(1.3, 1.1))

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    logger.info("Skyplot saved: %s", output_path)


def plot_snr_timeline(
    samples: List[Dict[str, Any]],
    pass_label: str,
    output_path: str,
) -> None:
    """Plot SNR vs time."""
    if not samples:
        logger.warning("No samples to plot SNR timeline")
        return
    timestamps = [datetime.fromisoformat(s["timestamp"]) for s in samples]
    snr_db = [s["snr_db"] for s in samples]
    peak_snr_db = [s["peak_snr_db"] for s in samples]

    fig, ax = plt.subplots(figsize=(12, 6))

    ax.plot(timestamps, snr_db, "b-", linewidth=2, label="SNR")
    ax.plot(timestamps, peak_snr_db, "r-", linewidth=1, alpha=0.7, label="Peak SNR")
    ax.set_xlabel("Time")
    ax.set_ylabel("SNR (dB)")
    ax.set_title(f"SNR Timeline - {pass_label}")
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.autofmt_xdate()

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    logger.info("SNR timeline saved: %s", output_path)


def plot_signal_state(
    samples: List[Dict[str, Any]],
    pass_label: str,
    output_path: str,
) -> None:
    """Plot viterbi and deframer state transitions."""
    if not samples:
        logger.warning("No samples to plot signal state")
        return
    timestamps = [datetime.fromisoformat(s["timestamp"]) for s in samples]

    # Map state names to numeric values for plotting
    state_map = {"NOSYNC": 0, "SYNCING": 1, "SYNCED": 2}
    viterbi_states = [state_map.get(s["viterbi_state"], -1) for s in samples]
    deframer_states = [state_map.get(s["deframer_state"], -1) for s in samples]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)

    # Viterbi state
    ax1.plot(timestamps, viterbi_states, "b-", linewidth=2, marker=".", markersize=4)
    ax1.set_yticks([0, 1, 2])
    ax1.set_yticklabels(["NOSYNC", "SYNCING", "SYNCED"])
    ax1.set_ylabel("Viterbi State")
    ax1.set_title(f"Signal States - {pass_label}")
    ax1.grid(True, alpha=0.3)
    ax1.set_ylim(-0.5, 2.5)

    # Deframer state
    ax2.plot(timestamps, deframer_states, "r-", linewidth=2, marker=".", markersize=4)
    ax2.set_yticks([0, 1, 2])
    ax2.set_yticklabels(["NOSYNC", "SYNCING", "SYNCED"])
    ax2.set_xlabel("Time")
    ax2.set_ylabel("Deframer State")
    ax2.grid(True, alpha=0.3)
    ax2.set_ylim(-0.5, 2.5)

    fig.autofmt_xdate()
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    logger.info("Signal state plot saved: %s", output_path)


# --- CLI & Main --------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Plot reception data from SQLite database (using composite key: date + start_time + satellite)",
        epilog="""
PASS SELECTION (use either --pass-id OR --date/--start-time/--satellite):
  --pass-id "YYYY-MM-DD_HH-MM-SS_SATELLITE"
                             Composite pass identifier (e.g., 2026-05-04_13-45-30_METEOR-M2-X)
  --date YYYY-MM-DD
                             Pass date (required if --pass-id not provided)
  --start-time HH:MM:SS
                             Pass start time (required if --pass-id not provided)
  --satellite "SATELLITE_NAME"
                             Satellite name (required if --pass-id not provided)

DATABASE:
  --db-path /path/to/reception.db
                             Path to SQLite database (default: ~/satpi/results/database/reception.db)

OUTPUT:
  --output-dir /path/to/plots
                             Directory for output plots (default: current directory)

EXAMPLES:
  # Plot using pass_id
  python3 bin/plot_reception.py --pass-id "2026-05-04_13-45-30_METEOR-M2-X"

  # Plot using individual parameters
  python3 bin/plot_reception.py --date 2026-05-04 --start-time 13:45:30 \\
    --satellite "METEOR M2-X"

  # With custom database path
  python3 bin/plot_reception.py --pass-id "2026-05-04_13-45-30_METEOR-M2-X" \\
    --db-path /var/lib/satpi/reception.db
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    p.add_argument(
        "--pass-id",
        metavar="ID",
        help="Composite pass ID (YYYY-MM-DD_HH-MM-SS_SATELLITE)",
    )
    p.add_argument(
        "--date",
        metavar="DATE",
        help="Pass date (YYYY-MM-DD, e.g., 2026-05-04)",
    )
    p.add_argument(
        "--start-time",
        metavar="TIME",
        help="Pass start time (HH:MM:SS, e.g., 13:45:30)",
    )
    p.add_argument(
        "--satellite",
        metavar="SATELLITE",
        help="Satellite name (e.g., METEOR M2-X)",
    )
    p.add_argument(
        "--db-path",
        metavar="PATH",
        help="Path to SQLite database",
    )
    p.add_argument(
        "--output-dir",
        metavar="DIR",
        default=".",
        help="Directory for output plots (default: current directory)",
    )
    p.add_argument(
        "--config",
        metavar="FILE",
        help="Path to config.ini (used to find database if --db-path not specified)",
    )

    return p.parse_args()


def main() -> int:
    args = parse_args()

    setup_logger()

    # Determine pass_id and pass_label
    if args.pass_id:
        # Use pass_id directly
        pass_id = args.pass_id
        pass_label = args.pass_id
        logger.info("Using pass_id: %s", pass_id)
    else:
        # Use individual arguments (backward compatibility)
        if not args.date or not args.start_time or not args.satellite:
            logger.error("Either --pass-id or all of --date, --start-time, --satellite must be provided")
            return 1

        # Construct pass_id from individual arguments
        pass_start_time = args.start_time.replace(":", "-")  # Convert HH:MM:SS to HH-MM-SS
        pass_id = f"{args.date}_{pass_start_time}_{args.satellite}"
        pass_label = pass_id
        logger.info("Constructed pass_id from arguments: %s", pass_id)

    # Determine database path
    db_path = args.db_path
    if not db_path:
        if args.config:
            try:
                config = read_config(args.config)
                db_path = config.get("paths", {}).get("database_path")
            except ConfigError:
                pass

        if not db_path:
            db_path = os.path.expanduser("~/satpi/results/database/reception.db")

    logger.info("Using database: %s", db_path)

    try:
        conn = db_connect(db_path)
    except FileNotFoundError as e:
        logger.error(str(e))
        return 1

    # Load samples
    samples = load_reception_samples(conn, pass_id)
    conn.close()

    if not samples:
        logger.error("No samples found for pass: %s", pass_id)
        return 1

    logger.info("Loaded %d samples for pass: %s", len(samples), pass_id)

    # Create output directory
    output_dir = os.path.expanduser(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)

    # Generate plots
    try:
        plot_skyplot(
            samples, pass_label,
            os.path.join(output_dir, f"{pass_label}_skyplot.png")
        )
        plot_snr_timeline(
            samples, pass_label,
            os.path.join(output_dir, f"{pass_label}_snr.png")
        )
        plot_signal_state(
            samples, pass_label,
            os.path.join(output_dir, f"{pass_label}_states.png")
        )
        logger.info("All plots generated successfully")
    except Exception as e:
        logger.error("Error generating plots: %s", e)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
