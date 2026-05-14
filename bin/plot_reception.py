#!/usr/bin/env python3
"""satpi – plot_reception

Plot reception data from SQLite database:
  - Skyplot (polar azimuth/elevation; zenith at center, horizon at edge)
  - SNR timeline
  - Signal-state timeline (Viterbi / Deframer)

Can be called with either --pass-id (composite key) or individual
--date / --start-time / --satellite parameters.

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
from typing import Any, Dict, List, Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent / "lib"))
from read_config import read_config, ConfigError  # noqa: E402

logger = logging.getLogger("satpi.plot_reception")


# --- Helpers -----------------------------------------------------------------

def setup_logger(log_level: str = "INFO") -> None:
    logger.setLevel(getattr(logging, log_level, logging.INFO))
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)


def db_connect(db_path: str) -> sqlite3.Connection:
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database not found: {db_path}")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def load_reception_samples(conn: sqlite3.Connection, pass_id: str) -> List[Dict[str, Any]]:
    cursor = conn.cursor()
    cursor.execute("""
        SELECT timestamp, snr_db, peak_snr_db, ber,
               viterbi_state, deframer_state, azimuth_deg, elevation_deg
        FROM pass_detail
        WHERE pass_id = ?
        ORDER BY timestamp ASC
    """, (pass_id,))
    return [
        {
            "timestamp": row["timestamp"],
            "snr_db": row["snr_db"],
            "peak_snr_db": row["peak_snr_db"],
            "ber": row["ber"],
            "viterbi_state": row["viterbi_state"],
            "deframer_state": row["deframer_state"],
            "azimuth_deg": row["azimuth_deg"],
            "elevation_deg": row["elevation_deg"],
        }
        for row in cursor.fetchall()
    ]


# --- Skyplot -----------------------------------------------------------------

def _split_at_az_wrap(theta_rad: np.ndarray, r: np.ndarray, az_deg: np.ndarray):
    """Split (theta, r) into segments where the azimuth wraps across 0/360.

    Without splitting, matplotlib draws a chord across the polar plot when
    the azimuth jumps from e.g. 358° to 2°.
    Yields (theta_seg, r_seg) tuples.
    """
    if len(az_deg) < 2:
        yield theta_rad, r
        return

    diffs = np.abs(np.diff(az_deg))
    wrap_idx = np.where(diffs > 180)[0]
    if len(wrap_idx) == 0:
        yield theta_rad, r
        return

    start = 0
    for w in wrap_idx:
        end = w + 1
        if end > start:
            yield theta_rad[start:end], r[start:end]
        start = end
    if start < len(theta_rad):
        yield theta_rad[start:], r[start:]


def plot_skyplot(samples: List[Dict[str, Any]], pass_label: str, output_path: str) -> None:
    """Polar skyplot following the standard astronomy convention:
        - Zenith (90°)  → centre of the plot
        - Horizon (0°)  → outer edge of the plot
        - North (0° az) → top
        - East  (90° az)→ right
        - 360° az → 0° az smoothly (no chord across plot)
    """
    valid = [
        s for s in samples
        if s.get("azimuth_deg") is not None and s.get("elevation_deg") is not None
    ]
    if not valid:
        logger.warning("No samples with valid az/el – skyplot skipped")
        return

    az_deg = np.array([s["azimuth_deg"] for s in valid], dtype=float)
    el_deg = np.array([s["elevation_deg"] for s in valid], dtype=float)

    # Clip elevation to [0, 90]: anything below the horizon shows on the rim.
    el_clipped = np.clip(el_deg, 0.0, 90.0)

    # Polar coords for matplotlib:
    #   theta: azimuth in radians
    #   r:     90 - elevation, so 90° elev → r=0 (centre), 0° elev → r=90 (rim)
    theta = np.deg2rad(az_deg)
    r = 90.0 - el_clipped

    fig = plt.figure(figsize=(10, 10))
    ax = fig.add_subplot(111, projection="polar")

    # Trajectory — split at any azimuth wrap-around so we don't draw a chord.
    first_segment = True
    for theta_seg, r_seg in _split_at_az_wrap(theta, r, az_deg):
        if len(theta_seg) < 2:
            continue
        label = "Pass trajectory" if first_segment else None
        ax.plot(theta_seg, r_seg, "b-", linewidth=2, label=label)
        first_segment = False

    # Start / end markers — labels include actual az/el values.
    ax.plot(theta[0], r[0], "go", markersize=12,
            label=f"Start (az={az_deg[0]:.0f}°, el={el_deg[0]:.1f}°)")
    ax.plot(theta[-1], r[-1], "r^", markersize=12,
            label=f"End (az={az_deg[-1]:.0f}°, el={el_deg[-1]:.1f}°)")

    # Polar axes configuration — North up, clockwise, zenith at centre.
    ax.set_theta_zero_location("N")
    ax.set_theta_direction(-1)

    # Radial: 0..90 with labels reversed so the centre reads "90°" and the rim "0°".
    ax.set_ylim(0, 90)
    ax.set_yticks([0, 30, 60, 90])
    ax.set_yticklabels(["90°", "60°", "30°", "0°"])

    # Azimuth grid in 8 cardinal/intercardinal directions with letter labels.
    ax.set_thetagrids(
        [0, 45, 90, 135, 180, 225, 270, 315],
        ["N", "NE", "E", "SE", "S", "SW", "W", "NW"],
    )

    ax.grid(True, alpha=0.5)
    ax.set_title(f"Skyplot — {pass_label}", pad=20, fontsize=14)
    ax.legend(loc="upper right", bbox_to_anchor=(1.35, 1.10), fontsize=9)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Skyplot saved: %s", output_path)


# --- SNR timeline ------------------------------------------------------------

def plot_snr_timeline(samples: List[Dict[str, Any]], pass_label: str, output_path: str) -> None:
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
    ax.set_title(f"SNR Timeline — {pass_label}")
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.autofmt_xdate()

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("SNR timeline saved: %s", output_path)


# --- Signal-state timeline ---------------------------------------------------

def plot_signal_state(samples: List[Dict[str, Any]], pass_label: str, output_path: str) -> None:
    if not samples:
        logger.warning("No samples to plot signal state")
        return

    timestamps = [datetime.fromisoformat(s["timestamp"]) for s in samples]
    state_map = {"NOSYNC": 0, "SYNCING": 1, "SYNCED": 2}
    viterbi_states = [state_map.get(s["viterbi_state"], -1) for s in samples]
    deframer_states = [state_map.get(s["deframer_state"], -1) for s in samples]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)

    ax1.plot(timestamps, viterbi_states, "b-", linewidth=2, marker=".", markersize=4)
    ax1.set_yticks([0, 1, 2])
    ax1.set_yticklabels(["NOSYNC", "SYNCING", "SYNCED"])
    ax1.set_ylabel("Viterbi State")
    ax1.set_title(f"Signal States — {pass_label}")
    ax1.grid(True, alpha=0.3)
    ax1.set_ylim(-0.5, 2.5)

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
    plt.close(fig)
    logger.info("Signal state plot saved: %s", output_path)


# --- CLI & Main --------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Plot reception data from SQLite database "
                    "(using composite key: date + start_time + satellite)",
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
                             Path to SQLite database (default: from config or ~/satpi/results/database/reception.db)

EXAMPLES:
  python3 bin/plot_reception.py --pass-id "2026-05-04_13-45-30_METEOR-M2-X"
  python3 bin/plot_reception.py --date 2026-05-04 --start-time 13:45:30 --satellite "METEOR M2-X"
  python3 bin/plot_reception.py --pass-id "2026-05-04_13-45-30_METEOR-M2-X" --db-path /var/lib/satpi/reception.db
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    p.add_argument("--pass-id", metavar="ID",
                   help="Composite pass ID (YYYY-MM-DD_HH-MM-SS_SATELLITE)")
    p.add_argument("--date", metavar="DATE",
                   help="Pass date (YYYY-MM-DD, e.g., 2026-05-04)")
    p.add_argument("--start-time", metavar="TIME",
                   help="Pass start time (HH:MM:SS, e.g., 13:45:30)")
    p.add_argument("--satellite", metavar="SATELLITE",
                   help="Satellite name (e.g., METEOR M2-X)")
    p.add_argument("--db-path", metavar="PATH", help="Path to SQLite database")
    p.add_argument("--config", metavar="FILE",
                   help="Path to config.ini (used to find database and output directory)")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    setup_logger()

    if args.pass_id:
        pass_id = args.pass_id
        pass_label = args.pass_id
        logger.info("Using pass_id: %s", pass_id)
    else:
        if not args.date or not args.start_time or not args.satellite:
            logger.error(
                "Either --pass-id or all of --date, --start-time, --satellite must be provided"
            )
            return 1
        pass_start_time = args.start_time.replace(":", "-")
        pass_id = f"{args.date}_{pass_start_time}_{args.satellite}"
        pass_label = pass_id
        logger.info("Constructed pass_id from arguments: %s", pass_id)

    config = None
    if args.config:
        try:
            config = read_config(args.config)
        except ConfigError:
            pass

    db_path = args.db_path
    if not db_path:
        if config:
            db_path = config.get("paths", {}).get("reception_db_file")
        if not db_path:
            db_path = os.path.expanduser("~/satpi/results/database/reception.db")
    logger.info("Using database: %s", db_path)

    try:
        conn = db_connect(db_path)
    except FileNotFoundError as e:
        logger.error(str(e))
        return 1

    samples = load_reception_samples(conn, pass_id)
    conn.close()

    if not samples:
        logger.error("No samples found for pass: %s", pass_id)
        return 1
    logger.info("Loaded %d samples for pass: %s", len(samples), pass_id)

    # paths.output_dir is already the directory holding all per-pass dirs
    # (typically ~/satpi/results/passes), so we only need to append pass_id.
    output_dir = "."
    if config:
        base_output = config.get("paths", {}).get("output_dir")
        if base_output:
            base_output = os.path.expanduser(base_output)
            output_dir = os.path.join(base_output, pass_id)
        else:
            output_dir = os.path.join(os.path.expanduser("~/satpi/results/passes"), pass_id)
    else:
        output_dir = os.path.join(os.path.expanduser("~/satpi/results/passes"), pass_id)
    os.makedirs(output_dir, exist_ok=True)
    logger.info("Using output directory: %s", output_dir)

    try:
        plot_skyplot(samples, pass_label,
                     os.path.join(output_dir, f"{pass_label}_skyplot.png"))
        plot_snr_timeline(samples, pass_label,
                          os.path.join(output_dir, f"{pass_label}_snr.png"))
        plot_signal_state(samples, pass_label,
                          os.path.join(output_dir, f"{pass_label}_states.png"))
        logger.info("All plots generated successfully")
    except Exception as e:
        logger.exception("Error generating plots: %s", e)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
