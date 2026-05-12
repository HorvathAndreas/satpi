#!/usr/bin/env python3
# satpi
# Creates reception plots from the satpi SQLite database.
# Refactored for composite key structure: (pass_date, pass_start_time, satellite)
# - If --date/--start-time/--satellite are set, creates skyplot and timeseries for exactly one pass
# - Otherwise, creates a combined overview skyplot across all passes matching the filters
# Author: Andreas Horvath
# Project: Autonomous, Config-driven satellite reception pipeline for Raspberry Pi

import argparse
import math
import os
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt

SCRIPT_DIR = Path(__file__).resolve().parent

sys.path.insert(0, str(Path(__file__).parent.parent / "lib"))
from read_config import read_config, ConfigError

def get_config_path() -> str:
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_dir, "config", "config.ini")


def parse_utc(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def derive_sync_state(viterbi_state: str, deframer_state: str) -> str:
    if deframer_state == "SYNCED":
        return "SYNCED"
    if viterbi_state == "SYNCED":
        return "SYNCING"
    return "NOSYNC"


def state_color(state: str) -> str:
    if state == "SYNCED":
        return "green"
    if state == "SYNCING":
        return "gold"
    return "red"


def angular_delta_deg(a1: float, a2: float) -> float:
    diff = abs(a2 - a1)
    if diff > 180:
        diff = 360 - diff
    return diff


def sanitize_filename_component(value: str) -> str:
    value = value.strip().replace(" ", "_").replace("/", "_")
    value = value.replace(":", "-")
    return value


def normalize_multi_values(raw_values):
    if not raw_values:
        return None

    values = []
    for item in raw_values:
        for part in item.split(","):
            value = part.strip()
            if value:
                values.append(value)

    return values or None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Create reception plots from the satpi SQLite database. "
            "If --date/--start-time/--satellite are set, a single-pass skyplot and timechart are generated. "
            "Otherwise a combined skyplot is generated."
        ),
        epilog="""
SINGLE-PASS MODE (choose one):
  --pass-id "YYYY-MM-DD_HH-MM-SS_SATELLITE"
                             Pass ID (e.g., 2026-05-05_04-01-37_METEOR-M2_4)
  OR all three:
  --date YYYY-MM-DD
                             Pass date
  --start-time HH:MM:SS
                             Pass start time
  --satellite "SATELLITE_NAME"
                             Satellite name

COMBINED MODE (all optional, AND-combined):
  --satellite "NAME"
                             Filter by satellite (repeat or comma-separated)
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Pass-ID shortcut
    parser.add_argument(
        "--pass-id",
        default=None,
        help="Pass ID in format YYYY-MM-DD_HH-MM-SS_SATELLITE (alternative to --date/--start-time/--satellite)",
    )

    # Single-pass mode arguments
    parser.add_argument(
        "--date",
        default=None,
        help="Pass date (YYYY-MM-DD) for single-pass mode",
    )
    parser.add_argument(
        "--start-time",
        default=None,
        help="Pass start time (HH:MM:SS) for single-pass mode",
    )
    parser.add_argument(
        "--satellite",
        action="append",
        default=None,
        help="Filter by satellite (combined mode) or specify for single-pass",
    )

    # Combined mode options
    parser.add_argument(
        "--highlight-pass-id",
        default=None,
        help="Highlight a specific pass (pass_date_start-time_satellite) in combined plot",
    )
    parser.add_argument(
        "--highlight-label",
        default="winning pass",
        help="Label text for highlighted pass",
    )

    # Output
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory for plots",
    )

    return parser


def open_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def load_single_pass(conn: sqlite3.Connection, pass_id: str):
    """Load header and detail rows for a specific pass."""
    header = conn.execute("""
        SELECT * FROM pass_header
        WHERE pass_id = ?
    """, (pass_id,)).fetchone()

    if header is None:
        return None, []

    detail_rows = conn.execute("""
        SELECT * FROM pass_detail
        WHERE pass_id = ?
        ORDER BY timestamp
    """, (pass_id,)).fetchall()

    return header, detail_rows


def load_all_samples(conn: sqlite3.Connection, satellite_filter: list[str] = None):
    """Load all samples, optionally filtered by satellite."""
    sql = """
    SELECT h.pass_id, h.satellite, h.pipeline, h.frequency_hz,
           h.bandwidth_hz, h.gain, h.source_id, h.bias_t,
           h.pass_start, h.pass_end, h.scheduled_start, h.scheduled_end,
           d.timestamp, d.snr_db, d.peak_snr_db, d.ber,
           d.viterbi_state, d.deframer_state, d.azimuth_deg, d.elevation_deg
    FROM pass_detail d
    JOIN pass_header h ON h.pass_id = d.pass_id
    """
    params = []

    if satellite_filter:
        placeholders = ",".join("?" * len(satellite_filter))
        sql += f" WHERE h.satellite IN ({placeholders})"
        params = satellite_filter

    sql += " ORDER BY h.satellite, h.pass_start, d.timestamp"

    return conn.execute(sql, params).fetchall()


def prepare_samples_from_detail_rows(rows) -> list[dict]:
    """Convert detail rows to sample dictionaries."""
    prepared = []

    for row in rows:
        if row["azimuth_deg"] is None or row["elevation_deg"] is None:
            continue

        viterbi_state = row["viterbi_state"] or "NOSYNC"
        deframer_state = row["deframer_state"] or "NOSYNC"
        sync_state = derive_sync_state(viterbi_state, deframer_state)

        prepared.append({
            "timestamp": parse_utc(row["timestamp"]),
            "snr_db": float(row["snr_db"]) if row["snr_db"] is not None else 0.0,
            "peak_snr_db": float(row["peak_snr_db"]) if row["peak_snr_db"] is not None else 0.0,
            "ber": float(row["ber"]) if row["ber"] is not None else 0.0,
            "viterbi_state": viterbi_state,
            "deframer_state": deframer_state,
            "sync_state": sync_state,
            "azimuth_deg": float(row["azimuth_deg"]),
            "elevation_deg": float(row["elevation_deg"]),
        })

    prepared.sort(key=lambda x: x["timestamp"])
    return prepared


def build_single_data(header_row) -> dict:
    """Build single-pass metadata dictionary."""
    # Extract pass_date and pass_start_time from pass_start (ISO format: "2026-05-05T15:26:38Z")
    pass_start = header_row["pass_start"]
    pass_date = pass_start.split("T")[0] if "T" in pass_start else ""
    pass_start_time = pass_start.split("T")[1].replace("Z", "").split("+")[0] if "T" in pass_start else ""

    return {
        "pass_date": pass_date,
        "pass_start_time": pass_start_time,
        "satellite": header_row["satellite"],
        "pipeline": header_row["pipeline"],
        "frequency_hz": header_row["frequency_hz"],
        "bandwidth_hz": header_row["bandwidth_hz"],
        "gain": header_row["gain"],
        "source_id": header_row["source_id"],
        "bias_t": bool(header_row["bias_t"]),
        "pass_end": header_row["pass_end"],
        "scheduled_start": header_row["scheduled_start"],
        "scheduled_end": header_row["scheduled_end"],
    }


def get_visible_samples(samples: list[dict]) -> list[dict]:
    """Filter samples with elevation >= 0."""
    return [s for s in samples if s["elevation_deg"] >= 0.0]


def merge_segments_by_state(samples: list[dict]) -> list[tuple[datetime, datetime, str]]:
    """Group consecutive samples by sync state."""
    if not samples:
        return []

    segments = []
    seg_start = samples[0]["timestamp"]
    current_state = samples[0]["sync_state"]

    for i in range(1, len(samples)):
        state = samples[i]["sync_state"]
        if state != current_state:
            segments.append((seg_start, samples[i]["timestamp"], current_state))
            seg_start = samples[i]["timestamp"]
            current_state = state

    segments.append((seg_start, samples[-1]["timestamp"], current_state))
    return segments


def format_box_value(value) -> str:
    """Format value for display boxes."""
    if value is None:
        return "-"
    text = str(value).strip()
    return text if text else "-"


def build_single_metadata_text(data: dict) -> str:
    """Build metadata text box for single-pass plot."""
    lines = [
        f"Pass Date: {format_box_value(data.get('pass_date', '-'))}",
        f"Pass Start: {format_box_value(data.get('pass_start_time', '-'))}",
        f"Satellite: {format_box_value(data.get('satellite', '-'))}",
        f"Pipeline: {format_box_value(data.get('pipeline', '-'))}",
        f"Frequency: {format_box_value(data.get('frequency_hz', '-'))} Hz",
        f"Bandwidth: {format_box_value(data.get('bandwidth_hz', '-'))} Hz",
        f"Gain: {format_box_value(data.get('gain', '-'))}",
        f"Source ID: {format_box_value(data.get('source_id', '-'))}",
        f"Bias-T: {format_box_value(data.get('bias_t', '-'))}",
        f"Pass End: {format_box_value(data.get('pass_end_time', '-'))}",
        f"Scheduled Start: {format_box_value(data.get('scheduled_start', '-'))}",
        f"Scheduled End: {format_box_value(data.get('scheduled_end', '-'))}",
    ]
    return "\n".join(lines)


def plot_skyplot(data: dict, samples: list[dict], output_path: str):
    """Plot single-pass skyplot."""
    visible_samples = get_visible_samples(samples)

    if len(visible_samples) < 2:
        raise ValueError("Need at least 2 samples with elevation >= 0 for skyplot")

    fig = plt.figure(figsize=(11.69, 8.27))
    ax = fig.add_axes([0.05, 0.10, 0.54, 0.80], projection="polar")

    ax.set_theta_zero_location("N")
    ax.set_theta_direction(-1)
    ax.set_rlim(0, 90)
    ax.set_rticks([0, 10, 20, 30, 40, 50, 60, 70, 80, 90])
    ax.set_yticklabels(["90", "80", "70", "60", "50", "40", "30", "20", "10", "0"])
    ax.set_rlabel_position(225)

    # Plot line segments with state-based coloring
    for i in range(len(visible_samples) - 1):
        s1 = visible_samples[i]
        s2 = visible_samples[i + 1]

        az1 = s1["azimuth_deg"]
        az2 = s2["azimuth_deg"]
        el1 = s1["elevation_deg"]
        el2 = s2["elevation_deg"]

        if angular_delta_deg(az1, az2) > 60:
            continue

        theta = [math.radians(az1), math.radians(az2)]
        radius = [90.0 - el1, 90.0 - el2]

        color = state_color(s1["sync_state"])
        ax.plot(theta, radius, linewidth=2.5, color=color)

    # Mark start and end
    start = visible_samples[0]
    end = visible_samples[-1]

    start_theta = math.radians(start["azimuth_deg"])
    start_radius = 90.0 - start["elevation_deg"]
    end_theta = math.radians(end["azimuth_deg"])
    end_radius = 90.0 - end["elevation_deg"]

    ax.scatter([start_theta], [start_radius], marker="o", s=140, facecolor="blue",
               edgecolor="white", linewidth=1.5, zorder=10, label="Start")
    ax.scatter([end_theta], [end_radius], marker="x", s=140, color="black",
               linewidth=2.5, zorder=11, label="End")

    ax.annotate("Start", xy=(start_theta, start_radius), xytext=(8, 8),
                textcoords="offset points", fontsize=6, color="blue", weight="bold")
    ax.annotate("End", xy=(end_theta, end_radius), xytext=(8, -12),
                textcoords="offset points", fontsize=6, color="black", weight="bold")

    pass_label = f"{data['pass_date']} {data['pass_start_time']} {data['satellite']}"
    ax.set_title(f"Skyplot {pass_label}", va="bottom")

    fig.text(0.62, 0.86, build_single_metadata_text(data), va="top", ha="left",
             fontsize=6, bbox=dict(boxstyle="round", facecolor="white", alpha=0.90))

    legend_handles = [
        plt.Line2D([0], [0], color="red", lw=3, label="NOSYNC"),
        plt.Line2D([0], [0], color="gold", lw=3, label="SYNCING"),
        plt.Line2D([0], [0], color="green", lw=3, label="SYNCED"),
        plt.Line2D([0], [0], marker="o", color="blue", lw=0, label="Start"),
        plt.Line2D([0], [0], marker="x", color="black", lw=0, label="End"),
    ]
    fig.legend(handles=legend_handles, loc="lower left",
               bbox_to_anchor=(0.62, 0.14), fontsize=6, frameon=True)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def plot_timeseries(data: dict, samples: list[dict], output_path: str):
    """Plot single-pass timeseries (SNR and BER)."""
    if not samples:
        raise ValueError("No samples available")

    times = [s["timestamp"] for s in samples]
    snr = [s["snr_db"] for s in samples]
    ber = [s["ber"] for s in samples]

    fig, ax1 = plt.subplots(figsize=(16, 6))
    fig.subplots_adjust(right=0.72)

    for start, end, state in merge_segments_by_state(samples):
        ax1.axvspan(start, end, alpha=0.15, color=state_color(state))

    ax1.plot(times, snr, linewidth=1.8, label="SNR (dB)")
    ax1.set_ylabel("SNR (dB)")
    ax1.set_xlabel("Time (UTC)")
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))

    ax2 = ax1.twinx()
    ax2.plot(times, ber, linewidth=1.2, linestyle="--", label="BER")
    ax2.set_ylabel("BER", labelpad=14)

    pass_label = f"{data['pass_date']} {data['pass_start_time']} {data['satellite']}"
    ax1.set_title(f"Timeseries {pass_label}")

    fig.text(0.75, 0.87, build_single_metadata_text(data), va="top", ha="left",
             fontsize=6, bbox=dict(boxstyle="round", facecolor="white", alpha=0.85))

    handles = [
        plt.Line2D([0], [0], color="red", lw=6, alpha=0.3, label="NOSYNC"),
        plt.Line2D([0], [0], color="gold", lw=6, alpha=0.3, label="SYNCING"),
        plt.Line2D([0], [0], color="green", lw=6, alpha=0.3, label="SYNCED"),
        plt.Line2D([0], [0], lw=1.8, label="SNR (dB)"),
        plt.Line2D([0], [0], lw=1.2, linestyle="--", label="BER"),
    ]
    ax1.legend(handles=handles, loc="upper left")

    fig.autofmt_xdate()
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig.savefig(output_path, dpi=150)
    plt.close(fig)

def build_pass_map(rows):
    """Build dictionary of passes with their samples."""
    passes = defaultdict(list)

    for row in rows:
        az = row["azimuth_deg"]
        el = row["elevation_deg"]

        if az is None or el is None:
            continue

        sync_state = derive_sync_state(row["viterbi_state"], row["deframer_state"])
        pass_id = row["pass_id"]

        passes[pass_id].append({
            "timestamp": row["timestamp"],
            "satellite": row["satellite"],
            "pipeline": row["pipeline"],
            "azimuth_deg": float(az),
            "elevation_deg": float(el),
            "snr_db": float(row["snr_db"]) if row["snr_db"] is not None else None,
            "ber": float(row["ber"]) if row["ber"] is not None else None,
            "sync_state": sync_state,
        })

    return passes

def build_satellite_arrow_colors(pass_map):
    """Assign colors to satellites for visualization."""
    color_cycle = ["blue", "magenta", "cyan", "black", "orange", "purple", "brown", "navy"]
    satellites = sorted({samples[0]["satellite"] for samples in pass_map.values() if samples})
    return {sat: color_cycle[idx % len(color_cycle)] for idx, sat in enumerate(satellites)}


def build_combined_metadata_text(pass_map: dict) -> str:
    """Build metadata text for combined plots."""
    satellites = sorted(set(s[0]["satellite"] for s in pass_map.values() if s))
    return f"Satellites: {', '.join(satellites) if satellites else 'N/A'}"


def build_combined_title(pass_map: dict) -> str:
    """Build title for combined plot."""
    satellites = sorted(set(s[0]["satellite"] for s in pass_map.values() if s))
    if len(satellites) == 1:
        return f"Skyplot {satellites[0]}"
    return "Skyplot Combined"


def draw_combined_plot(pass_map, output_path: str, highlight_pass_id: str = None, highlight_label: str = "winning pass"):
    """Plot combined skyplot from multiple passes."""
    fig = plt.figure(figsize=(12, 9))
    ax = fig.add_axes([0.06, 0.08, 0.62, 0.84], projection="polar")

    ax.set_theta_zero_location("N")
    ax.set_theta_direction(-1)
    ax.set_rlim(0, 90)
    ax.set_rticks([0, 10, 20, 30, 40, 50, 60, 70, 80, 90])
    ax.set_yticklabels(["90", "80", "70", "60", "50", "40", "30", "20", "10", "0"])
    ax.set_rlabel_position(225)
    ax.set_title(build_combined_title(pass_map), va="bottom")

    satellite_colors = build_satellite_arrow_colors(pass_map)
    total_segments = 0
    pass_count = 0

    for pass_id, samples in sorted(pass_map.items()):
        if len(samples) < 2:
            continue

        pass_count += 1
        satellite = samples[0]["satellite"]
        arrow_color = satellite_colors[satellite]
        is_highlighted = highlight_pass_id and pass_id == highlight_pass_id

        for i in range(len(samples) - 1):
            s1 = samples[i]
            s2 = samples[i + 1]

            az1 = s1["azimuth_deg"]
            az2 = s2["azimuth_deg"]
            el1 = s1["elevation_deg"]
            el2 = s2["elevation_deg"]

            if angular_delta_deg(az1, az2) > 60:
                continue

            theta = [math.radians(az1), math.radians(az2)]
            radius = [90.0 - el1, 90.0 - el2]

            color = state_color(s1["sync_state"])
            linewidth = 1.2 if s1["sync_state"] == "NOSYNC" else 2.0
            alpha = 0.35 if s1["sync_state"] == "NOSYNC" else 0.8
            zorder = 10

            if is_highlighted:
                linewidth += 1.4
                alpha = 1.0
                zorder = 40

            ax.plot(theta, radius, color=color, linewidth=linewidth, alpha=alpha,
                    linestyle="-", zorder=zorder)
            total_segments += 1

        # Draw direction arrow
        visible = [s for s in samples if s["elevation_deg"] >= 0.0]
        if len(visible) >= 3:
            start = visible[0]
            next_point = visible[2]

            start_theta = math.radians(start["azimuth_deg"])
            start_radius = 90.0 - start["elevation_deg"]
            next_theta = math.radians(next_point["azimuth_deg"])
            next_radius = 90.0 - next_point["elevation_deg"]

            ax.annotate("", xy=(next_theta, next_radius), xytext=(start_theta, start_radius),
                        arrowprops=dict(arrowstyle="simple", fc=arrow_color, ec=arrow_color,
                                      lw=0.0, alpha=0.95, shrinkA=0, shrinkB=0, mutation_scale=14),
                        zorder=80 if not is_highlighted else 90)

        if is_highlighted and visible:
            mid = visible[len(visible) // 2]
            mid_theta = math.radians(mid["azimuth_deg"])
            mid_radius = 90.0 - mid["elevation_deg"]
            ax.annotate(highlight_label, xy=(mid_theta, mid_radius), xytext=(-70, -45),
                        textcoords="offset points", ha="right", va="top", fontsize=8, fontweight="bold",
                        color="black", bbox=dict(boxstyle="round,pad=0.25", facecolor="white", alpha=0.9),
                        arrowprops=dict(arrowstyle="->", color="black", lw=1.2, shrinkA=0, shrinkB=0),
                        zorder=120)

    # Legends
    sync_handles = [
        plt.Line2D([0], [0], color="red", lw=3, label="NOSYNC"),
        plt.Line2D([0], [0], color="gold", lw=3, label="SYNCING"),
        plt.Line2D([0], [0], color="green", lw=3, label="SYNCED"),
    ]
    legend1 = fig.legend(handles=sync_handles, loc="lower left",
                         bbox_to_anchor=(0.72, 0.22), fontsize=8, frameon=True, title="Sync state")
    fig.add_artist(legend1)

    sat_handles = [plt.Line2D([0], [0], color=c, lw=0, marker=">", markersize=9, label=s)
                   for s, c in sorted(satellite_colors.items())]
    fig.legend(handles=sat_handles, loc="lower left",
               bbox_to_anchor=(0.72, 0.02), fontsize=8, frameon=True, title="Satellite")

    info = f"Passes: {pass_count} | Segments: {total_segments}"
    fig.text(0.72, 0.82, info, va="top", ha="left", fontsize=9,
             bbox=dict(boxstyle="round", facecolor="white", alpha=0.9))
    fig.text(0.72, 0.68, build_combined_metadata_text(pass_map), va="top", ha="left", fontsize=7,
             bbox=dict(boxstyle="round", facecolor="white", alpha=0.9))

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def main():
    config_path = get_config_path()

    try:
        config = read_config(config_path)
    except ConfigError as e:
        raise SystemExit(f"CONFIG ERROR: {e}")

    parser = build_parser()
    args = parser.parse_args()

    # If --pass-id is provided, extract components
    if args.pass_id:
        parts = args.pass_id.split("_", 2)
        if len(parts) >= 3:
            args.date = parts[0]
            args.start_time = parts[1].replace("-", ":")
            args.satellite = parts[2]
        else:
            raise SystemExit(f"Invalid pass_id format: {args.pass_id}")

    # If no arguments provided, show help
    if not args.date and not args.satellite:
        parser.print_help()
        return

    db_path = config["paths"].get("reception_db_file", os.path.expanduser("~/satpi/results/database/reception.db"))
    base_dir = config["paths"].get("base_dir", os.path.expanduser("~/satpi"))
    output_dir_config = config["paths"].get("output_dir", "results/passes")
    reports_dir = os.path.join(base_dir, output_dir_config)

    if args.output_dir:
        reports_dir = args.output_dir

    conn = open_db(db_path)

    try:
        # Single-pass mode
        if args.date and args.start_time and args.satellite:
            # Handle satellite as list (from action="append")
            satellite = args.satellite[0] if isinstance(args.satellite, list) else args.satellite
            # Construct pass_id from components
            pass_id = f"{args.date}_{args.start_time.replace(':', '-')}_{satellite.replace(' ', '_')}"
            header_row, detail_rows = load_single_pass(conn, pass_id)

            if header_row is None:
                raise SystemExit(f"No pass found: {args.date} {args.start_time} {satellite}")

            data = build_single_data(header_row)
            samples = prepare_samples_from_detail_rows(detail_rows)

            if not samples:
                raise SystemExit(f"No samples for pass: {args.date} {args.start_time} {satellite}")

            safe_filename = f"{args.date}_{args.start_time.replace(':', '-')}_{satellite}"

            try:
                skyplot_path = os.path.join(reports_dir, f"skyplot_{safe_filename}.png")
                plot_skyplot(data, samples, skyplot_path)
                print(f"Created: {skyplot_path}")
            except Exception as e:
                print(f"Skyplot failed: {e}")

            try:
                timeseries_path = os.path.join(reports_dir, f"timeseries_{safe_filename}.png")
                plot_timeseries(data, samples, timeseries_path)
                print(f"Created: {timeseries_path}")
            except Exception as e:
                print(f"Timeseries failed: {e}")

            return

        # Combined mode
        satellite_filter = normalize_multi_values(args.satellite) if args.satellite else None
        rows = load_all_samples(conn, satellite_filter)

        if not rows:
            raise SystemExit("No matching data found")

        pass_map = build_pass_map(rows)
        usable = {k: v for k, v in pass_map.items() if len(v) >= 2}

        if not usable:
            raise SystemExit("No usable passes")

        output_path = os.path.join(reports_dir, "skyplot_combined.png")
        draw_combined_plot(usable, output_path, args.highlight_pass_id, args.highlight_label)
        print(f"Created: {output_path}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
