#!/usr/bin/env python3
# satpi
# Creates reception plots from the satpi SQLite database.
# - If --pass-id is set, it creates skyplot and time-series plots for exactly one pass.
# - Otherwise, it creates a combined overview skyplot across all passes matching the filters.
# Filters are AND-combined across different parameters and OR-combined within repeated parameters.
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
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from load_config import load_config, ConfigError


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


def build_parser(config) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Create reception plots from the satpi SQLite database. "
            "If --pass-id is set, a single-pass skyplot and timechart are generated. "
            "Otherwise a combined skyplot is generated."
        )
    )

    parser.add_argument(
        "--pass-id",
        default=None,
        help="Plot exactly this pass_id as single-pass plots",
    )
    parser.add_argument(
        "--pass-id-list",
        action="append",
        default=None,
        help="Plot multiple pass_ids as one combined skyplot. Repeat option or use comma-separated values",
    )
    parser.add_argument(
        "--highlight-pass-id",
        default=None,
        help="Highlight exactly this pass_id in a combined skyplot",
    )
    parser.add_argument(
        "--highlight-label",
        default="winning pass",
        help="Label text for the highlighted pass in a combined skyplot",
    )

    parser.add_argument(
        "--satellite",
        action="append",
        default=None,
        help="Filter by satellite. Repeat option or use comma-separated values",
    )

    setup_group = parser.add_argument_group("reception setup filters")
    for key in config["reception_setup"].keys():
        option = "--" + key.replace("_", "-")
        setup_group.add_argument(
            option,
            action="append",
            default=None,
            dest=key,
            help=f"Filter by reception setup field {key}",
        )

    return parser


def open_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def build_header_filters(args, setup_keys):
    filters = {}
    satellites = normalize_multi_values(args.satellite)
    if satellites:
        filters["satellite"] = satellites

    for key in setup_keys:
        values = normalize_multi_values(getattr(args, key))
        if values:
            filters[key] = values

    return filters

def build_pass_id_list(args):
    return normalize_multi_values(args.pass_id_list)

def apply_header_filters(sql: str, params: list, filters: dict) -> tuple[str, list]:
    column_map = {
        "satellite": "h.satellite",
        "antenna_type": "s.antenna_type",
        "antenna_location": "s.antenna_location",
        "antenna_orientation": "s.antenna_orientation",
        "lna": "s.lna",
        "rf_filter": "s.rf_filter",
        "feedline": "s.feedline",
        "sdr": "s.sdr",
        "raspberry_pi": "s.raspberry_pi",
        "power_supply": "s.power_supply",
        "additional_info": "s.additional_info",
    }

    for key, values in filters.items():
        column = column_map[key]
        placeholders = ",".join("?" for _ in values)
        sql += f" AND {column} IN ({placeholders})"
        params.extend(values)

    return sql, params


def load_single_pass(conn: sqlite3.Connection, pass_id: str, filters: dict):
    sql = """
    SELECT
        h.pass_id,
        h.setup_id,
        h.source_file,
        h.satellite,
        h.pipeline,
        h.frequency_hz,
        h.bandwidth_hz,
        h.gain,
        h.source_id,
        h.bias_t,
        h.pass_start,
        h.pass_end,
        h.scheduled_start,
        h.scheduled_end,
        h.sample_count,
        h.visible_sample_count,
        h.aos_azimuth_deg,
        h.culmination_azimuth_deg,
        h.los_azimuth_deg,
        h.culmination_elevation_deg,
        h.direction,
        h.first_deframer_sync_delay_seconds,
        h.total_deframer_synced_seconds,
        h.sync_drop_count,
        h.median_snr_synced,
        h.median_ber_synced,
        h.peak_snr_db,
        s.antenna_type,
        s.antenna_location,
        s.antenna_orientation,
        s.lna,
        s.rf_filter,
        s.feedline,
        s.sdr,
        s.raspberry_pi,
        s.power_supply,
        s.additional_info
    FROM pass_header h
    JOIN setup s ON h.setup_id = s.setup_id
    WHERE h.pass_id = ?
    """
    params = [pass_id]
    sql, params = apply_header_filters(sql, params, filters)

    row = conn.execute(sql, params).fetchone()
    if row is None:
        return None, []

    detail_rows = conn.execute(
        """
        SELECT
            pass_id,
            timestamp,
            snr_db,
            peak_snr_db,
            ber,
            viterbi_state,
            deframer_state,
            azimuth_deg,
            elevation_deg
        FROM pass_detail
        WHERE pass_id = ?
        ORDER BY timestamp
        """,
        [pass_id],
    ).fetchall()

    return row, detail_rows


def load_all_samples(conn: sqlite3.Connection, filters: dict):
    sql = """
    SELECT
        h.pass_id,
        h.satellite,
        h.pipeline,
        d.timestamp,
        d.snr_db,
        d.peak_snr_db,
        d.ber,
        d.viterbi_state,
        d.deframer_state,
        d.azimuth_deg,
        d.elevation_deg,
        s.antenna_type,
        s.antenna_location,
        s.antenna_orientation,
        s.lna,
        s.rf_filter,
        s.feedline,
        s.sdr,
        s.raspberry_pi,
        s.power_supply,
        s.additional_info
    FROM pass_detail d
    JOIN pass_header h ON h.pass_id = d.pass_id
    JOIN setup s ON h.setup_id = s.setup_id
    WHERE 1=1
    """
    params = []
    sql, params = apply_header_filters(sql, params, filters)
    sql += " ORDER BY h.satellite, h.pass_id, d.timestamp"

    return conn.execute(sql, params).fetchall()

def load_samples_for_pass_ids(conn: sqlite3.Connection, pass_ids: list[str], filters: dict):
    if not pass_ids:
        return []

    placeholders = ",".join("?" for _ in pass_ids)

    sql = f"""
    SELECT
        h.pass_id,
        h.satellite,
        h.pipeline,
        d.timestamp,
        d.snr_db,
        d.peak_snr_db,
        d.ber,
        d.viterbi_state,
        d.deframer_state,
        d.azimuth_deg,
        d.elevation_deg,
        s.antenna_type,
        s.antenna_location,
        s.antenna_orientation,
        s.lna,
        s.rf_filter,
        s.feedline,
        s.sdr,
        s.raspberry_pi,
        s.power_supply,
        s.additional_info
    FROM pass_detail d
    JOIN pass_header h ON h.pass_id = d.pass_id
    JOIN setup s ON h.setup_id = s.setup_id
    WHERE h.pass_id IN ({placeholders})
    """
    params = list(pass_ids)
    sql, params = apply_header_filters(sql, params, filters)
    sql += " ORDER BY h.satellite, h.pass_id, d.timestamp"

    return conn.execute(sql, params).fetchall()


def prepare_samples_from_detail_rows(rows) -> list[dict]:
    prepared = []

    for row in rows:
        if row["azimuth_deg"] is None or row["elevation_deg"] is None:
            continue

        viterbi_state = row["viterbi_state"] or "NOSYNC"
        deframer_state = row["deframer_state"] or "NOSYNC"
        sync_state = derive_sync_state(viterbi_state, deframer_state)

        prepared.append(
            {
                "timestamp": parse_utc(row["timestamp"]),
                "snr_db": float(row["snr_db"]) if row["snr_db"] is not None else 0.0,
                "peak_snr_db": float(row["peak_snr_db"]) if row["peak_snr_db"] is not None else 0.0,
                "ber": float(row["ber"]) if row["ber"] is not None else 0.0,
                "viterbi_state": viterbi_state,
                "deframer_state": deframer_state,
                "sync_state": sync_state,
                "azimuth_deg": float(row["azimuth_deg"]),
                "elevation_deg": float(row["elevation_deg"]),
            }
        )

    prepared.sort(key=lambda x: x["timestamp"])
    return prepared


def build_single_data(header_row) -> dict:
    return {
        "pass_id": header_row["pass_id"],
        "satellite": header_row["satellite"],
        "pipeline": header_row["pipeline"],
        "frequency_hz": header_row["frequency_hz"],
        "bandwidth_hz": header_row["bandwidth_hz"],
        "gain": header_row["gain"],
        "source_id": header_row["source_id"],
        "bias_t": bool(header_row["bias_t"]),
        "pass_start": header_row["pass_start"],
        "pass_end": header_row["pass_end"],
        "scheduled_start": header_row["scheduled_start"],
        "scheduled_end": header_row["scheduled_end"],
        "reception_setup": {
            "antenna_type": header_row["antenna_type"],
            "antenna_location": header_row["antenna_location"],
            "antenna_orientation": header_row["antenna_orientation"],
            "lna": header_row["lna"],
            "rf_filter": header_row["rf_filter"],
            "feedline": header_row["feedline"],
            "sdr": header_row["sdr"],
            "raspberry_pi": header_row["raspberry_pi"],
            "power_supply": header_row["power_supply"],
            "additional_info": header_row["additional_info"],
        },
    }


def get_visible_samples(samples: list[dict]) -> list[dict]:
    return [s for s in samples if s["elevation_deg"] >= 0.0]


def merge_segments_by_state(samples: list[dict]) -> list[tuple[datetime, datetime, str]]:
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


def format_box_value(value: str) -> str:
    text = str(value).strip()
    if not text:
        return "-"
    return text


def build_single_metadata_text(data: dict) -> str:
    reception_setup = data.get("reception_setup", {})

    lines = [
        f"Satellite: {format_box_value(data.get('satellite', '-'))}",
        f"Pipeline: {format_box_value(data.get('pipeline', '-'))}",
        f"Frequency: {format_box_value(data.get('frequency_hz', '-'))} Hz",
        f"Bandwidth: {format_box_value(data.get('bandwidth_hz', '-'))} Hz",
        f"Gain: {format_box_value(data.get('gain', '-'))}",
        f"Source ID: {format_box_value(data.get('source_id', '-'))}",
        f"Bias-T: {format_box_value(data.get('bias_t', '-'))}",
        f"Antenna type: {format_box_value(reception_setup.get('antenna_type', '-'))}",
        f"Antenna location: {format_box_value(reception_setup.get('antenna_location', '-'))}",
        f"Antenna orientation: {format_box_value(reception_setup.get('antenna_orientation', '-'))}",
        f"LNA: {format_box_value(reception_setup.get('lna', '-'))}",
        f"RF filter: {format_box_value(reception_setup.get('rf_filter', '-'))}",
        f"Feedline: {format_box_value(reception_setup.get('feedline', '-'))}",
        f"SDR: {format_box_value(reception_setup.get('sdr', '-'))}",
        f"Raspberry Pi: {format_box_value(reception_setup.get('raspberry_pi', '-'))}",
        f"Power supply: {format_box_value(reception_setup.get('power_supply', '-'))}",
        f"Additional info: {format_box_value(reception_setup.get('additional_info', '-'))}",
        f"Pass start: {format_box_value(data.get('pass_start', '-'))}",
        f"Pass end: {format_box_value(data.get('pass_end', '-'))}",
    ]

    return "\n".join(lines)


def plot_skyplot(data: dict, samples: list[dict], output_path: str):
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

    start = visible_samples[0]
    end = visible_samples[-1]

    start_theta = math.radians(start["azimuth_deg"])
    start_radius = 90.0 - start["elevation_deg"]
    end_theta = math.radians(end["azimuth_deg"])
    end_radius = 90.0 - end["elevation_deg"]

    ax.scatter(
        [start_theta],
        [start_radius],
        marker="o",
        s=140,
        facecolor="blue",
        edgecolor="white",
        linewidth=1.5,
        zorder=10,
        label="Start",
    )
    ax.scatter(
        [end_theta],
        [end_radius],
        marker="x",
        s=140,
        color="black",
        linewidth=2.5,
        zorder=11,
        label="End",
    )

    ax.annotate(
        "Start",
        xy=(start_theta, start_radius),
        xytext=(8, 8),
        textcoords="offset points",
        fontsize=6,
        color="blue",
        weight="bold",
    )
    ax.annotate(
        "End",
        xy=(end_theta, end_radius),
        xytext=(8, -12),
        textcoords="offset points",
        fontsize=6,
        color="black",
        weight="bold",
    )

    ax.set_title(f"Skyplot {data['pass_id']}", va="bottom")

    fig.text(
        0.62,
        0.86,
        build_single_metadata_text(data),
        va="top",
        ha="left",
        fontsize=6,
        bbox=dict(boxstyle="round", facecolor="white", alpha=0.90),
    )

    legend_handles = [
        plt.Line2D([0], [0], color="red", lw=3, label="NOSYNC"),
        plt.Line2D([0], [0], color="gold", lw=3, label="SYNCING"),
        plt.Line2D([0], [0], color="green", lw=3, label="SYNCED"),
        plt.Line2D([0], [0], marker="o", color="blue", lw=0, label="Start"),
        plt.Line2D([0], [0], marker="x", color="black", lw=0, label="End"),
    ]

    fig.legend(
        handles=legend_handles,
        loc="lower left",
        bbox_to_anchor=(0.62, 0.14),
        fontsize=6,
        frameon=True,
    )

    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def plot_timeseries(data: dict, samples: list[dict], output_path: str):
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
    ax2.tick_params(axis="y", pad=6)

    ax1.set_title(f"Timechart {data['pass_id']}")

    fig.text(
        0.75,
        0.87,
        build_single_metadata_text(data),
        va="top",
        ha="left",
        fontsize=6,
        bbox=dict(boxstyle="round", facecolor="white", alpha=0.85),
    )

    handles = [
        plt.Line2D([0], [0], color="red", lw=6, alpha=0.3, label="NOSYNC"),
        plt.Line2D([0], [0], color="gold", lw=6, alpha=0.3, label="SYNCING"),
        plt.Line2D([0], [0], color="green", lw=6, alpha=0.3, label="SYNCED"),
        plt.Line2D([0], [0], lw=1.8, label="SNR (dB)"),
        plt.Line2D([0], [0], lw=1.2, linestyle="--", label="BER"),
    ]
    ax1.legend(handles=handles, loc="upper left")

    fig.autofmt_xdate()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def build_pass_map(rows):
    passes = defaultdict(list)

    for row in rows:
        az = row["azimuth_deg"]
        el = row["elevation_deg"]

        if az is None or el is None:
            continue

        sync_state = derive_sync_state(row["viterbi_state"], row["deframer_state"])

        passes[row["pass_id"]].append(
            {
                "timestamp": row["timestamp"],
                "satellite": row["satellite"],
                "pipeline": row["pipeline"],
                "azimuth_deg": float(az),
                "elevation_deg": float(el),
                "snr_db": float(row["snr_db"]) if row["snr_db"] is not None else None,
                "peak_snr_db": float(row["peak_snr_db"]) if row["peak_snr_db"] is not None else None,
                "ber": float(row["ber"]) if row["ber"] is not None else None,
                "sync_state": sync_state,
                "antenna_type": row["antenna_type"],
                "antenna_location": row["antenna_location"],
                "antenna_orientation": row["antenna_orientation"],
                "lna": row["lna"],
                "rf_filter": row["rf_filter"],
                "feedline": row["feedline"],
                "sdr": row["sdr"],
                "raspberry_pi": row["raspberry_pi"],
                "power_supply": row["power_supply"],
                "additional_info": row["additional_info"],
            }
        )

    return passes


def build_satellite_arrow_colors(pass_map):
    color_cycle = [
        "blue",
        "magenta",
        "cyan",
        "black",
        "orange",
        "purple",
        "brown",
        "navy",
    ]

    satellites = sorted({samples[0]["satellite"] for samples in pass_map.values() if samples})
    mapping = {}

    for idx, satellite in enumerate(satellites):
        mapping[satellite] = color_cycle[idx % len(color_cycle)]

    return mapping


def ensure_parent_dir(path: str):
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)


def summarize_combined_metadata(pass_map: dict[str, list[dict]]) -> dict[str, str]:
    field_order = [
        ("satellite", "Satellite"),
        ("antenna_type", "Antenna type"),
        ("antenna_location", "Antenna location"),
        ("antenna_orientation", "Antenna orientation"),
        ("lna", "LNA"),
        ("rf_filter", "RF filter"),
        ("feedline", "Feedline"),
        ("sdr", "SDR"),
        ("raspberry_pi", "Raspberry Pi"),
        ("power_supply", "Power supply"),
        ("additional_info", "Additional info"),
    ]

    values_by_field: dict[str, set[str]] = {key: set() for key, _ in field_order}

    for samples in pass_map.values():
        if not samples:
            continue
        first = samples[0]
        for key, _label in field_order:
            raw_value = first.get(key, "")
            text = str(raw_value).strip()
            if text:
                values_by_field[key].add(text)

    result = {}
    for key, _label in field_order:
        values = values_by_field[key]
        if not values:
            result[key] = "-"
        elif len(values) == 1:
            result[key] = next(iter(values))
        else:
            result[key] = "various"

    return result


def build_combined_metadata_text(pass_map: dict[str, list[dict]]) -> str:
    summary = summarize_combined_metadata(pass_map)

    lines = [
        f"Satellite: {summary['satellite']}",
        f"Antenna type: {summary['antenna_type']}",
        f"Antenna location: {summary['antenna_location']}",
        f"Antenna orientation: {summary['antenna_orientation']}",
        f"LNA: {summary['lna']}",
        f"RF filter: {summary['rf_filter']}",
        f"Feedline: {summary['feedline']}",
        f"SDR: {summary['sdr']}",
        f"Raspberry Pi: {summary['raspberry_pi']}",
        f"Power supply: {summary['power_supply']}",
        f"Additional info: {summary['additional_info']}",
    ]

    return "\n".join(lines)


def build_combined_title(pass_map: dict[str, list[dict]]) -> str:
    summary = summarize_combined_metadata(pass_map)
    if summary["satellite"] != "various" and summary["satellite"] != "-":
        return f"Skyplot {summary['satellite']}"
    return "Skyplot"


def build_combined_output_filename(filters: dict) -> str:
    if "satellite" in filters and len(filters["satellite"]) == 1 and len(filters) == 1:
        value = sanitize_filename_component(filters["satellite"][0])
        return f"skyplot_{value}.png"

    if "satellite" in filters and len(filters["satellite"]) == 1 and len(filters) > 1:
        value = sanitize_filename_component(filters["satellite"][0])
        return f"skyplot_{value}_and_others.png"

    non_satellite_keys = [key for key in filters.keys() if key != "satellite"]

    if len(non_satellite_keys) == 1 and len(filters[non_satellite_keys[0]]) == 1 and "satellite" not in filters:
        value = sanitize_filename_component(filters[non_satellite_keys[0]][0])
        return f"skyplot_{value}.png"

    if len(non_satellite_keys) >= 1 and "satellite" not in filters:
        first_key = non_satellite_keys[0]
        first_values = filters[first_key]
        if len(first_values) == 1:
            value = sanitize_filename_component(first_values[0])
            if len(filters) == 1:
                return f"skyplot_{value}.png"
            return f"skyplot_{value}_and_others.png"

    return "skyplot_filtered.png"


def draw_combined_plot(
    pass_map,
    output_path: str,
    highlight_pass_id: str | None = None,
    highlight_label: str = "winning pass",
):
    fig = plt.figure(figsize=(12, 9))
    ax = fig.add_axes([0.06, 0.08, 0.62, 0.84], projection="polar")

    ax.set_theta_zero_location("N")
    ax.set_theta_direction(-1)
    ax.set_rlim(0, 90)
    ax.set_rticks([0, 10, 20, 30, 40, 50, 60, 70, 80, 90])
    ax.set_yticklabels(["90", "80", "70", "60", "50", "40", "30", "20", "10", "0"])
    ax.set_rlabel_position(225)
    ax.set_title(build_combined_title(pass_map), va="bottom")

    satellite_arrow_colors = build_satellite_arrow_colors(pass_map)

    total_segments = 0
    pass_count = 0

    for pass_id, samples in sorted(pass_map.items()):
        if len(samples) < 2:
            continue

        pass_count += 1
        satellite_name = samples[0]["satellite"]
        arrow_color = satellite_arrow_colors[satellite_name]
        is_highlighted = highlight_pass_id is not None and pass_id == highlight_pass_id

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

            ax.plot(
                theta,
                radius,
                color=color,
                linewidth=linewidth,
                alpha=alpha,
                linestyle="-",
                zorder=zorder,
            )
            total_segments += 1

        visible_samples = [s for s in samples if s["elevation_deg"] >= 0.0]
        if len(visible_samples) >= 3:
            start = visible_samples[0]
            next_point = visible_samples[2]

            start_theta = math.radians(start["azimuth_deg"])
            start_radius = 90.0 - start["elevation_deg"]
            next_theta = math.radians(next_point["azimuth_deg"])
            next_radius = 90.0 - next_point["elevation_deg"]

            ax.annotate(
                "",
                xy=(next_theta, next_radius),
                xytext=(start_theta, start_radius),
                arrowprops=dict(
                    arrowstyle="simple",
                    fc=arrow_color,
                    ec=arrow_color,
                    lw=0.0,
                    alpha=0.95,
                    shrinkA=0,
                    shrinkB=0,
                    mutation_scale=14,
                ),
                zorder=80 if not is_highlighted else 90,
            )

        if is_highlighted and visible_samples:
            mid = visible_samples[len(visible_samples) // 2]
            mid_theta = math.radians(mid["azimuth_deg"])
            mid_radius = 90.0 - mid["elevation_deg"]

            ax.annotate(
                highlight_label,
                xy=(mid_theta, mid_radius),
                xytext=(-70, -45),
                textcoords="offset points",
                ha="right",
                va="top",
                fontsize=8,
                fontweight="bold",
                color="black",
                bbox=dict(boxstyle="round,pad=0.25", facecolor="white", alpha=0.9),
                arrowprops=dict(
                    arrowstyle="->",
                    color="black",
                    lw=1.2,
                    shrinkA=0,
                    shrinkB=0,
                ),
                zorder=120,
            )

    sync_legend_handles = [
        plt.Line2D([0], [0], color="red", lw=3, label="NOSYNC"),
        plt.Line2D([0], [0], color="gold", lw=3, label="SYNCING"),
        plt.Line2D([0], [0], color="green", lw=3, label="SYNCED"),
    ]

    satellite_legend_handles = [
        plt.Line2D([0], [0], color=color, lw=0, marker=">", markersize=9, label=satellite)
        for satellite, color in sorted(satellite_arrow_colors.items())
    ]

    legend1 = fig.legend(
        handles=sync_legend_handles,
        loc="lower left",
        bbox_to_anchor=(0.72, 0.22),
        fontsize=8,
        frameon=True,
        title="Sync state",
    )
    fig.add_artist(legend1)

    fig.legend(
        handles=satellite_legend_handles,
        loc="lower left",
        bbox_to_anchor=(0.72, 0.02),
        fontsize=8,
        frameon=True,
        title="Satellite",
    )

    info_lines = [
        f"Passes plotted: {pass_count}",
        f"Segments plotted: {total_segments}",
    ]

    fig.text(
        0.72,
        0.82,
        "\n".join(info_lines),
        va="top",
        ha="left",
        fontsize=9,
        bbox=dict(boxstyle="round", facecolor="white", alpha=0.9),
    )

    fig.text(
        0.72,
        0.68,
        build_combined_metadata_text(pass_map),
        va="top",
        ha="left",
        fontsize=7,
        bbox=dict(boxstyle="round", facecolor="white", alpha=0.9),
    )

    ensure_parent_dir(output_path)
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def main():
    config_path = get_config_path()

    try:
        config = load_config(config_path)
    except ConfigError as e:
        raise SystemExit(f"CONFIG ERROR: {e}")

    parser = build_parser(config)
    args = parser.parse_args()

    db_path = config["paths"]["reception_db_file"]
    reports_dir = os.path.join(config["paths"]["base_dir"], "results", "reports")
    captures_dir = config["paths"]["output_dir"]

    filters = build_header_filters(args, config["reception_setup"].keys())
    pass_id_list = build_pass_id_list(args)

    conn = open_db(db_path)
    try:
        if args.pass_id:
            header_row, detail_rows = load_single_pass(conn, args.pass_id, filters)
            if header_row is None:
                raise SystemExit(f"No matching pass found for pass_id: {args.pass_id}")

            data = build_single_data(header_row)
            samples = prepare_samples_from_detail_rows(detail_rows)
            if not samples:
                raise SystemExit(f"No samples found for pass_id: {args.pass_id}")

            source_file = header_row["source_file"] or ""
            if source_file and os.path.exists(source_file):
                single_output_dir = os.path.dirname(source_file)
            else:
                single_output_dir = os.path.join(captures_dir, header_row["pass_id"])

            os.makedirs(single_output_dir, exist_ok=True)

            skyplot_path = os.path.join(single_output_dir, f"skyplot_{header_row['pass_id']}.png")
            timeseries_path = os.path.join(single_output_dir, f"timeseries_{header_row['pass_id']}.png")

            skyplot_ok = False
            timeseries_ok = False

            try:
                plot_skyplot(data, samples, skyplot_path)
                print(f"Created: {skyplot_path}")
                skyplot_ok = True
            except Exception as e:
                print(f"Skyplot skipped for {header_row['pass_id']}: {e}")

            try:
                plot_timeseries(data, samples, timeseries_path)
                print(f"Created: {timeseries_path}")
                timeseries_ok = True
            except Exception as e:
                print(f"Timeseries skipped for {header_row['pass_id']}: {e}")

            if not skyplot_ok and not timeseries_ok:
                raise SystemExit(f"No plots could be created for pass_id: {args.pass_id}")

            return

        if pass_id_list:
            rows = load_samples_for_pass_ids(conn, pass_id_list, filters)
            if not rows:
                raise SystemExit("No matching rows found for pass_id_list")

            pass_map = build_pass_map(rows)
            usable_passes = {k: v for k, v in pass_map.items() if len(v) >= 2}
            if not usable_passes:
                raise SystemExit("No usable passes after filtering")

            output_filename = "skyplot_grouped_passes.png"
            output_path = os.path.join(reports_dir, output_filename)

            draw_combined_plot(
                usable_passes,
                output_path,
                highlight_pass_id=args.highlight_pass_id,
                highlight_label=args.highlight_label,
            )
            print(f"Created: {output_path}")
            return

        rows = load_all_samples(conn, filters)
        if not rows:
            raise SystemExit("No matching rows found in database")

        pass_map = build_pass_map(rows)
        usable_passes = {k: v for k, v in pass_map.items() if len(v) >= 2}
        if not usable_passes:
            raise SystemExit("No usable passes after filtering")

        output_filename = build_combined_output_filename(filters)
        output_path = os.path.join(reports_dir, output_filename)

        draw_combined_plot(
            usable_passes,
            output_path,
            highlight_pass_id=args.highlight_pass_id,
            highlight_label=args.highlight_label,
        )
        print(f"Created: {output_path}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
