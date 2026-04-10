#!/usr/bin/env python3
# satpi
# Renders graphical views from structured reception JSON data.
# Creates:
# - skyplot of the pass path across the sky
# - time series plot for SNR and BER with sync-state background
# Author: Andreas Horvath
# Project: Autonomous, Config-driven satellite reception pipeline for Raspberry Pi

import argparse
import json
import math
import os
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.dates as mdates


def parse_args():
    parser = argparse.ArgumentParser(description="Render reception plots from satpi reception JSON")
    parser.add_argument("input_json", help="Path to reception.json")
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory for generated PNG files (default: same directory as input JSON)",
    )
    return parser.parse_args()


def load_reception_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


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


def prepare_samples(samples: list[dict]) -> list[dict]:
    prepared = []

    for sample in samples:
        ts = parse_utc(sample["timestamp"])
        viterbi_state = sample.get("viterbi_state", "NOSYNC")
        deframer_state = sample.get("deframer_state", "NOSYNC")
        sync_state = derive_sync_state(viterbi_state, deframer_state)

        prepared.append(
            {
                "timestamp": ts,
                "snr_db": float(sample["snr_db"]),
                "peak_snr_db": float(sample["peak_snr_db"]),
                "ber": float(sample["ber"]),
                "viterbi_state": viterbi_state,
                "deframer_state": deframer_state,
                "sync_state": sync_state,
                "azimuth_deg": float(sample["azimuth_deg"]),
                "elevation_deg": float(sample["elevation_deg"]),
            }
        )

    prepared.sort(key=lambda x: x["timestamp"])
    return prepared

def plot_skyplot(data: dict, samples: list[dict], output_path: str):
    visible_samples = get_visible_samples(samples)

    if len(visible_samples) < 2:
        raise ValueError("Need at least 2 samples with elevation >= 0 for skyplot")

    fig = plt.figure(figsize=(11.69, 8.27))
    ax = fig.add_axes([0.05, 0.10, 0.54, 0.80], projection="polar")

    # Skyplot convention:
    # - North at top
    # - clockwise azimuth
    # - zenith in center
    # - horizon at outer ring
    ax.set_theta_zero_location("N")
    ax.set_theta_direction(-1)
    ax.set_rlim(0, 90)
    ax.set_rticks([0, 10, 20, 30, 40, 50, 60, 70, 80, 90])
    ax.set_yticklabels(["90", "80", "70", "60", "50", "40", "30", "20", "10", "0"])
    ax.set_rlabel_position(225)

    for i in range(len(visible_samples) - 1):
        s1 = visible_samples[i]
        s2 = visible_samples[i + 1]

        az1 = float(s1["azimuth_deg"])
        az2 = float(s2["azimuth_deg"])
        el1 = float(s1["elevation_deg"])
        el2 = float(s2["elevation_deg"])

        az_diff = abs(az2 - az1)
        if az_diff > 180:
            az_diff = 360 - az_diff

        # Break the line if azimuth jumps too much between consecutive points.
        # This avoids drawing a false line across the sky near zenith / azimuth wrap.
        if az_diff > 60:
            continue

        theta = [math.radians(az1), math.radians(az2)]
        radius = [90.0 - el1, 90.0 - el2]

        color = state_color(s1["sync_state"])
        ax.plot(theta, radius, linewidth=2.5, color=color)

    # mark first and last visible points
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

    # optional helper labels so the markers are unmistakable
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

    title = f"{data['pass_id']} Skyplot"
    ax.set_title(title, va="bottom")

    metadata_text = build_metadata_text(data)

    fig.text(
        0.62,
        0.86,
        metadata_text,
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

def get_visible_samples(samples: list[dict]) -> list[dict]:
    return [s for s in samples if s["elevation_deg"] >= 0.0]

def plot_timeseries(data: dict, samples: list[dict], output_path: str):
    if not samples:
        raise ValueError("No samples available")

    times = [s["timestamp"] for s in samples]
    snr = [s["snr_db"] for s in samples]
    ber = [s["ber"] for s in samples]

    fig, ax1 = plt.subplots(figsize=(16, 6))
    fig.subplots_adjust(right=0.72)

    # background sync-state bands
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

    title = f"{data['pass_id']} Reception Time Series"
    ax1.set_title(title)

    metadata_text = build_metadata_text(data)

    fig.text(
        0.75,
        0.87,
        metadata_text,
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

def build_metadata_text(data: dict) -> str:
    reception_setup = data.get("reception_setup", {})

    lines = [
        f"Satellite: {data.get('satellite', '-')}",
        f"Pipeline: {data.get('pipeline', '-')}",
        f"Frequency: {data.get('frequency_hz', '-')} Hz",
        f"Bandwidth: {data.get('bandwidth_hz', '-')} Hz",
        f"Gain: {data.get('gain', '-')}",
        f"Source ID: {data.get('source_id', '-')}",
        f"Bias-T: {data.get('bias_t', '-')}",
        f"Antenna type: {reception_setup.get('antenna_type', '-')}",
        f"Antenna location: {reception_setup.get('antenna_location', '-')}",
        f"Antenna orientation: {reception_setup.get('antenna_orientation', '-')}",
        f"LNA: {reception_setup.get('lna', '-')}",
        f"RF filter: {reception_setup.get('rf_filter', '-')}",
        f"Feedline: {reception_setup.get('feedline', '-')}",
        f"Raspberry Pi: {reception_setup.get('raspberry_pi', '-')}",
        f"Power supply: {reception_setup.get('power_supply', '-')}",
        f"Pass start: {data.get('pass_start', '-')}",
        f"Pass end: {data.get('pass_end', '-')}",
    ]

    additional_info = str(reception_setup.get("additional_info", "")).strip()
    if additional_info and additional_info.lower() != "n/a":
        lines.append(f"Additional info: {additional_info}")

    return "\n".join(lines)

def main():
    args = parse_args()

    input_json = os.path.abspath(args.input_json)
    data = load_reception_json(input_json)
    samples = prepare_samples(data.get("samples", []))

    if not samples:
        raise SystemExit("No samples found in JSON")

    if args.output_dir:
        output_dir = os.path.abspath(args.output_dir)
    else:
       output_dir = os.path.dirname(input_json)

    os.makedirs(output_dir, exist_ok=True)

    pass_id = data["pass_id"]
    skyplot_path = os.path.join(output_dir, f"{pass_id}-skyplot.png")
    timeseries_path = os.path.join(output_dir, f"{pass_id}-timeseries.png")

    plot_skyplot(data, samples, skyplot_path)
    plot_timeseries(data, samples, timeseries_path)

    print(f"Created: {skyplot_path}")
    print(f"Created: {timeseries_path}")


if __name__ == "__main__":
    raise SystemExit(main())
