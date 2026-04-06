#!/usr/bin/env python3
# satpi
# Analyze recorded reception JSON files and recommend better gain settings.
# Version 1: gain-only optimization.
# Author: Andreas Horvath
# Project: Autonomous, Config-driven satellite reception pipeline for Raspberry Pi

import argparse
import configparser
import json
import math
import os
from collections import defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any

from load_config import load_config, ConfigError


def parse_args():
    parser = argparse.ArgumentParser(description="Optimize satpi reception settings from recorded passes")
    parser.add_argument(
        "--config",
        default=None,
        help="Path to config.ini (default: ../config/config.ini relative to this script)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply suggested gain to config.ini",
    )
    return parser.parse_args()


def parse_utc(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def derive_sync_state(viterbi_state: str, deframer_state: str) -> str:
    if deframer_state == "SYNCED":
        return "SYNCED"
    if viterbi_state == "SYNCED":
        return "SYNCING"
    return "NOSYNC"


@dataclass
class PassMetrics:
    path: str
    pass_id: str
    satellite: str
    pipeline: str
    gain: float
    max_elevation_deg: float
    direction: str
    sample_count: int
    first_deframer_sync_delay_seconds: float | None
    total_deframer_synced_seconds: float
    sync_drop_count: int
    median_snr_synced: float | None
    median_ber_synced: float | None
    peak_snr_db: float | None
    score: float | None = None


def load_reception_json(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_config_path(cli_value: str | None) -> str:
    if cli_value:
        return os.path.abspath(cli_value)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_dir, "config", "config.ini")


def load_optimizer_settings(config_path: str) -> dict[str, Any]:
    p = configparser.ConfigParser()
    p.read(config_path, encoding="utf-8")

    if not p.has_section("optimize_reception"):
        raise ConfigError("Missing [optimize_reception] section in config.ini")

    s = p["optimize_reception"]
    return {
        "enabled": s.getboolean("enabled", fallback=True),
        "apply_changes": s.getboolean("apply_changes", fallback=False),
        "write_suggested_config": s.getboolean("write_suggested_config", fallback=True),
        "satellite": s.get("satellite"),
        "pipeline": s.get("pipeline"),
        "min_max_elevation_deg": s.getfloat("min_max_elevation_deg", fallback=30.0),
        "max_max_elevation_delta_deg": s.getfloat("max_max_elevation_delta_deg", fallback=10.0),
        "same_pass_direction_only": s.getboolean("same_pass_direction_only", fallback=True),
        "evaluation_min_elevation_deg": s.getfloat("evaluation_min_elevation_deg", fallback=10.0),
        "evaluation_max_elevation_deg": s.getfloat("evaluation_max_elevation_deg", fallback=85.0),
        "min_passes_per_gain": s.getint("min_passes_per_gain", fallback=2),
        "min_total_passes": s.getint("min_total_passes", fallback=4),
        "weight_deframer_synced_seconds": s.getfloat("weight_deframer_synced_seconds", fallback=1.0),
        "weight_first_deframer_sync_delay": s.getfloat("weight_first_deframer_sync_delay", fallback=-0.4),
        "weight_sync_drop_count": s.getfloat("weight_sync_drop_count", fallback=-0.5),
        "weight_median_snr_synced": s.getfloat("weight_median_snr_synced", fallback=0.3),
        "weight_median_ber_synced": s.getfloat("weight_median_ber_synced", fallback=-0.8),
        "output_dir": s.get("output_dir"),
    }


def list_reception_json_files(base_dir: str) -> list[str]:
    passes_dir = os.path.join(base_dir, "results", "passes")
    return sorted(str(p) for p in Path(passes_dir).glob("*-reception.json"))


def classify_direction(samples: list[dict[str, Any]]) -> str:
    if len(samples) < 2:
        return "unknown"
    first_az = float(samples[0]["azimuth_deg"])
    last_az = float(samples[-1]["azimuth_deg"])
    return "increasing_azimuth" if last_az >= first_az else "decreasing_azimuth"


def filter_samples_by_elevation(samples: list[dict[str, Any]], min_el: float, max_el: float) -> list[dict[str, Any]]:
    return [
        s for s in samples
        if min_el <= float(s["elevation_deg"]) <= max_el
    ]


def compute_pass_metrics(data: dict[str, Any], settings: dict[str, Any]) -> PassMetrics | None:
    samples = data.get("samples", [])
    if not samples:
        return None

    samples = sorted(samples, key=lambda s: parse_utc(s["timestamp"]))
    eval_samples = filter_samples_by_elevation(
        samples,
        settings["evaluation_min_elevation_deg"],
        settings["evaluation_max_elevation_deg"],
    )

    if not eval_samples:
        return None

    max_elevation_deg = max(float(s["elevation_deg"]) for s in samples)
    if max_elevation_deg < settings["min_max_elevation_deg"]:
        return None

    direction = classify_direction(samples)

    first_ts = parse_utc(eval_samples[0]["timestamp"])
    first_sync_delay = None
    total_deframer_synced_seconds = 0.0
    sync_drop_count = 0
    prev_sync = False
    prev_ts = None

    snr_synced = []
    ber_synced = []
    peak_snr = None

    for s in eval_samples:
        ts = parse_utc(s["timestamp"])
        snr = float(s["snr_db"])
        ber = float(s["ber"])
        viterbi_state = s.get("viterbi_state", "NOSYNC")
        deframer_state = s.get("deframer_state", "NOSYNC")
        state = derive_sync_state(viterbi_state, deframer_state)

        peak_snr = snr if peak_snr is None else max(peak_snr, snr)

        is_synced = (state == "SYNCED")

        if is_synced and first_sync_delay is None:
            first_sync_delay = (ts - first_ts).total_seconds()

        if is_synced:
            snr_synced.append(snr)
            ber_synced.append(ber)

        if prev_ts is not None and prev_sync:
            dt = (ts - prev_ts).total_seconds()
            if dt > 0:
                total_deframer_synced_seconds += dt

        if prev_sync and not is_synced:
            sync_drop_count += 1

        prev_sync = is_synced
        prev_ts = ts

    gain = float(data["gain"])

    metrics = PassMetrics(
        path=data.get("_source_path", ""),
        pass_id=data["pass_id"],
        satellite=data["satellite"],
        pipeline=data["pipeline"],
        gain=gain,
        max_elevation_deg=max_elevation_deg,
        direction=direction,
        sample_count=len(eval_samples),
        first_deframer_sync_delay_seconds=first_sync_delay,
        total_deframer_synced_seconds=total_deframer_synced_seconds,
        sync_drop_count=sync_drop_count,
        median_snr_synced=median(snr_synced) if snr_synced else None,
        median_ber_synced=median(ber_synced) if ber_synced else None,
        peak_snr_db=peak_snr,
    )

    metrics.score = compute_score(metrics, settings)
    return metrics


def compute_score(m: PassMetrics, settings: dict[str, Any]) -> float:
    first_sync_delay = m.first_deframer_sync_delay_seconds
    median_snr_synced = m.median_snr_synced
    median_ber_synced = m.median_ber_synced

    score = 0.0
    score += settings["weight_deframer_synced_seconds"] * m.total_deframer_synced_seconds
    score += settings["weight_first_deframer_sync_delay"] * (first_sync_delay if first_sync_delay is not None else 9999.0)
    score += settings["weight_sync_drop_count"] * m.sync_drop_count
    score += settings["weight_median_snr_synced"] * (median_snr_synced if median_snr_synced is not None else 0.0)
    score += settings["weight_median_ber_synced"] * (median_ber_synced if median_ber_synced is not None else 1.0)
    return score


def passes_are_comparable(a: PassMetrics, b: PassMetrics, settings: dict[str, Any]) -> bool:
    if a.satellite != b.satellite:
        return False
    if a.pipeline != b.pipeline:
        return False
    if settings["same_pass_direction_only"] and a.direction != b.direction:
        return False
    if abs(a.max_elevation_deg - b.max_elevation_deg) > settings["max_max_elevation_delta_deg"]:
        return False
    return True


def select_comparable_passes(metrics_list: list[PassMetrics], settings: dict[str, Any]):
    satellite_pipeline_matches = [
        m for m in metrics_list
        if m.satellite == settings["satellite"] and m.pipeline == settings["pipeline"]
    ]

    if not satellite_pipeline_matches:
        return [], {
            "total_metrics": len(metrics_list),
            "satellite_pipeline_matches": 0,
            "comparable_passes": 0,
            "reference_pass_id": None,
        }

    reference = max(satellite_pipeline_matches, key=lambda m: m.max_elevation_deg)
    comparable = [m for m in satellite_pipeline_matches if passes_are_comparable(reference, m, settings)]

    stats = {
        "total_metrics": len(metrics_list),
        "satellite_pipeline_matches": len(satellite_pipeline_matches),
        "comparable_passes": len(comparable),
        "reference_pass_id": reference.pass_id,
    }
    return comparable, stats

def group_by_gain(metrics_list: list[PassMetrics]) -> dict[float, list[PassMetrics]]:
    grouped = defaultdict(list)
    for m in metrics_list:
        grouped[m.gain].append(m)
    return dict(sorted(grouped.items(), key=lambda kv: kv[0]))


def summarize_gain_group(gain: float, items: list[PassMetrics]) -> dict[str, Any]:
    return {
        "gain": gain,
        "pass_count": len(items),
        "avg_score": sum(m.score for m in items if m.score is not None) / len(items),
        "avg_total_deframer_synced_seconds": sum(m.total_deframer_synced_seconds for m in items) / len(items),
        "avg_first_deframer_sync_delay_seconds": sum(
            (m.first_deframer_sync_delay_seconds if m.first_deframer_sync_delay_seconds is not None else 9999.0)
            for m in items
        ) / len(items),
        "avg_sync_drop_count": sum(m.sync_drop_count for m in items) / len(items),
        "avg_median_snr_synced": sum((m.median_snr_synced or 0.0) for m in items) / len(items),
        "avg_median_ber_synced": sum((m.median_ber_synced if m.median_ber_synced is not None else 1.0) for m in items) / len(items),
    }


def choose_recommended_gain(grouped: dict[float, list[PassMetrics]], settings: dict[str, Any]) -> tuple[float | None, list[dict[str, Any]]]:
    summaries = []
    for gain, items in grouped.items():
        if len(items) < settings["min_passes_per_gain"]:
            continue
        summaries.append(summarize_gain_group(gain, items))

    if not summaries:
        return None, summaries

    summaries.sort(key=lambda x: x["avg_score"], reverse=True)
    return summaries[0]["gain"], summaries


def detect_current_gain(config: dict[str, Any]) -> float:
    return float(config["hardware"]["gain"])

def fmt(value, digits=2, none_value="-"):
    if value is None:
        return none_value
    return f"{value:.{digits}f}"

def write_report_txt(
    output_path: str,
    current_gain: float,
    recommended_gain: float | None,
    comparable_metrics: list[PassMetrics],
    summaries: list[dict[str, Any]],
):
    lines = []
    lines.append("satpi optimize_reception report")
    lines.append("==============================")
    lines.append("")
    lines.append(f"Current gain: {fmt(current_gain, 1)}")
    lines.append(f"Recommended gain: {fmt(recommended_gain, 1) if recommended_gain is not None else 'no recommendation'}")
    lines.append(f"Comparable passes analyzed: {len(comparable_metrics)}")
    lines.append("")

    best = summaries[0] if summaries else None
    current_summary = None
    for s in summaries:
        if abs(s["gain"] - current_gain) < 1e-9:
            current_summary = s
            break

    if best:
        lines.append("Best gain group")
        lines.append("---------------")
        lines.append(f"Gain: {fmt(best['gain'], 1)}")
        lines.append(f"Pass count: {best['pass_count']}")
        lines.append(f"Average score: {fmt(best['avg_score'], 2)}")
        lines.append(f"Average synced seconds: {fmt(best['avg_total_deframer_synced_seconds'], 1)}")
        lines.append(f"Average first sync delay: {fmt(best['avg_first_deframer_sync_delay_seconds'], 1)}")
        lines.append(f"Average sync drops: {fmt(best['avg_sync_drop_count'], 2)}")
        lines.append(f"Average median SNR: {fmt(best['avg_median_snr_synced'], 2)}")
        lines.append(f"Average median BER: {fmt(best['avg_median_ber_synced'], 4)}")
        lines.append("")

    lines.append("Gain group summary")
    lines.append("------------------")
    for s in summaries:
        marker = "  "
        if recommended_gain is not None and abs(s["gain"] - recommended_gain) < 1e-9:
            marker = "* "
        lines.append(
            f"{marker}gain={fmt(s['gain'], 1)}: "
            f"passes={s['pass_count']}, "
            f"avg_score={fmt(s['avg_score'], 2)}, "
            f"avg_synced_seconds={fmt(s['avg_total_deframer_synced_seconds'], 1)}, "
            f"avg_first_sync_delay={fmt(s['avg_first_deframer_sync_delay_seconds'], 1)}, "
            f"avg_sync_drops={fmt(s['avg_sync_drop_count'], 2)}, "
            f"avg_median_snr={fmt(s['avg_median_snr_synced'], 2)}, "
            f"avg_median_ber={fmt(s['avg_median_ber_synced'], 4)}"
        )

    lines.append("")

    if recommended_gain is not None and best is not None:
        lines.append("Recommendation rationale")
        lines.append("------------------------")

        if current_summary is None:
            lines.append(
                f"Recommended gain {fmt(recommended_gain, 1)} has the best score among the comparable pass groups."
            )
        elif abs(recommended_gain - current_gain) < 1e-9:
            lines.append(
                f"No gain change is recommended. The current gain {fmt(current_gain, 1)} already performs best."
            )
        else:
            lines.append(
                f"Change gain from {fmt(current_gain, 1)} to {fmt(recommended_gain, 1)}."
            )
            lines.append(
                f"The recommended group shows longer sync time "
                f"({fmt(best['avg_total_deframer_synced_seconds'], 1)} s vs {fmt(current_summary['avg_total_deframer_synced_seconds'], 1)} s), "
                f"earlier first sync "
                f"({fmt(best['avg_first_deframer_sync_delay_seconds'], 1)} s vs {fmt(current_summary['avg_first_deframer_sync_delay_seconds'], 1)} s), "
                f"better median SNR "
                f"({fmt(best['avg_median_snr_synced'], 2)} vs {fmt(current_summary['avg_median_snr_synced'], 2)}), "
                f"and lower median BER "
                f"({fmt(best['avg_median_ber_synced'], 4)} vs {fmt(current_summary['avg_median_ber_synced'], 4)})."
            )

        lines.append("")

    lines.append("Comparable passes")
    lines.append("-----------------")
    for m in comparable_metrics:
        lines.append(
            f"- {m.pass_id}: "
            f"gain={fmt(m.gain, 1)}, "
            f"max_el={fmt(m.max_elevation_deg, 1)}, "
            f"score={fmt(m.score, 2)}, "
            f"synced_seconds={fmt(m.total_deframer_synced_seconds, 1)}, "
            f"first_sync_delay={fmt(m.first_deframer_sync_delay_seconds, 1)}, "
            f"sync_drops={m.sync_drop_count}, "
            f"median_snr={fmt(m.median_snr_synced, 2)}, "
            f"median_ber={fmt(m.median_ber_synced, 4)}"
        )

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

def write_report_json(output_path: str, current_gain: float, recommended_gain: float | None, comparable_metrics: list[PassMetrics], summaries: list[dict[str, Any]]):
    payload = {
        "current_gain": current_gain,
        "recommended_gain": recommended_gain,
        "comparable_pass_count": len(comparable_metrics),
        "gain_summaries": summaries,
        "passes": [asdict(m) for m in comparable_metrics],
    }
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
        f.write("\n")


def write_suggested_config(config_path: str, output_path: str, recommended_gain: float):
    p = configparser.ConfigParser()
    p.read(config_path, encoding="utf-8")

    if not p.has_section("hardware"):
        raise ConfigError("Missing [hardware] section in config.ini")

    p["hardware"]["gain"] = str(recommended_gain)

    with open(output_path, "w", encoding="utf-8") as f:
        p.write(f)


def apply_gain_to_config(config_path: str, recommended_gain: float):
    p = configparser.ConfigParser()
    p.read(config_path, encoding="utf-8")

    if not p.has_section("hardware"):
        raise ConfigError("Missing [hardware] section in config.ini")

    p["hardware"]["gain"] = str(recommended_gain)

    with open(config_path, "w", encoding="utf-8") as f:
        p.write(f)

def backup_config(config_path: str) -> str:
    backup_path = config_path + ".bak"
    with open(config_path, "r", encoding="utf-8") as src, open(backup_path, "w", encoding="utf-8") as dst:
        dst.write(src.read())
    return backup_path

def main():
    args = parse_args()
    config_path = get_config_path(args.config)

    try:
        config = load_config(config_path)
        settings = load_optimizer_settings(config_path)
    except ConfigError as e:
        print(f"[optimize_reception] CONFIG ERROR: {e}")
        return 1

    if not settings["enabled"]:
        print("[optimize_reception] disabled in config")
        return 0

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_files = list_reception_json_files(base_dir)

    if not input_files:
        print("[optimize_reception] no reception JSON files found")
        return 1

    metrics_all = []
    for path in input_files:
        data = load_reception_json(path)
        data["_source_path"] = path
        metrics = compute_pass_metrics(data, settings)
        if metrics is not None:
            metrics_all.append(metrics)

    comparable_metrics, selection_stats = select_comparable_passes(metrics_all, settings)

    print(f"[optimize_reception] total analyzed metrics: {selection_stats['total_metrics']}")
    print(f"[optimize_reception] satellite/pipeline matches: {selection_stats['satellite_pipeline_matches']}")
    print(f"[optimize_reception] comparable passes: {selection_stats['comparable_passes']}")
    print(f"[optimize_reception] reference pass: {selection_stats['reference_pass_id']}")

    if len(comparable_metrics) < settings["min_total_passes"]:
        print(
            f"[optimize_reception] not enough comparable passes: "
            f"{len(comparable_metrics)} < {settings['min_total_passes']}"
        )
        return 1

    grouped = group_by_gain(comparable_metrics)
    current_gain = detect_current_gain(config)
    recommended_gain, summaries = choose_recommended_gain(grouped, settings)

    output_dir = settings["output_dir"]
    os.makedirs(output_dir, exist_ok=True)

    txt_report = os.path.join(output_dir, "optimization-report.txt")
    json_report = os.path.join(output_dir, "optimization-report.json")

    write_report_txt(txt_report, current_gain, recommended_gain, comparable_metrics, summaries)
    write_report_json(json_report, current_gain, recommended_gain, comparable_metrics, summaries)

    print(f"[optimize_reception] wrote: {txt_report}")
    print(f"[optimize_reception] wrote: {json_report}")

    if recommended_gain is not None and settings["write_suggested_config"]:
        suggested_config = os.path.join(output_dir, "config.suggested.ini")
        write_suggested_config(config_path, suggested_config, recommended_gain)
        print(f"[optimize_reception] wrote: {suggested_config}")

    should_apply = args.apply or settings["apply_changes"]

    if should_apply and recommended_gain is not None:
        backup_path = backup_config(config_path)
        print(f"[optimize_reception] backup written: {backup_path}")
        apply_gain_to_config(config_path, recommended_gain)
        print(f"[optimize_reception] applied new gain to config.ini: {recommended_gain}")

    if recommended_gain is None:
        print("[optimize_reception] no gain recommendation possible")
    else:
        print(f"[optimize_reception] current gain: {current_gain}")
        print(f"[optimize_reception] recommended gain: {recommended_gain}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
