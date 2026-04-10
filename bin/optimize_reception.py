#!/usr/bin/env python3
# satpi
# Analyze recorded reception data from SQLite and recommend better reception setup.
# Current implementation groups by setup_id and reports the best-performing setup group.
# Author: Andreas Horvath
# Project: Autonomous, Config-driven satellite reception pipeline for Raspberry Pi

import argparse
import configparser
import json
import os
import sqlite3
from collections import defaultdict
from dataclasses import dataclass, asdict
from typing import Any

from load_config import load_config, ConfigError


def parse_args():
    parser = argparse.ArgumentParser(description="Optimize satpi reception settings from recorded passes in SQLite")
    parser.add_argument(
        "--config",
        default=None,
        help="Path to config.ini (default: ../config/config.ini relative to this script)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply suggested gain to config.ini if the recommended setup differs only by gain",
    )
    return parser.parse_args()

@dataclass
class PassMetrics:
    path: str
    pass_id: str
    satellite: str
    pipeline: str
    gain: float
    setup_id: int
    antenna_type: str
    antenna_location: str
    antenna_orientation: str
    lna: str
    rf_filter: str
    feedline: str
    raspberry_pi: str
    power_supply: str
    additional_info: str
    max_elevation_deg: float
    aos_azimuth: float | None
    los_azimuth: float | None
    culmination_azimuth: float | None
    culmination_elevation: float | None
    direction: str
    sample_count: int
    first_deframer_sync_delay_seconds: float | None
    total_deframer_synced_seconds: float
    sync_drop_count: int
    median_snr_synced: float | None
    median_ber_synced: float | None
    peak_snr_db: float | None
    score: float | None = None

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
        "same_pass_direction_only": s.getboolean("same_pass_direction_only", fallback=True),
        "max_delta_aos_azimuth": s.getfloat("max_delta_aos_azimuth", fallback=20.0),
        "max_delta_los_azimuth": s.getfloat("max_delta_los_azimuth", fallback=20.0),
        "max_delta_culmination_azimuth": s.getfloat("max_delta_culmination_azimuth", fallback=15.0),
        "max_delta_culmination_elevation": s.getfloat("max_delta_culmination_elevation", fallback=10.0),
        "min_total_passes": s.getint("min_total_passes", fallback=4),
        "weight_deframer_synced_seconds": s.getfloat("weight_deframer_synced_seconds", fallback=1.0),
        "weight_first_deframer_sync_delay": s.getfloat("weight_first_deframer_sync_delay", fallback=-0.4),
        "weight_sync_drop_count": s.getfloat("weight_sync_drop_count", fallback=-0.5),
        "weight_median_snr_synced": s.getfloat("weight_median_snr_synced", fallback=0.3),
        "weight_median_ber_synced": s.getfloat("weight_median_ber_synced", fallback=-0.8),
        "output_dir": s.get("output_dir"),
    }

def open_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def load_metrics_from_db(db_path: str) -> list[PassMetrics]:
    conn = open_db(db_path)
    try:
        rows = conn.execute(
            """
            SELECT
                h.pass_id,
                h.source_file,
                h.satellite,
                h.pipeline,
                h.gain,
                h.setup_id,
                s.antenna_type,
                s.antenna_location,
                s.antenna_orientation,
                s.lna,
                s.rf_filter,
                s.feedline,
                s.raspberry_pi,
                s.power_supply,
                s.additional_info,
                h.max_elevation_deg,
                h.aos_azimuth,
                h.los_azimuth,
                h.culmination_azimuth,
                h.culmination_elevation,
                h.direction,
                h.sample_count,
                h.first_deframer_sync_delay_seconds,
                h.total_deframer_synced_seconds,
                h.sync_drop_count,
                h.median_snr_synced,
                h.median_ber_synced,
                h.peak_snr_db
            FROM pass_header h
            JOIN setup s ON h.setup_id = s.setup_id
            ORDER BY h.pass_start
            """
        ).fetchall()
    finally:
        conn.close()

    metrics_all: list[PassMetrics] = []
    for row in rows:
        max_elevation_deg = 0.0
        if row["max_elevation_deg"] is not None:
            max_elevation_deg = float(row["max_elevation_deg"])

        metrics = PassMetrics(
            path=str(row["source_file"] or ""),
            pass_id=str(row["pass_id"]),
            satellite=str(row["satellite"]),
            pipeline=str(row["pipeline"]),
            gain=float(row["gain"]),
            setup_id=int(row["setup_id"]),
            antenna_type=str(row["antenna_type"] or ""),
            antenna_location=str(row["antenna_location"] or ""),
            antenna_orientation=str(row["antenna_orientation"] or ""),
            lna=str(row["lna"] or ""),
            rf_filter=str(row["rf_filter"] or ""),
            feedline=str(row["feedline"] or ""),
            raspberry_pi=str(row["raspberry_pi"] or ""),
            power_supply=str(row["power_supply"] or ""),
            additional_info=str(row["additional_info"] or ""),
            max_elevation_deg=max_elevation_deg,
            aos_azimuth=(
                float(row["aos_azimuth"])
                if row["aos_azimuth"] is not None
                else None
            ),
            los_azimuth=(
                float(row["los_azimuth"])
                if row["los_azimuth"] is not None
                else None
            ),
            culmination_azimuth=(
                float(row["culmination_azimuth"])
                if row["culmination_azimuth"] is not None
                else None
            ),
            culmination_elevation=(
                float(row["culmination_elevation"])
                if row["culmination_elevation"] is not None
                else None
            ),
            direction=str(row["direction"] or "unknown"),
            sample_count=int(row["sample_count"] or 0),
            first_deframer_sync_delay_seconds=(
                float(row["first_deframer_sync_delay_seconds"])
                if row["first_deframer_sync_delay_seconds"] is not None
                else None
            ),
            total_deframer_synced_seconds=float(row["total_deframer_synced_seconds"] or 0.0),
            sync_drop_count=int(row["sync_drop_count"] or 0),
            median_snr_synced=(
                float(row["median_snr_synced"])
                if row["median_snr_synced"] is not None
                else None
            ),
            median_ber_synced=(
                float(row["median_ber_synced"])
                if row["median_ber_synced"] is not None
                else None
            ),
            peak_snr_db=(
                float(row["peak_snr_db"])
                if row["peak_snr_db"] is not None
                else None
            ),
        )

        metrics_all.append(metrics)

    return metrics_all


def compute_score(m: PassMetrics, settings: dict[str, Any]) -> float:
    first_sync_delay = m.first_deframer_sync_delay_seconds
    median_snr_synced = m.median_snr_synced
    median_ber_synced = m.median_ber_synced

    score = 0.0
    score += settings["weight_deframer_synced_seconds"] * m.total_deframer_synced_seconds
    score += settings["weight_first_deframer_sync_delay"] * (
        first_sync_delay if first_sync_delay is not None else 9999.0
    )
    score += settings["weight_sync_drop_count"] * m.sync_drop_count
    score += settings["weight_median_snr_synced"] * (
        median_snr_synced if median_snr_synced is not None else 0.0
    )
    score += settings["weight_median_ber_synced"] * (
        median_ber_synced if median_ber_synced is not None else 1.0
    )
    return score

def score_metrics_list(metrics_list: list[PassMetrics], settings: dict[str, Any]) -> list[PassMetrics]:
    scored: list[PassMetrics] = []
    for m in metrics_list:
        m.score = compute_score(m, settings)
        scored.append(m)
    return scored

def angular_delta_deg(a: float | None, b: float | None) -> float | None:
    if a is None or b is None:
        return None

    delta = abs(a - b)
    if delta > 180.0:
        delta = 360.0 - delta
    return delta


def passes_are_comparable(a: PassMetrics, b: PassMetrics, settings: dict[str, Any]) -> bool:
    if settings["same_pass_direction_only"] and a.direction != b.direction:
        return False

    aos_delta = angular_delta_deg(a.aos_azimuth, b.aos_azimuth)
    if aos_delta is None or aos_delta > settings["max_delta_aos_azimuth"]:
        return False

    los_delta = angular_delta_deg(a.los_azimuth, b.los_azimuth)
    if los_delta is None or los_delta > settings["max_delta_los_azimuth"]:
        return False

    culmination_azimuth_delta = angular_delta_deg(a.culmination_azimuth, b.culmination_azimuth)
    if (
        culmination_azimuth_delta is None
        or culmination_azimuth_delta > settings["max_delta_culmination_azimuth"]
    ):
        return False

    if a.culmination_elevation is None or b.culmination_elevation is None:
        return False

    if abs(a.culmination_elevation - b.culmination_elevation) > settings["max_delta_culmination_elevation"]:
        return False

    return True

def select_comparable_passes(metrics_list: list[PassMetrics], settings: dict[str, Any]):
    candidates = [
        m for m in metrics_list
        if m.culmination_elevation is not None
        and m.aos_azimuth is not None
        and m.los_azimuth is not None
        and m.culmination_azimuth is not None
    ]

    if not candidates:
        return [], {
            "total_metrics": len(metrics_list),
            "candidate_passes": 0,
            "comparable_passes": 0,
            "reference_pass_id": None,
        }

    reference = max(candidates, key=lambda m: m.culmination_elevation)
    comparable = [m for m in candidates if passes_are_comparable(reference, m, settings)]

    stats = {
        "total_metrics": len(metrics_list),
        "candidate_passes": len(candidates),
        "comparable_passes": len(comparable),
        "reference_pass_id": reference.pass_id,
    }
    return comparable, stats

def fmt(value, digits=2, none_value="-"):
    if value is None:
        return none_value
    return f"{value:.{digits}f}"


def setup_label(m: PassMetrics) -> str:
    parts = [
        f"setup_id={m.setup_id}",
        f"gain={fmt(m.gain, 1)}",
    ]
    if m.antenna_type:
        parts.append(f"antenna={m.antenna_type}")
    if m.antenna_location:
        parts.append(f"location={m.antenna_location}")
    if m.feedline:
        parts.append(f"feedline={m.feedline}")
    if m.raspberry_pi:
        parts.append(f"pi={m.raspberry_pi}")
    if m.power_supply:
        parts.append(f"psu={m.power_supply}")
    return ", ".join(parts)


def group_by_setup(metrics_list: list[PassMetrics]) -> dict[int, list[PassMetrics]]:
    grouped = defaultdict(list)
    for m in metrics_list:
        grouped[m.setup_id].append(m)
    return dict(sorted(grouped.items(), key=lambda kv: kv[0]))


def summarize_setup_group(setup_id: int, items: list[PassMetrics]) -> dict[str, Any]:
    ref = items[0]
    return {
        "setup_id": setup_id,
        "setup_label": setup_label(ref),
        "gain": ref.gain,
        "antenna_type": ref.antenna_type,
        "antenna_location": ref.antenna_location,
        "antenna_orientation": ref.antenna_orientation,
        "lna": ref.lna,
        "rf_filter": ref.rf_filter,
        "feedline": ref.feedline,
        "raspberry_pi": ref.raspberry_pi,
        "power_supply": ref.power_supply,
        "additional_info": ref.additional_info,
        "pass_count": len(items),
        "avg_score": sum(m.score for m in items if m.score is not None) / len(items),
        "avg_total_deframer_synced_seconds": sum(m.total_deframer_synced_seconds for m in items) / len(items),
        "avg_first_deframer_sync_delay_seconds": sum(
            (m.first_deframer_sync_delay_seconds if m.first_deframer_sync_delay_seconds is not None else 9999.0)
            for m in items
        ) / len(items),
        "avg_sync_drop_count": sum(m.sync_drop_count for m in items) / len(items),
        "avg_median_snr_synced": sum((m.median_snr_synced or 0.0) for m in items) / len(items),
        "avg_median_ber_synced": sum(
            (m.median_ber_synced if m.median_ber_synced is not None else 1.0) for m in items
        ) / len(items),
    }


def choose_recommended_setup(
    grouped: dict[int, list[PassMetrics]],
    settings: dict[str, Any],
) -> tuple[int | None, list[dict[str, Any]]]:
    summaries = []
    for setup_id, items in grouped.items():
        summaries.append(summarize_setup_group(setup_id, items))

    if not summaries:
        return None, summaries

    summaries.sort(key=lambda x: x["avg_score"], reverse=True)
    return summaries[0]["setup_id"], summaries

def detect_current_gain(config: dict[str, Any]) -> float:
    return float(config["hardware"]["gain"])


def current_setup_from_config(config: dict[str, Any]) -> dict[str, Any]:
    s = config["reception_setup"]
    return {
        "gain": float(config["hardware"]["gain"]),
        "antenna_type": str(s.get("antenna_type", "")),
        "antenna_location": str(s.get("antenna_location", "")),
        "antenna_orientation": str(s.get("antenna_orientation", "")),
        "lna": str(s.get("lna", "")),
        "rf_filter": str(s.get("rf_filter", "")),
        "feedline": str(s.get("feedline", "")),
        "raspberry_pi": str(s.get("raspberry_pi", "")),
        "power_supply": str(s.get("power_supply", "")),
        "additional_info": str(s.get("additional_info", "")),
    }


def summary_matches_current_setup(summary: dict[str, Any], current_setup: dict[str, Any]) -> bool:
    keys = [
        "gain",
        "antenna_type",
        "antenna_location",
        "antenna_orientation",
        "lna",
        "rf_filter",
        "feedline",
        "raspberry_pi",
        "power_supply",
        "additional_info",
    ]
    for key in keys:
        if summary.get(key) != current_setup.get(key):
            return False
    return True


def write_report_txt(
    output_path: str,
    current_gain: float,
    current_setup: dict[str, Any],
    recommended_setup_id: int | None,
    comparable_metrics: list[PassMetrics],
    summaries: list[dict[str, Any]],
):
    lines = []
    lines.append("satpi optimize_reception report")
    lines.append("==============================")
    lines.append("")
    lines.append(f"Current gain: {fmt(current_gain, 1)}")
    lines.append(f"Comparable passes analyzed: {len(comparable_metrics)}")
    lines.append("")

    best = summaries[0] if summaries else None
    current_summary = None
    for s in summaries:
        if summary_matches_current_setup(s, current_setup):
            current_summary = s
            break

    if best:
        lines.append("Best setup group")
        lines.append("----------------")
        lines.append(f"Setup ID: {best['setup_id']}")
        lines.append(f"Setup: {best['setup_label']}")
        lines.append(f"Gain: {fmt(best['gain'], 1)}")
        lines.append(f"Pass count: {best['pass_count']}")
        lines.append(f"Average score: {fmt(best['avg_score'], 2)}")
        lines.append(f"Average synced seconds: {fmt(best['avg_total_deframer_synced_seconds'], 1)}")
        lines.append(f"Average first sync delay: {fmt(best['avg_first_deframer_sync_delay_seconds'], 1)}")
        lines.append(f"Average sync drops: {fmt(best['avg_sync_drop_count'], 2)}")
        lines.append(f"Average median SNR: {fmt(best['avg_median_snr_synced'], 2)}")
        lines.append(f"Average median BER: {fmt(best['avg_median_ber_synced'], 4)}")
        lines.append("")

    lines.append("Setup group summary")
    lines.append("-------------------")
    for s in summaries:
        marker = "  "
        if recommended_setup_id is not None and s["setup_id"] == recommended_setup_id:
            marker = "* "
        lines.append(
            f"{marker}setup_id={s['setup_id']}, "
            f"gain={fmt(s['gain'], 1)}, "
            f"setup='{s['setup_label']}', "
            f"passes={s['pass_count']}, "
            f"avg_score={fmt(s['avg_score'], 2)}, "
            f"avg_synced_seconds={fmt(s['avg_total_deframer_synced_seconds'], 1)}, "
            f"avg_first_sync_delay={fmt(s['avg_first_deframer_sync_delay_seconds'], 1)}, "
            f"avg_sync_drops={fmt(s['avg_sync_drop_count'], 2)}, "
            f"avg_median_snr={fmt(s['avg_median_snr_synced'], 2)}, "
            f"avg_median_ber={fmt(s['avg_median_ber_synced'], 4)}"
        )

    lines.append("")

    if recommended_setup_id is not None and best is not None:
        lines.append("Recommendation rationale")
        lines.append("------------------------")

        if current_summary is None:
            lines.append(
                f"Recommended setup_id {recommended_setup_id} has the best score among the comparable setup groups."
            )
        elif best["setup_id"] == current_summary["setup_id"]:
            lines.append("No setup change is recommended. The current setup already performs best.")
        else:
            lines.append(f"Recommended setup ID: {recommended_setup_id}")
            lines.append(f"Recommended setup: {best['setup_label']}")
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
            f"setup_id={m.setup_id}, "
            f"gain={fmt(m.gain, 1)}, "
            f"max_el={fmt(m.max_elevation_deg, 1)}, "
            f"score={fmt(m.score, 2)}, "
            f"synced_seconds={fmt(m.total_deframer_synced_seconds, 1)}, "
            f"first_sync_delay={fmt(m.first_deframer_sync_delay_seconds, 1)}, "
            f"sync_drops={m.sync_drop_count}, "
            f"median_snr={fmt(m.median_snr_synced, 2)}, "
            f"median_ber={fmt(m.median_ber_synced, 4)}, "
            f"setup='{setup_label(m)}'"
        )

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def write_report_json(
    output_path: str,
    current_gain: float,
    current_setup: dict[str, Any],
    recommended_setup_id: int | None,
    comparable_metrics: list[PassMetrics],
    summaries: list[dict[str, Any]],
):
    payload = {
        "current_gain": current_gain,
        "current_setup": current_setup,
        "recommended_setup_id": recommended_setup_id,
        "comparable_pass_count": len(comparable_metrics),
        "setup_summaries": summaries,
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

    if not config["reception_db"]["enabled"]:
        print("[optimize_reception] reception_db disabled in config")
        return 1

    db_path = config["reception_db"]["db_path"]
    if not os.path.exists(db_path):
        print(f"[optimize_reception] database not found: {db_path}")
        return 1

    metrics_all = load_metrics_from_db(db_path)
    metrics_all = score_metrics_list(metrics_all, settings)

    if not metrics_all:
        print("[optimize_reception] no usable pass metrics found in database")
        return 1

    comparable_metrics, selection_stats = select_comparable_passes(metrics_all, settings)

    print(f"[optimize_reception] total analyzed metrics: {selection_stats['total_metrics']}")
    print(f"[optimize_reception] candidate passes: {selection_stats['candidate_passes']}")
    print(f"[optimize_reception] comparable passes: {selection_stats['comparable_passes']}")
    print(f"[optimize_reception] reference pass: {selection_stats['reference_pass_id']}")

    if len(comparable_metrics) < settings["min_total_passes"]:
        print(
            f"[optimize_reception] not enough comparable passes: "
            f"{len(comparable_metrics)} < {settings['min_total_passes']}"
        )
        return 1

    grouped = group_by_setup(comparable_metrics)
    current_gain = detect_current_gain(config)
    current_setup = current_setup_from_config(config)
    recommended_setup_id, summaries = choose_recommended_setup(grouped, settings)

    output_dir = settings["output_dir"]
    os.makedirs(output_dir, exist_ok=True)

    txt_report = os.path.join(output_dir, "optimization-report.txt")
    json_report = os.path.join(output_dir, "optimization-report.json")

    write_report_txt(
        txt_report,
        current_gain,
        current_setup,
        recommended_setup_id,
        comparable_metrics,
        summaries,
    )
    write_report_json(
        json_report,
        current_gain,
        current_setup,
        recommended_setup_id,
        comparable_metrics,
        summaries,
    )

    print(f"[optimize_reception] wrote: {txt_report}")
    print(f"[optimize_reception] wrote: {json_report}")

    best_summary = summaries[0] if summaries else None
    should_apply = args.apply or settings["apply_changes"]

    # conservative apply logic:
    # only apply automatically if the recommended setup differs from the current one by gain only
    if should_apply and best_summary is not None:
        same_non_gain = (
            best_summary["antenna_type"] == current_setup["antenna_type"]
            and best_summary["antenna_location"] == current_setup["antenna_location"]
            and best_summary["antenna_orientation"] == current_setup["antenna_orientation"]
            and best_summary["lna"] == current_setup["lna"]
            and best_summary["rf_filter"] == current_setup["rf_filter"]
            and best_summary["feedline"] == current_setup["feedline"]
            and best_summary["raspberry_pi"] == current_setup["raspberry_pi"]
            and best_summary["power_supply"] == current_setup["power_supply"]
            and best_summary["additional_info"] == current_setup["additional_info"]
        )

        if same_non_gain and best_summary["gain"] != current_gain:
            backup_path = backup_config(config_path)
            print(f"[optimize_reception] backup written: {backup_path}")
            apply_gain_to_config(config_path, float(best_summary["gain"]))
            print(f"[optimize_reception] applied new gain to config.ini: {best_summary['gain']}")
        elif same_non_gain:
            print("[optimize_reception] current setup already matches recommended setup")
        else:
            print("[optimize_reception] recommended setup differs in more than gain; no automatic config change applied")

    if recommended_setup_id is None:
        print("[optimize_reception] no setup recommendation possible")
    else:
        print(f"[optimize_reception] current gain: {current_gain}")
        print(f"[optimize_reception] recommended setup_id: {recommended_setup_id}")
        if best_summary is not None:
            print(f"[optimize_reception] recommended setup: {best_summary['setup_label']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
