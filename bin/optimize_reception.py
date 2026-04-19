#!/usr/bin/env python3
# satpi
# Groups geometrically similar passes, compares reception setups within each group,
# and writes a landscape PDF + JSON report with real skyplots per group.
# Author: Andreas Horvath
# Project: Autonomous, Config-driven satellite reception pipeline for Raspberry Pi

import argparse
import configparser
import json
import os
import shutil
import sqlite3
import subprocess
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    Image as RLImage,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from plot_receptions import plot_skyplot
from load_config import ConfigError, load_config


def parse_args():
    parser = argparse.ArgumentParser(
        description="Build similar-pass groups from reception.db and compare setup performance."
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Path to config.ini (default: ../config/config.ini relative to this script)",
    )
    parser.add_argument(
        "--min-passes-per-group",
        type=int,
        default=2,
        help="Minimum number of passes required for a group to be considered evaluable",
    )
    parser.add_argument(
        "--min-setups-per-group",
        type=int,
        default=2,
        help="Minimum number of different setups required for a group to be considered evaluable",
    )
    parser.add_argument(
        "--json-only",
        action="store_true",
        help="Write only JSON report, skip PDF generation",
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
    aos_azimuth_deg: float | None
    culmination_azimuth_deg: float | None
    los_azimuth_deg: float | None
    culmination_elevation_deg: float | None
    direction: str
    sample_count: int
    first_deframer_sync_delay_seconds: float | None
    total_deframer_synced_seconds: float
    sync_drop_count: int
    median_snr_synced: float | None
    median_ber_synced: float | None
    peak_snr_db: float | None
    score: int | None = None


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
    band_limits = [
        s.getfloat("elevation_band_1_max", fallback=20.0),
        s.getfloat("elevation_band_2_max", fallback=35.0),
        s.getfloat("elevation_band_3_max", fallback=50.0),
        s.getfloat("elevation_band_4_max", fallback=65.0),
        s.getfloat("elevation_band_5_max", fallback=80.0),
    ]
    band_limits = sorted(band_limits)

    return {
        "enabled": s.getboolean("enabled", fallback=True),
        "max_delta_aos_azimuth": s.getfloat("max_delta_aos_azimuth", fallback=20.0),
        "max_delta_los_azimuth": s.getfloat("max_delta_los_azimuth", fallback=20.0),
        "max_delta_culmination_azimuth": s.getfloat("max_delta_culmination_azimuth", fallback=15.0),
        "max_delta_culmination_elevation": s.getfloat("max_delta_culmination_elevation", fallback=10.0),
        "elevation_band_limits": band_limits,
        "weight_deframer_synced_seconds": s.getfloat("weight_deframer_synced_seconds", fallback=1.0),
        "weight_first_deframer_sync_delay": s.getfloat("weight_first_deframer_sync_delay", fallback=-0.4),
        "weight_sync_drop_count": s.getfloat("weight_sync_drop_count", fallback=-0.5),
        "weight_median_snr_synced": s.getfloat("weight_median_snr_synced", fallback=0.3),
        "weight_median_ber_synced": s.getfloat("weight_median_ber_synced", fallback=-0.8),
        "output_dir": s.get(
            "output_dir",
            fallback=os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(config_path))),
                "results",
                "reports",
            ),
        ),
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
                h.aos_azimuth_deg,
                h.culmination_azimuth_deg,
                h.los_azimuth_deg,
                h.culmination_elevation_deg,
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
        metrics_all.append(
            PassMetrics(
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
                aos_azimuth_deg=float(row["aos_azimuth_deg"]) if row["aos_azimuth_deg"] is not None else None,
                culmination_azimuth_deg=float(row["culmination_azimuth_deg"]) if row["culmination_azimuth_deg"] is not None else None,
                los_azimuth_deg=float(row["los_azimuth_deg"]) if row["los_azimuth_deg"] is not None else None,
                culmination_elevation_deg=float(row["culmination_elevation_deg"]) if row["culmination_elevation_deg"] is not None else None,
                direction=str(row["direction"] or "unknown"),
                sample_count=int(row["sample_count"] or 0),
                first_deframer_sync_delay_seconds=(
                    float(row["first_deframer_sync_delay_seconds"])
                    if row["first_deframer_sync_delay_seconds"] is not None
                    else None
                ),
                total_deframer_synced_seconds=float(row["total_deframer_synced_seconds"] or 0.0),
                sync_drop_count=int(row["sync_drop_count"] or 0),
                median_snr_synced=float(row["median_snr_synced"]) if row["median_snr_synced"] is not None else None,
                median_ber_synced=float(row["median_ber_synced"]) if row["median_ber_synced"] is not None else None,
                peak_snr_db=float(row["peak_snr_db"]) if row["peak_snr_db"] is not None else None,
            )
        )

    return metrics_all


def compute_score(m: PassMetrics, settings: dict[str, Any]) -> int:
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
    return max(0, int(round(score)))


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

def elevation_band_index(culmination_elevation_deg: float | None, settings: dict[str, Any]) -> int | None:
    if culmination_elevation_deg is None:
        return None

    limits = settings["elevation_band_limits"]

    for idx, limit in enumerate(limits):
        if culmination_elevation_deg < limit:
            return idx

    return len(limits)

def passes_are_comparable(a: PassMetrics, b: PassMetrics, settings: dict[str, Any]) -> bool:
    if a.satellite != b.satellite:
        return False
    if a.pipeline != b.pipeline:
        return False

    if a.culmination_elevation_deg is None or b.culmination_elevation_deg is None:
        return False

    a_band = elevation_band_index(a.culmination_elevation_deg, settings)
    b_band = elevation_band_index(b.culmination_elevation_deg, settings)
    if a_band is None or b_band is None:
        return False
    if a_band != b_band:
        return False

    aos_delta = angular_delta_deg(a.aos_azimuth_deg, b.aos_azimuth_deg)
    if aos_delta is None or aos_delta > settings["max_delta_aos_azimuth"]:
        return False

    los_delta = angular_delta_deg(a.los_azimuth_deg, b.los_azimuth_deg)
    if los_delta is None or los_delta > settings["max_delta_los_azimuth"]:
        return False

    culm_az_delta = angular_delta_deg(a.culmination_azimuth_deg, b.culmination_azimuth_deg)
    if culm_az_delta is None or culm_az_delta > settings["max_delta_culmination_azimuth"]:
        return False

    if abs(a.culmination_elevation_deg - b.culmination_elevation_deg) > settings["max_delta_culmination_elevation"]:
        return False

    return True

def comparable_candidates(metrics_list: list[PassMetrics]) -> list[PassMetrics]:
    return [
        m for m in metrics_list
        if m.aos_azimuth_deg is not None
        and m.los_azimuth_deg is not None
        and m.culmination_azimuth_deg is not None
        and m.culmination_elevation_deg is not None
    ]


def build_similar_pass_groups(metrics_list: list[PassMetrics], settings: dict[str, Any]) -> list[list[PassMetrics]]:
    candidates = comparable_candidates(metrics_list)
    groups: list[list[PassMetrics]] = []
    used: set[str] = set()

    for seed in candidates:
        if seed.pass_id in used:
            continue

        group = []
        for cand in candidates:
            if cand.pass_id in used:
                continue
            if passes_are_comparable(seed, cand, settings):
                group.append(cand)

        if len(group) > 1:
            group.sort(key=lambda m: m.pass_id)
            for item in group:
                used.add(item.pass_id)
            groups.append(group)

    groups.sort(
        key=lambda g: (
            g[0].satellite if g else "",
            g[0].pipeline if g else "",
            g[0].direction if g else "",
            safe_min([m.culmination_elevation_deg for m in g]) if g else 0,
            g[0].pass_id if g else "",
        )
    )
    return groups


def fmt(value, digits=2, none_value="-"):
    if value is None:
        return none_value
    return f"{value:.{digits}f}"


def fmt_int(value, none_value="-"):
    if value is None:
        return none_value
    return str(int(round(value)))


def average(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def safe_min(values: list[float | None]) -> float | None:
    vals = [v for v in values if v is not None]
    return min(vals) if vals else None


def safe_max(values: list[float | None]) -> float | None:
    vals = [v for v in values if v is not None]
    return max(vals) if vals else None


def setup_fingerprint(m: PassMetrics) -> str:
    return " | ".join([
        f"gain={fmt(m.gain, 1)}",
        f"antenna={m.antenna_type or '-'}",
        f"location={m.antenna_location or '-'}",
        f"orientation={m.antenna_orientation or '-'}",
        f"lna={m.lna or '-'}",
        f"filter={m.rf_filter or '-'}",
        f"feedline={m.feedline or '-'}",
        f"pi={m.raspberry_pi or '-'}",
        f"psu={m.power_supply or '-'}",
        f"info={m.additional_info or '-'}",
    ])


def setup_label(m: PassMetrics) -> str:
    parts = [
        f"setup_id={m.setup_id}",
        f"gain={fmt(m.gain, 1)}",
    ]
    if m.antenna_type:
        parts.append(f"antenna={m.antenna_type}")
    if m.antenna_location:
        parts.append(f"location={m.antenna_location}")
    if m.antenna_orientation:
        parts.append(f"orientation={m.antenna_orientation}")
    if m.lna:
        parts.append(f"lna={m.lna}")
    if m.rf_filter:
        parts.append(f"filter={m.rf_filter}")
    if m.feedline:
        parts.append(f"feedline={m.feedline}")
    if m.raspberry_pi:
        parts.append(f"pi={m.raspberry_pi}")
    if m.power_supply:
        parts.append(f"psu={m.power_supply}")
    if m.additional_info:
        parts.append(f"info={m.additional_info}")
    return ", ".join(parts)


def direction_label_from_pass(group: list[PassMetrics]) -> str:
    aos = average([m.aos_azimuth_deg for m in group if m.aos_azimuth_deg is not None])
    los = average([m.los_azimuth_deg for m in group if m.los_azimuth_deg is not None])
    if aos is None or los is None:
        return group[0].direction

    def sector_name(az: float) -> str:
        sectors = [
            (22.5, "north"),
            (67.5, "north-east"),
            (112.5, "east"),
            (157.5, "south-east"),
            (202.5, "south"),
            (247.5, "south-west"),
            (292.5, "west"),
            (337.5, "north-west"),
            (360.1, "north"),
        ]
        for limit, name in sectors:
            if az < limit:
                return name
        return "unknown"

    return f"{sector_name(aos)} to {sector_name(los)}"


def elevation_band_label(group: list[PassMetrics], settings: dict[str, Any]) -> str:
    if not group:
        return "unknown elevation"

    limits = settings["elevation_band_limits"]
    band_indices = {
        elevation_band_index(m.culmination_elevation_deg, settings)
        for m in group
        if m.culmination_elevation_deg is not None
    }

    if len(band_indices) != 1:
        return "mixed elevation bands"

    band = next(iter(band_indices))

    if band == 0:
        return f"very low elevation (<{int(limits[0])} degrees)"
    if band == 1:
        return f"low elevation ({int(limits[0])}-{int(limits[1])} degrees)"
    if band == 2:
        return f"lower medium elevation ({int(limits[1])}-{int(limits[2])} degrees)"
    if band == 3:
        return f"upper medium elevation ({int(limits[2])}-{int(limits[3])} degrees)"
    if band == 4:
        return f"high elevation ({int(limits[3])}-{int(limits[4])} degrees)"
    return f"very high elevation (>{int(limits[4])} degrees)"

def group_title(group: list[PassMetrics], settings: dict[str, Any]) -> str:
    sat = group[0].satellite
    return f"{sat}, {direction_label_from_pass(group)}, {elevation_band_label(group, settings)}"

def load_reception_samples_for_pass(m: PassMetrics) -> list[dict[str, Any]]:
    if not m.path:
        return []

    source_path = Path(m.path)
    pass_dir = source_path.parent if source_path.parent.exists() else None
    if pass_dir is None:
        return []

    reception_json = pass_dir / "reception.json"
    if not reception_json.exists():
        return []

    try:
        with open(reception_json, "r", encoding="utf-8") as f:
            payload = json.load(f)
        samples = payload.get("samples", [])
        if not isinstance(samples, list):
            return []
        return samples
    except Exception:
        return []

def make_group_skyplot(group_id: int, items: list[PassMetrics], output_dir: Path) -> str | None:
    if len(items) < 2:
        return None

    base_dir = Path(__file__).resolve().parent.parent
    plot_script = base_dir / "bin" / "plot_receptions.py"
    python_bin = "python3"

    target_path = output_dir / f"group_{group_id:02d}_skyplot.png"
    temp_output = base_dir / "results" / "reports" / "skyplot_grouped_passes.png"

    if not plot_script.exists():
        return None

    if temp_output.exists():
        temp_output.unlink()

    cmd = [python_bin, str(plot_script)]
    for item in items:
        cmd.extend(["--pass-id-list", item.pass_id])

    result = subprocess.run(
        cmd,
        cwd=str(base_dir),
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        return None

    if not temp_output.exists():
        return None

    shutil.move(str(temp_output), str(target_path))
    return str(target_path)

def summarize_setup_items(items: list[PassMetrics]) -> dict[str, Any]:
    ref = items[0]
    valid_scores = [float(m.score) for m in items if m.score is not None]
    return {
        "setup_id": ref.setup_id,
        "setup_label": setup_label(ref),
        "setup_fingerprint": setup_fingerprint(ref),
        "pass_count": len(items),
        "avg_score": average(valid_scores),
        "avg_total_deframer_synced_seconds": average([m.total_deframer_synced_seconds for m in items]),
        "avg_first_deframer_sync_delay_seconds": average([
            m.first_deframer_sync_delay_seconds if m.first_deframer_sync_delay_seconds is not None else 9999.0
            for m in items
        ]),
        "avg_sync_drop_count": average([float(m.sync_drop_count) for m in items]),
        "avg_median_snr_synced": average([(m.median_snr_synced or 0.0) for m in items]),
        "avg_median_ber_synced": average([
            (m.median_ber_synced if m.median_ber_synced is not None else 1.0)
            for m in items
        ]),
        "passes": [m.pass_id for m in items],
    }


def group_by_setup(items: list[PassMetrics]) -> dict[int, list[PassMetrics]]:
    grouped: dict[int, list[PassMetrics]] = defaultdict(list)
    for m in items:
        grouped[m.setup_id].append(m)
    return dict(sorted(grouped.items(), key=lambda kv: kv[0]))


def detect_duplicate_setup_fingerprints(metrics_all: list[PassMetrics]) -> dict[str, list[int]]:
    fp_to_ids: dict[str, set[int]] = defaultdict(set)
    for m in metrics_all:
        fp_to_ids[setup_fingerprint(m)].add(m.setup_id)

    result = {}
    for fp, ids in fp_to_ids.items():
        if len(ids) > 1:
            result[fp] = sorted(ids)
    return result


def evaluate_group(
    group_id: int,
    items: list[PassMetrics],
    settings: dict[str, Any],
    min_passes: int,
    min_setups: int,
    plot_dir: Path,
) -> dict[str, Any]:
    grouped = group_by_setup(items)
    setup_summaries = [summarize_setup_items(v) for _, v in grouped.items()]
    setup_summaries.sort(key=lambda s: (s["avg_score"] is not None, s["avg_score"]), reverse=True)

    unique_setups = sorted(grouped.keys())
    evaluable = len(items) >= min_passes and len(unique_setups) >= min_setups
    winner_setup_id = setup_summaries[0]["setup_id"] if evaluable and setup_summaries else None
    score_values = [float(m.score) for m in items if m.score is not None]
    skyplot_path = make_group_skyplot(group_id, items, plot_dir)

    note = ""
    if not evaluable:
        if len(items) < min_passes:
            note = f"Not evaluable: only {len(items)} passes in group."
        elif len(unique_setups) < min_setups:
            note = f"Not evaluable: only {len(unique_setups)} different setup(s) in group."
        else:
            note = "Not evaluable."
    else:
        if len(setup_summaries) >= 2:
            best = setup_summaries[0]
            second = setup_summaries[1]
            if best["avg_score"] is not None and second["avg_score"] is not None:
                delta = best["avg_score"] - second["avg_score"]
                if delta >= 50:
                    note = "Clear winner within this similar-pass group."
                elif delta >= 15:
                    note = "Moderate advantage for the best setup within this similar-pass group."
                else:
                    note = "Only a small difference between the best two setups in this group."
            else:
                note = "Group evaluated, but score difference could not be determined cleanly."
        else:
            note = "Group evaluated."

    return {
        "group_id": group_id,
        "title": group_title(items, settings),
        "satellite": items[0].satellite,
        "pipeline": items[0].pipeline,
        "direction": items[0].direction,
        "pass_count": len(items),
        "setup_count": len(unique_setups),
        "setup_ids": unique_setups,
        "criteria": {
            "same_satellite": True,
            "same_pipeline": True,
            "max_delta_aos_azimuth": settings["max_delta_aos_azimuth"],
            "max_delta_los_azimuth": settings["max_delta_los_azimuth"],
            "max_delta_culmination_azimuth": settings["max_delta_culmination_azimuth"],
            "max_delta_culmination_elevation": settings["max_delta_culmination_elevation"],
        },
        "geometry_span": {
            "aos_azimuth_min": safe_min([m.aos_azimuth_deg for m in items]),
            "aos_azimuth_max": safe_max([m.aos_azimuth_deg for m in items]),
            "los_azimuth_min": safe_min([m.los_azimuth_deg for m in items]),
            "los_azimuth_max": safe_max([m.los_azimuth_deg for m in items]),
            "culmination_azimuth_min": safe_min([m.culmination_azimuth_deg for m in items]),
            "culmination_azimuth_max": safe_max([m.culmination_azimuth_deg for m in items]),
            "culmination_elevation_min": safe_min([m.culmination_elevation_deg for m in items]),
            "culmination_elevation_max": safe_max([m.culmination_elevation_deg for m in items]),
        },
        "score_range": {
            "min_score": safe_min(score_values) if score_values else None,
            "max_score": safe_max(score_values) if score_values else None,
        },
        "evaluable": evaluable,
        "winner_setup_id": winner_setup_id,
        "evaluation_note": note,
        "setup_summaries": setup_summaries,
        "passes": [asdict(m) for m in items],
        "skyplot_path": skyplot_path,
    }


def evaluate_groups(
    groups: list[list[PassMetrics]],
    settings: dict[str, Any],
    min_passes: int,
    min_setups: int,
    plot_dir: Path,
) -> list[dict[str, Any]]:
    evaluated = []
    for idx, group in enumerate(groups, start=1):
        evaluated.append(evaluate_group(idx, group, settings, min_passes, min_setups, plot_dir))
    return evaluated


def summarize_across_groups(group_reports: list[dict[str, Any]]) -> dict[str, Any]:
    evaluable_groups = [g for g in group_reports if g["evaluable"]]
    winner_counter = Counter()
    setup_group_scores: dict[int, list[float]] = defaultdict(list)
    setup_labels: dict[int, str] = {}
    setup_fingerprints: dict[int, str] = {}

    for g in evaluable_groups:
        winner = g["winner_setup_id"]
        if winner is not None:
            winner_counter[winner] += 1

        for s in g["setup_summaries"]:
            setup_id = s["setup_id"]
            setup_labels[setup_id] = s["setup_label"]
            setup_fingerprints[setup_id] = s["setup_fingerprint"]
            if s["avg_score"] is not None:
                setup_group_scores[setup_id].append(float(s["avg_score"]))

    setup_overview = []
    for setup_id in sorted(set(setup_labels.keys()) | set(setup_group_scores.keys())):
        scores = setup_group_scores.get(setup_id, [])
        setup_overview.append({
            "setup_id": setup_id,
            "setup_label": setup_labels.get(setup_id, f"setup_id={setup_id}"),
            "setup_fingerprint": setup_fingerprints.get(setup_id, ""),
            "group_wins": int(winner_counter.get(setup_id, 0)),
            "groups_observed": len(scores),
            "avg_group_score": average(scores),
        })

    setup_overview.sort(key=lambda x: x["setup_id"])

    return {
        "total_groups": len(group_reports),
        "evaluable_groups": len(evaluable_groups),
        "setup_overview": setup_overview,
    }


def write_report_json(output_path: str, payload: dict[str, Any]) -> None:
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
        f.write("\n")


def ptext(text: str) -> str:
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def make_para(text: str, style):
    return Paragraph(ptext(text), style)


def add_table(story, rows, col_widths_mm, header_bg="#EAF2F8", font_size=8.5):
    table = Table(rows, colWidths=[w * mm for w in col_widths_mm], repeatRows=1)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(header_bg)),
        ("GRID", (0, 0), (-1, -1), 0.35, colors.grey),
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 0), (-1, -1), font_size),
        ("LEADING", (0, 0), (-1, -1), font_size + 2),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("WORDWRAP", (0, 0), (-1, -1), "CJK"),
    ]))
    story.append(table)


def write_report_pdf(output_path: str, payload: dict[str, Any]) -> None:
    doc = SimpleDocTemplate(
        output_path,
        pagesize=landscape(A4),
        leftMargin=10 * mm,
        rightMargin=10 * mm,
        topMargin=10 * mm,
        bottomMargin=10 * mm,
    )

    styles = getSampleStyleSheet()
    title = styles["Title"]
    h1 = styles["Heading1"]
    h2 = styles["Heading2"]
    body = styles["BodyText"]

    body.fontName = "Helvetica"
    body.fontSize = 8.5
    body.leading = 11

    small = ParagraphStyle(
        "Small",
        parent=body,
        fontSize=7.5,
        leading=9,
        alignment=TA_LEFT,
    )

    story = []

    story.append(Paragraph("SATPI Similar-Pass Group Analysis", title))
    story.append(Spacer(1, 3 * mm))
    story.append(Paragraph(
        ptext(
            "This report groups geometrically similar satellite passes and compares the resulting "
            "reception quality across different reception and antenna setups."
        ),
        body,
    ))
    story.append(Spacer(1, 4 * mm))

    story.append(Paragraph("1. Grouping Criteria", h1))
    c = payload["grouping_criteria"]
    criteria_rows = [
        [make_para("Criterion", small), make_para("Value", small)],
        [make_para("Same satellite", small), make_para("required", small)],
        [make_para("Same pipeline", small), make_para("required", small)],
        [make_para("Max delta AOS azimuth [deg]", small), make_para(str(c["max_delta_aos_azimuth"]), small)],
        [make_para("Max delta LOS azimuth [deg]", small), make_para(str(c["max_delta_los_azimuth"]), small)],
        [make_para("Elevation bands [deg]", small), make_para(", ".join(str(int(x)) for x in c["elevation_band_limits"]), small)],
        [make_para("Max delta culmination azimuth [deg]", small), make_para(str(c["max_delta_culmination_azimuth"]), small)],
        [make_para("Max delta culmination elevation [deg]", small), make_para(str(c["max_delta_culmination_elevation"]), small)],
        [make_para("Minimum passes per evaluable group", small), make_para(str(payload["min_passes_per_group"]), small)],
        [make_para("Minimum setups per evaluable group", small), make_para(str(payload["min_setups_per_group"]), small)],
    ]

    add_table(story, criteria_rows, [80, 120], font_size=8)
    story.append(Spacer(1, 4 * mm))

    story.append(Paragraph("2. Global Summary", h1))
    g = payload["global_summary"]
    global_rows = [
        [make_para("Metric", small), make_para("Value", small)],
        [make_para("Total passes analyzed", small), make_para(str(payload["total_passes_analyzed"]), small)],
        [make_para("Candidate passes with full geometry", small), make_para(str(payload["candidate_passes"]), small)],
        [make_para("Total groups", small), make_para(str(g["total_groups"]), small)],
        [make_para("Evaluable groups", small), make_para(str(g["evaluable_groups"]), small)],
    ]
    add_table(story, global_rows, [80, 120], font_size=8)
    story.append(Spacer(1, 4 * mm))

    story.append(Paragraph("3. Setup Performance Across Groups", h1))
    if payload["duplicate_setup_fingerprints"]:
        story.append(Paragraph(
            ptext(
                "Note: Some setup IDs appear to have identical visible configuration fields. "
                "These may be duplicate setup records in the database."
            ),
            small,
        ))
        story.append(Spacer(1, 2 * mm))
        dup_rows = [[make_para("Duplicate fingerprint", small), make_para("Setup IDs", small)]]
        for fp, ids in payload["duplicate_setup_fingerprints"].items():
            dup_rows.append([
                make_para(fp, small),
                make_para(", ".join(str(x) for x in ids), small),
            ])
        add_table(story, dup_rows, [170, 40], font_size=7.2)
        story.append(Spacer(1, 4 * mm))

    so_rows = [[
        make_para("Setup ID", small),
        make_para("Wins", small),
        make_para("Groups Observed", small),
        make_para("Avg Group Score", small),
        make_para("Configuration", small),
    ]]
    for s in g["setup_overview"]:
        so_rows.append([
            make_para(str(s["setup_id"]), small),
            make_para(str(s["group_wins"]), small),
            make_para(str(s["groups_observed"]), small),
            make_para(fmt_int(s["avg_group_score"]), small),
            make_para(s["setup_fingerprint"], small),
        ])
    add_table(story, so_rows, [18, 18, 28, 24, 180], font_size=7.2)
    story.append(Spacer(1, 5 * mm))

    story.append(Paragraph("4. Similar-Pass Groups", h1))
    for idx, group in enumerate(payload["groups"]):

        story.append(Paragraph(ptext(f"Group {group['group_id']}: {group['title']}"), h2))

        meta_rows = [
            [make_para("Property", small), make_para("Value", small)],
            [make_para("Pass count", small), make_para(str(group["pass_count"]), small)],
            [make_para("Setup count", small), make_para(str(group["setup_count"]), small)],
            [make_para("Setup IDs", small), make_para(", ".join(str(x) for x in group["setup_ids"]), small)],
            [make_para("AOS azimuth span [deg]", small), make_para(f"{fmt(group['geometry_span']['aos_azimuth_min'],1)} .. {fmt(group['geometry_span']['aos_azimuth_max'],1)}", small)],
            [make_para("LOS azimuth span [deg]", small), make_para(f"{fmt(group['geometry_span']['los_azimuth_min'],1)} .. {fmt(group['geometry_span']['los_azimuth_max'],1)}", small)],
            [make_para("Culmination azimuth span [deg]", small), make_para(f"{fmt(group['geometry_span']['culmination_azimuth_min'],1)} .. {fmt(group['geometry_span']['culmination_azimuth_max'],1)}", small)],
            [make_para("Culmination elevation span [deg]", small), make_para(f"{fmt(group['geometry_span']['culmination_elevation_min'],1)} .. {fmt(group['geometry_span']['culmination_elevation_max'],1)}", small)],
            [make_para("Evaluable", small), make_para(str(group["evaluable"]), small)],
            [make_para("Winner setup_id", small), make_para(str(group["winner_setup_id"]) if group["winner_setup_id"] is not None else "-", small)],
            [make_para("Evaluation note", small), make_para(group["evaluation_note"], small)],
        ]
        add_table(story, meta_rows, [70, 170], font_size=7.5)
        story.append(Spacer(1, 3 * mm))

        if group.get("skyplot_path") and os.path.exists(group["skyplot_path"]):
            story.append(Paragraph("Skyplot of passes in this group", small))
            story.append(Spacer(1, 1 * mm))

            img = RLImage(group["skyplot_path"])

            available_width = doc.width
            available_height = doc.height - 35 * mm

            width_scale = available_width / img.drawWidth
            height_scale = available_height / img.drawHeight
            scale = min(width_scale, height_scale, 1.0)

            img.drawWidth *= scale
            img.drawHeight *= scale

            story.append(img)
            story.append(Spacer(1, 2 * mm))

        story.append(PageBreak())

        story.append(Paragraph(ptext(f"Group {group['group_id']}: {group['title']}"), h2))
        story.append(Paragraph("Passes in this group", small))

        pass_rows = [[
            make_para("Pass ID", small),
            make_para("Setup", small),
            make_para("Gain", small),
            make_para("AOS", small),
            make_para("CulmAz", small),
            make_para("LOS", small),
            make_para("CulmEl", small),
            make_para("Score", small),
            make_para("Sync s", small),
            make_para("1st Sync", small),
            make_para("Drops", small),
            make_para("SNR", small),
            make_para("BER", small),
        ]]
        for p in group["passes"]:
            pass_rows.append([
                make_para(p["pass_id"], small),
                make_para(str(p["setup_id"]), small),
                make_para(fmt(p["gain"], 1), small),
                make_para(fmt(p["aos_azimuth_deg"], 1), small),
                make_para(fmt(p["culmination_azimuth_deg"], 1), small),
                make_para(fmt(p["los_azimuth_deg"], 1), small),
                make_para(fmt(p["culmination_elevation_deg"], 1), small),
                make_para(fmt_int(p["score"]), small),
                make_para(fmt(p["total_deframer_synced_seconds"], 1), small),
                make_para(fmt(p["first_deframer_sync_delay_seconds"], 1), small),
                make_para(str(p["sync_drop_count"]), small),
                make_para(fmt(p["median_snr_synced"], 2), small),
                make_para(fmt(p["median_ber_synced"], 4), small),
            ])
        add_table(
            story,
            pass_rows,
            [62, 14, 14, 16, 16, 16, 18, 16, 18, 18, 14, 16, 18],
            font_size=7,
        )
        story.append(Spacer(1, 3 * mm))

        story.append(Paragraph("Setup comparison within this group", small))
        setup_rows = [[
            make_para("Setup", small),
            make_para("Passes", small),
            make_para("Avg Score", small),
            make_para("Avg Sync s", small),
            make_para("Avg 1st Sync", small),
            make_para("Avg Drops", small),
            make_para("Avg SNR", small),
            make_para("Avg BER", small),
            make_para("Configuration", small),
        ]]
        for s in group["setup_summaries"]:
            setup_rows.append([
                make_para(str(s["setup_id"]), small),
                make_para(str(s["pass_count"]), small),
                make_para(fmt_int(s["avg_score"]), small),
                make_para(fmt(s["avg_total_deframer_synced_seconds"], 1), small),
                make_para(fmt(s["avg_first_deframer_sync_delay_seconds"], 1), small),
                make_para(fmt(s["avg_sync_drop_count"], 2), small),
                make_para(fmt(s["avg_median_snr_synced"], 2), small),
                make_para(fmt(s["avg_median_ber_synced"], 4), small),
                make_para(s["setup_fingerprint"], small),
            ])
        add_table(
            story,
            setup_rows,
            [14, 16, 18, 20, 22, 18, 18, 20, 150],
            font_size=7,
        )
        story.append(Spacer(1, 4 * mm))

        if idx != len(payload["groups"]) - 1:
            story.append(PageBreak())

    doc.build(story)


def build_payload(
    metrics_all: list[PassMetrics],
    groups: list[list[PassMetrics]],
    group_reports: list[dict[str, Any]],
    settings: dict[str, Any],
    min_passes_per_group: int,
    min_setups_per_group: int,
) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "total_passes_analyzed": len(metrics_all),
        "candidate_passes": len(comparable_candidates(metrics_all)),
        "min_passes_per_group": min_passes_per_group,
        "min_setups_per_group": min_setups_per_group,
        "grouping_criteria": {
            "max_delta_aos_azimuth": settings["max_delta_aos_azimuth"],
            "max_delta_los_azimuth": settings["max_delta_los_azimuth"],
            "max_delta_culmination_azimuth": settings["max_delta_culmination_azimuth"],
            "max_delta_culmination_elevation": settings["max_delta_culmination_elevation"],
            "elevation_band_limits": settings["elevation_band_limits"],
        },
        "duplicate_setup_fingerprints": detect_duplicate_setup_fingerprints(metrics_all),
        "global_summary": summarize_across_groups(group_reports),
        "groups": group_reports,
    }


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

    db_path = config["paths"]["reception_db_file"]
    if not os.path.exists(db_path):
        print(f"[optimize_reception] database not found: {db_path}")
        return 1

    metrics_all = load_metrics_from_db(db_path)
    metrics_all = score_metrics_list(metrics_all, settings)

    if not metrics_all:
        print("[optimize_reception] no usable pass metrics found in database")
        return 1

    groups = build_similar_pass_groups(metrics_all, settings)
    if not groups:
        print("[optimize_reception] no similar-pass groups with more than one member found")
        return 1

    output_dir = Path(settings["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)
    plot_dir = output_dir / "group_skyplots"
    plot_dir.mkdir(parents=True, exist_ok=True)

    group_reports = evaluate_groups(
        groups,
        settings,
        args.min_passes_per_group,
        args.min_setups_per_group,
        plot_dir,
    )

    group_reports = [g for g in group_reports if g["setup_count"] >= 2]

    if not group_reports:
        print("[optimize_reception] no reportable groups with at least 2 setups found")
        return 1

    payload = build_payload(
        metrics_all,
        groups,
        group_reports,
        settings,
        args.min_passes_per_group,
        args.min_setups_per_group,
    )

    json_report = output_dir / "similar-pass-groups-report.json"
    pdf_report = output_dir / "similar-pass-groups-report.pdf"

    write_report_json(str(json_report), payload)
    print(f"[optimize_reception] wrote: {json_report}")

    if not args.json_only:
        write_report_pdf(str(pdf_report), payload)
        print(f"[optimize_reception] wrote: {pdf_report}")

    print(f"[optimize_reception] total passes analyzed: {payload['total_passes_analyzed']}")
    print(f"[optimize_reception] candidate passes: {payload['candidate_passes']}")
    print(f"[optimize_reception] total groups: {payload['global_summary']['total_groups']}")
    print(f"[optimize_reception] evaluable groups: {payload['global_summary']['evaluable_groups']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
