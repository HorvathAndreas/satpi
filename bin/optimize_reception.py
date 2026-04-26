#!/usr/bin/env python3
"""satpi – optimize_reception

Groups geometrically similar satellite passes and compares reception setups.

Reads per-pass metrics and setup metadata from reception.db, clusters passes
that observed the sky in a similar geometry (same satellite + pipeline, same
elevation band, similar AOS/LOS/culmination azimuths), then compares which
reception setup performed best within each cluster. Produces a JSON report
and a landscape A4 PDF with per-group skyplots.

Author: Andreas Horvath
Project: Autonomous, config-driven satellite reception pipeline for Raspberry Pi
"""

from __future__ import annotations

import argparse
import configparser
import json
import logging
import math
import os
import signal
import sqlite3
import subprocess
import sys
import tempfile
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Iterable, List, Optional, Sequence

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

from load_config import ConfigError, load_config


# --- Constants ---------------------------------------------------------------

LOG_MAX_BYTES = 1_000_000
LOG_BACKUP_COUNT = 5
SKYPLOT_TIMEOUT_SECONDS = 180
NEVER_SYNCED_SENTINEL = 9999.0   # preserved for scoring compatibility
DEFAULT_BAND_LIMITS = (20.0, 35.0, 50.0, 65.0, 80.0)

REPORT_JSON_NAME = "similar-pass-groups-report.json"
REPORT_PDF_NAME = "similar-pass-groups-report.pdf"

logger = logging.getLogger("satpi.optimize")

_STOP_REQUESTED = False


# --- Signal handling ---------------------------------------------------------

def _install_signal_handlers() -> None:
    def _handler(signum, _frame):
        global _STOP_REQUESTED
        _STOP_REQUESTED = True
        logger.warning("Signal %s received; finishing current work then stopping.", signum)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handler)
        except (ValueError, OSError):
            # e.g. running in a thread; ignore.
            pass


# --- Logging -----------------------------------------------------------------

def setup_logging(log_dir: str) -> None:
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "optimize_reception.log")

    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    fh = RotatingFileHandler(log_file, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)


# --- CLI ---------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build similar-pass groups from reception.db and compare setup performance."
    )
    parser.add_argument(
        "--config", default=None,
        help="Path to config.ini (default: ../config/config.ini relative to this script)",
    )
    parser.add_argument(
        "--min-passes-per-group", type=int, default=2,
        help="Minimum number of passes required for a group to be evaluable",
    )
    parser.add_argument(
        "--min-setups-per-group", type=int, default=2,
        help="Minimum number of different setups required for a group to be evaluable",
    )
    parser.add_argument(
        "--json-only", action="store_true",
        help="Write only JSON report, skip PDF generation",
    )
    parser.add_argument(
        "--no-skyplots", action="store_true",
        help="Skip per-group skyplot generation (faster)",
    )
    return parser.parse_args()


def get_config_path(cli_value: str | None) -> str:
    if cli_value:
        return os.path.abspath(cli_value)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_dir, "config", "config.ini")


# --- Optimizer settings ------------------------------------------------------

def _coerce_bool(v: Any, default: bool) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "on"):
        return True
    if s in ("0", "false", "no", "off"):
        return False
    return default


def _coerce_float(v: Any, default: float) -> float:
    if v is None or v == "":
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def load_optimizer_settings(config_path: str, config: dict[str, Any]) -> dict[str, Any]:
    """Return parsed [optimize_reception] settings.

    Prefers the dict parsed by load_config(); falls back to reading the INI
    directly if that section is not present in the parsed config.
    """
    section: dict[str, Any] = dict(config.get("optimize_reception") or {})

    if not section:
        parser = configparser.ConfigParser(interpolation=None)
        try:
            read = parser.read(config_path, encoding="utf-8")
        except configparser.Error as e:
            raise ConfigError(f"Failed to parse {config_path}: {e}") from e
        if not read:
            raise ConfigError(f"Could not read config file: {config_path}")
        if not parser.has_section("optimize_reception"):
            raise ConfigError("Missing [optimize_reception] section in config.ini")
        s = parser["optimize_reception"]
        for key in s:
            section[key] = s[key]

    band_limits = sorted(
        _coerce_float(
            section.get(f"elevation_band_{i}_max"), DEFAULT_BAND_LIMITS[i - 1]
        )
        for i in range(1, 6)
    )

    output_dir = str(section.get("output_dir") or "").strip()
    if not output_dir:
        base = os.path.dirname(os.path.dirname(os.path.abspath(config_path)))
        output_dir = os.path.join(base, "results", "reports")

    return {
        "enabled": _coerce_bool(section.get("enabled"), True),
        "max_delta_aos_azimuth": _coerce_float(section.get("max_delta_aos_azimuth"), 20.0),
        "max_delta_los_azimuth": _coerce_float(section.get("max_delta_los_azimuth"), 20.0),
        "max_delta_culmination_azimuth": _coerce_float(section.get("max_delta_culmination_azimuth"), 15.0),
        "max_delta_culmination_elevation": _coerce_float(section.get("max_delta_culmination_elevation"), 10.0),
        "elevation_band_limits": band_limits,
        "weight_deframer_synced_seconds": _coerce_float(section.get("weight_deframer_synced_seconds"), 1.0),
        "weight_first_deframer_sync_delay": _coerce_float(section.get("weight_first_deframer_sync_delay"), -0.4),
        "weight_sync_drop_count": _coerce_float(section.get("weight_sync_drop_count"), -0.5),
        "weight_median_snr_synced": _coerce_float(section.get("weight_median_snr_synced"), 0.3),
        "weight_median_ber_synced": _coerce_float(section.get("weight_median_ber_synced"), -0.8),
        "output_dir": output_dir,
    }


# --- Data model --------------------------------------------------------------

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


# --- Small helpers -----------------------------------------------------------

def fmt(value: Any, digits: int = 2, none_value: str = "-") -> str:
    if value is None:
        return none_value
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return str(value)


def fmt_int(value: Any, none_value: str = "-") -> str:
    if value is None:
        return none_value
    try:
        return str(int(round(float(value))))
    except (TypeError, ValueError):
        return str(value)


def average(values: Iterable[float]) -> float | None:
    vals = [v for v in values if v is not None]
    return sum(vals) / len(vals) if vals else None


def safe_min(values: Iterable[float | None]) -> float | None:
    vals = [v for v in values if v is not None]
    return min(vals) if vals else None


def safe_max(values: Iterable[float | None]) -> float | None:
    vals = [v for v in values if v is not None]
    return max(vals) if vals else None


def angular_delta_deg(a: float | None, b: float | None) -> float | None:
    """Shortest signed-magnitude difference between two headings, in [0, 180]."""
    if a is None or b is None:
        return None
    d = (a - b + 180.0) % 360.0 - 180.0
    return abs(d)


def circular_mean_deg(values: Sequence[float | None]) -> float | None:
    """Mean heading (0–360) of a list of degrees, handling wrap-around."""
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    x = sum(math.cos(math.radians(v)) for v in vals) / len(vals)
    y = sum(math.sin(math.radians(v)) for v in vals) / len(vals)
    if abs(x) < 1e-12 and abs(y) < 1e-12:
        return None
    return math.degrees(math.atan2(y, x)) % 360.0


# --- DB load -----------------------------------------------------------------

def _opt_float(row: sqlite3.Row, key: str) -> float | None:
    v = row[key]
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _opt_float_default(row: sqlite3.Row, key: str, default: float = 0.0) -> float:
    v = row[key]
    if v is None:
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _opt_int_default(row: sqlite3.Row, key: str, default: int = 0) -> int:
    v = row[key]
    if v is None:
        return default
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _opt_str(row: sqlite3.Row, key: str, default: str = "") -> str:
    v = row[key]
    return str(v) if v is not None else default


def open_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    return conn


def load_metrics_from_db(db_path: str) -> list[PassMetrics]:
    conn = open_db(db_path)
    try:
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
        except sqlite3.Error as e:
            raise RuntimeError(f"Database query failed: {e}") from e
    finally:
        conn.close()

    metrics: list[PassMetrics] = []
    for row in rows:
        metrics.append(
            PassMetrics(
                path=_opt_str(row, "source_file"),
                pass_id=_opt_str(row, "pass_id"),
                satellite=_opt_str(row, "satellite"),
                pipeline=_opt_str(row, "pipeline"),
                gain=_opt_float_default(row, "gain", 0.0),
                setup_id=_opt_int_default(row, "setup_id", 0),
                antenna_type=_opt_str(row, "antenna_type"),
                antenna_location=_opt_str(row, "antenna_location"),
                antenna_orientation=_opt_str(row, "antenna_orientation"),
                lna=_opt_str(row, "lna"),
                rf_filter=_opt_str(row, "rf_filter"),
                feedline=_opt_str(row, "feedline"),
                raspberry_pi=_opt_str(row, "raspberry_pi"),
                power_supply=_opt_str(row, "power_supply"),
                additional_info=_opt_str(row, "additional_info"),
                aos_azimuth_deg=_opt_float(row, "aos_azimuth_deg"),
                culmination_azimuth_deg=_opt_float(row, "culmination_azimuth_deg"),
                los_azimuth_deg=_opt_float(row, "los_azimuth_deg"),
                culmination_elevation_deg=_opt_float(row, "culmination_elevation_deg"),
                direction=_opt_str(row, "direction", "unknown") or "unknown",
                sample_count=_opt_int_default(row, "sample_count", 0),
                first_deframer_sync_delay_seconds=_opt_float(row, "first_deframer_sync_delay_seconds"),
                total_deframer_synced_seconds=_opt_float_default(row, "total_deframer_synced_seconds", 0.0),
                sync_drop_count=_opt_int_default(row, "sync_drop_count", 0),
                median_snr_synced=_opt_float(row, "median_snr_synced"),
                median_ber_synced=_opt_float(row, "median_ber_synced"),
                peak_snr_db=_opt_float(row, "peak_snr_db"),
            )
        )

    return metrics


# --- Scoring -----------------------------------------------------------------

def compute_score(m: PassMetrics, settings: dict[str, Any]) -> int:
    """Weighted per-pass score.

    Missing values are replaced with conservative sentinels: 9999 s for
    first-sync delay, 0 dB for SNR, 1.0 for BER. Final score is clamped at 0.
    """
    first_sync = m.first_deframer_sync_delay_seconds
    snr = m.median_snr_synced
    ber = m.median_ber_synced

    score = 0.0
    score += settings["weight_deframer_synced_seconds"] * m.total_deframer_synced_seconds
    score += settings["weight_first_deframer_sync_delay"] * (
        first_sync if first_sync is not None else NEVER_SYNCED_SENTINEL
    )
    score += settings["weight_sync_drop_count"] * m.sync_drop_count
    score += settings["weight_median_snr_synced"] * (snr if snr is not None else 0.0)
    score += settings["weight_median_ber_synced"] * (ber if ber is not None else 1.0)
    return max(0, int(round(score)))


def score_metrics_list(metrics_list: list[PassMetrics], settings: dict[str, Any]) -> list[PassMetrics]:
    for m in metrics_list:
        m.score = compute_score(m, settings)
    return metrics_list


# --- Comparability / grouping ------------------------------------------------

def elevation_band_index(culmination_elevation_deg: float | None, settings: dict[str, Any]) -> int | None:
    if culmination_elevation_deg is None:
        return None
    limits = settings["elevation_band_limits"]
    for idx, limit in enumerate(limits):
        if culmination_elevation_deg < limit:
            return idx
    return len(limits)


def passes_are_comparable(a: PassMetrics, b: PassMetrics, settings: dict[str, Any]) -> bool:
    if a.satellite != b.satellite or a.pipeline != b.pipeline:
        return False
    if a.culmination_elevation_deg is None or b.culmination_elevation_deg is None:
        return False
    if elevation_band_index(a.culmination_elevation_deg, settings) != \
       elevation_band_index(b.culmination_elevation_deg, settings):
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
    """Seed-based greedy clustering.

    Each unused seed gathers all passes that are comparable to *the seed*.
    Members of the resulting group are guaranteed comparable to the seed but
    not necessarily pairwise. Order-dependent — stable because candidate list
    is kept in DB order (ORDER BY pass_start) and group members are sorted
    by pass_id afterwards.
    """
    candidates = comparable_candidates(metrics_list)
    groups: list[list[PassMetrics]] = []
    used: set[str] = set()

    for seed in candidates:
        if seed.pass_id in used:
            continue

        group = [
            cand for cand in candidates
            if cand.pass_id not in used and passes_are_comparable(seed, cand, settings)
        ]

        if len(group) > 1:
            group.sort(key=lambda m: m.pass_id)
            for item in group:
                used.add(item.pass_id)
            groups.append(group)

    def _group_sort_key(g: list[PassMetrics]) -> tuple:
        min_el = safe_min([m.culmination_elevation_deg for m in g])
        return (
            g[0].satellite,
            g[0].pipeline,
            g[0].direction,
            min_el if min_el is not None else float("inf"),
            g[0].pass_id,
        )

    groups.sort(key=_group_sort_key)
    return groups


# --- Labels ------------------------------------------------------------------

_SECTORS = [
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


def _sector_name(az: float) -> str:
    az = az % 360.0
    for limit, name in _SECTORS:
        if az < limit:
            return name
    return "unknown"


def direction_label_from_pass(group: list[PassMetrics]) -> str:
    aos = circular_mean_deg([m.aos_azimuth_deg for m in group])
    los = circular_mean_deg([m.los_azimuth_deg for m in group])
    if aos is None or los is None:
        return group[0].direction
    return f"{_sector_name(aos)} to {_sector_name(los)}"


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

    labels = [
        f"very low elevation (<{int(limits[0])} degrees)",
        f"low elevation ({int(limits[0])}-{int(limits[1])} degrees)",
        f"lower medium elevation ({int(limits[1])}-{int(limits[2])} degrees)",
        f"upper medium elevation ({int(limits[2])}-{int(limits[3])} degrees)",
        f"high elevation ({int(limits[3])}-{int(limits[4])} degrees)",
        f"very high elevation (>{int(limits[4])} degrees)",
    ]
    return labels[band] if 0 <= band < len(labels) else "unknown elevation"


def group_title(group: list[PassMetrics], settings: dict[str, Any]) -> str:
    sat = group[0].satellite
    return f"{sat}, {direction_label_from_pass(group)}, {elevation_band_label(group, settings)}"


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
    parts = [f"setup_id={m.setup_id}", f"gain={fmt(m.gain, 1)}"]
    for key, value in [
        ("antenna", m.antenna_type),
        ("location", m.antenna_location),
        ("orientation", m.antenna_orientation),
        ("lna", m.lna),
        ("filter", m.rf_filter),
        ("feedline", m.feedline),
        ("pi", m.raspberry_pi),
        ("psu", m.power_supply),
        ("info", m.additional_info),
    ]:
        if value:
            parts.append(f"{key}={value}")
    return ", ".join(parts)


# --- Skyplot subprocess ------------------------------------------------------

def make_group_skyplot(
    group_id: int,
    items: list[PassMetrics],
    output_dir: Path,
    base_dir: Path,
    highlight_pass_id: str | None = None,
    highlight_label: str = "winning pass",
) -> str | None:
    """Invoke plot_receptions.py as a subprocess to produce a skyplot PNG.

    The current plot_receptions.py writes to a fixed path; we atomically move
    it into the group-specific target. This function is NOT safe to run in
    parallel with another instance targeting the same fixed path.
    """
    if len(items) < 2:
        return None

    plot_script = base_dir / "bin" / "plot_receptions.py"
    if not plot_script.exists():
        logger.warning("skyplot skipped: plot script not found at %s", plot_script)
        return None

    target_path = output_dir / f"group_{group_id:02d}_skyplot.png"
    temp_output = base_dir / "results" / "reports" / "skyplot_grouped_passes.png"

    temp_output.parent.mkdir(parents=True, exist_ok=True)
    if temp_output.exists():
        try:
            temp_output.unlink()
        except OSError as e:
            logger.warning("could not clear previous skyplot temp: %s", e)

    cmd: list[str] = [sys.executable, str(plot_script)]
    for item in items:
        cmd.extend(["--pass-id-list", item.pass_id])
    if highlight_pass_id:
        cmd.extend(["--highlight-pass-id", highlight_pass_id, "--highlight-label", highlight_label])

    try:
        result = subprocess.run(
            cmd,
            cwd=str(base_dir),
            capture_output=True,
            text=True,
            timeout=SKYPLOT_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired:
        logger.warning("skyplot subprocess timed out for group %d", group_id)
        return None

    if result.returncode != 0:
        logger.warning(
            "skyplot subprocess failed for group %d (rc=%d): %s",
            group_id, result.returncode, (result.stderr or "").strip()[:500],
        )
        return None

    if not temp_output.exists():
        logger.warning("skyplot subprocess reported success but produced no output file")
        return None

    try:
        os.replace(str(temp_output), str(target_path))
    except OSError as e:
        logger.warning("could not move skyplot to target: %s", e)
        return None

    return str(target_path)


# --- Per-group / per-setup summary -------------------------------------------

def summarize_setup_items(items: list[PassMetrics]) -> dict[str, Any]:
    """Aggregate metrics for one setup within a group.

    Metrics with missing values are averaged only over observed values.
    Counts of missing-data passes are reported separately so the consumer
    can judge reliability.
    """
    ref = items[0]
    scores = [float(m.score) for m in items if m.score is not None]
    sync_seconds = [m.total_deframer_synced_seconds for m in items]
    first_syncs = [m.first_deframer_sync_delay_seconds for m in items
                   if m.first_deframer_sync_delay_seconds is not None]
    drops = [float(m.sync_drop_count) for m in items]
    snr = [m.median_snr_synced for m in items if m.median_snr_synced is not None]
    ber = [m.median_ber_synced for m in items if m.median_ber_synced is not None]

    never_synced = sum(1 for m in items if m.first_deframer_sync_delay_seconds is None)
    total = len(items)

    return {
        "setup_id": ref.setup_id,
        "setup_label": setup_label(ref),
        "setup_fingerprint": setup_fingerprint(ref),
        "pass_count": total,
        "passes_never_synced": never_synced,
        "avg_score": average(scores),
        "avg_total_deframer_synced_seconds": average(sync_seconds),
        "avg_first_deframer_sync_delay_seconds": average(first_syncs),
        "first_sync_sample_count": len(first_syncs),
        "avg_sync_drop_count": average(drops),
        "avg_median_snr_synced": average(snr),
        "snr_sample_count": len(snr),
        "avg_median_ber_synced": average(ber),
        "ber_sample_count": len(ber),
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
    return {
        fp: sorted(ids)
        for fp, ids in fp_to_ids.items()
        if len(ids) > 1
    }


# --- Group evaluation --------------------------------------------------------

def _score_sort_key(avg_score: float | None) -> float:
    # Non-evaluated setups sort to the bottom when reverse=True.
    return avg_score if avg_score is not None else float("-inf")


def evaluate_group(
    group_id: int,
    items: list[PassMetrics],
    settings: dict[str, Any],
    min_passes: int,
    min_setups: int,
    plot_dir: Path | None,
    base_dir: Path,
) -> dict[str, Any]:
    grouped = group_by_setup(items)
    setup_summaries = [summarize_setup_items(v) for v in grouped.values()]
    setup_summaries.sort(key=lambda s: _score_sort_key(s["avg_score"]), reverse=True)

    unique_setups = sorted(grouped.keys())
    evaluable = len(items) >= min_passes and len(unique_setups) >= min_setups

    winner_setup_id: int | None = None
    winner_pass_id: str | None = None
    if evaluable and setup_summaries and setup_summaries[0]["avg_score"] is not None:
        winner_setup_id = setup_summaries[0]["setup_id"]
        winner_candidates = [
            m for m in items
            if m.setup_id == winner_setup_id and m.score is not None
        ]
        if winner_candidates:
            winner_candidates.sort(
                key=lambda m: (m.score, m.total_deframer_synced_seconds),
                reverse=True,
            )
            winner_pass_id = winner_candidates[0].pass_id

    score_values = [float(m.score) for m in items if m.score is not None]

    skyplot_path: str | None = None
    if plot_dir is not None:
        skyplot_path = make_group_skyplot(
            group_id, items, plot_dir, base_dir,
            highlight_pass_id=winner_pass_id, highlight_label="winning pass",
        )

    note = _build_evaluation_note(items, unique_setups, setup_summaries, min_passes, min_setups, evaluable)

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
        "winner_pass_id": winner_pass_id,
        "evaluation_note": note,
        "setup_summaries": setup_summaries,
        "passes": [asdict(m) for m in items],
        "skyplot_path": skyplot_path,
    }


def _build_evaluation_note(
    items: list[PassMetrics],
    unique_setups: list[int],
    setup_summaries: list[dict[str, Any]],
    min_passes: int,
    min_setups: int,
    evaluable: bool,
) -> str:
    if not evaluable:
        if len(items) < min_passes:
            return f"Not evaluable: only {len(items)} passes in group."
        if len(unique_setups) < min_setups:
            return f"Not evaluable: only {len(unique_setups)} different setup(s) in group."
        return "Not evaluable."

    if len(setup_summaries) < 2:
        return "Group evaluated."

    best = setup_summaries[0]
    second = setup_summaries[1]
    if best["avg_score"] is None or second["avg_score"] is None:
        return "Group evaluated, but score difference could not be determined cleanly."

    delta = best["avg_score"] - second["avg_score"]
    if delta >= 50:
        return "Clear winner within this similar-pass group."
    if delta >= 15:
        return "Moderate advantage for the best setup within this similar-pass group."
    return "Only a small difference between the best two setups in this group."


def evaluate_groups(
    groups: list[list[PassMetrics]],
    settings: dict[str, Any],
    min_passes: int,
    min_setups: int,
    plot_dir: Path | None,
    base_dir: Path,
) -> list[dict[str, Any]]:
    evaluated: list[dict[str, Any]] = []
    total = len(groups)
    for idx, group in enumerate(groups, start=1):
        if _STOP_REQUESTED:
            logger.warning("Stop requested after %d/%d groups.", idx - 1, total)
            break
        logger.info("Evaluating group %d/%d (%d passes)", idx, total, len(group))
        evaluated.append(
            evaluate_group(idx, group, settings, min_passes, min_setups, plot_dir, base_dir)
        )
    return evaluated


# --- Cross-group summary -----------------------------------------------------

def summarize_across_groups(group_reports: list[dict[str, Any]]) -> dict[str, Any]:
    evaluable_groups = [g for g in group_reports if g["evaluable"]]
    winner_counter: Counter = Counter()
    setup_group_scores: dict[int, list[float]] = defaultdict(list)
    setup_labels: dict[int, str] = {}
    setup_fingerprints: dict[int, str] = {}

    for g in evaluable_groups:
        winner = g["winner_setup_id"]
        if winner is not None:
            winner_counter[winner] += 1
        for s in g["setup_summaries"]:
            sid = s["setup_id"]
            setup_labels[sid] = s["setup_label"]
            setup_fingerprints[sid] = s["setup_fingerprint"]
            if s["avg_score"] is not None:
                setup_group_scores[sid].append(float(s["avg_score"]))

    setup_overview: list[dict[str, Any]] = []
    all_ids = sorted(set(setup_labels.keys()) | set(setup_group_scores.keys()))
    for sid in all_ids:
        scores = setup_group_scores.get(sid, [])
        setup_overview.append({
            "setup_id": sid,
            "setup_label": setup_labels.get(sid, f"setup_id={sid}"),
            "setup_fingerprint": setup_fingerprints.get(sid, ""),
            "group_wins": int(winner_counter.get(sid, 0)),
            "groups_observed": len(scores),
            "avg_group_score": average(scores),
        })

    return {
        "total_groups": len(group_reports),
        "evaluable_groups": len(evaluable_groups),
        "setup_overview": setup_overview,
    }


# --- Payload / JSON ----------------------------------------------------------

def build_payload(
    metrics_all: list[PassMetrics],
    group_reports: list[dict[str, Any]],
    settings: dict[str, Any],
    min_passes_per_group: int,
    min_setups_per_group: int,
) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
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


def write_report_json(output_path: str, payload: dict[str, Any]) -> None:
    tmp_path = output_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
        f.write("\n")
    os.replace(tmp_path, output_path)


# --- PDF ---------------------------------------------------------------------

def ptext(text: Any) -> str:
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def make_para(text: Any, style) -> Paragraph:
    return Paragraph(ptext(text), style)


def add_table(story: list, rows: list, col_widths_mm: list[float],
              header_bg: str = "#EAF2F8", font_size: float = 8.5) -> None:
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
    tmp_path = output_path + ".tmp"
    doc = SimpleDocTemplate(
        tmp_path,
        pagesize=landscape(A4),
        leftMargin=10 * mm,
        rightMargin=10 * mm,
        topMargin=10 * mm,
        bottomMargin=10 * mm,
        title="SATPI Similar-Pass Group Analysis",
        author="satpi",
        subject="Reception setup comparison across geometrically similar passes",
    )

    styles = getSampleStyleSheet()
    title_style = styles["Title"]
    h1 = styles["Heading1"]
    h2 = styles["Heading2"]
    body = styles["BodyText"]

    body.fontName = "Helvetica"
    body.fontSize = 8.5
    body.leading = 11

    small = ParagraphStyle(
        "Small", parent=body, fontSize=7.5, leading=9, alignment=TA_LEFT,
    )

    story: list = []

    story.append(Paragraph("SATPI Similar-Pass Group Analysis", title_style))
    story.append(Spacer(1, 3 * mm))
    story.append(Paragraph(
        ptext(
            "This report groups geometrically similar satellite passes and compares the resulting "
            "reception quality across different reception and antenna setups."
        ),
        body,
    ))
    story.append(Paragraph(
        ptext(f"Generated: {payload['generated_at']}"),
        small,
    ))
    story.append(Spacer(1, 4 * mm))

    _pdf_criteria_section(story, payload, small, h1)
    _pdf_global_section(story, payload, small, h1)
    _pdf_setup_overview_section(story, payload, small, h1)
    _pdf_groups_section(story, payload, small, h1, h2, doc)

    doc.build(story)
    os.replace(tmp_path, output_path)


def _pdf_criteria_section(story, payload, small, h1):
    story.append(Paragraph("1. Grouping Criteria", h1))
    c = payload["grouping_criteria"]
    criteria_rows = [
        [make_para("Criterion", small), make_para("Value", small)],
        [make_para("Same satellite", small), make_para("required", small)],
        [make_para("Same pipeline", small), make_para("required", small)],
        [make_para("Max delta AOS azimuth [deg]", small), make_para(str(c["max_delta_aos_azimuth"]), small)],
        [make_para("Max delta LOS azimuth [deg]", small), make_para(str(c["max_delta_los_azimuth"]), small)],
        [make_para("Elevation bands [deg]", small),
         make_para(", ".join(str(int(x)) for x in c["elevation_band_limits"]), small)],
        [make_para("Max delta culmination azimuth [deg]", small), make_para(str(c["max_delta_culmination_azimuth"]), small)],
        [make_para("Max delta culmination elevation [deg]", small), make_para(str(c["max_delta_culmination_elevation"]), small)],
        [make_para("Minimum passes per evaluable group", small), make_para(str(payload["min_passes_per_group"]), small)],
        [make_para("Minimum setups per evaluable group", small), make_para(str(payload["min_setups_per_group"]), small)],
    ]
    add_table(story, criteria_rows, [80, 120], font_size=8)
    story.append(Spacer(1, 4 * mm))


def _pdf_global_section(story, payload, small, h1):
    story.append(Paragraph("2. Global Summary", h1))
    g = payload["global_summary"]
    rows = [
        [make_para("Metric", small), make_para("Value", small)],
        [make_para("Total passes analyzed", small), make_para(str(payload["total_passes_analyzed"]), small)],
        [make_para("Candidate passes with full geometry", small), make_para(str(payload["candidate_passes"]), small)],
        [make_para("Total groups", small), make_para(str(g["total_groups"]), small)],
        [make_para("Evaluable groups", small), make_para(str(g["evaluable_groups"]), small)],
    ]
    add_table(story, rows, [80, 120], font_size=8)
    story.append(Spacer(1, 4 * mm))


def _pdf_setup_overview_section(story, payload, small, h1):
    story.append(Paragraph("3. Setup Performance Across Groups", h1))
    g = payload["global_summary"]

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


def _pdf_groups_section(story, payload, small, h1, h2, doc):
    story.append(Paragraph("4. Similar-Pass Groups", h1))
    groups = payload["groups"]
    for idx, group in enumerate(groups):
        story.append(Paragraph(ptext(f"Group {group['group_id']}: {group['title']}"), h2))

        meta_rows = [
            [make_para("Property", small), make_para("Value", small)],
            [make_para("Pass count", small), make_para(str(group["pass_count"]), small)],
            [make_para("Setup count", small), make_para(str(group["setup_count"]), small)],
            [make_para("Setup IDs", small), make_para(", ".join(str(x) for x in group["setup_ids"]), small)],
            [make_para("AOS azimuth span [deg]", small),
             make_para(f"{fmt(group['geometry_span']['aos_azimuth_min'], 1)} .. "
                       f"{fmt(group['geometry_span']['aos_azimuth_max'], 1)}", small)],
            [make_para("LOS azimuth span [deg]", small),
             make_para(f"{fmt(group['geometry_span']['los_azimuth_min'], 1)} .. "
                       f"{fmt(group['geometry_span']['los_azimuth_max'], 1)}", small)],
            [make_para("Culmination azimuth span [deg]", small),
             make_para(f"{fmt(group['geometry_span']['culmination_azimuth_min'], 1)} .. "
                       f"{fmt(group['geometry_span']['culmination_azimuth_max'], 1)}", small)],
            [make_para("Culmination elevation span [deg]", small),
             make_para(f"{fmt(group['geometry_span']['culmination_elevation_min'], 1)} .. "
                       f"{fmt(group['geometry_span']['culmination_elevation_max'], 1)}", small)],
            [make_para("Evaluable", small), make_para(str(group["evaluable"]), small)],
            [make_para("Winner setup_id", small),
             make_para(str(group["winner_setup_id"]) if group["winner_setup_id"] is not None else "-", small)],
            [make_para("Evaluation note", small), make_para(group["evaluation_note"], small)],
        ]
        add_table(story, meta_rows, [70, 170], font_size=7.5)
        story.append(Spacer(1, 3 * mm))

        skyplot = group.get("skyplot_path")
        if skyplot and os.path.exists(skyplot):
            story.append(Paragraph("Skyplot of passes in this group", small))
            story.append(Spacer(1, 1 * mm))

            img = RLImage(skyplot)
            available_width = doc.width
            available_height = doc.height - 35 * mm
            scale = min(
                available_width / img.drawWidth,
                available_height / img.drawHeight,
                1.0,
            )
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
            story, pass_rows,
            [62, 14, 14, 16, 16, 16, 18, 16, 18, 18, 14, 16, 18],
            font_size=7,
        )
        story.append(Spacer(1, 3 * mm))

        story.append(Paragraph("Setup comparison within this group", small))
        setup_rows = [[
            make_para("Setup", small),
            make_para("Passes", small),
            make_para("Never synced", small),
            make_para("Avg Score", small),
            make_para("Avg Sync s", small),
            make_para("Avg 1st Sync (n)", small),
            make_para("Avg Drops", small),
            make_para("Avg SNR (n)", small),
            make_para("Avg BER (n)", small),
            make_para("Configuration", small),
        ]]
        for s in group["setup_summaries"]:
            setup_rows.append([
                make_para(str(s["setup_id"]), small),
                make_para(str(s["pass_count"]), small),
                make_para(str(s["passes_never_synced"]), small),
                make_para(fmt_int(s["avg_score"]), small),
                make_para(fmt(s["avg_total_deframer_synced_seconds"], 1), small),
                make_para(
                    f"{fmt(s['avg_first_deframer_sync_delay_seconds'], 1)} "
                    f"(n={s['first_sync_sample_count']})",
                    small,
                ),
                make_para(fmt(s["avg_sync_drop_count"], 2), small),
                make_para(
                    f"{fmt(s['avg_median_snr_synced'], 2)} (n={s['snr_sample_count']})",
                    small,
                ),
                make_para(
                    f"{fmt(s['avg_median_ber_synced'], 4)} (n={s['ber_sample_count']})",
                    small,
                ),
                make_para(s["setup_fingerprint"], small),
            ])
        add_table(
            story, setup_rows,
            [14, 14, 18, 16, 18, 28, 16, 24, 24, 118],
            font_size=7,
        )
        story.append(Spacer(1, 4 * mm))

        if idx != len(groups) - 1:
            story.append(PageBreak())


# --- Main --------------------------------------------------------------------

def main() -> int:
    args = parse_args()
    config_path = get_config_path(args.config)

    try:
        config = load_config(config_path)
    except ConfigError as e:
        print(f"[optimize_reception] CONFIG ERROR: {e}", file=sys.stderr)
        return 2

    try:
        settings = load_optimizer_settings(config_path, config)
    except ConfigError as e:
        print(f"[optimize_reception] CONFIG ERROR: {e}", file=sys.stderr)
        return 2

    log_dir = config.get("paths", {}).get("log_dir", "/tmp")
    setup_logging(log_dir)
    _install_signal_handlers()

    if not settings["enabled"]:
        logger.info("disabled in config; nothing to do")
        return 0

    db_path = config["paths"]["reception_db_file"]
    if not os.path.exists(db_path):
        logger.error("database not found: %s", db_path)
        return 1

    try:
        metrics_all = load_metrics_from_db(db_path)
    except RuntimeError as e:
        logger.exception("failed to load metrics: %s", e)
        return 1

    if not metrics_all:
        logger.error("no usable pass metrics found in database")
        return 1

    metrics_all = score_metrics_list(metrics_all, settings)

    groups = build_similar_pass_groups(metrics_all, settings)
    if not groups:
        logger.error("no similar-pass groups with more than one member found")
        return 1

    # Pre-filter: only keep groups that already have >= min_setups distinct
    # setups. Skipping the rest avoids launching the expensive skyplot
    # subprocess for groups that will be dropped anyway.
    filtered_groups = []
    for g in groups:
        distinct_setups = {m.setup_id for m in g}
        if len(distinct_setups) >= args.min_setups_per_group:
            filtered_groups.append(g)

    if not filtered_groups:
        logger.error("no reportable groups with at least %d setups found", args.min_setups_per_group)
        return 1

    output_dir = Path(settings["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    plot_dir: Path | None = None
    if not args.no_skyplots and not args.json_only:
        plot_dir = output_dir / "group_skyplots"
        plot_dir.mkdir(parents=True, exist_ok=True)

    base_dir = Path(__file__).resolve().parent.parent

    try:
        group_reports = evaluate_groups(
            filtered_groups, settings,
            args.min_passes_per_group, args.min_setups_per_group,
            plot_dir, base_dir,
        )
    except Exception as e:
        logger.exception("group evaluation failed: %s", e)
        return 1

    if not group_reports:
        logger.error("no group reports produced (stop requested or evaluation skipped all groups)")
        return 1

    payload = build_payload(
        metrics_all, group_reports, settings,
        args.min_passes_per_group, args.min_setups_per_group,
    )

    json_report = output_dir / REPORT_JSON_NAME
    try:
        write_report_json(str(json_report), payload)
    except OSError as e:
        logger.exception("failed to write JSON report: %s", e)
        return 1
    logger.info("wrote: %s", json_report)

    if not args.json_only:
        pdf_report = output_dir / REPORT_PDF_NAME
        try:
            write_report_pdf(str(pdf_report), payload)
        except Exception as e:
            logger.exception("failed to write PDF report: %s", e)
            return 1
        logger.info("wrote: %s", pdf_report)

    logger.info("total passes analyzed: %d", payload["total_passes_analyzed"])
    logger.info("candidate passes: %d", payload["candidate_passes"])
    logger.info("total groups: %d", payload["global_summary"]["total_groups"])
    logger.info("evaluable groups: %d", payload["global_summary"]["evaluable_groups"])

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
