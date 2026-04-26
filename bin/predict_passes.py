#!/usr/bin/env python3
"""satpi – predict_passes

Predicts upcoming satellite passes for the configured ground station.

Uses the local filtered TLE file together with the configured station position,
elevation limits and scheduling window to calculate which future passes are
relevant for reception. The resulting pass data is written in a structured form
so that later steps can generate concrete jobs from it.

Author: Andreas Horvath
Project: Autonomous, config-driven satellite reception pipeline for Raspberry Pi
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from skyfield.api import Loader, wgs84
from skyfield.sgp4lib import EarthSatellite

from load_config import load_config, ConfigError


# --- Constants ---------------------------------------------------------------

LOG_MAX_BYTES = 1_000_000
LOG_BACKUP_COUNT = 5

# Skyfield data (leap seconds etc.) — keep local so we don't need network.
SKYFIELD_DATA_DIR = os.environ.get(
    "SATPI_SKYFIELD_DATA",
    os.path.join(os.path.expanduser("~"), ".cache", "satpi", "skyfield"),
)

logger = logging.getLogger("satpi.predict")


# --- Logging -----------------------------------------------------------------

def setup_logging(log_dir: str) -> None:
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "predict_passes.log")

    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    file_handler = RotatingFileHandler(
        log_file, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)


# --- Helpers -----------------------------------------------------------------

def normalize_sat_name(name: str) -> str:
    return " ".join(name.strip().upper().replace("-", " ").replace("_", " ").split())


def isoformat_utc(dt: datetime) -> str:
    return (
        dt.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def build_satellite_map(sat_objects: Sequence[EarthSatellite]) -> Dict[str, EarthSatellite]:
    sat_map: Dict[str, EarthSatellite] = {}
    duplicates: Dict[str, int] = {}
    for sat in sat_objects:
        key = normalize_sat_name(sat.name)
        if key in sat_map:
            duplicates[key] = duplicates.get(key, 1) + 1
        sat_map[key] = sat  # last wins — Celestrak occasionally duplicates
    for key, count in duplicates.items():
        logger.debug("Satellite %s appears %d times in TLE; using the last one.", key, count)
    return sat_map


# --- Pass direction ----------------------------------------------------------

def azimuth_to_cardinal(az_deg: float) -> str:
    az = az_deg % 360.0
    if az >= 337.5 or az < 22.5:
        return "north"
    if az < 67.5:
        return "northeast"
    if az < 112.5:
        return "east"
    if az < 157.5:
        return "southeast"
    if az < 202.5:
        return "south"
    if az < 247.5:
        return "southwest"
    if az < 292.5:
        return "west"
    return "northwest"


def derive_pass_direction(aos_azimuth_deg: float, los_azimuth_deg: float) -> str:
    """Return an 8-way direction label describing how the pass moved across the sky.

    For truly lateral passes where AOS and LOS fall in the same cardinal octant,
    we fall back to the signed azimuth difference to infer east/west motion.
    """
    start_dir = azimuth_to_cardinal(aos_azimuth_deg)
    end_dir = azimuth_to_cardinal(los_azimuth_deg)

    direct_pairs = {
        ("north", "south"): "north_to_south",
        ("south", "north"): "south_to_north",
        ("west", "east"): "west_to_east",
        ("east", "west"): "east_to_west",
        ("southwest", "northeast"): "southwest_to_northeast",
        ("southeast", "northwest"): "southeast_to_northwest",
        ("northwest", "southeast"): "northwest_to_southeast",
        ("northeast", "southwest"): "northeast_to_southwest",
    }
    if (start_dir, end_dir) in direct_pairs:
        return direct_pairs[(start_dir, end_dir)]

    start_x = 1 if "east" in start_dir else -1 if "west" in start_dir else 0
    start_y = 1 if "north" in start_dir else -1 if "south" in start_dir else 0
    end_x = 1 if "east" in end_dir else -1 if "west" in end_dir else 0
    end_y = 1 if "north" in end_dir else -1 if "south" in end_dir else 0

    dx = end_x - start_x
    dy = end_y - start_y

    if abs(dx) > abs(dy):
        return "west_to_east" if dx > 0 else "east_to_west"
    if abs(dy) > abs(dx):
        return "south_to_north" if dy > 0 else "north_to_south"

    if dx != 0 and dy != 0:
        if dx > 0 and dy > 0:
            return "southwest_to_northeast"
        if dx < 0 and dy > 0:
            return "southeast_to_northwest"
        if dx > 0 and dy < 0:
            return "northwest_to_southeast"
        return "northeast_to_southwest"

    # Same cardinal for AOS and LOS (short lateral pass): use signed az-delta.
    delta = (los_azimuth_deg - aos_azimuth_deg + 540) % 360 - 180  # in (-180, 180]
    if abs(delta) < 1e-6:
        return "lateral"
    return "west_to_east" if delta > 0 else "east_to_west"


# --- Pass computation --------------------------------------------------------

def _finalize_pass(
    current_pass: Dict[str, Any],
    sat_cfg: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Convert an accumulated pass dict into the JSON-ready record, or None if filtered."""
    if (
        current_pass["start"] is None
        or current_pass["end"] is None
        or current_pass["max_elevation"] is None
    ):
        return None
    if current_pass["max_elevation"] < sat_cfg["min_elevation"]:
        return None
    if current_pass["aos_azimuth_deg"] is None or current_pass["los_azimuth_deg"] is None:
        return None

    return {
        "satellite": current_pass["satellite"],
        "start": isoformat_utc(current_pass["start"]),
        "end": isoformat_utc(current_pass["end"]),
        "max_elevation": current_pass["max_elevation"],
        "max_elevation_time": isoformat_utc(current_pass["max_elevation_time"]),
        "aos_azimuth_deg": current_pass["aos_azimuth_deg"],
        "los_azimuth_deg": current_pass["los_azimuth_deg"],
        "direction": derive_pass_direction(
            current_pass["aos_azimuth_deg"],
            current_pass["los_azimuth_deg"],
        ),
        "frequency_hz": current_pass["frequency_hz"],
        "bandwidth_hz": current_pass["bandwidth_hz"],
        "pipeline": current_pass["pipeline"],
    }


def _new_pass(sat_cfg: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "satellite": sat_cfg["name"],
        "start": None,
        "end": None,
        "max_elevation": None,
        "max_elevation_time": None,
        "aos_azimuth_deg": None,
        "los_azimuth_deg": None,
        "frequency_hz": sat_cfg["frequency"],
        "bandwidth_hz": sat_cfg["bandwidth"],
        "pipeline": sat_cfg["pipeline"],
    }


def compute_passes_for_satellite(
    ts,
    observer,
    sat_obj: EarthSatellite,
    sat_cfg: Dict[str, Any],
    start_dt: datetime,
    end_dt: datetime,
) -> List[Dict[str, Any]]:
    """Compute all passes of *sat_obj* between start_dt and end_dt that meet sat_cfg limits."""
    t0 = ts.from_datetime(start_dt)
    t1 = ts.from_datetime(end_dt)

    # Horizon-based AOS/LOS for honest azimuths; quality filter via max_elevation below.
    t_events, events = sat_obj.find_events(
        observer, t0, t1, altitude_degrees=0.0
    )

    # Compute the relative vector once per satellite rather than per event.
    difference = sat_obj - observer

    passes: List[Dict[str, Any]] = []
    current_pass: Optional[Dict[str, Any]] = None
    skipped_partial = 0

    for t, event in zip(t_events, events):
        dt = t.utc_datetime()  # already UTC-aware
        alt, az, _ = difference.at(t).altaz()

        if event == 0:  # rise (AOS)
            current_pass = _new_pass(sat_cfg)
            current_pass["start"] = dt
            current_pass["aos_azimuth_deg"] = round(az.degrees, 2)

        elif event == 1:  # culmination
            if current_pass is None:
                # Window began mid-pass — no AOS available; can't schedule retroactively.
                skipped_partial += 1
                continue
            current_pass["max_elevation"] = round(alt.degrees, 2)
            current_pass["max_elevation_time"] = dt

        elif event == 2:  # set (LOS)
            if current_pass is None:
                skipped_partial += 1
                continue
            current_pass["end"] = dt
            current_pass["los_azimuth_deg"] = round(az.degrees, 2)

            record = _finalize_pass(current_pass, sat_cfg)
            if record is not None:
                passes.append(record)
            current_pass = None

    if current_pass is not None:
        # Pass still open at end of window — no LOS observed.
        skipped_partial += 1

    if skipped_partial:
        logger.debug(
            "Skipped %d partial pass event(s) for %s (outside time window).",
            skipped_partial, sat_cfg["name"],
        )

    return passes


# --- JSON output -------------------------------------------------------------

def write_passes_json(pass_file: str, passes: Sequence[Dict[str, Any]]) -> None:
    pass_dir = os.path.dirname(pass_file)
    if pass_dir:
        os.makedirs(pass_dir, exist_ok=True)

    tmp_file = pass_file + ".tmp"
    payload = {
        "generated_at": isoformat_utc(datetime.now(timezone.utc)),
        "pass_count": len(passes),
        "passes": sorted(passes, key=lambda p: p["start"]),
    }

    try:
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
            f.write("\n")
        os.replace(tmp_file, pass_file)
    finally:
        if os.path.exists(tmp_file):
            try:
                os.remove(tmp_file)
            except OSError:
                pass


# --- Main --------------------------------------------------------------------

def _prediction_window_hours(scheduling: Dict[str, Any]) -> int:
    """Read the prediction horizon from config, accepting the legacy name.

    The legacy key `max_pass_age_hours` is misleading — the value is the
    *future* horizon for pass prediction, not the age of stale data. Prefer
    `prediction_window_hours` going forward; we keep the old key as fallback.
    """
    if "prediction_window_hours" in scheduling:
        return int(scheduling["prediction_window_hours"])
    if "max_pass_age_hours" in scheduling:
        logger.warning(
            "scheduling.max_pass_age_hours is deprecated; "
            "rename it to scheduling.prediction_window_hours in your config."
        )
        return int(scheduling["max_pass_age_hours"])
    raise ConfigError("scheduling.prediction_window_hours is required")


def main() -> int:
    base_dir = Path(__file__).resolve().parent.parent
    config_path = base_dir / "config" / "config.ini"

    try:
        config = load_config(str(config_path))
    except ConfigError as e:
        print(f"[predict] CONFIG ERROR: {e}", file=sys.stderr)
        return 2

    setup_logging(config["paths"]["log_dir"])

    try:
        qth = config["qth"]
        scheduling = config["scheduling"]
        paths = config["paths"]
        satellites = [s for s in config["satellites"] if s["enabled"]]

        tle_file = paths["tle_file"]
        pass_file = paths["pass_file"]
        window_hours = _prediction_window_hours(scheduling)
    except (KeyError, ConfigError) as e:
        logger.error("Config error: %s", e)
        return 2

    if not satellites:
        logger.error("No enabled satellites in config – nothing to do.")
        return 2

    if not os.path.exists(tle_file):
        logger.error("TLE file not found: %s", tle_file)
        return 1

    logger.info("Loading TLE file: %s", tle_file)

    # Keep Skyfield's leap-second / ephemeris data local (no network at runtime).
    os.makedirs(SKYFIELD_DATA_DIR, exist_ok=True)
    sf_load = Loader(SKYFIELD_DATA_DIR)
    try:
        ts = sf_load.timescale()
    except Exception:
        logger.warning("Skyfield timescale download failed; falling back to builtin data.")
        ts = sf_load.timescale(builtin=True)

    sat_objects = sf_load.tle_file(tle_file)
    sat_map = build_satellite_map(sat_objects)

    observer = wgs84.latlon(
        qth["latitude"],
        qth["longitude"],
        elevation_m=qth["altitude"],
    )

    start_dt = datetime.now(timezone.utc)
    end_dt = start_dt + timedelta(hours=window_hours)

    logger.info(
        "Computing passes from %s until %s (window=%dh)",
        isoformat_utc(start_dt), isoformat_utc(end_dt), window_hours,
    )

    all_passes: List[Dict[str, Any]] = []

    for sat_cfg in satellites:
        sat_key = normalize_sat_name(sat_cfg["name"])
        sat_obj = sat_map.get(sat_key)
        if sat_obj is None:
            logger.warning("Satellite not found in TLE: %s", sat_cfg["name"])
            continue

        logger.info(
            "Computing passes for %s (min_elevation=%.1f°)",
            sat_cfg["name"], float(sat_cfg["min_elevation"]),
        )

        sat_passes = compute_passes_for_satellite(
            ts, observer, sat_obj, sat_cfg, start_dt, end_dt
        )
        logger.info("Found %d passes for %s", len(sat_passes), sat_cfg["name"])
        all_passes.extend(sat_passes)

    if not all_passes:
        logger.warning(
            "No passes matched the configured constraints in the %dh window. "
            "Consider lowering min_elevation or extending the window.",
            window_hours,
        )

    write_passes_json(pass_file, all_passes)
    logger.info("Wrote %d total passes to %s", len(all_passes), pass_file)
    return 0


if __name__ == "__main__":
    sys.exit(main())
