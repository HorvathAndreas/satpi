#!/usr/bin/env python3
# satpi
# Predicts upcoming satellite passes for the configured ground station.
# This script uses the local filtered TLE file together with the configured
# station position, elevation limits and scheduling window to calculate which
# future passes are relevant for reception. The resulting pass data is written
# in a structured form so that later steps can generate concrete jobs from it.
# Author: Andreas Horvath
# Project: Autonomous, Config-driven satellite reception pipeline for Raspberry Pi

import json
import logging
import os
from datetime import datetime, timedelta, timezone

from load_config import load_config, ConfigError
from skyfield.api import load, wgs84


logger = logging.getLogger("satpi.predict")


def setup_logging(log_dir):
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "predict_passes.log")

    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)


def normalize_sat_name(name):
    return " ".join(name.strip().upper().replace("-", " ").replace("_", " ").split())


def isoformat_utc(dt):
    return (
        dt.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def build_satellite_map(sat_objects):
    return {normalize_sat_name(sat.name): sat for sat in sat_objects}


def load_satellites_from_tle(tle_file):
    return load.tle_file(tle_file)


def compute_passes_for_satellite(ts, observer, sat_obj, sat_cfg, start_dt, end_dt):
    t0 = ts.from_datetime(start_dt)
    t1 = ts.from_datetime(end_dt)

# Use true AOS/LOS at horizon for scheduling.
# Keep min_elevation only as a quality filter via max_elevation check below.
    t_events, events = sat_obj.find_events(
    observer,
    t0,
    t1,
    altitude_degrees=0.0,
    )

    passes = []
    current_pass = None

    for t, event in zip(t_events, events):
        dt = t.utc_datetime().replace(tzinfo=timezone.utc)

        if event == 0:
            current_pass = {
                "satellite": sat_cfg["name"],
                "start": dt,
                "end": None,
                "max_elevation": None,
                "max_elevation_time": None,
                "frequency_hz": sat_cfg["frequency"],
                "bandwidth_hz": sat_cfg["bandwidth"],
                "pipeline": sat_cfg["pipeline"],
            }

        elif event == 1:
            if current_pass is not None:
                difference = sat_obj - observer
                topocentric = difference.at(t)
                alt, az, distance = topocentric.altaz()
                current_pass["max_elevation"] = round(alt.degrees, 2)
                current_pass["max_elevation_time"] = dt

        elif event == 2:
            if current_pass is not None:
                current_pass["end"] = dt

                if (
                    current_pass["start"] is not None
                    and current_pass["end"] is not None
                    and current_pass["max_elevation"] is not None
                    and current_pass["max_elevation"] >= sat_cfg["min_elevation"]
                ):
                    passes.append({
                        "satellite": current_pass["satellite"],
                        "start": isoformat_utc(current_pass["start"]),
                        "end": isoformat_utc(current_pass["end"]),
                        "max_elevation": current_pass["max_elevation"],
                        "max_elevation_time": isoformat_utc(current_pass["max_elevation_time"]),
                        "frequency_hz": current_pass["frequency_hz"],
                        "bandwidth_hz": current_pass["bandwidth_hz"],
                        "pipeline": current_pass["pipeline"],
                    })

                current_pass = None

    return passes


def write_passes_json(pass_file, passes):
    os.makedirs(os.path.dirname(pass_file), exist_ok=True)
    tmp_file = pass_file + ".tmp"

    payload = {
        "generated_at": isoformat_utc(datetime.now(timezone.utc)),
        "pass_count": len(passes),
        "passes": sorted(passes, key=lambda p: p["start"]),
    }

    with open(tmp_file, "w") as f:
        json.dump(payload, f, indent=2)

    os.replace(tmp_file, pass_file)


def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(base_dir, "config", "config.ini")

    try:
        config = load_config(config_path)
    except ConfigError as e:
        print(f"[predict] CONFIG ERROR: {e}")
        return

    setup_logging(config["paths"]["log_dir"])

    qth = config["qth"]
    scheduling = config["scheduling"]
    network = config["network"]
    satellites = [s for s in config["satellites"] if s["enabled"]]

    tle_file = network["tle_file"]
    pass_file = config["paths"]["pass_file"]

    if not os.path.exists(tle_file):
        raise FileNotFoundError(f"TLE file not found: {tle_file}")

    logger.info("Loading TLE file: %s", tle_file)

    ts = load.timescale()
    sat_objects = load_satellites_from_tle(tle_file)
    sat_map = build_satellite_map(sat_objects)

    observer = wgs84.latlon(
        qth["latitude"],
        qth["longitude"],
        elevation_m=qth["altitude"],
    )

    start_dt = datetime.now(timezone.utc)
    end_dt = start_dt + timedelta(hours=scheduling["max_pass_age_hours"])

    logger.info(
        "Computing passes from %s until %s",
        isoformat_utc(start_dt),
        isoformat_utc(end_dt),
    )

    all_passes = []

    for sat_cfg in satellites:
        sat_key = normalize_sat_name(sat_cfg["name"])
        sat_obj = sat_map.get(sat_key)

        if sat_obj is None:
            logger.warning("Satellite not found in TLE: %s", sat_cfg["name"])
            continue

        logger.info(
            "Computing passes for %s (min_elevation=%s)",
            sat_cfg["name"],
            sat_cfg["min_elevation"],
        )

        sat_passes = compute_passes_for_satellite(
            ts,
            observer,
            sat_obj,
            sat_cfg,
            start_dt,
            end_dt,
        )

        logger.info("Found %d passes for %s", len(sat_passes), sat_cfg["name"])
        all_passes.extend(sat_passes)

    write_passes_json(pass_file, all_passes)
    logger.info("Wrote %d total passes to %s", len(all_passes), pass_file)


if __name__ == "__main__":
    main()
