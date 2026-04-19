#!/usr/bin/env python3
# satpi
# Loads, parses and validates the central satpi configuration file.
# This module converts configuration values into typed Python data structures
# and performs consistency checks so that the operational scripts fail early
# and with clear error messages if required settings are missing or invalid.
# Author: Andreas Horvath
# Project: Autonomous, Config-driven satellite reception pipeline for Raspberry Pi

import configparser
import os
from typing import Dict, Any, List


class ConfigError(Exception):
    pass


def _resolve_path(base_dir: str, value: str) -> str:
    value = value.strip()
    if os.path.isabs(value):
        return value
    return os.path.abspath(os.path.join(base_dir, value))


def load_config(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise ConfigError(f"Config file not found: {path}")

    parser = configparser.ConfigParser(inline_comment_prefixes=(";", "#"))
    parser.read(path)

    config = {}

    config["station"] = _parse_station(parser)
    config["qth"] = _parse_qth(parser)
    config["paths"] = _parse_paths(parser)
    config["hardware"] = _parse_hardware(parser)
    config["satellites"] = _parse_satellites(parser)
    config["scheduling"] = _parse_scheduling(parser)
    config["network"] = _parse_network(parser)
    config["decode"] = _parse_decode(parser)
    config["copytarget"] = _parse_copytarget(parser)
    config["notify"] = _parse_notify(parser)
    config["systemd"] = _parse_systemd(parser)
    config["reception_setup"] = _parse_reception_setup(parser)
    config["optimize_reception"] = _parse_optimize_reception(parser)
    config["optimize_reception_ai"] = _parse_optimize_reception_ai(parser)

    _validate_config(config)
    return config


# ==============================
# SECTION PARSERS
# ==============================

def _parse_station(p):
    return {
        "name": p.get("station", "name", fallback="satpi"),
        "timezone": p.get("station", "timezone", fallback="UTC"),
    }


def _parse_qth(p):
    return {
        "latitude": p.getfloat("qth", "latitude"),
        "longitude": p.getfloat("qth", "longitude"),
        "altitude": p.getfloat("qth", "altitude_m", fallback=0),
    }


def _parse_paths(p):
    base_dir = os.path.abspath(p.get("paths", "base_dir").strip())

    return {
        "base_dir": base_dir,
        "pass_file": _resolve_path(base_dir, p.get("paths", "pass_file")),
        "log_dir": _resolve_path(base_dir, p.get("paths", "log_dir")),
        "output_dir": _resolve_path(base_dir, p.get("paths", "output_dir")),
        "generated_units_dir": _resolve_path(base_dir, p.get("paths", "generated_units_dir")),
        "tle_file": _resolve_path(base_dir, p.get("paths", "tle_file")),
        "optimization_dir": _resolve_path(base_dir, p.get("paths", "optimization_dir")),
        "optimization_ai_report_file": _resolve_path(
            base_dir,
            p.get("paths", "optimization_ai_report_file"),
        ),
        "reception_db_file": _resolve_path(base_dir, p.get("paths", "reception_db_file")),
        "satdump_bin": p.get("paths", "satdump_bin").strip(),
        "mail_bin": p.get("paths", "mail_bin").strip(),
        "python_bin": p.get("paths", "python_bin").strip(),
    }


def _parse_hardware(p):
    return {
        "source_id": p.get("hardware", "source_id", fallback=None),
        "gain": p.getfloat("hardware", "gain", fallback=0),
        "sample_rate": float(p.get("hardware", "sample_rate", fallback="2.4e6")),
        "bias_t": p.getboolean("hardware", "bias_t", fallback=False),
    }


def _parse_satellites(p) -> List[Dict[str, Any]]:
    satellites = []

    for section in p.sections():
        if not section.startswith("satellite."):
            continue

        name = section.split(".", 1)[1]
        s = p[section]

        satellites.append({
            "name": name,
            "enabled": s.getboolean("enabled", fallback=True),
            "min_elevation": s.getint("min_elevation_deg", fallback=0),
            "frequency": s.getint("frequency_hz"),
            "bandwidth": s.getint("bandwidth_hz"),
            "pipeline": s.get("pipeline"),
            "pass_direction": s.get("pass_direction", fallback="all").strip().lower(),
        })

    return satellites

def _parse_scheduling(p):
    return {
        "frequency": p.get("scheduling", "pass_update_frequency", fallback="DAILY"),
        "time": p.get("scheduling", "pass_update_time", fallback="00:00"),
        "weekday": p.get("scheduling", "pass_update_weekday", fallback="MONDAY"),
        "pre_start": p.getint("scheduling", "pre_start_seconds", fallback=120),
        "post_stop": p.getint("scheduling", "post_stop_seconds", fallback=60),
        "max_pass_age_hours": p.getint("scheduling", "max_pass_age_hours", fallback=24),
    }


def _parse_network(p):
    return {
        "tle_url": p.get("network", "tle_url"),
        "tle_timeout": p.getint("network", "tle_timeout_seconds", fallback=30),
    }


def _parse_decode(p):
    return {
        "min_cadu_size_bytes": p.getint("decode", "min_cadu_size_bytes", fallback=1048576),
        "success_dir_relpath": p.get(
            "decode",
            "success_dir_relpath",
            fallback="MSU-MR",
        ),
    }


def _parse_copytarget(p):
    return {
        "enabled": p.getboolean("copytarget", "enabled", fallback=False),
        "type": p.get("copytarget", "type", fallback="rclone"),
        "rclone_remote": p.get("copytarget", "rclone_remote", fallback=None),
        "rclone_path": p.get("copytarget", "rclone_path", fallback=None),
        "create_link": p.getboolean("copytarget", "create_link", fallback=False),
    }


def _parse_notify(p):
    return {
        "enabled": p.getboolean("notify", "enabled", fallback=False),
        "mail_to": p.get("notify", "mail_to", fallback=None),
        "mail_subject_prefix": p.get("notify", "mail_subject_prefix", fallback="SATPI"),
    }


def _parse_systemd(p):
    return {
        "service_user": p.get("systemd", "service_user", fallback=None),
    }


def _parse_reception_setup(p):
    return {
        "antenna_type": p.get("reception_setup", "antenna_type", fallback=""),
        "antenna_location": p.get("reception_setup", "antenna_location", fallback=""),
        "antenna_orientation": p.get("reception_setup", "antenna_orientation", fallback=""),
        "lna": p.get("reception_setup", "lna", fallback=""),
        "rf_filter": p.get("reception_setup", "rf_filter", fallback=""),
        "feedline": p.get("reception_setup", "feedline", fallback=""),
        "sdr": p.get("reception_setup", "sdr", fallback=""),
        "raspberry_pi": p.get("reception_setup", "raspberry_pi", fallback=""),
        "power_supply": p.get("reception_setup", "power_supply", fallback=""),
        "additional_info": p.get("reception_setup", "additional_info", fallback=""),
    }


def _parse_optimize_reception(p):
    return {
        "enabled": p.getboolean("optimize_reception", "enabled", fallback=False),
        "max_delta_aos_azimuth": p.getfloat("optimize_reception", "max_delta_aos_azimuth", fallback=20.0),
        "max_delta_los_azimuth": p.getfloat("optimize_reception", "max_delta_los_azimuth", fallback=20.0),
        "max_delta_culmination_azimuth": p.getfloat(
            "optimize_reception",
            "max_delta_culmination_azimuth",
            fallback=15.0,
        ),
        "max_delta_culmination_elevation": p.getfloat(
            "optimize_reception",
            "max_delta_culmination_elevation",
            fallback=10.0,
        ),
        "min_total_passes": p.getint("optimize_reception", "min_total_passes", fallback=4),
        "weight_deframer_synced_seconds": p.getfloat(
            "optimize_reception",
            "weight_deframer_synced_seconds",
            fallback=1.0,
        ),
        "weight_first_deframer_sync_delay": p.getfloat(
            "optimize_reception",
            "weight_first_deframer_sync_delay",
            fallback=-0.4,
        ),
        "weight_sync_drop_count": p.getfloat(
            "optimize_reception",
            "weight_sync_drop_count",
            fallback=-0.5,
        ),
        "weight_median_snr_synced": p.getfloat(
            "optimize_reception",
            "weight_median_snr_synced",
            fallback=0.3,
        ),
        "weight_median_ber_synced": p.getfloat(
            "optimize_reception",
            "weight_median_ber_synced",
            fallback=-0.8,
        ),
    }


def _parse_optimize_reception_ai(p):
    return {
        "enabled": p.getboolean("optimize_reception_ai", "enabled", fallback=False),
        "max_passes": p.getint("optimize_reception_ai", "max_passes", fallback=25),
        "model": p.get("optimize_reception_ai", "model", fallback="gpt-5"),
        "include_optimizer_report": p.getboolean(
            "optimize_reception_ai",
            "include_optimizer_report",
            fallback=True,
        ),
        "temperature": p.getfloat(
            "optimize_reception_ai",
            "temperature",
            fallback=1.0,
        ),
        "api_key": p.get("optimize_reception_ai", "api_key", fallback="").strip(),
    }

# ==============================
# VALIDATION
# ==============================

def _validate_config(cfg: Dict[str, Any]):
    if not cfg["satellites"]:
        raise ConfigError("No satellites defined")

    active = [s for s in cfg["satellites"] if s["enabled"]]
    if not active:
        raise ConfigError("No enabled satellites")

    for sat in active:
        if sat["frequency"] <= 0:
            raise ConfigError(f"Invalid frequency for {sat['name']}")

    valid_directions = {
        "all",
        "north_to_south",
        "south_to_north",
        "west_to_east",
        "east_to_west",
        "southwest_to_northeast",
        "southeast_to_northwest",
        "northwest_to_southeast",
        "northeast_to_southwest",
    }

    for sat in cfg["satellites"]:
        if sat["pass_direction"] not in valid_directions:
            raise ConfigError(
                f"Invalid pass direction for {sat['name']}: {sat['pass_direction']}"
            )

    for key in [
        "base_dir",
        "log_dir",
        "output_dir",
        "generated_units_dir",
        "optimization_dir",
    ]:
        path = cfg["paths"][key]
        if not os.path.isdir(path):
            raise ConfigError(f"Directory does not exist: {path}")

    base_dir = cfg["paths"]["base_dir"]
    if not os.path.isdir(base_dir):
        raise ConfigError(f"Base directory does not exist: {base_dir}")

    satdump_bin = cfg["paths"]["satdump_bin"]
    if not os.path.exists(satdump_bin):
        raise ConfigError(f"SatDump binary not found: {satdump_bin}")

    mail_bin = cfg["paths"]["mail_bin"]
    if not os.path.exists(mail_bin):
        raise ConfigError(f"Mail binary not found: {mail_bin}")

    python_bin = cfg["paths"]["python_bin"]
    if not os.path.exists(python_bin):
        raise ConfigError(f"Python binary not found: {python_bin}")
