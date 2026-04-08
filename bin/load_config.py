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
    config["satdump"] = _parse_satdump(parser)
    config["scheduling"] = _parse_scheduling(parser)
    config["network"] = _parse_network(parser)
    config["decode"] = _parse_decode(parser)
    config["copytarget"] = _parse_copytarget(parser)
    config["notify"] = _parse_notify(parser)
    config["debug"] = _parse_debug(parser)
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
        "log_level": p.get("station", "log_level", fallback="INFO"),
    }


def _parse_qth(p):
    return {
        "latitude": p.getfloat("qth", "latitude"),
        "longitude": p.getfloat("qth", "longitude"),
        "altitude": p.getfloat("qth", "altitude_m", fallback=0),
    }


def _parse_paths(p):
    return {
        "base_dir": p.get("paths", "base_dir"),
        "pass_file": p.get("paths", "pass_file"),
        "state_file": p.get("paths", "state_file"),
        "log_dir": p.get("paths", "log_dir"),
        "output_dir": p.get("paths", "output_dir"),
        "generated_units_dir": p.get("paths", "generated_units_dir"),
    }

def _parse_hardware(p):
    return {
        "device_index": p.getint("hardware", "device_index", fallback=0),
        "device_serial": p.get("hardware", "device_serial", fallback=None),
        "source_id": p.get("hardware", "source_id", fallback=None),
        "gain": p.getfloat("hardware", "gain", fallback=0),
        "sample_rate": float(p.get("hardware", "sample_rate", fallback="2.4e6")),
        "bias_t": p.getboolean("hardware", "bias_t", fallback=False),
        "ppm_correction": p.getint("hardware", "ppm_correction", fallback=0),
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
        })

    return satellites


def _parse_satdump(p):
    return {
        "enabled": p.getboolean("satdump", "enabled", fallback=True),
        "binary_path": p.get("satdump", "binary_path"),
        "threads": p.getint("satdump", "threads", fallback=1),
        "realtime": p.getboolean("satdump", "realtime", fallback=True),
    }

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
        "tle_file": p.get("network", "tle_file"),
        "tle_timeout": p.getint("network", "tle_timeout_seconds", fallback=30),
    }

def _parse_decode(p):
    return {
        "enabled": p.getboolean("decode", "enabled", fallback=True),
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
        "type": p.get("copytarget", "type", fallback="local"),
        "local_path": p.get("copytarget", "local_path", fallback=None),
        "remote_user": p.get("copytarget", "remote_user", fallback=None),
        "remote_host": p.get("copytarget", "remote_host", fallback=None),
        "remote_path": p.get("copytarget", "remote_path", fallback=None),
        "ssh_port": p.getint("copytarget", "ssh_port", fallback=22),
        "rclone_remote": p.get("copytarget", "rclone_remote", fallback=None),
        "rclone_path": p.get("copytarget", "rclone_path", fallback=None),
        "create_link": p.getboolean("copytarget", "create_link", fallback=False),
    }

def _parse_notify(p):
    return {
        "enabled": p.getboolean("notify", "enabled", fallback=False),
        "mail_to": p.get("notify", "mail_to", fallback=None),
        "mail_subject_prefix": p.get("notify", "mail_subject_prefix", fallback="SATPI"),
        "mail_bin": p.get("notify", "mail_bin", fallback="/usr/bin/msmtp"),
    }

def _parse_debug(p):
    return {
        "dry_run": p.getboolean("debug", "dry_run", fallback=False),
        "verbose": p.getboolean("debug", "verbose_logging", fallback=False),
    }

def _parse_systemd(p):
    return {
        "service_user": p.get("systemd", "service_user", fallback=None),
        "python_bin": p.get("systemd", "python_bin", fallback="/usr/bin/python3"),
    }

def _parse_reception_setup(p):
    return {
        "antenna_type": p.get("reception_setup", "antenna_type", fallback=""),
        "antenna_location": p.get("reception_setup", "antenna_location", fallback=""),
        "antenna_orientation": p.get("reception_setup", "antenna_orientation", fallback=""),
        "lna": p.get("reception_setup", "lna", fallback=""),
        "rf_filter": p.get("reception_setup", "rf_filter", fallback=""),
        "feedline": p.get("reception_setup", "feedline", fallback=""),
        "raspberry_pi": p.get("reception_setup", "raspberry_pi", fallback=""),
        "power_supply": p.get("reception_setup", "power_supply", fallback=""),
        "additional_info": p.get("reception_setup", "additional_info", fallback=""),
    }

def _parse_optimize_reception(p):
    return {
        "enabled": p.getboolean("optimize_reception", "enabled", fallback=False),
        "apply_changes": p.getboolean("optimize_reception", "apply_changes", fallback=False),
        "write_suggested_config": p.getboolean("optimize_reception", "write_suggested_config", fallback=True),
        "satellite": p.get("optimize_reception", "satellite", fallback=""),
        "pipeline": p.get("optimize_reception", "pipeline", fallback=""),
        "min_max_elevation_deg": p.getfloat("optimize_reception", "min_max_elevation_deg", fallback=30.0),
        "max_max_elevation_delta_deg": p.getfloat("optimize_reception", "max_max_elevation_delta_deg", fallback=10.0),
        "same_pass_direction_only": p.getboolean("optimize_reception", "same_pass_direction_only", fallback=True),
        "evaluation_min_elevation_deg": p.getfloat("optimize_reception", "evaluation_min_elevation_deg", fallback=10.0),
        "evaluation_max_elevation_deg": p.getfloat("optimize_reception", "evaluation_max_elevation_deg", fallback=85.0),
        "min_passes_per_gain": p.getint("optimize_reception", "min_passes_per_gain", fallback=2),
        "min_total_passes": p.getint("optimize_reception", "min_total_passes", fallback=4),
        "weight_deframer_synced_seconds": p.getfloat("optimize_reception", "weight_deframer_synced_seconds", fallback=1.0),
        "weight_first_deframer_sync_delay": p.getfloat("optimize_reception", "weight_first_deframer_sync_delay", fallback=-0.4),
        "weight_sync_drop_count": p.getfloat("optimize_reception", "weight_sync_drop_count", fallback=-0.5),
        "weight_median_snr_synced": p.getfloat("optimize_reception", "weight_median_snr_synced", fallback=0.3),
        "weight_median_ber_synced": p.getfloat("optimize_reception", "weight_median_ber_synced", fallback=-0.8),
        "output_dir": p.get(
            "optimize_reception",
            "output_dir",
            fallback="/home/andreas/satpi/results/optimization",
        ),
    }

def _parse_optimize_reception_ai(p):
    return {
        "enabled": p.getboolean("optimize_reception_ai", "enabled", fallback=False),
        "max_passes": p.getint("optimize_reception_ai", "max_passes", fallback=25),
        "model": p.get("optimize_reception_ai", "model", fallback="gpt-5"),
        "output_file": p.get(
            "optimize_reception_ai",
            "output_file",
            fallback="/home/andreas/satpi/results/optimization/optimization-report-ai.txt",
        ),
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

    # Paths must exist (except files)
    for key in ["base_dir", "log_dir", "output_dir"]:
        path = cfg["paths"][key]
        if not os.path.isdir(path):
            raise ConfigError(f"Directory does not exist: {path}")

    # SatDump binary check
    if cfg["satdump"]["enabled"]:
        binary_path = cfg["satdump"]["binary_path"]
        if not os.path.exists(binary_path):
            raise ConfigError(f"SatDump binary_path not found: {binary_path}")

