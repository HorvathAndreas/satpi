#!/usr/bin/env python3
"""satpi – load_config

Loads, parses and validates the central satpi configuration file.

Converts configuration values into typed Python data structures and performs
consistency checks so that the operational scripts fail early and with clear
error messages if required settings are missing or invalid.

Author: Andreas Horvath
Project: Autonomous, config-driven satellite reception pipeline for Raspberry Pi
"""

from __future__ import annotations

import argparse
import configparser
import json
import os
import sys
from typing import Any, Dict, List, Optional, Set


class ConfigError(Exception):
    pass



def _parse_bandwidth(value: str) -> int:
    """Parse bandwidth from various formats (Hz, kHz, MHz) to Hz.

    Accepts: "1000000", "1000 kHz", "1000kHz", "1 MHz", "1.2MHz"
    Returns: bandwidth in Hz
    """
    value = value.strip()

    # Try to parse with suffix
    for suffix, multiplier in [("MHz", 1_000_000), ("kHz", 1_000), ("Hz", 1)]:
        # Case-insensitive check
        if value.upper().endswith(suffix.upper()):
            try:
                num_str = value[:-len(suffix)].strip()
                return int(float(num_str) * multiplier)
            except ValueError:
                pass

    # Try to parse as plain number (assume Hz)
    try:
        return int(float(value))
    except ValueError:
        raise ValueError(f"Invalid bandwidth format: {value}")

KNOWN_KEYS: Dict[str, Set[str]] = {
    "station": {"name", "timezone"},
    "qth": {"latitude", "longitude", "altitude_m"},
    "paths": {
        "base_dir", "pass_file", "log_dir", "output_dir",
        "generated_units_dir", "tle_file", "optimization_dir",
        "optimization_ai_report_file", "reception_db_file",
        "satdump_bin", "mail_bin", "python_bin",
    },
    "hardware": {"source_id", "gain", "sample_rate", "bias_t"},
    "scheduling": {
        "pass_update_frequency", "pass_update_time", "pass_update_weekday",
        "pre_start_seconds", "post_stop_seconds", "pass_max_prediction_hours",
    },
    "network": {"tle_url", "tle_timeout_seconds", "api_key", "tle_format"},
    "decode": {"min_cadu_size_bytes", "success_dir_relpath"},
    "copytarget": {"enabled", "type", "rclone_remote", "rclone_path", "create_link"},
    "notify": {"enabled", "mail_to", "mail_subject_prefix"},
    "systemd": {"service_user"},
    "reception_setup": {
        "antenna_type", "antenna_location", "antenna_orientation",
        "lna", "rf_filter", "feedline", "sdr", "raspberry_pi",
        "power_supply", "additional_info",
    },
    "optimize_reception": {
        "enabled",
        "max_delta_aos_azimuth", "max_delta_los_azimuth",
        "max_delta_culmination_azimuth", "max_delta_culmination_elevation",
        "min_total_passes",
        "weight_deframer_synced_seconds", "weight_first_deframer_sync_delay",
        "weight_sync_drop_count", "weight_median_snr_synced",
        "weight_median_ber_synced",
        "elevation_band_1_max", "elevation_band_2_max", "elevation_band_3_max",
        "elevation_band_4_max", "elevation_band_5_max",
        "output_dir",
    },
    "optimize_reception_ai": {
        "enabled", "max_passes", "model", "include_optimizer_report",
        "temperature", "api_key",
    },
    "noise_floor": {
        "measurement_duration", "schedule_minute",
        "center_freq_mhz", "bandwidth_mhz", "bin_size_khz",
        "freq_start_mhz", "freq_end_mhz",
        "upload_enabled", "rclone_remote", "rclone_path", "create_link",
    },
}

SATELLITE_KEYS: Set[str] = {
    "enabled", "min_elevation_deg", "frequency_hz", "bandwidth_hz",
    "pipeline", "pass_direction", "norad_id",
}

VALID_DIRECTIONS: Set[str] = {
    "all", "north_to_south", "south_to_north",
    "west_to_east", "east_to_west",
    "southwest_to_northeast", "southeast_to_northwest",
    "northwest_to_southeast", "northeast_to_southwest",
}

VALID_SCHEDULING_FREQUENCIES: Set[str] = {"HOURLY", "DAILY", "WEEKLY"}
VALID_WEEKDAYS: Set[str] = {
    "MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY",
    "FRIDAY", "SATURDAY", "SUNDAY",
}


def _resolve_path(base_dir: str, value: str) -> str:
    value = value.strip()
    if os.path.isabs(value):
        return value
    return os.path.abspath(os.path.join(base_dir, value))


def _check_unknown_keys(parser: configparser.ConfigParser, errors: List[str]) -> None:
    for section in parser.sections():
        if section.startswith("satellite."):
            allowed = SATELLITE_KEYS
        else:
            allowed = KNOWN_KEYS.get(section)
            if allowed is None:
                errors.append(f"Unknown config section: [{section}]")
                continue
        actual = set(parser.options(section))
        extra = actual - allowed
        if extra:
            errors.append(f"Unknown keys in [{section}]: {', '.join(sorted(extra))}")


def load_config(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise ConfigError(f"Config file not found: {path}")

    parser = configparser.ConfigParser(
        inline_comment_prefixes=(";", "#"),
        interpolation=None,
    )
    try:
        with open(path, "r", encoding="utf-8") as f:
            parser.read_file(f)
    except OSError as e:
        raise ConfigError(f"Cannot read config {path}: {e}") from e
    except configparser.Error as e:
        raise ConfigError(f"Invalid config syntax in {path}: {e}") from e

    errors: List[str] = []
    _check_unknown_keys(parser, errors)

    cfg: Dict[str, Any] = {}
    try:
        cfg["station"] = _parse_station(parser)
        cfg["qth"] = _parse_qth(parser, errors)
        cfg["paths"] = _parse_paths(parser)
        cfg["hardware"] = _parse_hardware(parser)
        cfg["satellites"] = _parse_satellites(parser, errors)
        cfg["scheduling"] = _parse_scheduling(parser, errors)
        cfg["network"] = _parse_network(parser, errors)
        cfg["decode"] = _parse_decode(parser)
        cfg["copytarget"] = _parse_copytarget(parser)
        cfg["notify"] = _parse_notify(parser)
        cfg["systemd"] = _parse_systemd(parser)
        cfg["reception_setup"] = _parse_reception_setup(parser)
        cfg["optimize_reception"] = _parse_optimize_reception(parser)
        cfg["optimize_reception_ai"] = _parse_optimize_reception_ai(parser)
        if parser.has_section("noise_floor"):
            cfg["noise_floor"] = _parse_noise_floor(parser)
    except Exception as e:
        errors.append(f"Error: {e}")

    _validate_config(cfg, errors)

    if errors:
        joined = "\n  - ".join(errors)
        raise ConfigError(f"Config problems:\n  - {joined}")

    return cfg


def _parse_station(p: configparser.ConfigParser) -> Dict[str, Any]:
    return {
        "name": p.get("station", "name", fallback="satpi"),
        "timezone": p.get("station", "timezone", fallback="UTC"),
    }


def _parse_qth(p: configparser.ConfigParser, errors: List[str]) -> Dict[str, Any]:
    lat = p.getfloat("qth", "latitude")
    lon = p.getfloat("qth", "longitude")
    alt = p.getfloat("qth", "altitude_m", fallback=0.0)
    return {"latitude": lat, "longitude": lon, "altitude": alt}


def _parse_paths(p: configparser.ConfigParser) -> Dict[str, Any]:
    base_dir = os.path.abspath(p.get("paths", "base_dir").strip())
    def rel(key: str) -> str:
        return _resolve_path(base_dir, p.get("paths", key))
    return {
        "base_dir": base_dir,
        "pass_file": rel("pass_file"),
        "log_dir": rel("log_dir"),
        "output_dir": rel("output_dir"),
        "generated_units_dir": rel("generated_units_dir"),
        "tle_file": rel("tle_file"),
        "optimization_dir": rel("optimization_dir"),
        "optimization_ai_report_file": rel("optimization_ai_report_file"),
        "reception_db_file": rel("reception_db_file"),
        "satdump_bin": _resolve_path(base_dir, p.get("paths", "satdump_bin")),
        "mail_bin": _resolve_path(base_dir, p.get("paths", "mail_bin")),
        "python_bin": _resolve_path(base_dir, p.get("paths", "python_bin")),
    }


def _parse_hardware(p: configparser.ConfigParser) -> Dict[str, Any]:
    return {
        "source_id": p.get("hardware", "source_id", fallback=None),
        "gain": p.getfloat("hardware", "gain", fallback=0.0),
        "sample_rate": p.getfloat("hardware", "sample_rate", fallback=2.4e6),
        "bias_t": p.getboolean("hardware", "bias_t", fallback=False),
    }


def _parse_satellites(p: configparser.ConfigParser, errors: List[str]) -> List[Dict[str, Any]]:
    satellites: List[Dict[str, Any]] = []
    for section in p.sections():
        if not section.startswith("satellite."):
            continue
        name = section.split(".", 1)[1]
        s = p[section]
        try:
            freq = s.getint("frequency_hz")
            bw_str = s.get("bandwidth_hz", s.get("bandwidth", ""))
            if not bw_str:
                errors.append(f"satellite '{name}': bandwidth_hz or bandwidth is required")
                continue
            try:
                bw = _parse_bandwidth(bw_str)
            except ValueError as e:
                errors.append(f"satellite '{name}': {e}")
                continue
            pipeline = s.get("pipeline")
        except (configparser.NoOptionError, ValueError) as e:
            errors.append(f"satellite '{name}': {e}")
            continue
        if pipeline is None or not pipeline.strip():
            errors.append(f"satellite '{name}': pipeline is required")
            continue
        direction = s.get("pass_direction", fallback="all").strip().lower()
        satellites.append({
            "name": name,
            "enabled": s.getboolean("enabled", fallback=True),
            "min_elevation": s.getint("min_elevation_deg", fallback=0),
            "frequency": freq,
            "bandwidth": bw,
            "pipeline": pipeline.strip(),
            "pass_direction": direction,
            "norad_id": s.getint("norad_id", fallback=0),
        })
    return satellites


def _parse_scheduling(p: configparser.ConfigParser, errors: List[str]) -> Dict[str, Any]:
    window = p.getint("scheduling", "pass_max_prediction_hours", fallback=24)
    pre_start = p.getint("scheduling", "pre_start_seconds", fallback=120)
    post_stop = p.getint("scheduling", "post_stop_seconds", fallback=60)
    freq = p.get("scheduling", "pass_update_frequency", fallback="DAILY").strip().upper()
    wday = p.get("scheduling", "pass_update_weekday", fallback="MONDAY").strip().upper()
    return {
        "frequency": freq,
        "time": p.get("scheduling", "pass_update_time", fallback="00:00"),
        "weekday": wday,
        "pre_start": pre_start,
        "post_stop": post_stop,
        "pass_max_prediction_hours": window,
    }


def _parse_network(p: configparser.ConfigParser, errors: List[str]) -> Dict[str, Any]:
    url = p.get("network", "tle_url").strip()
    timeout = p.getint("network", "tle_timeout_seconds", fallback=30)
    api_key = p.get("network", "api_key", fallback="").strip()
    if not api_key:
        api_key = os.environ.get("SATPI_N2YO_API_KEY") or ""
    tle_format = p.get("network", "tle_format", fallback="TXT").upper()
    if tle_format not in ("TXT", "JSON"):
        errors.append(f"[network] tle_format must be 'TXT' or 'JSON', got '{tle_format}'")
        tle_format = "TXT"
    return {"tle_url": url, "tle_timeout": timeout, "api_key": api_key, "tle_format": tle_format}


def _parse_decode(p: configparser.ConfigParser) -> Dict[str, Any]:
    return {
        "min_cadu_size_bytes": p.getint("decode", "min_cadu_size_bytes", fallback=1_048_576),
        "success_dir_relpath": p.get("decode", "success_dir_relpath", fallback="MSU-MR"),
    }


def _parse_copytarget(p: configparser.ConfigParser) -> Dict[str, Any]:
    return {
        "enabled": p.getboolean("copytarget", "enabled", fallback=False),
        "type": p.get("copytarget", "type", fallback="rclone"),
        "rclone_remote": p.get("copytarget", "rclone_remote", fallback=None),
        "rclone_path": p.get("copytarget", "rclone_path", fallback=None),
        "create_link": p.getboolean("copytarget", "create_link", fallback=False),
    }


def _parse_notify(p: configparser.ConfigParser) -> Dict[str, Any]:
    return {
        "enabled": p.getboolean("notify", "enabled", fallback=False),
        "mail_to": p.get("notify", "mail_to", fallback=None),
        "mail_subject_prefix": p.get("notify", "mail_subject_prefix", fallback="SATPI"),
    }


def _parse_systemd(p: configparser.ConfigParser) -> Dict[str, Any]:
    user = p.get("systemd", "service_user", fallback=None)
    if user is not None:
        user = user.strip() or None
    return {"service_user": user}


def _parse_reception_setup(p: configparser.ConfigParser) -> Dict[str, Any]:
    keys = ["antenna_type", "antenna_location", "antenna_orientation",
            "lna", "rf_filter", "feedline", "sdr", "raspberry_pi",
            "power_supply", "additional_info"]
    return {k: p.get("reception_setup", k, fallback="") for k in keys}


def _parse_optimize_reception(p: configparser.ConfigParser) -> Dict[str, Any]:
    def f(key: str, default: float) -> float:
        return p.getfloat("optimize_reception", key, fallback=default)
    def i(key: str, default: int) -> int:
        return p.getint("optimize_reception", key, fallback=default)
    return {
        "enabled": p.getboolean("optimize_reception", "enabled", fallback=False),
        "max_delta_aos_azimuth": f("max_delta_aos_azimuth", 20.0),
        "max_delta_los_azimuth": f("max_delta_los_azimuth", 20.0),
        "max_delta_culmination_azimuth": f("max_delta_culmination_azimuth", 15.0),
        "max_delta_culmination_elevation": f("max_delta_culmination_elevation", 10.0),
        "min_total_passes": i("min_total_passes", 4),
        "weight_deframer_synced_seconds": f("weight_deframer_synced_seconds", 1.0),
        "weight_first_deframer_sync_delay": f("weight_first_deframer_sync_delay", -0.4),
        "weight_sync_drop_count": f("weight_sync_drop_count", -0.5),
        "weight_median_snr_synced": f("weight_median_snr_synced", 0.3),
        "weight_median_ber_synced": f("weight_median_ber_synced", -0.8),
        "elevation_band_1_max": i("elevation_band_1_max", 20),
        "elevation_band_2_max": i("elevation_band_2_max", 35),
        "elevation_band_3_max": i("elevation_band_3_max", 50),
        "elevation_band_4_max": i("elevation_band_4_max", 65),
        "elevation_band_5_max": i("elevation_band_5_max", 80),
        "output_dir": p.get("optimize_reception", "output_dir", fallback="").strip() or None,
    }


def _parse_optimize_reception_ai(p: configparser.ConfigParser) -> Dict[str, Any]:
    api_key = p.get("optimize_reception_ai", "api_key", fallback="").strip()
    if not api_key:
        api_key = os.environ.get("SATPI_OPENAI_API_KEY") or os.environ.get("OPENAI_API_KEY") or ""
    return {
        "enabled": p.getboolean("optimize_reception_ai", "enabled", fallback=False),
        "max_passes": p.getint("optimize_reception_ai", "max_passes", fallback=25),
        "model": p.get("optimize_reception_ai", "model", fallback="gpt-5"),
        "include_optimizer_report": p.getboolean("optimize_reception_ai", "include_optimizer_report", fallback=True),
        "temperature": p.getfloat("optimize_reception_ai", "temperature", fallback=1.0),
        "api_key": api_key,
    }


def _parse_noise_floor(p: configparser.ConfigParser) -> Dict[str, Any]:
    return {
        "measurement_duration": p.getint("noise_floor", "measurement_duration", fallback=600),
        "schedule_minute": p.getint("noise_floor", "schedule_minute", fallback=0),
        "center_freq_mhz": p.getfloat("noise_floor", "center_freq_mhz", fallback=137.9),
        "bandwidth_mhz": p.getfloat("noise_floor", "bandwidth_mhz", fallback=0.4),
        "bin_size_khz": p.getfloat("noise_floor", "bin_size_khz", fallback=10.0),
        "upload_enabled": p.getboolean("noise_floor", "upload_enabled", fallback=False),
        "rclone_remote": p.get("noise_floor", "rclone_remote", fallback=""),
        "rclone_path": p.get("noise_floor", "rclone_path", fallback=""),
        "create_link": p.getboolean("noise_floor", "create_link", fallback=False),
    }


def _validate_config(cfg: Dict[str, Any], errors: List[str]) -> None:
    satellites = cfg.get("satellites", [])
    if not satellites:
        errors.append("No satellites defined")


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_config = os.path.abspath(os.path.join(script_dir, "..", "config", "config.ini"))

    parser = argparse.ArgumentParser(
        prog="load_config.py",
        description="SATPI Configuration Loader",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES:
  python3 load_config.py
  python3 load_config.py --config /path/to/config.ini
  python3 load_config.py --section station --key name
  python3 load_config.py --section hardware
  python3 load_config.py --verbose
        """
    )

    parser.add_argument("-c", "--config", default=default_config, metavar="PATH", help="Path to config.ini")
    parser.add_argument("-s", "--section", metavar="SECTION", help="Config section to query")
    parser.add_argument("-k", "--key", metavar="KEY", help="Config key to query")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")

    args = parser.parse_args()

    if args.key and not args.section:
        parser.error("--key requires --section")

    if not os.path.exists(args.config):
        print(f"Error: Config file not found: {args.config}", file=sys.stderr)
        sys.exit(1)

    try:
        cfg = load_config(args.config)
        
        if args.section:
            if args.key:
                if args.section in cfg and isinstance(cfg[args.section], dict):
                    if args.key in cfg[args.section]:
                        print(cfg[args.section][args.key])
                    else:
                        print(f"Error: Key '{args.key}' not found", file=sys.stderr)
                        sys.exit(1)
            else:
                if args.section in cfg:
                    if args.json:
                        print(json.dumps(cfg[args.section], indent=2, default=str))
                    else:
                        if isinstance(cfg[args.section], dict):
                            for k, v in cfg[args.section].items():
                                print(f"{k} = {v}")
        else:
            if args.verbose:
                print("Configuration validated successfully")
                print(f"Config file: {args.config}")
                print("\nLoaded sections:")
                for section in sorted(cfg.keys()):
                    if isinstance(cfg[section], dict):
                        print(f"  [{section}] - {len(cfg[section])} items")
                    elif isinstance(cfg[section], list):
                        print(f"  [{section}] - {len(cfg[section])} items")
            else:
                print("Config OK")
                print("\nUse --help for more options")

        sys.exit(0)

    except ConfigError as e:
        print(f"Config Error:\n{e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
