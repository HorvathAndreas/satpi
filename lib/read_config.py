#!/usr/bin/env python3
"""satpi – read_config

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
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

sys.path.insert(0, str(Path(__file__).parent.parent / "lib"))

from parse_frequency import parse_frequency

class ConfigError(Exception):
    pass

_parse_frequency = parse_frequency

# --- Known keys (for drift detection) ---------------------------------------

# Keys this parser actually reads, per section. Extra keys in the INI trigger
# a warning via ConfigError so that dead config or typos get surfaced.
KNOWN_KEYS: Dict[str, Set[str]] = {
    "station": {"name", "timezone"},
    "qth": {"latitude", "longitude", "altitude_m"},
    "paths": {
        "base_dir", "output_dir", "lib_dir", "pass_file", "log_dir", "reports_dir",
        "generated_units_dir", "tle_file", "reception_db_file",
        "satdump_bin", "mail_bin", "python_bin",
    },
    "hardware": {"source_id", "gain", "sample_rate", "bias_t"},
    "scheduling": {
        "pass_update_frequency", "pass_update_time", "pass_update_weekday",
        "pre_start_seconds", "post_stop_seconds", "pass_max_prediction_hours",
    },
    "network": {"tle_url", "tle_timeout_seconds", "api_key", "tle_format"},
    "decode": {"min_cadu_size_bytes", "success_dir_relpath"},
    "copytarget": {
        "enabled", "type", "rclone_remote", "rclone_dir", "rclone_reports_dir", "create_link",
    },
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
    "noise_floor": {
        "measurement_duration", "schedule_minute",
        "center_freq", "bandwidth", "bin_size",
        "freq_start", "freq_end",
        "upload_enabled", "rclone_remote", "rclone_path", "create_link",
    },
    "processing_thresholds": {
        "score_fair", "score_good",
        "channel_stddev_fair", "channel_stddev_good",
        "channel_range_fair", "channel_range_good",
        "lrpt_dir", "lrpt_channels", "lrpt_tolerate_missing_channel",
        "hrpt_dir", "hrpt_channels", "hrpt_tolerate_missing_channel",
    },
}

# Satellite section keys (dynamic section names)
SATELLITE_KEYS: Set[str] = {
    "enabled", "norad_id", "min_elevation_deg", "frequency", "bandwidth",
    "pipeline", "pass_direction", "pass_timeslot",
}

VALID_DIRECTIONS: Set[str] = {
    "all",
    "north_to_south", "south_to_north",
    "west_to_east", "east_to_west",
    "southwest_to_northeast", "southeast_to_northwest",
    "northwest_to_southeast", "northeast_to_southwest",
}

VALID_TIMESLOTS_PRESETS: Set[str] = {"all", "day", "night"}

VALID_SCHEDULING_FREQUENCIES: Set[str] = {"HOURLY", "DAILY", "WEEKLY"}
VALID_WEEKDAYS: Set[str] = {
    "MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY",
    "FRIDAY", "SATURDAY", "SUNDAY",
}


# --- Helpers ----------------------------------------------------------------

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
            errors.append(
                f"Unknown keys in [{section}]: {', '.join(sorted(extra))}"
            )


def _parse_channels(spec: str) -> List[int]:
    """Parse channel spec like "1-6", "1,3,5", or "1-3,5,7-9" into a sorted unique list of ints."""
    spec = (spec or "").strip()
    if not spec:
        return []
    out: Set[int] = set()
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a_s, b_s = part.split("-", 1)
            a, b = int(a_s.strip()), int(b_s.strip())
            if a > b:
                a, b = b, a
            out.update(range(a, b + 1))
        else:
            out.add(int(part))
    return sorted(out)


# --- Public entry point ------------------------------------------------------

def read_config(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise ConfigError(
            f"Config file not found: {path}\n"
            f"Use --config to specify a different path."
        )

    # interpolation=None avoids '%(x)s' surprises in URLs/paths.
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
        if parser.has_section("noise_floor"):
            cfg["noise_floor"] = _parse_noise_floor(parser)
        cfg["processing_thresholds"] = _parse_processing_thresholds(parser, errors)
    except configparser.NoOptionError as e:
        errors.append(f"Missing required option: {e}")
    except configparser.NoSectionError as e:
        errors.append(f"Missing required section: {e}")
    except ValueError as e:
        errors.append(f"Invalid value in config: {e}")

    _validate_config(cfg, errors)

    if errors:
        joined = "\n  - ".join(errors)
        raise ConfigError(f"Config problems in {path}:\n  - {joined}")

    return cfg


# --- Section parsers ---------------------------------------------------------

def _parse_station(p: configparser.ConfigParser) -> Dict[str, Any]:
    return {
        "name": p.get("station", "name", fallback="satpi"),
        "timezone": p.get("station", "timezone", fallback="UTC"),
    }


def _parse_qth(p: configparser.ConfigParser, errors: List[str]) -> Dict[str, Any]:
    lat = p.getfloat("qth", "latitude")
    lon = p.getfloat("qth", "longitude")
    alt = p.getfloat("qth", "altitude_m", fallback=0.0)

    if not -90.0 <= lat <= 90.0:
        errors.append(f"qth.latitude {lat} is out of range (-90..90)")
    if not -180.0 <= lon <= 180.0:
        errors.append(f"qth.longitude {lon} is out of range (-180..180)")
    if alt < -500 or alt > 9000:
        errors.append(f"qth.altitude_m {alt} looks implausible")

    return {"latitude": lat, "longitude": lon, "altitude": alt}


def _parse_paths(p: configparser.ConfigParser) -> Dict[str, Any]:
    base_dir = os.path.abspath(os.path.expanduser(p.get("paths", "base_dir").strip()))

    def rel(key: str, fallback: str = "") -> str:
        return _resolve_path(base_dir, p.get("paths", key, fallback=fallback))

    return {
        "base_dir": base_dir,
        "lib_dir": rel("lib_dir", fallback="lib"),
        "output_dir": rel("output_dir", fallback="results/passes"),
        "pass_file": rel("pass_file", fallback="results/passes/passes.json"),
        "log_dir": rel("log_dir", fallback="logs"),
        "reports_dir": rel("reports_dir", fallback="results/reports"),
        "generated_units_dir": rel("generated_units_dir", fallback="results/generated_units"),
        "tle_file": rel("tle_file", fallback="results/tle/weather.tle"),
        "reception_db_file": rel("reception_db_file", fallback="results/database/reception.db"),
        "satdump_bin": _resolve_path(base_dir, p.get("paths", "satdump_bin", fallback="/usr/bin/satdump")),
        "mail_bin": _resolve_path(base_dir, p.get("paths", "mail_bin", fallback="/usr/bin/msmtp")),
        "python_bin": _resolve_path(base_dir, p.get("paths", "python_bin", fallback="/usr/bin/python3")),
    }


def _parse_hardware(p: configparser.ConfigParser) -> Dict[str, Any]:
    return {
        "source_id": p.get("hardware", "source_id", fallback=None),
        "gain": p.getfloat("hardware", "gain", fallback=0.0),
        "sample_rate": p.getfloat("hardware", "sample_rate", fallback=2.4e6),
        "bias_t": p.getboolean("hardware", "bias_t", fallback=False),
    }


def _parse_satellites(
    p: configparser.ConfigParser, errors: List[str]
) -> List[Dict[str, Any]]:
    satellites: List[Dict[str, Any]] = []
    for section in p.sections():
        if not section.startswith("satellite."):
            continue
        name = section.split(".", 1)[1]
        s = p[section]

        try:
            # Parse frequency (supports units: MHz, kHz, GHz, Hz)
            freq_str = s.get("frequency", "").strip()
            if not freq_str:
                errors.append(f"satellite '{name}': frequency is required")
                continue
            try:
                frequency_hz = _parse_frequency(freq_str)
            except ValueError as e:
                errors.append(f"satellite '{name}': {e}")
                continue

            # Parse bandwidth (supports units: MHz, kHz, Hz)
            bw_str = s.get("bandwidth", "").strip()
            if not bw_str:
                errors.append(f"satellite '{name}': bandwidth is required")
                continue
            try:
                bandwidth_hz = _parse_frequency(bw_str)
            except ValueError as e:
                errors.append(f"satellite '{name}': {e}")
                continue

            # Parse pipeline
            pipeline = s.get("pipeline", "").strip()
            if not pipeline:
                errors.append(f"satellite '{name}': pipeline is required")
                continue

            # Parse pass_direction
            direction = s.get("pass_direction", "all").strip().lower()
            if direction not in VALID_DIRECTIONS:
                errors.append(f"satellite '{name}': invalid pass_direction '{direction}'")
                direction = "all"

	    # Parse pass_timeslot
            timeslot = s.get("pass_timeslot", "all").strip().lower()
            if timeslot not in VALID_TIMESLOTS_PRESETS:
                # Check if it's a valid time range format HHmm-HHmm
                if "-" in timeslot:
                    try:
                        parts = timeslot.split("-")
                        if len(parts) == 2:
                            start_hm = int(parts[0])
                            end_hm = int(parts[1])
                            if not (0 <= start_hm <= 2359 and 0 <= end_hm <= 2359):
                                errors.append(f"satellite '{name}': invalid pass_timeslot time range '{timeslot}'")
                                timeslot = "all"
                        else:
                            errors.append(f"satellite '{name}': invalid pass_timeslot format '{timeslot}'")
                            timeslot = "all"
                    except ValueError:
                        errors.append(f"satellite '{name}': invalid pass_timeslot format '{timeslot}'")
                        timeslot = "all"
                else:
                    errors.append(f"satellite '{name}': invalid pass_timeslot '{timeslot}'")
                    timeslot = "all"


            # Parse other fields
            enabled = s.getboolean("enabled", fallback=True)
            min_elevation = s.getint("min_elevation_deg", fallback=0)
            norad_id = s.getint("norad_id", fallback=0)

            satellites.append({
                "name": name,
                "enabled": enabled,
                "norad_id": norad_id,
                "min_elevation": min_elevation,
                "frequency": frequency_hz,
                "bandwidth": bandwidth_hz,
                "pipeline": pipeline,
                "pass_direction": direction,
		"pass_timeslot": timeslot,
            })

        except (configparser.NoOptionError, ValueError) as e:
            errors.append(f"satellite '{name}': {e}")
            continue

    return satellites


def _parse_scheduling(p: configparser.ConfigParser, errors: List[str]) -> Dict[str, Any]:
    freq = p.get("scheduling", "pass_update_frequency", fallback="DAILY").strip().upper()
    if freq not in VALID_SCHEDULING_FREQUENCIES:
        errors.append(f"[scheduling] pass_update_frequency must be HOURLY, DAILY, or WEEKLY, got '{freq}'")
        freq = "DAILY"

    wday = p.get("scheduling", "pass_update_weekday", fallback="MONDAY").strip().upper()
    if wday not in VALID_WEEKDAYS:
        errors.append(f"[scheduling] pass_update_weekday must be a valid weekday, got '{wday}'")
        wday = "MONDAY"

    return {
        "frequency": freq,
        "time": p.get("scheduling", "pass_update_time", fallback="00:00"),
        "weekday": wday,
        "pre_start": p.getint("scheduling", "pre_start_seconds", fallback=120),
        "post_stop": p.getint("scheduling", "post_stop_seconds", fallback=60),
        "pass_max_prediction_hours": p.getint("scheduling", "pass_max_prediction_hours", fallback=168),
    }


def _parse_network(p: configparser.ConfigParser, errors: List[str]) -> Dict[str, Any]:
    url = p.get("network", "tle_url").strip()
    timeout = p.getint("network", "tle_timeout_seconds", fallback=30)
    api_key = p.get("network", "api_key", fallback="").strip()

    # Fall back to environment variable if not in config
    if not api_key:
        api_key = os.environ.get("SATPI_N2YO_API_KEY") or ""

    tle_format = p.get("network", "tle_format", fallback="TXT").upper()
    if tle_format not in ("TXT", "JSON"):
        errors.append(f"[network] tle_format must be 'TXT' or 'JSON', got '{tle_format}'")
        tle_format = "TXT"

    return {
        "tle_url": url,
        "tle_timeout": timeout,
        "api_key": api_key,
        "tle_format": tle_format,
    }


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
        "rclone_dir": p.get("copytarget", "rclone_dir", fallback=None),
        "rclone_reports_dir": p.get("copytarget", "rclone_reports_dir", fallback=None),
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
    keys = [
        "antenna_type", "antenna_location", "antenna_orientation",
        "lna", "rf_filter", "feedline", "sdr", "raspberry_pi",
        "power_supply", "additional_info",
    ]
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


def _parse_noise_floor(p: configparser.ConfigParser) -> Dict[str, Any]:
    # Parse center_freq, bandwidth, and bin_size with flexible units
    center_freq_hz = _parse_frequency(p.get("noise_floor", "center_freq", fallback="137.9 MHz"))
    bandwidth_hz = _parse_frequency(p.get("noise_floor", "bandwidth", fallback="0.4 MHz"))
    bin_size_khz = _parse_frequency(p.get("noise_floor", "bin_size", fallback="10 kHz"))

    # Parse optional freq_start and freq_end if present
    freq_start = None
    freq_end = None
    if p.has_option("noise_floor", "freq_start"):
        freq_start = _parse_frequency(p.get("noise_floor", "freq_start").strip())
    if p.has_option("noise_floor", "freq_end"):
        freq_end = _parse_frequency(p.get("noise_floor", "freq_end").strip())

    return {
        "measurement_duration": p.getint("noise_floor", "measurement_duration", fallback=600),
        "schedule_minute": p.getint("noise_floor", "schedule_minute", fallback=0),
        "center_freq_hz": center_freq_hz,
        "bandwidth_hz": bandwidth_hz,
        "bin_size_khz": bin_size_khz,
        "freq_start_hz": freq_start,
        "freq_end_hz": freq_end,
        "upload_enabled": p.getboolean("noise_floor", "upload_enabled", fallback=False),
        "rclone_remote": p.get("noise_floor", "rclone_remote", fallback=""),
        "rclone_path": p.get("noise_floor", "rclone_path", fallback=""),
        "create_link": p.getboolean("noise_floor", "create_link", fallback=False),
    }


def _parse_processing_thresholds(
    p: configparser.ConfigParser, errors: List[str]
) -> Dict[str, Any]:
    """Parse [processing_thresholds] section.

    Returns a nested dict with score thresholds, per-channel classification
    thresholds, and a 'pipelines' sub-dict keyed by format prefix
    ('lrpt', 'hrpt') with each entry holding {dir, channels, tolerate_missing_channel}.
    The format prefix is selected by the caller from the pipeline name in
    reception.json (e.g. '*_lrpt' -> 'lrpt').

    If the section is missing, sensible defaults are returned with no
    pipeline configured.
    """
    if not p.has_section("processing_thresholds"):
        return {
            "score_fair": 40.0,
            "score_good": 70.0,
            "channel_stddev_fair": 5.0,
            "channel_stddev_good": 20.0,
            "channel_range_fair": 30,
            "channel_range_good": 80,
            "pipelines": {},
        }

    def f(key: str, default: float) -> float:
        return p.getfloat("processing_thresholds", key, fallback=default)

    def i(key: str, default: int) -> int:
        return p.getint("processing_thresholds", key, fallback=default)

    def s(key: str, default: str = "") -> str:
        return p.get("processing_thresholds", key, fallback=default).strip()

    score_fair = f("score_fair", 40.0)
    score_good = f("score_good", 70.0)
    if score_fair >= score_good:
        errors.append(
            f"[processing_thresholds] score_fair ({score_fair}) "
            f"must be < score_good ({score_good})"
        )

    ch_stddev_fair = f("channel_stddev_fair", 5.0)
    ch_stddev_good = f("channel_stddev_good", 20.0)
    if ch_stddev_fair >= ch_stddev_good:
        errors.append(
            f"[processing_thresholds] channel_stddev_fair ({ch_stddev_fair}) "
            f"must be < channel_stddev_good ({ch_stddev_good})"
        )

    ch_range_fair = i("channel_range_fair", 30)
    ch_range_good = i("channel_range_good", 80)
    if ch_range_fair >= ch_range_good:
        errors.append(
            f"[processing_thresholds] channel_range_fair ({ch_range_fair}) "
            f"must be < channel_range_good ({ch_range_good})"
        )

    pipelines: Dict[str, Dict[str, Any]] = {}
    for prefix in ("lrpt", "hrpt"):
        pdir = s(f"{prefix}_dir")
        pchan_str = s(f"{prefix}_channels")
        if not pdir and not pchan_str:
            # Neither configured — pipeline not in use here, skip silently.
            continue
        if not pdir:
            errors.append(
                f"[processing_thresholds] {prefix}_dir is missing while "
                f"{prefix}_channels is set"
            )
            continue
        try:
            channels = _parse_channels(pchan_str)
        except ValueError as e:
            errors.append(
                f"[processing_thresholds] invalid {prefix}_channels '{pchan_str}': {e}"
            )
            channels = []
        if not channels:
            errors.append(
                f"[processing_thresholds] {prefix}_channels yielded no channels"
            )
        tol = i(f"{prefix}_tolerate_missing_channel", 0)
        if tol < 0:
            errors.append(
                f"[processing_thresholds] {prefix}_tolerate_missing_channel "
                f"must be >= 0"
            )
            tol = 0
        if channels and tol > len(channels):
            errors.append(
                f"[processing_thresholds] {prefix}_tolerate_missing_channel "
                f"({tol}) exceeds number of channels ({len(channels)})"
            )
        pipelines[prefix] = {
            "dir": pdir,
            "channels": channels,
            "tolerate_missing_channel": tol,
        }

    return {
        "score_fair": score_fair,
        "score_good": score_good,
        "channel_stddev_fair": ch_stddev_fair,
        "channel_stddev_good": ch_stddev_good,
        "channel_range_fair": ch_range_fair,
        "channel_range_good": ch_range_good,
        "pipelines": pipelines,
    }


def _validate_config(cfg: Dict[str, Any], errors: List[str]) -> None:
    satellites = cfg.get("satellites", [])
    if not satellites:
        errors.append("No satellites defined")


# --- CLI interface ----------------------------------------------------------

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_config = os.path.abspath(os.path.join(script_dir, "..", "config", "config.ini"))

    parser = argparse.ArgumentParser(
        prog="read_config.py",
        description="SATPI Configuration Loader",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES:
  python3 read_config.py
  python3 read_config.py --config /path/to/config.ini
  python3 read_config.py --section station --key name
  python3 read_config.py --section hardware
  python3 read_config.py --verbose
        """
    )

    parser.add_argument("-c", "--config", default=default_config, metavar="PATH", help="Path to config.ini")
    parser.add_argument("-s", "--section", metavar="SECTION", help="Config section to query")
    parser.add_argument("-k", "--key", metavar="KEY", help="Config key to query")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")

    args = parser.parse_args()
    config_path = args.config

    if args.key and not args.section:
        parser.error("--key requires --section")

    if not os.path.exists(args.config):
        print(f"Error: Config file not found: {args.config}", file=sys.stderr)
        sys.exit(1)

    try:
        cfg = read_config(config_path)
        sys.path.insert(0, cfg["paths"]["lib_dir"])
        from parse_frequency import parse_frequency
        if args.section:
            if args.key:
                if args.section in cfg and isinstance(cfg[args.section], dict):
                    if args.key in cfg[args.section]:
                        print(cfg[args.section][args.key])
                    else:
                        print(f"Error: Key '{args.key}' not found in [{args.section}]", file=sys.stderr)
                        sys.exit(1)
                else:
                    print(f"Error: Section '{args.section}' not found or is not a dict", file=sys.stderr)
                    sys.exit(1)
            else:
                if args.section in cfg:
                    if args.json:
                        print(json.dumps(cfg[args.section], indent=2, default=str))
                    else:
                        if isinstance(cfg[args.section], dict):
                            for k, v in cfg[args.section].items():
                                print(f"{k} = {v}")
                        elif isinstance(cfg[args.section], list):
                            for item in cfg[args.section]:
                                print(item)
                else:
                    print(f"Error: Section '{args.section}' not found", file=sys.stderr)
                    sys.exit(1)
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

        sys.exit(0)

    except ConfigError as e:
        print(f"Config Error:\n{e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
