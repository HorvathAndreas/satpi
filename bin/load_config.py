#!/usr/bin/env python3
"""satpi – load_config

Loads, parses and validates the central satpi configuration file.

Converts configuration values into typed Python data structures and performs
consistency checks so that the operational scripts fail early and with clear
error messages if required settings are missing or invalid.

Improvements vs. the previous version:
  * configparser errors are wrapped in ConfigError
  * unknown keys in known sections are reported (catches INI/code drift)
  * validation is conditional on the *_enabled flags
  * range checks for QTH, timings, bandwidth
  * api_key falls back to SATPI_OPENAI_API_KEY / OPENAI_API_KEY env vars
  * errors are aggregated (you get all problems at once, not just the first)
  * prediction_window_hours / max_pass_age_hours rename is backwards-compat
  * binaries are checked for executability, not just existence
  * directories that the pipeline is allowed to create are auto-created

Author: Andreas Horvath
Project: Autonomous, config-driven satellite reception pipeline for Raspberry Pi
"""

from __future__ import annotations

import configparser
import os
from typing import Any, Dict, List, Optional, Sequence, Set


class ConfigError(Exception):
    pass


# --- Known keys (for drift detection) ---------------------------------------

# Keys this parser actually reads, per section. Extra keys in the INI trigger
# a warning via ConfigError so that dead config or typos get surfaced.
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
        "pre_start_seconds", "post_stop_seconds",
        "prediction_window_hours", "max_pass_age_hours",  # legacy name accepted
    },
    "network": {"tle_url", "tle_timeout_seconds"},
    "decode": {"min_cadu_size_bytes", "success_dir_relpath"},
    "copytarget": {
        "enabled", "type", "rclone_remote", "rclone_path", "create_link",
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

# Satellite section keys (dynamic section names)
SATELLITE_KEYS: Set[str] = {
    "enabled", "min_elevation_deg", "frequency_hz", "bandwidth_hz",
    "pipeline", "pass_direction",
}

VALID_DIRECTIONS: Set[str] = {
    "all",
    "north_to_south", "south_to_north",
    "west_to_east", "east_to_west",
    "southwest_to_northeast", "southeast_to_northwest",
    "northwest_to_southeast", "northeast_to_southwest",
}

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


# --- Public entry point ------------------------------------------------------

def load_config(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise ConfigError(f"Config file not found: {path}")

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
        cfg["optimize_reception_ai"] = _parse_optimize_reception_ai(parser)
        if parser.has_section("noise_floor"):
            cfg["noise_floor"] = _parse_noise_floor(parser)
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
            freq = s.getint("frequency_hz")
            bw = s.getint("bandwidth_hz")
            pipeline = s.get("pipeline")
        except (configparser.NoOptionError, ValueError) as e:
            errors.append(f"satellite '{name}': {e}")
            continue
        if pipeline is None or not pipeline.strip():
            errors.append(f"satellite '{name}': pipeline is required")
            continue

        direction = s.get("pass_direction", fallback="all").strip().lower()
        if direction not in VALID_DIRECTIONS:
            errors.append(
                f"satellite '{name}': invalid pass_direction '{direction}'"
            )

        if freq <= 0:
            errors.append(f"satellite '{name}': frequency_hz must be > 0")
        if bw <= 0:
            errors.append(f"satellite '{name}': bandwidth_hz must be > 0")

        satellites.append({
            "name": name,
            "enabled": s.getboolean("enabled", fallback=True),
            "min_elevation": s.getint("min_elevation_deg", fallback=0),
            "frequency": freq,
            "bandwidth": bw,
            "pipeline": pipeline.strip(),
            "pass_direction": direction,
        })
    return satellites


def _parse_scheduling(
    p: configparser.ConfigParser, errors: List[str]
) -> Dict[str, Any]:
    # Accept the legacy key for backwards compatibility.
    if p.has_option("scheduling", "prediction_window_hours"):
        window = p.getint("scheduling", "prediction_window_hours")
    elif p.has_option("scheduling", "max_pass_age_hours"):
        window = p.getint("scheduling", "max_pass_age_hours")
    else:
        window = 24

    pre_start = p.getint("scheduling", "pre_start_seconds", fallback=120)
    post_stop = p.getint("scheduling", "post_stop_seconds", fallback=60)

    freq = p.get("scheduling", "pass_update_frequency", fallback="DAILY").strip().upper()
    wday = p.get("scheduling", "pass_update_weekday", fallback="MONDAY").strip().upper()

    if freq not in VALID_SCHEDULING_FREQUENCIES:
        errors.append(
            f"scheduling.pass_update_frequency '{freq}' not in "
            f"{sorted(VALID_SCHEDULING_FREQUENCIES)}"
        )
    if wday not in VALID_WEEKDAYS:
        errors.append(f"scheduling.pass_update_weekday '{wday}' is not a valid weekday")
    if pre_start < 0:
        errors.append(f"scheduling.pre_start_seconds must be >= 0 (got {pre_start})")
    if post_stop < 0:
        errors.append(f"scheduling.post_stop_seconds must be >= 0 (got {post_stop})")
    if window <= 0:
        errors.append(f"scheduling.prediction_window_hours must be > 0 (got {window})")

    return {
        "frequency": freq,
        "time": p.get("scheduling", "pass_update_time", fallback="00:00"),
        "weekday": wday,
        "pre_start": pre_start,
        "post_stop": post_stop,
        "prediction_window_hours": window,
        # Keep the legacy alias for any caller still reading it.
        "max_pass_age_hours": window,
    }


def _parse_network(
    p: configparser.ConfigParser, errors: List[str]
) -> Dict[str, Any]:
    url = p.get("network", "tle_url").strip()
    timeout = p.getint("network", "tle_timeout_seconds", fallback=30)

    if not url:
        errors.append("network.tle_url is required")
    elif not (url.startswith("http://") or url.startswith("https://")):
        errors.append(f"network.tle_url must be http(s): {url}")
    if timeout <= 0:
        errors.append(f"network.tle_timeout_seconds must be > 0 (got {timeout})")

    return {"tle_url": url, "tle_timeout": timeout}


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
        # Elevation bands (previously ignored by the parser).
        "elevation_band_1_max": i("elevation_band_1_max", 20),
        "elevation_band_2_max": i("elevation_band_2_max", 35),
        "elevation_band_3_max": i("elevation_band_3_max", 50),
        "elevation_band_4_max": i("elevation_band_4_max", 65),
        "elevation_band_5_max": i("elevation_band_5_max", 80),
        "output_dir": p.get("optimize_reception", "output_dir", fallback="").strip() or None,
    }


def _parse_optimize_reception_ai(p: configparser.ConfigParser) -> Dict[str, Any]:
    api_key = p.get("optimize_reception_ai", "api_key", fallback="").strip()
    # Env fallback so secrets don't have to live in config.ini.
    if not api_key:
        api_key = (
            os.environ.get("SATPI_OPENAI_API_KEY")
            or os.environ.get("OPENAI_API_KEY")
            or ""
        )

    return {
        "enabled": p.getboolean("optimize_reception_ai", "enabled", fallback=False),
        "max_passes": p.getint("optimize_reception_ai", "max_passes", fallback=25),
        "model": p.get("optimize_reception_ai", "model", fallback="gpt-5"),
        "include_optimizer_report": p.getboolean(
            "optimize_reception_ai", "include_optimizer_report", fallback=True,
        ),
        "temperature": p.getfloat(
            "optimize_reception_ai", "temperature", fallback=1.0,
        ),
        "api_key": api_key,
    }


# --- Validation --------------------------------------------------------------


def _parse_noise_floor(p: configparser.ConfigParser) -> Dict[str, Any]:
    def _bool(key: str, default: bool) -> bool:
        return p.getboolean("noise_floor", key, fallback=default)
    def _str(key: str, default: str = "") -> str:
        return p.get("noise_floor", key, fallback=default).strip()
    def _int(key: str, default: int) -> int:
        return p.getint("noise_floor", key, fallback=default)
    def _float(key: str, default: float) -> float:
        return p.getfloat("noise_floor", key, fallback=default)
    def _float_opt(key: str) -> float | None:
        val = p.get("noise_floor", key, fallback="").strip()
        return float(val) if val else None

    center = _float("center_freq_mhz", 137.9)
    bw     = _float("bandwidth_mhz", 0.4)
    return {
        "measurement_duration": _int("measurement_duration", 600),
        "schedule_minute":      _int("schedule_minute", 0),
        "center_freq_mhz":      center,
        "bandwidth_mhz":        bw,
        "bin_size_khz":         _float("bin_size_khz", 10.0),
        # Explicit start/end override center+bandwidth when set
        "freq_start_mhz":       _float_opt("freq_start_mhz"),
        "freq_end_mhz":         _float_opt("freq_end_mhz"),
        "upload_enabled":       _bool("upload_enabled", False),
        "rclone_remote":        _str("rclone_remote"),
        "rclone_path":          _str("rclone_path"),
        "create_link":          _bool("create_link", False),
    }

def _is_executable(path: str) -> bool:
    return os.path.isfile(path) and os.access(path, os.X_OK)


def _validate_config(cfg: Dict[str, Any], errors: List[str]) -> None:
    # Require at least one enabled satellite.
    satellites = cfg.get("satellites", [])
    if not satellites:
        errors.append("No satellites defined")
    else:
        active = [s for s in satellites if s["enabled"]]
        if not active:
            errors.append("No enabled satellites")

    paths = cfg.get("paths", {})

    # base_dir must exist (we never create it).
    base_dir = paths.get("base_dir")
    if base_dir and not os.path.isdir(base_dir):
        errors.append(f"paths.base_dir does not exist: {base_dir}")

    # Directories the pipeline is allowed to create on first run.
    for key in ("log_dir", "output_dir", "generated_units_dir", "optimization_dir"):
        path = paths.get(key)
        if not path:
            continue
        try:
            os.makedirs(path, exist_ok=True)
        except OSError as e:
            errors.append(f"paths.{key} cannot be created ({path}): {e}")

    # Binaries: must exist and be executable.
    if paths.get("satdump_bin") and not _is_executable(paths["satdump_bin"]):
        errors.append(f"paths.satdump_bin is not an executable file: {paths['satdump_bin']}")
    if paths.get("python_bin") and not _is_executable(paths["python_bin"]):
        errors.append(f"paths.python_bin is not an executable file: {paths['python_bin']}")

    # Mail binary only required when notifications are on.
    notify = cfg.get("notify", {})
    if notify.get("enabled"):
        if not notify.get("mail_to"):
            errors.append("notify.enabled=true but notify.mail_to is missing")
        if paths.get("mail_bin") and not _is_executable(paths["mail_bin"]):
            errors.append(f"paths.mail_bin is not an executable file: {paths['mail_bin']}")

    # rclone target only required when copytarget is on.
    copytarget = cfg.get("copytarget", {})
    if copytarget.get("enabled"):
        if not copytarget.get("rclone_remote"):
            errors.append("copytarget.enabled=true but copytarget.rclone_remote is missing")
        if not copytarget.get("rclone_path"):
            errors.append("copytarget.enabled=true but copytarget.rclone_path is missing")

    # AI optimizer needs an API key when enabled.
    ai = cfg.get("optimize_reception_ai", {})
    if ai.get("enabled") and not ai.get("api_key"):
        errors.append(
            "optimize_reception_ai.enabled=true but no api_key is configured "
            "(set api_key in config, or SATPI_OPENAI_API_KEY / OPENAI_API_KEY env var)"
        )

    # Sanity: bandwidth not larger than sample rate (warning-level, but we add it).
    hardware = cfg.get("hardware", {})
    sr = hardware.get("sample_rate")
    if sr:
        for s in satellites:
            if s["bandwidth"] > sr:
                errors.append(
                    f"satellite '{s['name']}': bandwidth_hz ({s['bandwidth']}) "
                    f"exceeds hardware.sample_rate ({int(sr)})"
                )
