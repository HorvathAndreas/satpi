#!/usr/bin/env python3
"""satpi – update_tle

Downloads and filters TLE data for the configured satellites.

Retrieves current orbital data from the configured remote source, verifies that
the download succeeded and writes a filtered local TLE file containing only the
satellites used by this installation. It is the first step in the planning
chain because pass prediction depends on up-to-date orbital data.

Author: Andreas Horvath
Project: Autonomous, config-driven satellite reception pipeline for Raspberry Pi
"""

from __future__ import annotations

import logging
import os
import sys
import tempfile
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Iterable, List, Sequence, Set, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from load_config import load_config, ConfigError


# --- Constants ---------------------------------------------------------------

CONNECT_TIMEOUT = 5           # seconds
READ_TIMEOUT = 30             # seconds
HTTP_RETRIES = 3
RETRY_BACKOFF = 2             # seconds (exponential: 2, 4, 8, ...)
REACHABILITY_PROBE_URL = "https://www.google.com"
CELESTRAK_PROBE_URL = "https://celestrak.org"
MAX_FALLBACK_AGE_DAYS = 5     # warn-only fallback beyond this is risky
LOG_MAX_BYTES = 1_000_000     # 1 MB per log file
LOG_BACKUP_COUNT = 5

logger = logging.getLogger("satpi.update")


# --- Logging -----------------------------------------------------------------

def setup_logging(log_dir: str) -> None:
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "update_tle.log")

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


# --- HTTP helpers ------------------------------------------------------------

def _build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=HTTP_RETRIES,
        connect=HTTP_RETRIES,
        read=HTTP_RETRIES,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def check_url(url: str, timeout: Tuple[int, int] = (CONNECT_TIMEOUT, READ_TIMEOUT)) -> bool:
    """Return True if *url* is reachable with a 2xx/3xx response."""
    try:
        with _build_session() as s:
            # Some servers (including Celestrak at times) don't support HEAD,
            # so fall back to a streamed GET that we close immediately.
            r = s.head(url, timeout=timeout, allow_redirects=True)
            if r.status_code >= 400:
                r = s.get(url, timeout=timeout, stream=True)
                r.close()
            return r.status_code < 400
    except requests.RequestException:
        return False


def download_tle(url: str, target: str) -> None:
    """Stream-download *url* into *target*. Raises RuntimeError on failure."""
    try:
        with _build_session() as s, s.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), stream=True) as r:
            r.raise_for_status()
            with open(target, "wb") as f:
                for chunk in r.iter_content(chunk_size=64 * 1024):
                    if chunk:
                        f.write(chunk)
    except requests.RequestException as e:
        raise RuntimeError(f"TLE download failed: {e}") from e

    if not os.path.exists(target) or os.path.getsize(target) == 0:
        raise RuntimeError("TLE download failed: empty file")


# --- TLE parsing / filtering -------------------------------------------------

def normalize_sat_name(name: str) -> str:
    return " ".join(name.strip().upper().replace("-", " ").replace("_", " ").split())


def _is_tle_line1(line: str) -> bool:
    return line.startswith("1 ") and len(line) >= 24


def _is_tle_line2(line: str) -> bool:
    return line.startswith("2 ") and len(line) >= 24


def filter_tle(input_file: str, output_file: str, satellite_names: Sequence[str]) -> None:
    """Write the 3-line TLE blocks for satellite_names from input_file to output_file.

    Raises RuntimeError on malformed TLE entries or when no satellites match.
    Logs a warning for any configured satellite that wasn't found.
    """
    normalized_targets: Set[str] = {normalize_sat_name(s) for s in satellite_names}
    found_normalized: Set[str] = set()
    found_names: List[str] = []

    tmp_output = output_file + ".tmp"

    with open(input_file, "r", encoding="utf-8") as f:
        lines = f.readlines()

    try:
        with open(tmp_output, "w", encoding="utf-8") as out:
            i = 0
            n = len(lines)
            while i < n:
                raw = lines[i]
                name_line = raw.strip()

                # Skip blank lines and obvious TLE data lines — those can't be names.
                if not name_line or _is_tle_line1(name_line) or _is_tle_line2(name_line):
                    i += 1
                    continue

                if normalize_sat_name(name_line) not in normalized_targets:
                    i += 1
                    continue

                if i + 2 >= n:
                    raise RuntimeError(f"Incomplete TLE entry for satellite: {name_line}")

                l1, l2 = lines[i + 1].rstrip("\n"), lines[i + 2].rstrip("\n")
                if not _is_tle_line1(l1) or not _is_tle_line2(l2):
                    raise RuntimeError(
                        f"Malformed TLE entry for satellite {name_line!r}: "
                        f"expected '1 ...' / '2 ...' lines."
                    )

                out.write(raw)
                out.write(lines[i + 1])
                out.write(lines[i + 2])
                found_names.append(name_line)
                found_normalized.add(normalize_sat_name(name_line))
                i += 3

        if not os.path.exists(tmp_output) or os.path.getsize(tmp_output) == 0:
            raise RuntimeError(
                f"No matching satellites found in TLE. Configured: {list(satellite_names)}"
            )

        os.replace(tmp_output, output_file)
    finally:
        # If os.replace succeeded, the file is gone; otherwise clean it up.
        if os.path.exists(tmp_output):
            try:
                os.remove(tmp_output)
            except OSError:
                pass

    missing = normalized_targets - found_normalized
    if missing:
        logger.warning("Configured satellites NOT found in TLE: %s", sorted(missing))
    logger.info("Matched satellites in TLE: %s", found_names)


# --- Local TLE freshness checks ---------------------------------------------

def has_usable_tle_file(tle_file: str) -> bool:
    if not os.path.exists(tle_file) or os.path.getsize(tle_file) == 0:
        return False
    with open(tle_file, "r", encoding="utf-8") as f:
        non_empty = [ln.strip() for ln in f if ln.strip()]
    return len(non_empty) >= 3


def tle_age_days(tle_file: str) -> float:
    return (time.time() - os.path.getmtime(tle_file)) / 86400.0


def _use_existing_tle_if_possible(tle_file: str, reason: str) -> bool:
    """Log and return True if an existing local TLE can serve as fallback."""
    if not has_usable_tle_file(tle_file):
        return False
    age = tle_age_days(tle_file)
    if age > MAX_FALLBACK_AGE_DAYS:
        logger.error(
            "%s Existing local TLE is %.1f days old (> %d). Refusing stale fallback: %s",
            reason, age, MAX_FALLBACK_AGE_DAYS, tle_file,
        )
        return False
    logger.warning("%s Using existing local TLE (%.1f days old): %s", reason, age, tle_file)
    return True


# --- Main --------------------------------------------------------------------

def _fallback_after_failure(tle_file: str) -> None:
    """Inspect connectivity and either return (fallback accepted) or raise."""
    logger.warning("Direct access to Celestrak failed, checking general internet connectivity...")
    google_ok = check_url(REACHABILITY_PROBE_URL)
    celestrak_ok = check_url(CELESTRAK_PROBE_URL)

    if not google_ok:
        if _use_existing_tle_if_possible(
            tle_file,
            "TLE download failed and general internet connectivity check also failed.",
        ):
            return
        raise RuntimeError(
            "TLE download failed and general internet connectivity check also failed. "
            "The system appears to have no working internet connection."
        )

    if not celestrak_ok:
        if _use_existing_tle_if_possible(
            tle_file,
            "TLE download failed; Celestrak appears unavailable or blocked.",
        ):
            return
        raise RuntimeError(
            "TLE download failed, but general internet connectivity is working. "
            "Access to Celestrak appears to be blocked or unavailable. "
            "Celestrak sometimes blocks IP addresses after too many requests. "
            "If this happens, use a system-wide VPN connection and try again."
        )

    if _use_existing_tle_if_possible(
        tle_file,
        "TLE download failed although general internet connectivity appears to work.",
    ):
        return
    raise RuntimeError(
        "TLE download failed although general internet connectivity appears to work. "
        "Please check the configured TLE URL and remote availability."
    )


def main() -> int:
    base_dir = Path(__file__).resolve().parent.parent
    config_path = base_dir / "config" / "config.ini"

    try:
        config = load_config(str(config_path))
    except ConfigError as e:
        print(f"[update] CONFIG ERROR: {e}", file=sys.stderr)
        return 2

    setup_logging(config["paths"]["log_dir"])

    tle_url = config["network"]["tle_url"]
    tle_file = config["paths"]["tle_file"]
    satellites = [s for s in config["satellites"] if s["enabled"]]

    if not satellites:
        logger.error("No enabled satellites in config – nothing to do.")
        return 2

    tle_dir = os.path.dirname(tle_file)
    if tle_dir:
        os.makedirs(tle_dir, exist_ok=True)

    fd, tmp_file = tempfile.mkstemp(prefix="satpi_tle_", suffix=".tmp")
    os.close(fd)

    try:
        logger.info("Downloading TLE from %s", tle_url)
        try:
            download_tle(tle_url, tmp_file)

            sat_names = [s["name"] for s in satellites]
            logger.info("Filtering satellites: %s", sat_names)
            filter_tle(tmp_file, tle_file, sat_names)

            logger.info("TLE update successful: %s", tle_file)
            return 0

        except RuntimeError:
            _fallback_after_failure(tle_file)
            return 0  # fallback accepted

    except Exception as e:
        logger.exception("update_tle failed: %s", e)
        return 1

    finally:
        if os.path.exists(tmp_file):
            try:
                os.remove(tmp_file)
            except OSError:
                pass


if __name__ == "__main__":
    sys.exit(main())
