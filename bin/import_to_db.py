#!/usr/bin/env python3
"""satpi – Import reception JSON data into SQLite database
Primary Key Structure: pass_id (YYYY-MM-DD_HH-MM-SS_SATELLITE_NAME)

Fills all 28 pass_header columns with reception data and calculated metrics:
  - Culmination point detection (highest elevation azimuth/elevation)
  - Deframer sync analysis (first sync delay, total synced seconds, sync drops)
  - SNR/BER statistics (median values from synced samples only)
  - Peak SNR calculation
  - Sample counting and visibility filtering

Author: Andreas Horvath
Project: satpi
"""

import argparse
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Tuple, Optional

sys.path.insert(0, str(Path(__file__).parent.parent / "lib"))
from read_config import read_config, ConfigError


def utc_now_iso() -> str:
    """Get current UTC time in ISO format."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_args():
    """Parse command line arguments."""
    if len(sys.argv) == 1:
        sys.argv.append('--help')

    parser = argparse.ArgumentParser(
        description="Import satellite reception data from reception.json files into SQLite database. Fills all pass_header columns with calculated metrics from sample data.",
        epilog="""
WHAT IT DOES:
  - Reads reception.json files from pass directories in output_dir
  - Generates pass_id from (pass_date, pass_start_time, satellite)
  - Calculates metrics from sample data:
    * Culmination point (highest elevation azimuth/elevation)
    * Deframer sync timing (first sync, total synced seconds, sync drops)
    * SNR/BER statistics (medians from synced samples only)
  - Checks for duplicates using pass_id (prevents double imports)
  - Stores data in SQLite with all 27 pass_header columns populated
  - Saves detailed sample metrics (SNR, BER, azimuth, elevation, etc.)

INPUT:
  Single pass:  python3 import_to_db.py --pass-id 2026-05-05_15-26-38_METEOR-M2_4
  All files:    python3 import_to_db.py --all

OUTPUT:
  SQLite database: reception.db (2 tables)
    * pass_header  - Pass summary (28 columns) with pass_id PRIMARY KEY and all calculated metrics
    * pass_detail  - Per-sample telemetry linked via pass_id foreign key

REQUIRED CONFIG:
  [paths] reception_db_file    - Database path (e.g., results/database/reception.db)
  [paths] output_dir           - Directory containing pass subdirectories

EXAMPLES:
  # Import single pass by ID
  python3 import_to_db.py --pass-id 2026-05-05_15-26-38_METEOR-M2_4

  # Import all receptions from output_dir
  python3 import_to_db.py --all

OUTPUT SUMMARY:
  [import_to_db] imported: N        - Number of passes added
  [import_to_db] skipped:  N        - Number of duplicates skipped
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--pass-id",
        metavar="ID",
        help="Pass ID to import (format: YYYY-MM-DD_HH-MM-SS_SATELLITE_NAME). Script finds the corresponding directory in output_dir.",
    )
    input_group.add_argument(
        "--all",
        action="store_true",
        help="Import all reception.json files from output_dir",
    )
    return parser.parse_args()


def get_config_path() -> str:
    """Get path to config.ini."""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_dir, "config", "config.ini")


def init_db() -> int:
    """Initialize database by calling init_reception_db.py."""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    init_script = os.path.join(base_dir, "bin", "init_reception_db.py")

    result = subprocess.run(
        [sys.executable, init_script],
        capture_output=True,
        text=True,
    )

    if result.stdout:
        print(result.stdout.strip())

    if result.returncode != 0:
        if result.stderr:
            print(f"[import_to_db] ERROR: {result.stderr.strip()}")
        return result.returncode

    return 0


def open_db(db_path: str) -> sqlite3.Connection:
    """Open and configure SQLite database."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def create_setup_key(reception_setup: dict[str, Any]) -> str:
    """Create a unique hash key from reception setup hardware configuration.

    Uses SHA256 hash of sorted hardware field values to ensure consistency.
    Returns: 8-character hex string
    """
    # Sort keys and create consistent string representation
    sorted_items = sorted(reception_setup.items())
    combined = "|".join(f"{k}:{v}" for k, v in sorted_items)

    # Create SHA256 hash and use first 8 characters
    hash_obj = hashlib.sha256(combined.encode('utf-8'))
    return hash_obj.hexdigest()[:8]


def create_or_get_setup_id(conn: sqlite3.Connection, config: dict[str, Any]) -> int:
    """Create or get setup_id from reception_setup configuration.

    Reads [reception_setup] from config, creates a unique setup_key hash,
    and either inserts a new setup record or returns existing setup_id.

    Returns: setup_id (INTEGER)
    """
    reception_setup = config.get("reception_setup", {})
    if not reception_setup:
        raise ValueError("No [reception_setup] section found in config.ini")

    # Create unique key from hardware configuration
    setup_key = create_setup_key(reception_setup)

    # Check if setup already exists
    row = conn.execute(
        "SELECT setup_id FROM setup WHERE setup_key = ?",
        (setup_key,),
    ).fetchone()

    if row:
        return row[0]

    # Create new setup record
    conn.execute(
        """
        INSERT INTO setup (
            setup_key, antenna_type, antenna_location, antenna_orientation,
            lna, rf_filter, feedline, sdr, raspberry_pi, power_supply, additional_info
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            setup_key,
            str(reception_setup.get("antenna_type", "")),
            str(reception_setup.get("antenna_location", "")),
            str(reception_setup.get("antenna_orientation", "")),
            str(reception_setup.get("lna", "")),
            str(reception_setup.get("rf_filter", "")),
            str(reception_setup.get("feedline", "")),
            str(reception_setup.get("sdr", "")),
            str(reception_setup.get("raspberry_pi", "")),
            str(reception_setup.get("power_supply", "")),
            str(reception_setup.get("additional_info", "")),
        ),
    )
    conn.commit()

    # Get the newly created setup_id
    row = conn.execute(
        "SELECT setup_id FROM setup WHERE setup_key = ?",
        (setup_key,),
    ).fetchone()

    return row[0] if row else 1


def load_json(path: str) -> dict[str, Any]:
    """Load JSON file."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def extract_pass_info(reception_data: dict[str, Any]) -> Tuple[str, str, str, str]:
    """Extract pass_date, pass_start_time, satellite, and generate pass_id.

    pass_start format: "2026-05-05T15:26:38Z" → pass_date="2026-05-05", pass_start_time="15:26:38"
    pass_id format: "2026-05-05_15-26-38_SATELLITE_NAME"

    Returns: (pass_id, pass_date, pass_start_time, satellite)
    """
    pass_start = str(reception_data["pass_start"])
    if "T" in pass_start:
        pass_date, time_part = pass_start.split("T")
        pass_start_time = time_part.replace("Z", "").split("+")[0]
    else:
        pass_date = ""
        pass_start_time = ""

    satellite = str(reception_data["satellite"])

    # Generate pass_id
    if pass_date and pass_start_time and satellite:
        pass_start_time_normalized = pass_start_time.replace(":", "-")
        pass_id = f"{pass_date}_{pass_start_time_normalized}_{satellite.replace(' ', '_')}"
    else:
        pass_id = ""

    return pass_id, pass_date, pass_start_time, satellite


def calculate_pass_metrics(samples: list[dict[str, Any]]) -> dict[str, Any]:
    """Calculate all pass metrics from sample data.

    Returns: dict with keys:
      - sample_count
      - visible_sample_count (elevation >= 0)
      - culmination_azimuth_deg, culmination_elevation_deg
      - first_deframer_sync_delay_seconds
      - total_deframer_synced_seconds
      - sync_drop_count
      - median_snr_synced, median_ber_synced
      - peak_snr_db
    """
    metrics = {
        "sample_count": len(samples),
        "visible_sample_count": 0,
        "culmination_azimuth_deg": None,
        "culmination_elevation_deg": None,
        "first_deframer_sync_delay_seconds": None,
        "total_deframer_synced_seconds": 0,
        "sync_drop_count": 0,
        "median_snr_synced": None,
        "median_ber_synced": None,
        "peak_snr_db": None,
    }

    if not samples:
        return metrics

    # Calculate visible samples and culmination point
    visible_samples = []
    max_elevation = None
    culmination_idx = None

    for i, s in enumerate(samples):
        elevation = float(s.get("elevation_deg", -1.0))
        if elevation >= 0:
            metrics["visible_sample_count"] += 1
            visible_samples.append(s)
            if max_elevation is None or elevation > max_elevation:
                max_elevation = elevation
                culmination_idx = i

    if culmination_idx is not None and culmination_idx < len(samples):
        s = samples[culmination_idx]
        metrics["culmination_azimuth_deg"] = float(s.get("azimuth_deg", 0.0))
        metrics["culmination_elevation_deg"] = float(s.get("elevation_deg", 0.0))

    # Calculate sync metrics
    synced_samples = []
    was_synced = False
    for s in samples:
        deframer_state = str(s.get("deframer_state", "NOSYNC"))
        is_synced = (deframer_state == "SYNCED")

        if is_synced:
            synced_samples.append(s)
            metrics["total_deframer_synced_seconds"] += 1

            # Record first sync delay
            if metrics["first_deframer_sync_delay_seconds"] is None:
                metrics["first_deframer_sync_delay_seconds"] = len(synced_samples) - 1

        # Count sync drops
        if was_synced and not is_synced:
            metrics["sync_drop_count"] += 1

        was_synced = is_synced

    # Calculate SNR/BER metrics from synced samples
    if synced_samples:
        snr_values = []
        ber_values = []
        for s in synced_samples:
            snr = float(s.get("snr_db", 0.0))
            ber = float(s.get("ber", 0.0))
            snr_values.append(snr)
            ber_values.append(ber)

        if snr_values:
            snr_values.sort()
            metrics["median_snr_synced"] = snr_values[len(snr_values) // 2]

        if ber_values:
            ber_values.sort()
            metrics["median_ber_synced"] = ber_values[len(ber_values) // 2]

    # Calculate peak SNR from all samples
    peak_snr = None
    for s in samples:
        peak = float(s.get("peak_snr_db", 0.0))
        if peak_snr is None or peak > peak_snr:
            peak_snr = peak
    if peak_snr is not None:
        metrics["peak_snr_db"] = peak_snr

    return metrics


def pass_exists(conn: sqlite3.Connection, pass_id: str) -> bool:
    """Check if pass already exists in database by pass_id."""
    row = conn.execute(
        "SELECT 1 FROM pass_header WHERE pass_id = ?",
        (pass_id,),
    ).fetchone()
    return row is not None


def insert_pass(
    conn: sqlite3.Connection,
    source_file: str,
    data: dict[str, Any],
    setup_id: int,
) -> None:
    """Insert a single pass into the database with all pass_header columns filled."""
    pass_id, pass_date, pass_start_time, satellite = extract_pass_info(data)
    samples = list(data.get("samples", []))

    # Calculate all metrics from samples
    metrics = calculate_pass_metrics(samples)

    # INSERT pass_header with all columns
    conn.execute(
        """
        INSERT INTO pass_header (
            pass_id, setup_id, source_file, satellite, pipeline, frequency_hz, bandwidth_hz,
            gain, source_id, bias_t, pass_start, pass_end, scheduled_start, scheduled_end,
            sample_count, visible_sample_count, aos_azimuth_deg, culmination_azimuth_deg,
            los_azimuth_deg, culmination_elevation_deg, direction, first_deframer_sync_delay_seconds,
            total_deframer_synced_seconds, sync_drop_count, median_snr_synced, median_ber_synced,
            peak_snr_db, imported_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            pass_id,
            setup_id,
            str(source_file),
            satellite,
            str(data.get("pipeline", "")),
            int(data.get("frequency_hz", 0)),
            int(data.get("bandwidth_hz", 0)),
            float(data.get("gain", 0.0)),
            str(data.get("source_id", "")),
            1 if bool(data.get("bias_t", False)) else 0,
            str(data.get("pass_start", "")),
            str(data.get("pass_end", "")),
            str(data.get("scheduled_start", "")),
            str(data.get("scheduled_end", "")),
            metrics["sample_count"],
            metrics["visible_sample_count"],
            float(data.get("aos_azimuth_deg", 0.0)) if data.get("aos_azimuth_deg") else None,
            metrics["culmination_azimuth_deg"],
            float(data.get("los_azimuth_deg", 0.0)) if data.get("los_azimuth_deg") else None,
            metrics["culmination_elevation_deg"],
            str(data.get("direction", "")),
            metrics["first_deframer_sync_delay_seconds"],
            metrics["total_deframer_synced_seconds"],
            metrics["sync_drop_count"],
            metrics["median_snr_synced"],
            metrics["median_ber_synced"],
            metrics["peak_snr_db"],
            utc_now_iso(),
        ),
    )

    # INSERT pass_detail (samples) with pass_id foreign key
    if samples:
        detail_rows = []
        for s in samples:
            detail_rows.append(
                (
                    pass_id,
                    str(s["timestamp"]),
                    float(s.get("snr_db", 0.0)),
                    float(s.get("peak_snr_db", 0.0)),
                    float(s.get("ber", 0.0)),
                    str(s.get("viterbi_state", "NOSYNC")),
                    str(s.get("deframer_state", "NOSYNC")),
                    float(s.get("azimuth_deg", 0.0)),
                    float(s.get("elevation_deg", 0.0)),
                )
            )

        conn.executemany(
            """
            INSERT INTO pass_detail (
                pass_id, timestamp, snr_db, peak_snr_db, ber,
                viterbi_state, deframer_state, azimuth_deg, elevation_deg
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            detail_rows,
        )


def find_reception_file(pass_id: str, output_dir: str) -> Optional[str]:
    """Find reception.json file for given pass_id.

    Searches in output_dir for a directory starting with pass_id,
    then returns the path to reception.json inside it.

    Returns: Full path to reception.json or None if not found.
    """
    output_dir = os.path.expanduser(output_dir)

    try:
        for entry in os.listdir(output_dir):
            entry_path = os.path.join(output_dir, entry)
            if os.path.isdir(entry_path) and entry.startswith(pass_id):
                reception_file = os.path.join(entry_path, "reception.json")
                if os.path.isfile(reception_file):
                    return reception_file
    except OSError:
        pass

    return None


def resolve_input_files(args, output_dir: str) -> list[str]:
    """Resolve input file paths from arguments."""
    if args.all:
        return sorted(str(p) for p in Path(output_dir).glob("*/reception.json"))

    if args.pass_id:
        reception_file = find_reception_file(args.pass_id, output_dir)
        if reception_file and os.path.exists(reception_file):
            return [reception_file]
        else:
            raise SystemExit(f"[import_to_db] ERROR: No reception.json found for pass_id: {args.pass_id}")

    raise SystemExit("Provide --pass-id or --all")


def main() -> int:
    """Main import function."""
    args = parse_args()
    config_path = get_config_path()

    try:
        config = read_config(config_path)
    except ConfigError as e:
        print(f"[import_to_db] CONFIG ERROR: {e}")
        return 1

    # Initialize database before importing
    rc = init_db()
    if rc != 0:
        return rc

    db_path = str(config["paths"]["reception_db_file"])
    output_dir = str(config["paths"]["output_dir"])

    input_files = resolve_input_files(args, output_dir)

    if not input_files:
        print("[import_to_db] no input files found")
        return 1

    conn = open_db(db_path)
    imported = 0
    skipped = 0

    try:
        # Create or get setup_id from reception_setup configuration
        setup_id = create_or_get_setup_id(conn, config)

        for path in input_files:
            if not os.path.exists(path):
                print(f"[import_to_db] missing file: {path}")
                continue

            data = load_json(path)
            pass_id, pass_date, pass_start_time, satellite = extract_pass_info(data)

            # Check if pass already exists
            if pass_exists(conn, pass_id):
                print(
                    f"[import_to_db] skipped (already exists): {pass_id}"
                )
                skipped += 1
                continue

            insert_pass(conn, path, data, setup_id)
            imported += 1
            print(f"[import_to_db] imported: {pass_id}")

        conn.commit()
    except Exception as e:
        print(f"[import_to_db] ERROR: {e}")
        conn.rollback()
        return 1
    finally:
        conn.close()

    print(f"[import_to_db] database: {db_path}")
    print(f"[import_to_db] imported: {imported}")
    print(f"[import_to_db] skipped (duplicates): {skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
