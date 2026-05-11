#!/usr/bin/env python3
"""satpi – Import reception JSON data into SQLite database
Composite Key Structure: (pass_date, pass_start_time, satellite)

Author: Andreas Horvath
Project: satpi
"""

import argparse
import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent / "lib"))
from read_config import read_config, ConfigError


def utc_now_iso() -> str:
    """Get current UTC time in ISO format."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_args():
    """Parse command line arguments."""
    # If no arguments provided, show help
    if len(sys.argv) == 1:
        sys.argv.append('--help')

    parser = argparse.ArgumentParser(
        description="Import satellite reception data from reception.json files into SQLite database using composite key structure (pass_date, pass_start_time, satellite).",
        epilog="""
WHAT IT DOES:
  - Reads reception.json files from output_dir configured in config.ini
  - Extracts pass metadata (date, start time, satellite name)
  - Checks for duplicates using composite key (prevents double imports)
  - Stores data in SQLite with enforced foreign key constraints
  - Saves detailed sample metrics (SNR, BER, azimuth, elevation, etc.)

INPUT:
  Single file:  python3 import_to_db.py /path/to/reception.json
  All files:    python3 import_to_db.py --all

OUTPUT:
  SQLite database: reception.db (2 tables)
    * pass_header  - Pass summary with composite primary key
    * pass_detail  - Per-sample telemetry linked via composite foreign key

REQUIRED CONFIG:
  [paths] reception_db_file    - Database path (e.g., results/database/reception.db)
  [paths] output_dir           - Directory containing pass subdirectories

EXAMPLES:
  # Import single reception.json
  python3 import_to_db.py results/passes/2026-05-05_15-26-38_METEOR-M2_4/reception.json

  # Import all receptions from output_dir
  python3 import_to_db.py --all

OUTPUT SUMMARY:
  [import_to_db] imported: N        - Number of passes added
  [import_to_db] skipped:  N        - Number of duplicates skipped
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Input: mutually exclusive, required
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "input",
        nargs="?",
        help="Path to one reception.json file.",
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


def load_json(path: str) -> dict[str, Any]:
    """Load JSON file."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def extract_composite_key(reception_data: dict[str, Any]) -> Tuple[str, str, str]:
    """Extract pass_date, pass_start_time, satellite from reception.json data.

    pass_start format: "2026-05-05T15:26:38Z" → pass_date="2026-05-05", pass_start_time="15:26:38"

    Returns: (pass_date, pass_start_time, satellite)
    """
    # Extract date and time from pass_start
    pass_start = str(reception_data["pass_start"])
    if "T" in pass_start:
        pass_date, time_part = pass_start.split("T")
        pass_start_time = time_part.replace("Z", "").split("+")[0]
    else:
        pass_date = ""
        pass_start_time = ""

    satellite = str(reception_data["satellite"])
    return pass_date, pass_start_time, satellite


def pass_exists(
    conn: sqlite3.Connection, pass_date: str, pass_start_time: str, satellite: str
) -> bool:
    """Check if pass already exists in database using composite key."""
    row = conn.execute(
        "SELECT 1 FROM pass_header WHERE pass_date = ? AND pass_start_time = ? AND satellite = ?",
        (pass_date, pass_start_time, satellite),
    ).fetchone()
    return row is not None


def insert_pass(
    conn: sqlite3.Connection,
    source_file: str,
    data: dict[str, Any],
) -> None:
    """Insert a single pass into the database using composite key structure."""
    pass_date, pass_start_time, satellite = extract_composite_key(data)

    # Extract pass_end_time from pass_end timestamp
    pass_end = str(data.get("pass_end", ""))
    pass_end_time = None
    if "T" in pass_end:
        _, time_part = pass_end.split("T")
        pass_end_time = time_part.replace("Z", "").split("+")[0]

    samples = list(data.get("samples", []))

    # INSERT pass_header
    conn.execute(
        """
        INSERT INTO pass_header (
            pass_date, pass_start_time, satellite, frequency_hz, bandwidth_hz, pipeline,
            gain, source_id, bias_t, antenna_type, antenna_orientation,
            pass_end_time, scheduled_start, scheduled_end, max_elevation,
            aos_azimuth_deg, los_azimuth_deg, direction
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            pass_date,
            pass_start_time,
            satellite,
            int(data.get("frequency_hz", 0)),
            int(data.get("bandwidth_hz", 0)),
            str(data.get("pipeline", "")),
            float(data.get("gain", 0.0)),
            str(data.get("source_id", "")),
            1 if bool(data.get("bias_t", False)) else 0,
            str(data.get("antenna_type", "")),
            str(data.get("antenna_orientation", "")),
            pass_end_time,
            str(data.get("scheduled_start", "")),
            str(data.get("scheduled_end", "")),
            float(data.get("max_elevation", 0.0)) if data.get("max_elevation") else None,
            float(data.get("aos_azimuth_deg", 0.0)) if data.get("aos_azimuth_deg") else None,
            float(data.get("los_azimuth_deg", 0.0)) if data.get("los_azimuth_deg") else None,
            str(data.get("direction", "")),
        ),
    )

    # INSERT pass_detail (samples)
    if samples:
        detail_rows = []
        for s in samples:
            detail_rows.append(
                (
                    pass_date,
                    pass_start_time,
                    satellite,
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
                pass_date, pass_start_time, satellite, timestamp, snr_db, peak_snr_db, ber,
                viterbi_state, deframer_state, azimuth_deg, elevation_deg
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            detail_rows,
        )


def resolve_input_files(args, output_dir: str) -> list[str]:
    """Resolve input file paths from arguments."""
    if args.all:
        return sorted(str(p) for p in Path(output_dir).glob("*/reception.json"))

    if args.input:
        return [os.path.abspath(args.input)]

    raise SystemExit("Provide one input JSON file or use --all")


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
        for path in input_files:
            if not os.path.exists(path):
                print(f"[import_to_db] missing file: {path}")
                continue

            data = load_json(path)
            pass_date, pass_start_time, satellite = extract_composite_key(data)

            # Check if pass already exists
            if pass_exists(conn, pass_date, pass_start_time, satellite):
                print(
                    f"[import_to_db] skipped (already exists): "
                    f"{pass_date} {pass_start_time} {satellite}"
                )
                skipped += 1
                continue

            insert_pass(conn, path, data)
            imported += 1
            print(f"[import_to_db] imported: {pass_date} {pass_start_time} {satellite}")

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
