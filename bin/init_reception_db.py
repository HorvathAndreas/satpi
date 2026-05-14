#!/usr/bin/env python3
"""satpi – Initialize SQLite database for reception analysis

Creates or validates the reception database schema with two main tables:
  - pass_header: Summary metadata for each pass (27 columns + pass_id PK)
  - pass_detail: Per-sample telemetry data linked to passes

The database stores:
  - Setup configuration (antenna, LNA, SDR, etc.)
  - Pass reception metrics (SNR, BER, duration, direction, culmination)
  - Sample-level telemetry (timestamp, SNR, BER, position, deframer state)

Author: Andreas Horvath
Project: satpi
"""

import argparse
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "lib"))
from read_config import read_config, ConfigError


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS setup (
    setup_id INTEGER PRIMARY KEY AUTOINCREMENT,
    setup_key TEXT NOT NULL UNIQUE,
    antenna_type TEXT,
    antenna_location TEXT,
    antenna_orientation TEXT,
    lna TEXT,
    rf_filter TEXT,
    feedline TEXT,
    sdr TEXT,
    raspberry_pi TEXT,
    power_supply TEXT,
    additional_info TEXT
);

CREATE TABLE IF NOT EXISTS pass_header (
    pass_id TEXT PRIMARY KEY,
    setup_id INTEGER NOT NULL,
    source_file TEXT NOT NULL,
    satellite TEXT NOT NULL,
    pipeline TEXT NOT NULL,
    frequency_hz INTEGER NOT NULL,
    bandwidth_hz INTEGER NOT NULL,
    gain REAL NOT NULL,
    source_id TEXT,
    bias_t INTEGER NOT NULL,
    pass_start TEXT NOT NULL,
    pass_end TEXT NOT NULL,
    scheduled_start TEXT NOT NULL,
    scheduled_end TEXT NOT NULL,
    sample_count INTEGER NOT NULL DEFAULT 0,
    visible_sample_count INTEGER NOT NULL DEFAULT 0,
    aos_azimuth_deg REAL,
    culmination_azimuth_deg REAL,
    los_azimuth_deg REAL,
    culmination_elevation_deg REAL,
    direction TEXT,
    first_deframer_sync_delay_seconds REAL,
    total_deframer_synced_seconds REAL,
    sync_drop_count INTEGER DEFAULT 0,
    median_snr_synced REAL,
    median_ber_synced REAL,
    peak_snr_db REAL,
    imported_at TEXT NOT NULL,
    FOREIGN KEY(setup_id) REFERENCES setup(setup_id)
);

CREATE TABLE IF NOT EXISTS pass_detail (
    pass_id TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    snr_db REAL,
    peak_snr_db REAL,
    ber REAL,
    viterbi_state TEXT,
    deframer_state TEXT,
    azimuth_deg REAL,
    elevation_deg REAL,
    PRIMARY KEY (pass_id, timestamp),
    FOREIGN KEY(pass_id) REFERENCES pass_header(pass_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_setup_key
    ON setup(setup_key);

CREATE INDEX IF NOT EXISTS idx_pass_header_satellite
    ON pass_header(satellite);

CREATE INDEX IF NOT EXISTS idx_pass_header_pipeline
    ON pass_header(pipeline);

CREATE INDEX IF NOT EXISTS idx_pass_header_pass_start
    ON pass_header(pass_start);

CREATE INDEX IF NOT EXISTS idx_pass_header_gain
    ON pass_header(gain);

CREATE INDEX IF NOT EXISTS idx_pass_header_culmination_elevation
    ON pass_header(culmination_elevation_deg);

CREATE INDEX IF NOT EXISTS idx_pass_header_culmination_azimuth
    ON pass_header(culmination_azimuth_deg);

CREATE INDEX IF NOT EXISTS idx_pass_header_direction
    ON pass_header(direction);

CREATE INDEX IF NOT EXISTS idx_pass_detail_pass_id
    ON pass_detail(pass_id);

CREATE INDEX IF NOT EXISTS idx_pass_detail_timestamp
    ON pass_detail(timestamp);
"""


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Initialize or validate SQLite reception database for satellite pass analysis",
        epilog="""
SCHEMA:
  Tables created:
    - setup        — Station/receiver configuration (antenna, LNA, SDR, etc.)
    - pass_header  — Pass summary (28 columns) with pass_id as primary key
    - pass_detail  — Per-sample telemetry linked via pass_id foreign key

USAGE:
  # Initialize database (prompts if exists)
  python3 init_reception_db.py

  # Initialize without confirmation
  python3 init_reception_db.py --no-confirm

  # Force reinitialize (backup old DB)
  python3 init_reception_db.py --reset --backup

  # Check if database is valid
  python3 init_reception_db.py --check
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--no-confirm",
        action="store_true",
        help="Skip confirmation prompt if database already exists (useful for scripts)",
    )

    parser.add_argument(
        "--reset",
        action="store_true",
        help="Reinitialize database (destructive — deletes all data unless --backup is used)",
    )

    parser.add_argument(
        "--backup",
        action="store_true",
        help="Create backup of existing database before resetting (only with --reset)",
    )

    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify database integrity without modifying it",
    )

    return parser.parse_args()


def backup_database(db_path: str) -> str:
    """Create a backup copy of existing database.

    Returns: Path to backup file
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{db_path}.backup_{timestamp}"
    try:
        with open(db_path, "rb") as src, open(backup_path, "wb") as dst:
            dst.write(src.read())
        print(f"[init_reception_db] Backup created: {backup_path}")
        return backup_path
    except Exception as e:
        print(f"[init_reception_db] ERROR: Failed to backup database: {e}")
        return ""


def check_database(db_path: str) -> bool:
    """Verify database integrity.

    Returns: True if valid, False otherwise
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Check if tables exist
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = [row[0] for row in cursor.fetchall()]
        expected_tables = {"setup", "pass_header", "pass_detail"}

        if not expected_tables.issubset(set(tables)):
            print(f"[init_reception_db] ERROR: Missing tables. Found: {tables}")
            return False

        # Check pass_header row count
        cursor.execute("SELECT COUNT(*) FROM pass_header")
        pass_count = cursor.fetchone()[0]
        print(f"[init_reception_db] Database is valid ({pass_count} passes imported)")

        conn.close()
        return True

    except Exception as e:
        print(f"[init_reception_db] ERROR: Database check failed: {e}")
        return False


def main() -> int:
    """Main entry point."""
    args = parse_args()

    # Load configuration
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(base_dir, "config", "config.ini")

    try:
        config = read_config(config_path)
    except ConfigError as e:
        print(f"[init_reception_db] CONFIG ERROR: {e}", file=sys.stderr)
        return 2

    db_path = config["paths"]["reception_db_file"]
    db_dir = os.path.dirname(db_path)

    # Create directory if needed
    os.makedirs(db_dir, exist_ok=True)

    # Handle --check mode
    if args.check:
        if not os.path.exists(db_path):
            print(f"[init_reception_db] ERROR: Database does not exist: {db_path}")
            return 1
        return 0 if check_database(db_path) else 1

    # Handle existing database
    if os.path.exists(db_path):
        if args.reset:
            if args.backup:
                backup_database(db_path)
            os.remove(db_path)
            print(f"[init_reception_db] Database reset: {db_path}")
        elif not args.no_confirm:
            response = input(
                f"[init_reception_db] Database exists: {db_path}\n"
                "Reinitialize? (y/N): "
            )
            if response.lower() != "y":
                print("[init_reception_db] Aborted")
                return 0
            # User confirmed reinitialize — remove the database file so SCHEMA creates fresh tables
            os.remove(db_path)
            print(f"[init_reception_db] Database removed for reinitialization")
        # else: --no-confirm is set, proceed silently (does not delete — only creates/validates schema)

    # Initialize/reinitialize database
    try:
        conn = sqlite3.connect(db_path)
        conn.executescript(SCHEMA)
        conn.commit()
        conn.close()
        print(f"[init_reception_db] Initialized: {db_path}")
        return 0

    except Exception as e:
        print(f"[init_reception_db] ERROR: Failed to initialize database: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
