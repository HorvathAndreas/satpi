#!/usr/bin/env python3
# satpi
# Initialize SQLite database for reception analysis.

import os
import sqlite3

import sys
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


def main() -> int:
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(base_dir, "config", "config.ini")

    try:
        config = read_config(config_path)
    except ConfigError as e:
        print(f"[init_reception_db] CONFIG ERROR: {e}")
        return 1

    db_path = config["paths"]["reception_db_file"]

    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()

    print(f"[init_reception_db] initialized: {db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
