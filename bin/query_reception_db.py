#!/usr/bin/env python3
# satpi
# Query SQLite reception database.

import argparse
import os
import sqlite3
from typing import Any

from load_config import load_config, ConfigError


def parse_args():
    parser = argparse.ArgumentParser(description="Query reception.db")
    parser.add_argument(
        "--config",
        default=None,
        help="Path to config.ini (default: ../config/config.ini relative to this script)",
    )
    parser.add_argument(
        "--latest",
        type=int,
        default=None,
        help="Show latest N passes",
    )
    parser.add_argument(
        "--satellite",
        default=None,
        help="Filter by satellite name",
    )
    parser.add_argument(
        "--pass-id",
        default=None,
        help="Show one pass by pass_id",
    )
    parser.add_argument(
        "--similar-pass-id",
        default=None,
        help="Find similar passes for the given pass_id",
    )
    parser.add_argument(
        "--max-elevation-delta",
        type=float,
        default=10.0,
        help="Maximum allowed delta for max_elevation_deg",
    )
    parser.add_argument(
        "--max-mid-azimuth-delta",
        type=float,
        default=20.0,
        help="Maximum allowed delta for mid_azimuth_deg",
    )
    parser.add_argument(
        "--same-direction-only",
        action="store_true",
        help="Require same direction for similar-pass search",
    )
    parser.add_argument(
        "--show-setup",
        action="store_true",
        help="Include setup columns in latest/satellite output",
    )
    return parser.parse_args()


def get_config_path(cli_value: str | None) -> str:
    if cli_value:
        return os.path.abspath(cli_value)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_dir, "config", "config.ini")


def open_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def angular_delta_deg(a: float | None, b: float | None) -> float | None:
    if a is None or b is None:
        return None
    diff = abs(a - b) % 360.0
    return min(diff, 360.0 - diff)

def print_rows(rows: list[Any]) -> None:
    if not rows:
        print("[query_reception_db] no rows")
        return

    headers = list(rows[0].keys())
    widths: dict[str, int] = {h: len(h) for h in headers}

    for row in rows:
        for h in headers:
            value = "" if row[h] is None else str(row[h])
            widths[h] = max(widths[h], len(value))

    header_line = " | ".join(h.ljust(widths[h]) for h in headers)
    sep_line = "-+-".join("-" * widths[h] for h in headers)

    print(header_line)
    print(sep_line)

    for row in rows:
        print(" | ".join(("" if row[h] is None else str(row[h])).ljust(widths[h]) for h in headers))


def query_latest(conn: sqlite3.Connection, limit: int, satellite: str | None, show_setup: bool) -> list[sqlite3.Row]:
    if show_setup:
        sql = """
        SELECT
            h.pass_id,
            h.satellite,
            h.pass_start,
            h.gain,
            h.max_elevation_deg,
            h.total_deframer_synced_seconds,
            h.median_snr_synced,
            h.median_ber_synced,
            s.antenna_type,
            s.antenna_location,
            s.feedline,
            s.raspberry_pi,
            s.power_supply
        FROM pass_header h
        JOIN setup s ON h.setup_id = s.setup_id
        """
    else:
        sql = """
        SELECT
            h.pass_id,
            h.satellite,
            h.pass_start,
            h.gain,
            h.max_elevation_deg,
            h.total_deframer_synced_seconds,
            h.median_snr_synced,
            h.median_ber_synced
        FROM pass_header h
        """

    params: list[Any] = []

    if satellite:
        sql += " WHERE h.satellite = ?"
        params.append(satellite)

    sql += " ORDER BY h.pass_start DESC LIMIT ?"
    params.append(limit)

    return list(conn.execute(sql, params).fetchall())


def query_pass_id(conn: sqlite3.Connection, pass_id: str) -> list[sqlite3.Row]:
    sql = """
    SELECT
        h.pass_id,
        h.satellite,
        h.pipeline,
        h.frequency_hz,
        h.bandwidth_hz,
        h.gain,
        h.source_id,
        h.bias_t,
        h.pass_start,
        h.pass_end,
        h.scheduled_start,
        h.scheduled_end,
        h.sample_count,
        h.visible_sample_count,
        h.start_azimuth_deg,
        h.mid_azimuth_deg,
        h.end_azimuth_deg,
        h.max_elevation_deg,
        h.direction,
        h.first_deframer_sync_delay_seconds,
        h.total_deframer_synced_seconds,
        h.sync_drop_count,
        h.median_snr_synced,
        h.median_ber_synced,
        h.peak_snr_db,
        h.imported_at,
        s.antenna_type,
        s.antenna_location,
        s.antenna_orientation,
        s.lna,
        s.rf_filter,
        s.feedline,
        s.raspberry_pi,
        s.power_supply,
        s.additional_info
    FROM pass_header h
    JOIN setup s ON h.setup_id = s.setup_id
    WHERE h.pass_id = ?
    """
    return list(conn.execute(sql, (pass_id,)).fetchall())

def query_similar_passes(
    conn: sqlite3.Connection,
    pass_id: str,
    max_elevation_delta: float,
    max_mid_azimuth_delta: float,
    same_direction_only: bool,
) -> list[dict[str, Any]]:
    ref_sql = """
    SELECT
        h.pass_id,
        h.satellite,
        h.pipeline,
        h.pass_start,
        h.gain,
        h.max_elevation_deg,
        h.mid_azimuth_deg,
        h.direction
    FROM pass_header h
    WHERE h.pass_id = ?
    """

    ref = conn.execute(ref_sql, (pass_id,)).fetchone()
    if ref is None:
        return []

    sql = """
    SELECT
        h.pass_id,
        h.satellite,
        h.pass_start,
        h.gain,
        h.max_elevation_deg,
        h.mid_azimuth_deg,
        h.direction,
        h.total_deframer_synced_seconds,
        h.median_snr_synced,
        h.median_ber_synced,
        s.antenna_type,
        s.antenna_location,
        s.feedline,
        s.raspberry_pi,
       s.power_supply
    FROM pass_header h
    JOIN setup s ON h.setup_id = s.setup_id
    WHERE h.pass_id != ?
      AND h.satellite = ?
      AND h.pipeline = ?
    ORDER BY h.pass_start DESC
    """

    candidates = conn.execute(sql, (pass_id, ref["satellite"], ref["pipeline"])).fetchall()

    filtered: list[dict[str, Any]] = []

    for row in candidates:
        if ref["max_elevation_deg"] is None or row["max_elevation_deg"] is None:
            continue

        elevation_delta = abs(float(row["max_elevation_deg"]) - float(ref["max_elevation_deg"]))
        if elevation_delta > max_elevation_delta:
            continue

        mid_delta = angular_delta_deg(
            float(ref["mid_azimuth_deg"]) if ref["mid_azimuth_deg"] is not None else None,
            float(row["mid_azimuth_deg"]) if row["mid_azimuth_deg"] is not None else None,
        )
        if mid_delta is None or mid_delta > max_mid_azimuth_delta:
            continue

        if same_direction_only and row["direction"] != ref["direction"]:
            continue

        filtered.append(
            {
                "pass_id": row["pass_id"],
                "satellite": row["satellite"],
                "pass_start": row["pass_start"],
                "gain": row["gain"],
                "max_elevation_deg": row["max_elevation_deg"],
                "mid_azimuth_deg": row["mid_azimuth_deg"],
                "direction": row["direction"],
                "total_deframer_synced_seconds": row["total_deframer_synced_seconds"],
                "median_snr_synced": row["median_snr_synced"],
                "median_ber_synced": row["median_ber_synced"],
                "antenna_type": row["antenna_type"],
                "antenna_location": row["antenna_location"],
                "feedline": row["feedline"],
                "raspberry_pi": row["raspberry_pi"],
                "power_supply": row["power_supply"],
                "elevation_delta_deg": elevation_delta,
                "mid_azimuth_delta_deg": mid_delta,
            }
        )

    filtered.sort(
        key=lambda r: (
            r["elevation_delta_deg"],
            r["mid_azimuth_delta_deg"],
            r["pass_start"],
        )
    )
    return filtered

def main() -> int:
    args = parse_args()
    config_path = get_config_path(args.config)

    try:
        config = load_config(config_path)
    except ConfigError as e:
        print(f"[query_reception_db] CONFIG ERROR: {e}")
        return 1

    if not config["reception_db"]["enabled"]:
        print("[query_reception_db] reception_db disabled in config")
        return 1

    db_path = config["reception_db"]["db_path"]

    if not os.path.exists(db_path):
        print(f"[query_reception_db] database not found: {db_path}")
        return 1

    conn = open_db(db_path)
    try:
        if args.pass_id:
            rows = query_pass_id(conn, args.pass_id)
            print_rows(rows)
            return 0

        if args.similar_pass_id:
            rows = query_similar_passes(
                conn,
                args.similar_pass_id,
                args.max_elevation_delta,
                args.max_mid_azimuth_delta,
                args.same_direction_only,
            )
            print_rows(rows)
            return 0

        limit = args.latest if args.latest is not None else 10
        rows = query_latest(conn, limit, args.satellite, args.show_setup)
        print_rows(rows)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
