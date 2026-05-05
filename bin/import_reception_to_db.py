#!/usr/bin/env python3
# satpi
# Import one or more reception JSON files into the SQLite reception database.

import argparse
import hashlib
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "lib"))
from read_config import read_config, ConfigError

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_ts(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def parse_args():
    parser = argparse.ArgumentParser(description="Import reception JSON data into reception.db")
    parser.add_argument(
        "input",
        nargs="?",
        help="Path to one reception.json file. If omitted with --all, imports all files.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Import all reception.json files from results/passes",
    )
    return parser.parse_args()


def get_config_path() -> str:
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_dir, "config", "config.ini")


def open_db(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def load_json(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_setup_payload(data: dict[str, Any], setup_keys: list[str]) -> dict[str, str]:
    s = data.get("reception_setup", {})
    payload = {}

    for key in setup_keys:
        payload[key] = str(s.get(key, ""))

    return payload


def build_setup_key(setup: dict[str, str]) -> str:
    canonical = json.dumps(setup, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def get_or_create_setup_id(
    conn: sqlite3.Connection,
    data: dict[str, Any],
    setup_keys: list[str],
) -> int:
    setup = build_setup_payload(data, setup_keys)
    setup_key = build_setup_key(setup)

    row = conn.execute(
        "SELECT setup_id FROM setup WHERE setup_key = ?",
        (setup_key,),
    ).fetchone()
    if row:
        return int(row[0])

    insert_columns = ["setup_key"] + setup_keys
    placeholders = ", ".join("?" for _ in insert_columns)
    column_sql = ", ".join(insert_columns)
    values = [setup_key] + [setup[key] for key in setup_keys]

    conn.execute(
        f"""
        INSERT INTO setup (
            {column_sql}
        ) VALUES ({placeholders})
        """,
        values,
    )

    row = conn.execute(
        "SELECT setup_id FROM setup WHERE setup_key = ?",
        (setup_key,),
    ).fetchone()
    if row is None:
        raise RuntimeError("Failed to create or retrieve setup_id")
    return int(row[0])


def derive_sync_state(viterbi_state: str, deframer_state: str) -> str:
    if deframer_state == "SYNCED":
        return "SYNCED"
    if viterbi_state == "SYNCED":
        return "SYNCING"
    return "NOSYNC"


def compute_metrics(samples: list[dict[str, Any]]) -> dict[str, Any]:
    if not samples:
        return {
            "sample_count": 0,
            "visible_sample_count": 0,
            "aos_azimuth_deg": None,
            "culmination_azimuth_deg": None,
            "los_azimuth_deg": None,
            "culmination_elevation_deg": None,
            "direction": "unknown",
            "first_deframer_sync_delay_seconds": None,
            "total_deframer_synced_seconds": 0.0,
            "sync_drop_count": 0,
            "median_snr_synced": None,
            "median_ber_synced": None,
            "peak_snr_db": None,
        }

    samples_sorted = sorted(samples, key=lambda s: parse_ts(str(s["timestamp"])))
    visible = [s for s in samples_sorted if float(s["elevation_deg"]) >= 0.0]

    sample_count = len(samples_sorted)
    visible_sample_count = len(visible)

    aos_azimuth_deg = float(visible[0]["azimuth_deg"]) if visible else None
    los_azimuth_deg = float(visible[-1]["azimuth_deg"]) if visible else None

    culmination_sample = None
    if samples_sorted:
        culmination_sample = max(samples_sorted, key=lambda s: float(s["elevation_deg"]))

    culmination_azimuth_deg = (
        float(culmination_sample["azimuth_deg"])
        if culmination_sample and culmination_sample.get("azimuth_deg") is not None
        else None
    )
    culmination_elevation_deg = (
        float(culmination_sample["elevation_deg"])
        if culmination_sample and culmination_sample.get("elevation_deg") is not None
        else None
    )

    if len(visible) >= 2:
        direction = (
            "increasing_azimuth"
            if float(visible[-1]["azimuth_deg"]) >= float(visible[0]["azimuth_deg"])
            else "decreasing_azimuth"
        )
    else:
        direction = "unknown"

    first_ts = parse_ts(str(samples_sorted[0]["timestamp"]))
    first_sync_delay = None
    total_deframer_synced_seconds = 0.0
    sync_drop_count = 0
    synced_snrs: list[float] = []
    synced_bers: list[float] = []
    peak_snr_db = None

    prev_ts = None
    prev_sync = False

    for s in samples_sorted:
        ts = parse_ts(str(s["timestamp"]))
        snr = float(s["snr_db"])
        peak_snr = float(s["peak_snr_db"])
        ber = float(s["ber"])
        state = derive_sync_state(
            str(s.get("viterbi_state", "NOSYNC")),
            str(s.get("deframer_state", "NOSYNC")),
        )
        is_synced = state == "SYNCED"

        peak_snr_db = peak_snr if peak_snr_db is None else max(peak_snr_db, peak_snr)

        if is_synced and first_sync_delay is None:
            first_sync_delay = (ts - first_ts).total_seconds()

        if is_synced:
            synced_snrs.append(snr)
            synced_bers.append(ber)

        if prev_ts is not None and prev_sync:
            dt = (ts - prev_ts).total_seconds()
            if dt > 0:
                total_deframer_synced_seconds += dt

        if prev_sync and not is_synced:
            sync_drop_count += 1

        prev_ts = ts
        prev_sync = is_synced

    return {
        "sample_count": sample_count,
        "visible_sample_count": visible_sample_count,
        "aos_azimuth_deg": aos_azimuth_deg,
        "culmination_azimuth_deg": culmination_azimuth_deg,
        "los_azimuth_deg": los_azimuth_deg,
        "culmination_elevation_deg": culmination_elevation_deg,
        "direction": direction,
        "first_deframer_sync_delay_seconds": first_sync_delay,
        "total_deframer_synced_seconds": total_deframer_synced_seconds,
        "sync_drop_count": sync_drop_count,
        "median_snr_synced": median(synced_snrs) if synced_snrs else None,
        "median_ber_synced": median(synced_bers) if synced_bers else None,
        "peak_snr_db": peak_snr_db,
    }


def upsert_pass(
    conn: sqlite3.Connection,
    source_file: str,
    data: dict[str, Any],
    setup_keys: list[str],
) -> None:
    pass_id = str(data["pass_id"])
    setup_id = get_or_create_setup_id(conn, data, setup_keys)
    samples = list(data.get("samples", []))
    metrics = compute_metrics(samples)
    imported_at = utc_now_iso()

    conn.execute("DELETE FROM pass_detail WHERE pass_id = ?", (pass_id,))
    conn.execute("DELETE FROM pass_header WHERE pass_id = ?", (pass_id,))

    conn.execute(
        """
        INSERT INTO pass_header (
            pass_id, setup_id, source_file, satellite, pipeline,
            frequency_hz, bandwidth_hz, gain, source_id, bias_t,
            pass_start, pass_end, scheduled_start, scheduled_end,
            sample_count, visible_sample_count,
            aos_azimuth_deg, culmination_azimuth_deg, los_azimuth_deg, culmination_elevation_deg,
            direction, first_deframer_sync_delay_seconds, total_deframer_synced_seconds,
            sync_drop_count, median_snr_synced, median_ber_synced, peak_snr_db,
            imported_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            pass_id,
            setup_id,
            source_file,
            str(data["satellite"]),
            str(data["pipeline"]),
            int(data["frequency_hz"]),
            int(data["bandwidth_hz"]),
            float(data["gain"]),
            str(data.get("source_id", "")),
            1 if bool(data.get("bias_t", False)) else 0,
            str(data["pass_start"]),
            str(data["pass_end"]),
            str(data["scheduled_start"]),
            str(data["scheduled_end"]),
            int(metrics["sample_count"]),
            int(metrics["visible_sample_count"]),
            metrics["aos_azimuth_deg"],
            metrics["culmination_azimuth_deg"],
            metrics["los_azimuth_deg"],
            metrics["culmination_elevation_deg"],
            metrics["direction"],
            metrics["first_deframer_sync_delay_seconds"],
            metrics["total_deframer_synced_seconds"],
            int(metrics["sync_drop_count"]),
            metrics["median_snr_synced"],
            metrics["median_ber_synced"],
            metrics["peak_snr_db"],
            imported_at,
        ),
    )

    if samples:
        detail_rows = []
        for s in samples:
            detail_rows.append(
                (
                    pass_id,
                    str(s["timestamp"]),
                    float(s["snr_db"]),
                    float(s["peak_snr_db"]),
                    float(s["ber"]),
                    str(s.get("viterbi_state", "NOSYNC")),
                    str(s.get("deframer_state", "NOSYNC")),
                    float(s["azimuth_deg"]),
                    float(s["elevation_deg"]),
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


def resolve_input_files(args, captures_dir: str) -> list[str]:
    if args.all:
        return sorted(str(p) for p in Path(captures_dir).glob("*/reception.json"))

    if args.input:
        return [os.path.abspath(args.input)]

    raise SystemExit("Provide one input JSON file or use --all")


def main() -> int:
    args = parse_args()
    config_path = get_config_path()

    try:
        config = read_config(config_path)
    except ConfigError as e:
        print(f"[import_reception_to_db] CONFIG ERROR: {e}")
        return 1

    db_path = str(config["paths"]["reception_db_file"])
    captures_dir = str(config["paths"]["output_dir"])
    setup_keys = list(config["reception_setup"].keys())

    input_files = resolve_input_files(args, captures_dir)

    if not input_files:
        print("[import_reception_to_db] no input files found")
        return 1

    conn = open_db(db_path)
    imported = 0

    try:
        for path in input_files:
            if not os.path.exists(path):
                print(f"[import_reception_to_db] missing file: {path}")
                continue

            data = load_json(path)
            upsert_pass(conn, path, data, setup_keys)
            imported += 1
            print(f"[import_reception_to_db] imported: {path}")

        conn.commit()
    finally:
        conn.close()

    print(f"[import_reception_to_db] database: {db_path}")
    print(f"[import_reception_to_db] imported count: {imported}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
