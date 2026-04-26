#!/usr/bin/env python3
"""Vergleicht gelabelte Noise-Floor-Messungen aus der Datenbank."""

import sqlite3

DB = '/home/andreas/satpi/results/database/noise_floor.db'
con = sqlite3.connect(DB)

def get_stats(mid):
    row = con.execute(
        "SELECT COUNT(*), MIN(power_dbm), AVG(power_dbm), MAX(power_dbm) "
        "FROM noise_samples WHERE measurement_id = ?", (mid,)
    ).fetchone()
    count, mn, avg, mx = row
    return count, round(mn,2) if mn else None, round(avg,2) if avg else None, round(mx,2) if mx else None

def print_section(title, measurements, baseline_label=None):
    print(f"\n{'='*100}")
    print(f"  {title}")
    print(f"{'='*100}")
    print(f"{'Messung':<30} {'Zeit (UTC)':<22} {'Samples':>8} {'Min':>8} {'Mittel':>10} {'Max':>8} {'Delta':>9}")
    print('-' * 100)

    baseline_mean = None

    for label, name, is_baseline in measurements:
        if label is None:
            # Letzte Messung ohne Label (regulärer Timer)
            row = con.execute(
                "SELECT id, timestamp_utc FROM noise_measurements "
                "WHERE label IS NULL ORDER BY timestamp_utc DESC LIMIT 1"
            ).fetchone()
        else:
            row = con.execute(
                "SELECT id, timestamp_utc FROM noise_measurements "
                "WHERE label = ? ORDER BY id DESC LIMIT 1", (label,)
            ).fetchone()

        if not row:
            print(f"{name:<30} {'(keine Messung)'}")
            continue

        mid, ts = row
        count, mn, avg, mx = get_stats(mid)
        if is_baseline:
            baseline_mean = avg
        delta = f"{avg - baseline_mean:+.2f} dB" if baseline_mean is not None and avg and not is_baseline else "—"
        print(f"{name:<30} {ts[:19]:<22} {count:>8} {mn:>8} {avg:>10} {mx:>8} {delta:>9}")

# === Solarinverter / Aquarium Test ===
print_section("TEST 1: Interferenzquellen", [
    (None,           'Baseline 18:00 (alles ein)',   True),
    ('inverter_off', 'Solarinverter aus',             False),
    ('aquarium_off', 'Aquarienbeleuchtung aus',       False),
    ('all_on',       'Alles wieder ein',              False),
])

# === Hausstrom Test ===
print_section("TEST 2: Hausstrom aus (Powerbank)", [
    ('house_baseline', 'Baseline (Hausstrom an)',     True),
    ('house_off',      'Hausstrom aus (Powerbank)',   False),
])

con.close()
print()
