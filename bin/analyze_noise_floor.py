#!/usr/bin/env python3
"""satpi – analyze_noise_floor

Reads noise floor measurements from noise_floor.db, cross-references with
reception.db, and produces plots and a summary report.

Outputs:
  - Hourly noise profile at target frequency (137.9 MHz by default)
  - Noise heatmap: hour of day vs. frequency
  - Timeline: noise level over time with pass outcomes overlaid
  - PDF summary report

Usage examples:
    # Full analysis, all data
    python3 analyze_noise_floor.py

    # Only data from a specific host
    python3 analyze_noise_floor.py --host satpi4

    # Custom target frequency and output dir
    python3 analyze_noise_floor.py --freq 137.1 --output-dir /tmp/noise_analysis

    # JSON output only, no plots
    python3 analyze_noise_floor.py --json-only

Author: Andreas Horvath
Project: Autonomous, config-driven satellite reception pipeline for Raspberry Pi
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from load_config import load_config, ConfigError

logger = logging.getLogger("satpi.analyze_noise")

NOISE_DB_NAME    = "noise_floor.db"
RECEPTION_DB_NAME = "reception.db"
REPORT_JSON_NAME = "noise_floor_report.json"
REPORT_PDF_NAME  = "noise_floor_report.pdf"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze noise floor measurements and correlate with pass outcomes."
    )
    parser.add_argument("--config", default=None,
                        help="Path to config.ini (default: ../config/config.ini)")
    parser.add_argument("--freq", type=float, default=137.9,
                        help="Target frequency in MHz for analysis (default: 137.9)")
    parser.add_argument("--freq-tolerance", type=float, default=0.1,
                        help="Frequency tolerance in MHz (default: 0.1)")
    parser.add_argument("--since", default=None,
                        help="Only analyse data since this datetime (ISO format, e.g. 2026-04-01). "
                             "Overrides --hours-back.")
    parser.add_argument("--hours-back", type=int, default=24,
                        help="Number of hours to look back for the main report (default: 24). "
                             "Ignored if --since is set.")
    parser.add_argument("--no-48h", action="store_true",
                        help="Skip the 48-hour overview section in the PDF")
    parser.add_argument("--host", default=None,
                        help="Filter measurements by host name")
    parser.add_argument("--output-dir", default=None,
                        help="Output directory for plots and report (default: results/reports)")
    parser.add_argument("--json-only", action="store_true",
                        help="Write JSON report only, skip plots and PDF")
    parser.add_argument("--no-pdf", action="store_true",
                        help="Skip PDF generation, write PNG plots only")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def get_config_path(cli_value: str | None) -> str:
    if cli_value:
        return os.path.abspath(cli_value)
    return str(SCRIPT_DIR.parent / "config" / "config.ini")


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging(log_dir: str, verbose: bool = False) -> None:
    os.makedirs(log_dir, exist_ok=True)
    level = logging.DEBUG if verbose else logging.INFO
    logger.setLevel(level)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    from logging.handlers import RotatingFileHandler
    fh = RotatingFileHandler(os.path.join(log_dir, "analyze_noise_floor.log"),
                             maxBytes=500_000, backupCount=2)
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def open_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    return conn


def load_noise_at_freq(
    db_path: str,
    target_hz: int,
    tolerance_hz: int,
    since: str | None,
    host: str | None,
) -> list[dict]:
    """Load per-measurement average noise at the target frequency."""
    conn = open_db(db_path)
    try:
        where = ["s.frequency_hz BETWEEN ? AND ?"]
        params: list = [target_hz - tolerance_hz, target_hz + tolerance_hz]
        if since:
            where.append("m.timestamp_utc >= ?")
            params.append(since)
        if host:
            where.append("m.host = ?")
            params.append(host)
        sql = f"""
            SELECT
                m.id,
                m.timestamp_utc,
                m.host,
                m.sdr_device,
                m.antenna,
                m.gain,
                m.label,
                AVG(s.power_dbm) AS avg_power_dbm,
                MIN(s.power_dbm) AS min_power_dbm,
                MAX(s.power_dbm) AS max_power_dbm,
                COUNT(s.id)      AS sample_count
            FROM noise_measurements m
            JOIN noise_samples s ON s.measurement_id = m.id
            WHERE {" AND ".join(where)}
            GROUP BY m.id
            ORDER BY m.timestamp_utc
        """
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def load_noise_by_freq_and_hour(
    db_path: str,
    since: str | None,
    host: str | None,
) -> dict:
    """Load average noise grouped by (hour_of_day, frequency_hz)."""
    conn = open_db(db_path)
    try:
        where = ["1=1"]
        params: list = []
        if since:
            where.append("m.timestamp_utc >= ?")
            params.append(since)
        if host:
            where.append("m.host = ?")
            params.append(host)
        sql = f"""
            SELECT
                CAST(SUBSTR(m.timestamp_utc, 12, 2) AS INTEGER) AS hour_utc,
                s.frequency_hz,
                AVG(s.power_dbm) AS avg_dbm
            FROM noise_measurements m
            JOIN noise_samples s ON s.measurement_id = m.id
            WHERE {" AND ".join(where)}
            GROUP BY hour_utc, s.frequency_hz
            ORDER BY hour_utc, s.frequency_hz
        """
        rows = conn.execute(sql, params).fetchall()
        result = defaultdict(dict)
        for r in rows:
            result[r["hour_utc"]][r["frequency_hz"]] = r["avg_dbm"]
        return dict(result)
    finally:
        conn.close()


def load_pass_outcomes(reception_db: str, since: str | None) -> list[dict]:
    """Load pass outcomes: timestamp + whether deframer synced."""
    if not os.path.exists(reception_db):
        return []
    conn = open_db(reception_db)
    try:
        where = ["1=1"]
        params: list = []
        if since:
            where.append("pass_start >= ?")
            params.append(since)
        sql = f"""
            SELECT
                pass_id,
                satellite,
                pass_start,
                culmination_elevation_deg,
                total_deframer_synced_seconds,
                first_deframer_sync_delay_seconds
            FROM pass_header
            WHERE {" AND ".join(where)}
            ORDER BY pass_start
        """
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]
    except sqlite3.Error as e:
        logger.warning("Could not load pass outcomes: %s", e)
        return []
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

def hour_of_day_utc(ts: str) -> int:
    """Extract hour from ISO timestamp string."""
    try:
        return int(ts[11:13])
    except (ValueError, IndexError):
        return -1


def compute_hourly_stats(rows: list[dict]) -> dict:
    """Group noise measurements by hour of day, compute mean/min/max."""
    by_hour: dict[int, list[float]] = defaultdict(list)
    for r in rows:
        h = hour_of_day_utc(r["timestamp_utc"])
        if h >= 0:
            by_hour[h].append(r["avg_power_dbm"])
    stats = {}
    for h, vals in sorted(by_hour.items()):
        stats[h] = {
            "hour_utc": h,
            "count": len(vals),
            "mean_dbm": round(sum(vals) / len(vals), 2),
            "min_dbm": round(min(vals), 2),
            "max_dbm": round(max(vals), 2),
        }
    return stats


def day_night_split(rows: list[dict], day_hours_utc: tuple = (6, 18)) -> dict:
    """Split measurements into day and night, return stats."""
    day, night = [], []
    for r in rows:
        h = hour_of_day_utc(r["timestamp_utc"])
        if day_hours_utc[0] <= h < day_hours_utc[1]:
            day.append(r["avg_power_dbm"])
        else:
            night.append(r["avg_power_dbm"])
    def _stats(vals):
        if not vals:
            return None
        return {
            "count": len(vals),
            "mean_dbm": round(sum(vals) / len(vals), 2),
            "min_dbm": round(min(vals), 2),
            "max_dbm": round(max(vals), 2),
        }
    day_stats = _stats(day)
    night_stats = _stats(night)
    delta = None
    if day_stats and night_stats:
        delta = round(day_stats["mean_dbm"] - night_stats["mean_dbm"], 2)
    return {
        "day_utc_hours": f"{day_hours_utc[0]:02d}:00–{day_hours_utc[1]:02d}:00",
        "day": day_stats,
        "night": night_stats,
        "day_vs_night_delta_dbm": delta,
        "conclusion": _conclusion(delta),
    }


def _conclusion(delta: float | None) -> str:
    if delta is None:
        return "Insufficient data for comparison."
    if delta >= 6:
        return (
            f"Strong daytime noise elevation (+{delta} dB above night baseline). "
            "A significant interference source appears to be active during daytime hours."
        )
    if delta >= 3:
        return (
            f"Moderate daytime noise elevation (+{delta} dB above night baseline). "
            "Some daytime interference is present; exact source unknown."
        )
    if delta >= 1:
        return (
            f"Slight daytime noise elevation (+{delta} dB above night baseline). "
            "Weak or occasional daytime interference detected."
        )
    if delta >= -1:
        return "No significant day/night difference. Noise floor appears stable."
    return (
        f"Noise is actually lower during the day ({delta} dB). "
        "Daytime interference is unlikely; look for other causes of pass failures."
    )



def load_waterfall_data(db_path: str, measurement_id: int) -> tuple[list, list, list]:
    """Load per-sample data for one measurement for waterfall plot.

    Returns (times, freqs_mhz, matrix) where matrix[time_idx][freq_idx] = power_dbm.
    """
    conn = open_db(db_path)
    try:
        rows = conn.execute("""
            SELECT sample_time_utc, frequency_hz, power_dbm
            FROM noise_samples
            WHERE measurement_id = ?
            ORDER BY sample_time_utc, frequency_hz
        """, (measurement_id,)).fetchall()
    finally:
        conn.close()

    if not rows:
        return [], [], []

    from collections import OrderedDict
    time_map: dict = OrderedDict()
    freq_set: set = set()
    for r in rows:
        t = r["sample_time_utc"]
        f = r["frequency_hz"]
        if t not in time_map:
            time_map[t] = {}
        time_map[t][f] = r["power_dbm"]
        freq_set.add(f)

    times     = list(time_map.keys())
    freqs_hz  = sorted(freq_set)
    freqs_mhz = [f / 1e6 for f in freqs_hz]
    matrix    = []
    for t in times:
        row = [time_map[t].get(f, float("nan")) for f in freqs_hz]
        matrix.append(row)
    return times, freqs_mhz, matrix


def correlate_with_passes(
    noise_rows: list[dict],
    pass_rows: list[dict],
    window_minutes: int = 30,
) -> list[dict]:
    """For each pass, find the nearest noise measurement and attach it."""
    results = []
    for p in pass_rows:
        try:
            pass_dt = datetime.fromisoformat(
                p["pass_start"].replace("Z", "+00:00")
            )
        except ValueError:
            continue
        best = None
        best_delta = timedelta.max
        for n in noise_rows:
            try:
                n_dt = datetime.fromisoformat(
                    n["timestamp_utc"].replace("Z", "+00:00")
                )
            except ValueError:
                continue
            delta = abs(pass_dt - n_dt)
            if delta < best_delta:
                best_delta = delta
                best = n
        if best and best_delta <= timedelta(minutes=window_minutes):
            results.append({
                "pass_id": p["pass_id"],
                "satellite": p["satellite"],
                "pass_start": p["pass_start"],
                "elevation_deg": p["culmination_elevation_deg"],
                "synced_seconds": p["total_deframer_synced_seconds"],
                "pass_ok": (p["total_deframer_synced_seconds"] or 0) > 10,
                "noise_dbm": best["avg_power_dbm"],
                "noise_ts": best["timestamp_utc"],
                "noise_delta_minutes": round(best_delta.total_seconds() / 60, 1),
            })
    return results


# ---------------------------------------------------------------------------
# Sunrise / sunset helper
# ---------------------------------------------------------------------------

def compute_sunrise_sunset(date, lat: float, lon: float, alt_m: float):
    """Return (sunrise_hour_utc, sunset_hour_utc) as floats, or (None, None).

    Uses skyfield + DE421 ephemeris. date may be a datetime.date or datetime.
    """
    try:
        from skyfield.api import Loader, wgs84
        from skyfield import almanac
        _loader = Loader("/tmp/skyfield")
        ts = _loader.timescale()
        eph = _loader("de421.bsp")
        location = wgs84.latlon(lat, lon, elevation_m=alt_m)
        import datetime as _dt
        d = date.date() if hasattr(date, "date") else date
        t0 = ts.utc(d.year, d.month, d.day, 0, 0, 0)
        t1 = ts.utc(d.year, d.month, d.day, 23, 59, 59)
        times, events = almanac.find_discrete(t0, t1, almanac.sunrise_sunset(eph, location))
        sunrise = sunset = None
        for t, ev in zip(times, events):
            dt = t.utc_datetime()
            h = dt.hour + dt.minute / 60 + dt.second / 3600
            if ev:   # 1 = sunrise
                sunrise = h
            else:    # 0 = sunset
                sunset = h
        return sunrise, sunset
    except Exception as e:
        logger.warning("Could not compute sunrise/sunset: %s", e)
        return None, None


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

def _try_import_matplotlib():
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        return plt, mdates
    except ImportError:
        logger.warning("matplotlib not available – skipping plots.")
        return None, None


def plot_hourly_profile(hourly_stats: dict, target_freq_mhz: float,
                        output_path: str, plt,
                        sunrise_utc: float | None = None,
                        sunset_utc: float | None = None) -> str | None:
    hours = sorted(hourly_stats.keys())
    if not hours:
        return None
    means = [hourly_stats[h]["mean_dbm"] for h in hours]
    mins  = [hourly_stats[h]["min_dbm"]  for h in hours]
    maxs  = [hourly_stats[h]["max_dbm"]  for h in hours]
    errl  = [means[i] - mins[i] for i in range(len(hours))]
    erru  = [maxs[i] - means[i] for i in range(len(hours))]

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.errorbar(hours, means, yerr=[errl, erru], fmt="o-",
                color="#1f77b4", ecolor="#aec7e8", capsize=4,
                linewidth=1.5, markersize=5, label=f"Noise @ {target_freq_mhz} MHz")
    # Day span: use actual sunrise/sunset if available, fallback to 06–18 UTC
    span_start = sunrise_utc if sunrise_utc is not None else 6
    span_end   = sunset_utc  if sunset_utc  is not None else 18
    if sunrise_utc is not None:
        span_label = f"Day ({sunrise_utc:.2f}h – {sunset_utc:.2f}h UTC)"
    else:
        span_label = "Day (06–18 UTC)"
    ax.axvspan(span_start, span_end, alpha=0.10, color="orange", label=span_label)

    # Sunrise / sunset lines from QTH
    if sunrise_utc is not None:
        ax.axvline(sunrise_utc, color="gold", linewidth=1.5, linestyle="--",
                   label=f"Sunrise {sunrise_utc:.2f}h UTC")
    if sunset_utc is not None:
        ax.axvline(sunset_utc, color="darkorange", linewidth=1.5, linestyle="--",
                   label=f"Sunset {sunset_utc:.2f}h UTC")

    ax.set_xlabel("Hour of day (UTC)")
    ax.set_ylabel("Avg noise floor (dBm)")
    ax.set_title(f"Noise floor by hour of day @ {target_freq_mhz} MHz")
    ax.set_xticks(range(0, 24))
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    logger.info("Saved hourly profile plot: %s", output_path)
    return output_path


def plot_heatmap(freq_hour_data: dict, output_path: str, plt) -> str | None:
    if not freq_hour_data:
        return None
    import numpy as np

    hours = sorted(freq_hour_data.keys())
    all_freqs = sorted({f for h_data in freq_hour_data.values() for f in h_data})
    if not all_freqs:
        return None

    matrix = []
    for freq in all_freqs:
        row = [freq_hour_data.get(h, {}).get(freq, float("nan")) for h in hours]
        matrix.append(row)
    matrix = np.array(matrix)

    fig, ax = plt.subplots(figsize=(14, 6))
    freq_mhz = [f / 1e6 for f in all_freqs]
    im = ax.imshow(matrix, aspect="auto", origin="lower",
                   extent=[min(hours) - 0.5, max(hours) + 0.5,
                           freq_mhz[0], freq_mhz[-1]],
                   cmap="RdYlGn_r", vmin=-25, vmax=0)
    plt.colorbar(im, ax=ax, label="Avg power (dBm)")
    ax.axvline(6, color="white", linestyle="--", alpha=0.5, linewidth=0.8)
    ax.axvline(18, color="white", linestyle="--", alpha=0.5, linewidth=0.8)
    ax.set_xlabel("Hour of day (UTC)")
    ax.set_ylabel("Frequency (MHz)")
    ax.set_title("Noise heatmap: frequency vs. hour of day")
    ax.set_xticks(range(0, 24))
    ax.axhline(137.9, color="cyan", linewidth=1.0, linestyle=":", label="137.9 MHz")
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    logger.info("Saved heatmap: %s", output_path)
    return output_path


def plot_timeline(noise_rows: list[dict], target_freq_mhz: float,
                  output_path: str, plt, mdates) -> str | None:
    if not noise_rows:
        return None

    ts_list, noise_list = [], []
    for r in noise_rows:
        try:
            dt = datetime.fromisoformat(r["timestamp_utc"].replace("Z", "+00:00"))
            ts_list.append(dt)
            noise_list.append(r["avg_power_dbm"])
        except ValueError:
            continue
    if not ts_list:
        return None

    fig, ax = plt.subplots(figsize=(14, 5))
    ax.plot(ts_list, noise_list, "b-o", markersize=4, linewidth=1.2,
            label=f"Noise @ {target_freq_mhz} MHz")

    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
    fig.autofmt_xdate()
    ax.set_ylabel("Noise floor (dBm)")
    ax.set_title(f"Noise floor timeline @ {target_freq_mhz} MHz")
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    logger.info("Saved timeline plot: %s", output_path)
    return output_path


def plot_waterfall(
    measurement_id: int,
    timestamp_utc: str,
    host: str,
    label: str | None,
    db_path: str,
    output_path: str,
    plt,
) -> str | None:
    """Generate a waterfall plot for a single measurement."""
    times, freqs_mhz, matrix = load_waterfall_data(db_path, measurement_id)
    if not matrix:
        return None

    import numpy as np
    mat = np.array(matrix)

    fig, ax = plt.subplots(figsize=(12, 5))  # fixed landscape size; aspect="auto" handles row count

    im = ax.imshow(
        mat,
        aspect="auto",
        origin="upper",
        extent=[freqs_mhz[0], freqs_mhz[-1], len(times), 0],
        cmap="inferno",
        vmin=-25,
        vmax=0,
        interpolation="nearest",
    )
    plt.colorbar(im, ax=ax, label="Power (dBm)")

    # Mark METEOR frequency
    ax.axvline(137.9, color="cyan", linewidth=1.0, linestyle="--", alpha=0.8)
    ax.text(137.92, 0.5, "137.9", color="cyan", fontsize=7, va="top")

    # Y-axis: show actual timestamps (every other label if many rows)
    step = max(1, len(times) // 8)
    ax.set_yticks(range(0, len(times), step))
    ax.set_yticklabels([times[i][11:19] for i in range(0, len(times), step)], fontsize=7)

    title = f"Waterfall – measurement {measurement_id} | {timestamp_utc[:16]} UTC | {host}"
    if label:
        title += f" | {label}"
    ax.set_title(title, fontsize=9)
    ax.set_xlabel("Frequency (MHz)")
    ax.set_ylabel("Time (UTC)")

    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    logger.info("Saved waterfall plot: %s", output_path)
    return output_path



# ---------------------------------------------------------------------------
# PDF
# ---------------------------------------------------------------------------

def write_pdf(output_path: str, report: dict, plot_paths: list[str],
              waterfall_paths: list[str] | None = None,
              plot_paths_48h: list[str] | None = None) -> None:
    try:
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.lib.units import mm
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.platypus import (
            SimpleDocTemplate, Paragraph, Spacer, Image as RLImage, PageBreak, Table, TableStyle
        )
        from reportlab.lib import colors
    except ImportError:
        logger.warning("reportlab not available – skipping PDF.")
        return

    tmp = output_path + ".tmp"
    doc = SimpleDocTemplate(tmp, pagesize=landscape(A4),
                            leftMargin=15*mm, rightMargin=15*mm,
                            topMargin=15*mm, bottomMargin=15*mm)
    styles = getSampleStyleSheet()
    story = []

    story.append(Paragraph("SATPI Noise Floor Analysis", styles["Title"]))
    story.append(Paragraph(
        f"Generated: {report['generated_at']} | "
        f"Target frequency: {report['target_freq_mhz']} MHz | "
        f"Measurements: {report['total_measurements']}",
        styles["Normal"]
    ))
    story.append(Spacer(1, 10*mm))

    dn = report.get("day_night_comparison", {})
    if dn:
        story.append(Paragraph("Day / Night Comparison", styles["Heading1"]))
        story.append(Paragraph(dn.get("conclusion", ""), styles["Normal"]))
        story.append(Spacer(1, 5*mm))

        rows = [["Period", "Count", "Mean (dBm)", "Min (dBm)", "Max (dBm)"]]
        for label, key in [("Day (06–18 UTC)", "day"), ("Night", "night")]:
            s = dn.get(key)
            if s:
                rows.append([label, str(s["count"]), str(s["mean_dbm"]),
                              str(s["min_dbm"]), str(s["max_dbm"])])
        if dn.get("day_vs_night_delta_dbm") is not None:
            rows.append(["Day – Night delta", "", str(dn["day_vs_night_delta_dbm"]) + " dB", "", ""])

        t = Table(rows, colWidths=[60*mm, 20*mm, 35*mm, 35*mm, 35*mm])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#D6EAF8")),
            ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
            ("FONTSIZE", (0,0), (-1,-1), 9),
        ]))
        story.append(t)
        story.append(Spacer(1, 8*mm))

    for p in plot_paths:
        if p and os.path.exists(p):
            story.append(PageBreak())
            img = RLImage(p)
            avail_w = doc.width
            avail_h = doc.height - 20*mm
            scale = min(avail_w / img.drawWidth, avail_h / img.drawHeight, 1.0)
            img.drawWidth  *= scale
            img.drawHeight *= scale
            story.append(img)

    if plot_paths_48h:
        story.append(PageBreak())
        story.append(Paragraph("Last 48 Hours – Overview", styles["Heading1"]))
        story.append(Spacer(1, 4*mm))
        for p in plot_paths_48h:
            if p and os.path.exists(p):
                story.append(PageBreak())
                img = RLImage(p)
                avail_w = doc.width
                avail_h = doc.height - 20*mm
                scale = min(avail_w / img.drawWidth, avail_h / img.drawHeight, 1.0)
                img.drawWidth  *= scale
                img.drawHeight *= scale
                story.append(img)

    if waterfall_paths:
        story.append(PageBreak())
        story.append(Paragraph("Appendix: Per-measurement waterfalls", styles["Heading1"]))
        story.append(Spacer(1, 4*mm))
        for wp in waterfall_paths:
            if wp and os.path.exists(wp):
                story.append(Paragraph(os.path.basename(wp).replace(".png", ""), styles["Normal"]))
                story.append(Spacer(1, 2*mm))
                img = RLImage(wp)
                avail_w = doc.width
                avail_h = doc.height - 30*mm
                scale = min(avail_w / img.drawWidth, avail_h / img.drawHeight, 1.0)
                img.drawWidth  *= scale
                img.drawHeight *= scale
                story.append(img)
                story.append(Spacer(1, 6*mm))

    doc.build(story)
    os.replace(tmp, output_path)
    logger.info("PDF written: %s", output_path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _load_all_measurements(db_path: str, since: str | None, host: str | None) -> list[dict]:
    """Load measurement metadata rows for waterfall generation."""
    conn = open_db(db_path)
    try:
        where = ["1=1"]
        params: list = []
        if since:
            where.append("timestamp_utc >= ?")
            params.append(since)
        if host:
            where.append("host = ?")
            params.append(host)
        rows = conn.execute(
            f"SELECT id, timestamp_utc, host, label FROM noise_measurements "
            f"WHERE {' AND '.join(where)} ORDER BY timestamp_utc",
            params
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()




def upload_results(output_dir: str, config: dict, log_dir: str) -> tuple[bool, str | None]:
    """Upload analysis output directory to rclone target if configured.

    Returns (success, share_link_or_None).
    """
    nf_cfg = config.get("noise_floor", {})
    if not nf_cfg:
        logger.warning("No [noise_floor] section in config — skipping upload.")
        return False, None

    enabled = str(nf_cfg.get("upload_enabled", "false")).strip().lower()
    if enabled not in ("1", "true", "yes", "on"):
        logger.info("Upload disabled in config (noise_floor.upload_enabled = false).")
        return False, None

    remote      = str(nf_cfg.get("rclone_remote", "")).strip()
    remote_path = str(nf_cfg.get("rclone_path", "")).strip()
    create_link = str(nf_cfg.get("create_link", "false")).strip().lower() in ("1", "true", "yes", "on")

    if not remote or not remote_path:
        logger.error("rclone_remote or rclone_path missing in [noise_floor] config.")
        return False, None

    target = f"{remote}:{remote_path}"
    upload_log = os.path.join(log_dir, "noise_floor_upload.log")
    logger.info("Uploading %s → %s", output_dir, target)

    try:
        with open(upload_log, "a", encoding="utf-8") as lf:
            result = subprocess.run(
                ["rclone", "copy", output_dir, target],
                stdout=lf, stderr=lf,
                timeout=300,
            )
        if result.returncode != 0:
            logger.error("rclone copy failed (rc=%d) — see %s", result.returncode, upload_log)
            return False, None
        logger.info("Upload successful → %s", target)
    except subprocess.TimeoutExpired:
        logger.error("rclone copy timed out after 300s")
        return False, None
    except FileNotFoundError:
        logger.error("rclone not found — is it installed?")
        return False, None

    link = None
    if create_link:
        try:
            link_result = subprocess.run(
                ["rclone", "link", target],
                capture_output=True, text=True, timeout=30,
            )
            if link_result.returncode == 0:
                link = link_result.stdout.strip()
                logger.info("Share link: %s", link)
        except Exception as e:
            logger.warning("Could not create share link: %s", e)

    return True, link


def main() -> int:
    args = parse_args()
    config_path = get_config_path(args.config)

    try:
        config = load_config(config_path)
    except ConfigError as e:
        print(f"[analyze_noise_floor] CONFIG ERROR: {e}", file=sys.stderr)
        return 2

    paths = config.get("paths", {})
    base_dir = paths.get("base_dir", str(SCRIPT_DIR.parent))
    log_dir  = os.path.join(base_dir, paths.get("log_dir", "logs"))
    db_dir   = os.path.join(base_dir, os.path.dirname(
        paths.get("reception_db_file", "results/database/reception.db")
    ))
    noise_db     = os.path.join(db_dir, NOISE_DB_NAME)
    reception_db = os.path.join(base_dir,
                                paths.get("reception_db_file", "results/database/reception.db"))
    output_dir   = args.output_dir or os.path.join(base_dir, "results", "reports")
    os.makedirs(output_dir, exist_ok=True)

    setup_logging(log_dir, verbose=args.verbose)
    logger.info("analyze_noise_floor.py started")

    if not os.path.exists(noise_db):
        logger.error("Noise floor database not found: %s", noise_db)
        logger.error("Run measure_noise_floor.py first to collect data.")
        return 1

    target_hz     = int(args.freq * 1e6)
    tolerance_hz  = int(args.freq_tolerance * 1e6)

    # Default time window: last N hours (unless --since was given explicitly)
    if args.since is None:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=args.hours_back)
        args.since = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
        logger.info("Time window: last %dh (since %s UTC)", args.hours_back, args.since)

    # 48h cutoff for the extended overview section
    cutoff_48h = (datetime.now(timezone.utc) - timedelta(hours=48)).strftime("%Y-%m-%dT%H:%M:%SZ")

    noise_rows = load_noise_at_freq(noise_db, target_hz, tolerance_hz,
                                    args.since, args.host)
    logger.info("Loaded %d noise measurements (last %dh)", len(noise_rows), args.hours_back)

    if not noise_rows:
        logger.error("No noise measurements found matching the filters.")
        return 1

    freq_hour_data = load_noise_by_freq_and_hour(noise_db, args.since, args.host)
    pass_rows      = load_pass_outcomes(reception_db, args.since)
    logger.info("Loaded %d pass records from reception.db", len(pass_rows))

    hourly_stats   = compute_hourly_stats(noise_rows)
    day_night      = day_night_split(noise_rows)
    pass_corr      = correlate_with_passes(noise_rows, pass_rows)

    report = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "target_freq_mhz": args.freq,
        "total_measurements": len(noise_rows),
        "total_passes_correlated": len(pass_corr),
        "day_night_comparison": day_night,
        "hourly_stats": hourly_stats,
        "pass_correlation": pass_corr,
    }

    json_path = os.path.join(output_dir, REPORT_JSON_NAME)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    logger.info("JSON report: %s", json_path)

    if day_night.get("conclusion"):
        logger.info("CONCLUSION: %s", day_night["conclusion"])

    if args.json_only:
        return 0

    plt, mdates = _try_import_matplotlib()
    plot_paths: list[str] = []

    if plt:
        # Compute sunrise / sunset from QTH in config for the most common measurement date
        sunrise_utc = sunset_utc = None
        qth = config.get("qth", {})
        lat = qth.get("latitude")
        lon = qth.get("longitude")
        alt = qth.get("altitude_m", 0)
        if lat is not None and lon is not None and noise_rows:
            # Pick the date with the most measurements
            from collections import Counter
            date_counts = Counter(
                r["timestamp_utc"][:10] for r in noise_rows
            )
            most_common_date_str = date_counts.most_common(1)[0][0]
            import datetime as _dt
            ref_date = _dt.date.fromisoformat(most_common_date_str)
            sunrise_utc, sunset_utc = compute_sunrise_sunset(ref_date, lat, lon, alt)
            if sunrise_utc is not None:
                logger.info("Sunrise: %.2fh UTC, Sunset: %.2fh UTC (QTH, %s)",
                            sunrise_utc, sunset_utc, most_common_date_str)

        p1 = plot_hourly_profile(
            hourly_stats, args.freq,
            os.path.join(output_dir, "noise_hourly_profile.png"), plt,
            sunrise_utc=sunrise_utc, sunset_utc=sunset_utc,
        )
        p2 = plot_heatmap(
            freq_hour_data,
            os.path.join(output_dir, "noise_heatmap.png"), plt
        )
        p3 = plot_timeline(
            noise_rows, args.freq,
            os.path.join(output_dir, "noise_timeline.png"), plt, mdates
        )
        plot_paths = [p for p in [p1, p2, p3] if p]

    # Generate per-measurement waterfall plots
    waterfall_paths: list[str] = []
    if plt and not args.json_only:
        measurements = _load_all_measurements(noise_db, args.since, args.host)
        wfall_dir = os.path.join(output_dir, "waterfalls")
        os.makedirs(wfall_dir, exist_ok=True)
        for m in measurements:
            wpath = os.path.join(
                wfall_dir,
                f"waterfall_{m['id']:04d}_{m['timestamp_utc'][:16].replace(':', '-')}.png"
            )
            result = plot_waterfall(
                m["id"], m["timestamp_utc"], m["host"], m.get("label"),
                noise_db, wpath, plt
            )
            if result:
                waterfall_paths.append(result)
        logger.info("Generated %d waterfall plots.", len(waterfall_paths))

    # 48h overview plots (hourly profile + timeline only, no waterfalls)
    plot_paths_48h: list[str] = []
    if plt and not args.json_only and not args.no_48h:
        noise_rows_48h = load_noise_at_freq(noise_db, target_hz, tolerance_hz, cutoff_48h, args.host)
        if len(noise_rows_48h) > len(noise_rows):
            logger.info("Generating 48h overview plots (%d measurements)...", len(noise_rows_48h))
            hourly_stats_48h = compute_hourly_stats(noise_rows_48h)
            # Sunrise/sunset for 48h: use most common date in the 48h window
            sunrise_48h = sunset_48h = None
            if lat is not None and lon is not None:
                from collections import Counter as _Counter
                date_counts_48h = _Counter(r["timestamp_utc"][:10] for r in noise_rows_48h)
                ref_date_48h_str = date_counts_48h.most_common(1)[0][0]
                import datetime as _dt
                ref_date_48h = _dt.date.fromisoformat(ref_date_48h_str)
                sunrise_48h, sunset_48h = compute_sunrise_sunset(ref_date_48h, lat, lon, alt)
            p48_hourly = plot_hourly_profile(
                hourly_stats_48h, args.freq,
                os.path.join(output_dir, "noise_hourly_profile_48h.png"), plt,
                sunrise_utc=sunrise_48h, sunset_utc=sunset_48h,
            )
            p48_timeline = plot_timeline(
                noise_rows_48h, args.freq,
                os.path.join(output_dir, "noise_timeline_48h.png"), plt, mdates,
            )
            plot_paths_48h = [p for p in [p48_hourly, p48_timeline] if p]
        else:
            logger.info("48h window contains same data as main window — skipping 48h section.")

    if not args.no_pdf:
        pdf_path = os.path.join(output_dir, REPORT_PDF_NAME)
        write_pdf(pdf_path, report, plot_paths, waterfall_paths,
                  plot_paths_48h=plot_paths_48h or None)

    # Upload results
    paths_cfg = config.get("paths", {})
    base_dir  = paths_cfg.get("base_dir", str(SCRIPT_DIR.parent))
    log_dir   = os.path.join(base_dir, paths_cfg.get("log_dir", "logs"))

    upload_ok, share_link = upload_results(output_dir, config, log_dir)
    if upload_ok:
        logger.info("Results uploaded to cloud storage.")
        if share_link:
            logger.info("Share link: %s", share_link)
    elif not args.json_only:
        logger.warning("Upload skipped or failed — results remain local at %s", output_dir)

    logger.info("Analysis complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
