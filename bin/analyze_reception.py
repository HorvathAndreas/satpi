#!/usr/bin/env python3
"""satpi - analyze_reception

Score the image quality of one or more decoded satellite passes.

Reads decoded channel images (e.g. MSU-MR-1.png) from a pass directory,
computes per-channel statistics (mean, stddev, dynamic range), and outputs
a quality score (0-100) plus a class (good/fair/bad). Recommends whether
the pass should be uploaded and/or notified by mail.

All thresholds and pipeline-specific image discovery rules are read from
[processing_thresholds] in config.ini via lib/read_config.py.

Author: Andreas Horvath
Project: Autonomous, config-driven satellite reception pipeline for Raspberry Pi
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from PIL import Image, ImageStat

sys.path.insert(0, str(Path(__file__).parent.parent / "lib"))
from read_config import read_config, ConfigError  # noqa: E402


# --- Exit codes --------------------------------------------------------------

EXIT_GOOD = 0
EXIT_FAIR = 1
EXIT_BAD = 2
EXIT_MISSING = 3


# --- Image stats -------------------------------------------------------------

def analyze_image(path: Path) -> Dict[str, Any]:
    img = Image.open(path).convert("L")
    stat = ImageStat.Stat(img)
    lo, hi = img.getextrema()
    return {
        "file": str(path),
        "size": img.size,
        "min": lo,
        "max": hi,
        "mean": round(stat.mean[0], 3),
        "stddev": round(stat.stddev[0], 3),
        "range": hi - lo,
    }


# --- Per-channel classification ---------------------------------------------

def classify_channel(ch: Dict[str, Any], thr: Dict[str, Any]) -> str:
    """A channel is:
        bad  if EITHER stddev <  channel_stddev_fair OR  range <  channel_range_fair
        good if BOTH   stddev >= channel_stddev_good AND range >= channel_range_good
        fair otherwise
    """
    if ch["stddev"] < thr["channel_stddev_fair"] or ch["range"] < thr["channel_range_fair"]:
        return "bad"
    if ch["stddev"] >= thr["channel_stddev_good"] and ch["range"] >= thr["channel_range_good"]:
        return "good"
    return "fair"


# --- Aggregate score ---------------------------------------------------------

def score_channels(channels: List[Dict[str, Any]], thr: Dict[str, Any]) -> Tuple[float, str, List[str], Dict[str, Any]]:
    """Compute aggregate score, class, findings, and summary stats."""
    findings: List[str] = []

    stddevs = [c["stddev"] for c in channels]
    ranges = [c["range"] for c in channels]
    means = [c["mean"] for c in channels]

    avg_stddev = sum(stddevs) / len(stddevs)
    avg_range = sum(ranges) / len(ranges)
    mean_spread = max(means) - min(means)

    score = 0.0

    # Internal-variation contribution.
    if avg_stddev >= 35:
        score += 45
        findings.append("Channels show strong internal variation.")
    elif avg_stddev >= 15:
        score += 25
        findings.append("Channels show moderate internal variation.")
    else:
        score += 5
        findings.append("Channels are nearly flat.")

    # Dynamic-range contribution.
    if avg_range >= 120:
        score += 30
        findings.append("Dynamic range is wide.")
    elif avg_range >= 60:
        score += 18
        findings.append("Dynamic range is moderate.")
    else:
        score += 5
        findings.append("Dynamic range is very limited.")

    # Channel-spread contribution.
    if mean_spread >= 15:
        score += 15
        findings.append("Channels differ meaningfully from each other.")
    elif mean_spread >= 5:
        score += 8
        findings.append("Channels differ slightly from each other.")
    else:
        score += 2
        findings.append("Channels are very similar to each other.")

    # Channel-health bonus / penalty.
    classes = [classify_channel(c, thr) for c in channels]
    good_count = classes.count("good")
    fair_count = classes.count("fair")
    bad_count = classes.count("bad")

    if good_count >= 2:
        score += 10
        findings.append("At least two channels look healthy.")
    elif bad_count >= 2:
        score -= 10
        findings.append("At least two channels look poor.")

    score = max(0.0, min(100.0, round(score, 1)))

    if score >= thr["score_good"]:
        quality_class = "good"
    elif score >= thr["score_fair"]:
        quality_class = "fair"
    else:
        quality_class = "bad"

    summary = {
        "avg_stddev": round(avg_stddev, 3),
        "avg_range": round(avg_range, 3),
        "mean_spread": round(mean_spread, 3),
        "good_channels": good_count,
        "fair_channels": fair_count,
        "bad_channels": bad_count,
    }
    return score, quality_class, findings, summary


# --- Action mapping ----------------------------------------------------------

def actions_for_class(quality_class: str) -> Tuple[bool, bool]:
    """Return (copy_recommended, email_recommended) for a quality class."""
    if quality_class == "good":
        return True, True
    if quality_class == "fair":
        return True, False
    return False, False


def exit_code_for_class(quality_class: str) -> int:
    return {
        "good": EXIT_GOOD,
        "fair": EXIT_FAIR,
        "bad": EXIT_BAD,
    }[quality_class]


# --- Pipeline detection ------------------------------------------------------

_PIPELINE_SUFFIX_RE = re.compile(r"_(?P<fmt>lrpt|hrpt)$", re.IGNORECASE)


def pipeline_to_format(pipeline: str) -> Optional[str]:
    """Map a pipeline name like 'meteor_m2-x_lrpt' to format prefix 'lrpt'.

    Returns 'lrpt' or 'hrpt', or None if the pipeline name does not end with
    a recognised suffix.
    """
    if not pipeline:
        return None
    m = _PIPELINE_SUFFIX_RE.search(pipeline.strip())
    return m.group("fmt").lower() if m else None


# --- Reception.json ----------------------------------------------------------

def load_reception_json(pass_dir: Path) -> Dict[str, Any]:
    """Load reception.json if present; return {} on any error."""
    rj = pass_dir / "reception.json"
    if not rj.exists():
        return {}
    try:
        with open(rj, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


# --- Channel discovery -------------------------------------------------------

def discover_channel_files(
    pass_dir: Path,
    subdir: str,
    wanted_channels: List[int],
) -> Tuple[Dict[int, Path], List[int]]:
    """Find channel image files in <pass_dir>/<subdir>/.

    Filename convention: <subdir>-N.png (e.g. MSU-MR-3.png).
    Returns (found_map, missing) where found_map maps channel number to file path
    and missing is the sorted list of wanted channels that weren't found.
    """
    sub = pass_dir / subdir
    found: Dict[int, Path] = {}
    if sub.is_dir():
        # Match files of the form <subdir>-<digits>.png at end
        # (use the configured subdir as the prefix to be picky).
        pattern = re.compile(rf"^{re.escape(subdir)}-(\d+)\.png$", re.IGNORECASE)
        for f in sub.iterdir():
            m = pattern.match(f.name)
            if not m:
                continue
            n = int(m.group(1))
            if n in wanted_channels:
                found[n] = f
    missing = sorted(set(wanted_channels) - set(found.keys()))
    return found, missing


# --- Single-pass analysis ----------------------------------------------------

def analyze_pass(pass_dir: Path, cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze one pass directory. Returns a result dict (always); the dict's
    'exit_code' field carries the per-pass exit code regardless of mode.
    """
    thr = cfg["processing_thresholds"]
    reception = load_reception_json(pass_dir)
    pipeline_name = reception.get("pipeline", "")

    fmt = pipeline_to_format(pipeline_name)
    if fmt is None:
        return _missing_result(
            pass_dir, reception,
            f"Cannot derive image format from pipeline '{pipeline_name}': "
            "expected name ending in '_lrpt' or '_hrpt'.",
        )

    pipelines = thr.get("pipelines", {})
    pipe_cfg = pipelines.get(fmt)
    if not pipe_cfg:
        return _missing_result(
            pass_dir, reception,
            f"No [processing_thresholds] entries for pipeline format '{fmt}' "
            f"(expected {fmt}_dir / {fmt}_channels in config.ini).",
        )

    subdir = pipe_cfg["dir"]
    wanted = pipe_cfg["channels"]
    tol_missing = pipe_cfg["tolerate_missing_channel"]

    found, missing = discover_channel_files(pass_dir, subdir, wanted)
    if len(missing) > tol_missing:
        return _missing_result(
            pass_dir, reception,
            f"{len(missing)} of {len(wanted)} channels missing in "
            f"{pass_dir / subdir} (tolerate up to {tol_missing}); "
            f"missing channel numbers: {missing}",
            pipeline=pipeline_name, fmt=fmt, subdir=subdir,
            wanted_channels=wanted,
        )

    if not found:
        return _missing_result(
            pass_dir, reception,
            f"No channel images found in {pass_dir / subdir}.",
            pipeline=pipeline_name, fmt=fmt, subdir=subdir,
            wanted_channels=wanted,
        )

    # Score the channels we have.
    channels = [analyze_image(found[n]) for n in sorted(found.keys())]
    for ch, n in zip(channels, sorted(found.keys())):
        ch["channel"] = n
        ch["channel_class"] = classify_channel(ch, thr)
    score, quality_class, findings, summary = score_channels(channels, thr)
    copy_rec, email_rec = actions_for_class(quality_class)

    return {
        "pass_dir": str(pass_dir),
        "pass_id": reception.get("pass_id"),
        "satellite": reception.get("satellite"),
        "direction": reception.get("direction"),
        "max_elevation": reception.get("max_elevation"),
        "gain": reception.get("gain"),
        "pipeline": pipeline_name,
        "format": fmt,
        "subdir": subdir,
        "wanted_channels": wanted,
        "missing_channels": missing,
        "tolerate_missing_channel": tol_missing,
        "quality_score": score,
        "quality_class": quality_class,
        "copy_recommended": copy_rec,
        "email_recommended": email_rec,
        "summary": summary,
        "channels": channels,
        "findings": findings,
        "exit_code": exit_code_for_class(quality_class),
    }


def _missing_result(
    pass_dir: Path,
    reception: Dict[str, Any],
    reason: str,
    *,
    pipeline: str = "",
    fmt: Optional[str] = None,
    subdir: Optional[str] = None,
    wanted_channels: Optional[List[int]] = None,
) -> Dict[str, Any]:
    return {
        "pass_dir": str(pass_dir),
        "pass_id": reception.get("pass_id"),
        "satellite": reception.get("satellite"),
        "direction": reception.get("direction"),
        "max_elevation": reception.get("max_elevation"),
        "gain": reception.get("gain"),
        "pipeline": pipeline or reception.get("pipeline", ""),
        "format": fmt,
        "subdir": subdir,
        "wanted_channels": wanted_channels,
        "missing_channels": None,
        "tolerate_missing_channel": None,
        "quality_score": 0.0,
        "quality_class": "missing",
        "copy_recommended": False,
        "email_recommended": False,
        "summary": None,
        "channels": [],
        "findings": [reason],
        "exit_code": EXIT_MISSING,
    }


# --- Output ------------------------------------------------------------------

def _fmt(value: Any, default: str = "?") -> str:
    return default if value is None else str(value)


def print_pass(result: Dict[str, Any]) -> None:
    print("=" * 80)
    name = Path(result["pass_dir"]).name
    print(f"Pass:               {name}")
    print(f"Satellite:          {_fmt(result.get('satellite'))}")
    print(f"Direction:          {_fmt(result.get('direction'))}")
    if result.get("max_elevation") is not None:
        print(f"Max elevation:      {result['max_elevation']}°")
    else:
        print(f"Max elevation:      ?")
    print(f"Gain:               {_fmt(result.get('gain'))}")
    pipe = _fmt(result.get("pipeline"))
    fmt = _fmt(result.get("format"))
    sub = _fmt(result.get("subdir"))
    print(f"Pipeline:           {pipe}  (format={fmt}, dir={sub})")
    if result.get("wanted_channels") is not None:
        wanted = ",".join(str(c) for c in result["wanted_channels"])
        print(f"Wanted channels:    {wanted}")
    print()
    print(f"Quality score:      {result['quality_score']} / 100")
    print(f"Quality class:      {result['quality_class']}")
    print(f"Copy recommended:   {str(result['copy_recommended']).lower()}")
    print(f"Email recommended:  {str(result['email_recommended']).lower()}")

    if result["quality_class"] == "missing":
        print()
        print("Reason:")
        for f in result["findings"]:
            print(f"  - {f}")
        print("=" * 80)
        return

    summary = result.get("summary") or {}
    if summary:
        print()
        print("Summary:")
        for k, v in summary.items():
            print(f"  {k}: {v}")

    channels = result.get("channels") or []
    if channels:
        print()
        print("Channels:")
        for ch in channels:
            n = ch.get("channel", "?")
            cls = ch.get("channel_class", "?")
            size = ch.get("size", (0, 0))
            print(
                f"  ch{n} {Path(ch['file']).name}: "
                f"mean={ch['mean']} stddev={ch['stddev']} range={ch['range']} "
                f"min={ch['min']} max={ch['max']} size={size[0]}x{size[1]}  → {cls}"
            )

    findings = result.get("findings") or []
    if findings:
        print()
        print("Findings:")
        for f in findings:
            print(f"  - {f}")

    if result.get("missing_channels"):
        print()
        print(f"Missing channels:   {result['missing_channels']} "
              f"(tolerated up to {result['tolerate_missing_channel']})")

    print("=" * 80)


# --- Pass directory enumeration ---------------------------------------------

def list_pass_dirs(output_dir: Path) -> List[Path]:
    """Return all pass directories in output_dir, sorted by name (which has a
    timestamp prefix → chronological order)."""
    if not output_dir.is_dir():
        return []
    return sorted(
        (p for p in output_dir.iterdir() if p.is_dir()),
        key=lambda p: p.name,
    )


# --- CLI ---------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="analyze_reception.py",
        description=(
            "Score the image quality of one or more decoded satellite passes. "
            "Reads decoded channel images from a pass directory, computes "
            "per-channel statistics, and outputs a quality score plus a class "
            "(good/fair/bad). Thresholds and pipeline-specific image discovery "
            "rules come from [processing_thresholds] in config.ini."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Pipeline format detection:\n"
            "  Reads 'pipeline' from reception.json in the pass directory.\n"
            "  A name ending in '_lrpt' uses the lrpt_* keys, ending in\n"
            "  '_hrpt' uses the hrpt_* keys. Other suffixes -> exit 3.\n"
            "\n"
            "Action mapping (driven only by the resulting quality class):\n"
            "  good  ->  copy_recommended = true,  email_recommended = true\n"
            "  fair  ->  copy_recommended = true,  email_recommended = false\n"
            "  bad   ->  copy_recommended = false, email_recommended = false\n"
            "\n"
            "Exit codes (single pass):\n"
            "  0   good quality\n"
            "  1   fair quality\n"
            "  2   bad quality\n"
            "  3   missing decoded files / configuration error\n"
            "Exit code in --last N mode (N >= 2): always 0.\n"
            "\n"
            "Examples:\n"
            "  analyze_reception.py\n"
            "      Score the most recent pass in [paths] output_dir.\n"
            "\n"
            "  analyze_reception.py --last 10\n"
            "      Score the 10 most recent passes (oldest first, newest last).\n"
            "\n"
            "  analyze_reception.py results/passes/2026-04-28_05-42-25_METEOR-M2_4\n"
            "      Score one specific pass.\n"
        ),
    )
    parser.add_argument(
        "pass_dir", nargs="?", default=None, metavar="PASS_DIR",
        help=(
            "Path to one pass directory. "
            "Default: the most recent directory in [paths] output_dir."
        ),
    )
    parser.add_argument(
        "--last", type=int, default=None, metavar="N",
        help="Process the N most recent pass directories (oldest first).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.pass_dir and args.last is not None:
        print("Error: PASS_DIR and --last are mutually exclusive.", file=sys.stderr)
        return EXIT_MISSING
    if args.last is not None and args.last < 1:
        print("Error: --last N must be >= 1.", file=sys.stderr)
        return EXIT_MISSING

    base_dir = Path(__file__).resolve().parent.parent
    config_path = base_dir / "config" / "config.ini"
    try:
        cfg = read_config(str(config_path))
    except ConfigError as e:
        print(f"Config Error:\n{e}", file=sys.stderr)
        return EXIT_MISSING

    output_dir = Path(cfg["paths"]["output_dir"]).expanduser().resolve()

    # Build the list of pass directories to analyse.
    if args.pass_dir:
        pass_dirs = [Path(args.pass_dir).expanduser().resolve()]
    else:
        all_dirs = list_pass_dirs(output_dir)
        if not all_dirs:
            print(f"No pass directories found in {output_dir}", file=sys.stderr)
            return EXIT_MISSING
        n = args.last if args.last is not None else 1
        pass_dirs = all_dirs[-n:]  # already sorted oldest -> newest

    # Analyse each in chronological order (oldest first, newest last).
    results = [analyze_pass(p, cfg) for p in pass_dirs]
    for r in results:
        print_pass(r)

    # Exit-code policy.
    if len(results) >= 2:
        return 0
    return results[0]["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
