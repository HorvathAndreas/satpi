#!/usr/bin/env python3

import argparse
import json
import sys
from pathlib import Path
from PIL import Image, ImageStat

sys.path.insert(0, str(Path(__file__).parent.parent / "lib"))
from read_config import read_config, ConfigError

def analyze_image(path: Path):
    img = Image.open(path).convert("L")
    stat = ImageStat.Stat(img)
    return {
        "file": str(path),
        "size": img.size,
        "min": img.getextrema()[0],
        "max": img.getextrema()[1],
        "mean": round(stat.mean[0], 3),
        "stddev": round(stat.stddev[0], 3),
        "range": img.getextrema()[1] - img.getextrema()[0],
    }


def classify_channel(ch):
    if ch["stddev"] < 5 or ch["range"] < 30:
        return "bad"
    if ch["stddev"] < 20 or ch["range"] < 80:
        return "medium"
    return "good"


def score_channels(channels):
    score = 0
    findings = []

    stddevs = [c["stddev"] for c in channels]
    ranges = [c["range"] for c in channels]
    means = [c["mean"] for c in channels]

    avg_stddev = sum(stddevs) / len(stddevs)
    avg_range = sum(ranges) / len(ranges)
    mean_spread = max(means) - min(means)

    if avg_stddev >= 35:
        score += 45
        findings.append("Channels show strong internal variation.")
    elif avg_stddev >= 15:
        score += 25
        findings.append("Channels show moderate internal variation.")
    else:
        score += 5
        findings.append("Channels are nearly flat.")

    if avg_range >= 120:
        score += 30
        findings.append("Dynamic range is wide.")
    elif avg_range >= 60:
        score += 18
        findings.append("Dynamic range is moderate.")
    else:
        score += 5
        findings.append("Dynamic range is very limited.")

    if mean_spread >= 15:
        score += 15
        findings.append("Channels differ meaningfully from each other.")
    elif mean_spread >= 5:
        score += 8
        findings.append("Channels differ slightly from each other.")
    else:
        score += 2
        findings.append("Channels are very similar to each other.")

    good_count = sum(1 for c in channels if classify_channel(c) == "good")
    medium_count = sum(1 for c in channels if classify_channel(c) == "medium")
    bad_count = sum(1 for c in channels if classify_channel(c) == "bad")

    if good_count >= 2:
        score += 10
        findings.append("At least two channels look healthy.")
    elif bad_count >= 2:
        score -= 10
        findings.append("At least two channels look poor.")

    score = max(0.0, min(100.0, round(score, 1)))

    if score >= 70:
        quality_class = "good"
    elif score >= 40:
        quality_class = "medium"
    else:
        quality_class = "bad"

    return score, quality_class, findings, {
        "avg_stddev": round(avg_stddev, 3),
        "avg_range": round(avg_range, 3),
        "mean_spread": round(mean_spread, 3),
        "good_channels": good_count,
        "medium_channels": medium_count,
        "bad_channels": bad_count,
    }


def load_gain(output_dir: Path):
    reception = output_dir / "reception.json"
    if not reception.exists():
        return None
    try:
        with open(reception, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("gain")
    except Exception:
        return None


def main():

    base_dir = Path(__file__).resolve().parent.parent
    config_path = base_dir / "config" / "config.ini"
    parser = argparse.ArgumentParser()
    parser.add_argument("output_dir", nargs="?", default=None,
                       help="Output directory (default: from config)")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    if args.output_dir is None:
        config = read_config(config_path)
        output_dir = Path(config["paths"]["output_dir"]).expanduser().resolve()
    else:
        output_dir = Path(args.output_dir).expanduser().resolve()

    msu_dir = output_dir / "MSU-MR"

    files = [
        msu_dir / "MSU-MR-1.png",
        msu_dir / "MSU-MR-2.png",
        msu_dir / "MSU-MR-3.png",
    ]

    missing = [str(f) for f in files if not f.exists()]
    if missing:
        result = {
            "output_dir": str(output_dir),
            "quality_score": 0.0,
            "quality_class": "bad",
            "copy_recommended": False,
            "email_recommended": False,
            "findings": ["Missing decoded channel images."],
            "missing_files": missing,
        }
        if args.json:
            print(json.dumps(result, indent=2))
        elif args.quiet:
            print("0.0 bad false false")
        else:
            print("Missing decoded channel images:")
            for m in missing:
                print(" ", m)
        return

    channels = [analyze_image(f) for f in files]
    score, quality_class, findings, summary = score_channels(channels)

    copy_recommended = score >= 40
    email_recommended = score >= 70

    result = {
        "output_dir": str(output_dir),
        "gain": load_gain(output_dir),
        "quality_score": score,
        "quality_class": quality_class,
        "copy_recommended": copy_recommended,
        "email_recommended": email_recommended,
        "summary": summary,
        "channels": channels,
        "findings": findings,
    }

    if args.json:
        print(json.dumps(result, indent=2))
        return

    if args.quiet:
        print(f"{score} {quality_class} {str(copy_recommended).lower()} {str(email_recommended).lower()}")
        return

    print(f"Pass:            {output_dir}")
    print(f"Gain:            {result['gain']}")
    print(f"Quality score:   {score}/100")
    print(f"Quality class:   {quality_class}")
    print(f"Copy recommended:  {copy_recommended}")
    print(f"Email recommended: {email_recommended}")
    print()
    print("Summary:")
    for k, v in summary.items():
        print(f"  {k}: {v}")
    print()
    print("Channels:")
    for ch in channels:
        print(
            f"  {Path(ch['file']).name}: "
            f"mean={ch['mean']} stddev={ch['stddev']} range={ch['range']} "
            f"min={ch['min']} max={ch['max']} size={ch['size'][0]}x{ch['size'][1]}"
        )
    print()
    print("Findings:")
    for f in findings:
        print(f"  - {f}")


if __name__ == "__main__":
    main()
