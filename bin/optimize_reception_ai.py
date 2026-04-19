#!/usr/bin/env python3
# satpi
# Sends raw reception data to OpenAI and requests optimization advice.
# Author: Andreas Horvath
# Project: Autonomous, Config-driven satellite reception pipeline for Raspberry Pi

import json
import argparse
import sys
from pathlib import Path

from openai import OpenAI, RateLimitError, APIError

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from load_config import load_config, ConfigError

DEFAULT_MODEL = "gpt-5"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Analyze SATPI reception JSON via OpenAI API"
    )
    parser.add_argument(
        "--model",
        default=None,
        help="Optional override for OpenAI model",
    )
    parser.add_argument(
        "--max-samples",
        type=int,
        default=200,
        help="Limit number of samples included in the prompt",
    )
    parser.add_argument(
        "--reception-json",
        default=None,
        help="Path to a specific reception.json file to analyze",
    )
    parser.add_argument(
        "--pass-id",
        default=None,
        help="Analyze the reception.json for a specific pass_id directory name",
    )
    return parser.parse_args()

def load_reception_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def find_latest_reception_json(config: dict) -> Path:
    passes_dir = Path(config["paths"]["output_dir"])

    if not passes_dir.exists():
        raise FileNotFoundError(f"Passes directory not found: {passes_dir}")

    json_files = sorted(
        passes_dir.glob("*/reception.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    if not json_files:
        raise FileNotFoundError(f"No reception JSON files found in: {passes_dir}")

    return json_files[0]

def find_reception_json_by_pass_id(config: dict, pass_id: str) -> Path:
    passes_dir = Path(config["paths"]["output_dir"])
    target = passes_dir / pass_id / "reception.json"

    if not target.exists():
        raise FileNotFoundError(f"reception.json not found for pass_id '{pass_id}': {target}")

    return target

def reduce_payload(data: dict, max_samples: int) -> dict:
    reduced = dict(data)
    samples = data.get("samples", [])

    if len(samples) > max_samples:
        reduced["samples"] = samples[:max_samples]
        reduced["_truncated"] = True
        reduced["_original_sample_count"] = len(samples)
        reduced["_included_sample_count"] = len(reduced["samples"])
    else:
        reduced["_truncated"] = False
        reduced["_original_sample_count"] = len(samples)
        reduced["_included_sample_count"] = len(samples)

    return reduced


def build_prompt(payload: dict, include_optimizer_report: bool) -> str:
    extra_note = ""
    if include_optimizer_report:
        extra_note = (
            "If the data is incomplete or ambiguous, say so explicitly and propose "
            "what additional telemetry or logs should be included next time.\n"
        )

    return f"""
You are analyzing raw satellite reception telemetry from an autonomous SDR reception pipeline.

Task:
1. Analyze the raw reception data.
2. Identify likely technical causes of poor reception quality.
3. Suggest concrete optimization actions.
4. Focus on antenna visibility, gain setting, frequency stability, horizon blockage, sync behavior, pass geometry, and data completeness.
5. Distinguish clearly between:
   - confirmed observations from the data
   - plausible hypotheses
   - recommended next tests
6. Be specific, technical, and concise.
7. Do not invent measurements that are not present in the data.
8. If there are signs that the telemetry logging itself is incomplete or misleading, point that out clearly.
{extra_note}
Please structure the answer with these sections:
- Summary
- Observations from the data
- Most likely causes
- Recommended actions
- What to measure next

Raw reception JSON:
{json.dumps(payload, indent=2)}
""".strip()


def write_output_file(output_path: Path, content: str) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)
        if not content.endswith("\n"):
            f.write("\n")


def main():
    args = parse_args()

    base_dir = Path(__file__).resolve().parent.parent
    config_path = base_dir / "config" / "config.ini"

    try:
        config = load_config(str(config_path))
    except ConfigError as e:
        print(f"[ERROR] CONFIG ERROR: {e}")
        return 1

    ai_cfg = config["optimize_reception_ai"]

    if not ai_cfg["enabled"]:
        print("[INFO] optimize_reception_ai is disabled in config.ini")
        return 0

    api_key = ai_cfg["api_key"]
    if not api_key:
        print("[ERROR] optimize_reception_ai.api_key is empty in config.ini")
        return 1

    model = args.model or ai_cfg["model"] or DEFAULT_MODEL
    output_file = Path(config["paths"]["optimization_ai_report_file"])
    include_optimizer_report = ai_cfg["include_optimizer_report"]

    try:
        if args.reception_json:
            reception_path = Path(args.reception_json).expanduser().resolve()
            if not reception_path.exists():
                print(f"[ERROR] reception JSON not found: {reception_path}")
                return 1
        elif args.pass_id:
            reception_path = find_reception_json_by_pass_id(config, args.pass_id)
        else:
            reception_path = find_latest_reception_json(config)
    except FileNotFoundError as e:
        print(f"[ERROR] {e}")
        return 1

    print(f"[INFO] Using reception JSON: {reception_path}")
    print(f"[INFO] Model: {model}")

    data = load_reception_json(reception_path)
    payload = reduce_payload(data, args.max_samples)
    prompt = build_prompt(payload, include_optimizer_report)

    print(f"[INFO] Prompt length: {len(prompt)} characters")
    print(f"[INFO] Included samples: {payload['_included_sample_count']} of {payload['_original_sample_count']}")

    client = OpenAI(api_key=api_key)

    client = OpenAI(
        api_key=api_key,
        timeout=60.0,
    )

    try:
        print("[INFO] Sending request to OpenAI API...")
        response = client.responses.create(
            model=model,
            input=prompt,
        )
        print("[INFO] OpenAI API response received.")

    except RateLimitError as e:
        print(f"[ERROR] OpenAI API quota/rate-limit problem: {e}")
        return 1
    except APIError as e:
        print(f"[ERROR] OpenAI API error: {e}")
        return 1
    except Exception as e:
        print(f"[ERROR] Unexpected error during OpenAI request: {e}")
        return 1

    result_text = response.output_text

    print(result_text)

    try:
        write_output_file(output_file, result_text)
        print(f"[INFO] Report written to: {output_file}")
    except Exception as e:
        print(f"[ERROR] Could not write output file: {e}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
