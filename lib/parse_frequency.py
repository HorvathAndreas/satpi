#!/usr/bin/env python3
"""satpi – parse_frequency

Utility to parse frequency/bandwidth/size values from various formats to Hz.

Converts strings like "137.9 MHz", "1000 kHz", "0.1379 GHz" to numeric Hz values.
Can be used as a module (import parse_frequency) or as a standalone CLI tool.

Examples:
    As module:
        from lib.parse_frequency import parse_frequency
        hz = parse_frequency("137.9 MHz")  # Returns 137900000

    As CLI:
        python3 lib/parse_frequency.py "137.9 MHz"
        python3 lib/parse_frequency.py --test

Author: Andreas Horvath
Project: satpi – Autonomous satellite reception pipeline
"""

import sys
import argparse


def parse_frequency(value: str) -> int:
    """Parse frequency from various formats to Hz.

    Accepts any of: "137900000", "137.9 MHz", "4,1 MHz", "1000 kHz", "0.1379 GHz"
    Returns: frequency in Hz as integer

    Args:
        value: Frequency string with optional unit suffix (supports . or , as decimal separator)

    Returns:
        Frequency in Hz

    Raises:
        ValueError: If the format is invalid or cannot be parsed
    """
    value = value.strip()
    # Support both . and , as decimal separator
    value = value.replace(",", ".")

    for suffix, multiplier in [("GHz", 1_000_000_000), ("MHz", 1_000_000), ("kHz", 1_000), ("Hz", 1)]:
        if value.upper().endswith(suffix.upper()):
            try:
                num_str = value[:-len(suffix)].strip()
                return int(round(float(num_str) * multiplier))
            except ValueError:
                pass

    try:
        return int(round(float(value)))
    except ValueError:
        raise ValueError(f"Invalid frequency format: {value}")

def main():
    parser = argparse.ArgumentParser(
        description="Parse frequency values from various formats to Hz",
        epilog="Examples: parse_frequency.py '137.9 MHz' | parse_frequency.py --test"
    )
    parser.add_argument(
        "value",
        nargs="?",
        help="Frequency value (e.g., '137.9 MHz', '1000 kHz', '137900000')"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run built-in tests"
    )

    args = parser.parse_args()

    if args.test:
        test_cases = [
            ("137900000", 137900000),
            ("137.9 MHz", 137900000),
            ("1000 kHz", 1000000),
            ("0.1379 GHz", 137900000),
            ("10 kHz", 10000),
            ("1 MHz", 1000000),
        ]
        print("Running parse_frequency tests:")
        all_pass = True
        for test_input, expected in test_cases:
            try:
                result = parse_frequency(test_input)
                status = "✓" if result == expected else "✗"
                if result != expected:
                    all_pass = False
                print(f"  {status} {test_input:20} → {result:>12} Hz (expected {expected})")
            except Exception as e:
                print(f"  ✗ {test_input:20} → ERROR: {e}")
                all_pass = False
        return 0 if all_pass else 1

    if not args.value:
        parser.print_help()
        return 1

    try:
        result = parse_frequency(args.value)
        print(result)
        return 0
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
