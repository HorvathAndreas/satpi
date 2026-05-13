#!/usr/bin/env python3
"""satpi – generate_refresh_units

Creates and enables the refresh service and timer for periodic pass planning.
This script manages the higher-level automation layer that regularly updates
TLE data, predicts future passes and regenerates all per-pass systemd units.
Its job is not to receive a pass directly, but to keep the full planning chain
running automatically over time without manual intervention.

Also sets up passwordless sudo rules for systemctl operations.

Author: Andreas Horvath
Project: Autonomous, Config-driven satellite reception pipeline for Raspberry Pi
"""

import logging
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "lib"))
from read_config import read_config, ConfigError

logger = logging.getLogger("satpi.generate_refresh_units")


def setup_logging(log_dir):
    """Setup logging to file and console."""
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "generate_refresh_units.log")

    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)


def run(cmd, *, input_text=None):
    """Run command with logging."""
    logger.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, input=input_text)

    if result.stdout.strip():
        logger.debug("stdout: %s", result.stdout.strip())
    if result.stderr.strip():
        (logger.debug if result.returncode == 0 else logger.warning)(
            "stderr: %s", result.stderr.strip()
        )

    if result.returncode != 0:
        raise RuntimeError(f"Command failed ({result.returncode}): {' '.join(cmd)}")

    return result


def write_file(path, content):
    """Write file atomically."""
    tmp_path = path + ".tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(content)
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass


def setup_sudoers(service_user):
    """Create sudoers rule for passwordless systemctl access.

    Creates /etc/sudoers.d/satpi-systemctl with a rule allowing the service
    user to run systemctl without a password.
    """
    sudoers_path = "/etc/sudoers.d/satpi-systemctl"
    sudoers_content = (
        "# satpi: Allow passwordless systemctl for satellite pass scheduling\n"
        f"{service_user} ALL=(root) NOPASSWD: /bin/systemctl\n"
    )

    logger.info("Setting up sudoers rule for user: %s", service_user)

    # First, validate syntax
    result = subprocess.run(
        ["sudo", "visudo", "-c", "-f", "-"],
        input=sudoers_content,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"sudoers syntax error: {result.stderr}")

    logger.debug("sudoers syntax valid")

    # Write the file using sudo tee (safe way to write to /etc)
    run(
        ["sudo", "tee", sudoers_path],
        input_text=sudoers_content,
    )

    # Set correct permissions
    run(["sudo", "chmod", "0440", sudoers_path])

    logger.info("Sudoers rule created: %s", sudoers_path)


def build_on_calendar(frequency, update_time, weekday):
    """Build systemd OnCalendar value from config parameters."""
    hh, mm = update_time.split(":", 1)

    if frequency == "DAILY":
        return f"*-*-* {hh}:{mm}:00"

    if frequency == "WEEKLY":
        weekday_map = {
            "MONDAY": "Mon",
            "TUESDAY": "Tue",
            "WEDNESDAY": "Wed",
            "THURSDAY": "Thu",
            "FRIDAY": "Fri",
            "SATURDAY": "Sat",
            "SUNDAY": "Sun",
        }
        if weekday not in weekday_map:
            raise ValueError(f"Invalid weekly weekday: {weekday}")
        return f"{weekday_map[weekday]} *-*-* {hh}:{mm}:00"

    raise ValueError(f"Unsupported pass_update_frequency: {frequency}")


def make_service_content(base_dir, service_user, python_bin):
    """Generate systemd service file content."""
    user_line = f"User={service_user}\n" if service_user else ""

    return (
        "[Unit]\n"
        "Description=SATPI refresh TLE, predict passes, and schedule timers\n"
        "After=network-online.target\n"
        "Wants=network-online.target\n"
        "\n"
        "[Service]\n"
        "Type=oneshot\n"
        f"{user_line}"
        f"WorkingDirectory={base_dir}\n"
        f"ExecStart={python_bin} {base_dir}/bin/update_tle.py\n"
        f"ExecStart={python_bin} {base_dir}/bin/predict_passes.py\n"
        f"ExecStart={python_bin} {base_dir}/bin/schedule_passes.py\n"
    )


def make_timer_content(on_calendar):
    """Generate systemd timer file content."""
    return (
        "[Unit]\n"
        "Description=Run SATPI refresh on schedule\n"
        "\n"
        "[Timer]\n"
        f"OnCalendar={on_calendar}\n"
        "Persistent=true\n"
        "Unit=satpi-refresh.service\n"
        "\n"
        "[Install]\n"
        "WantedBy=timers.target\n"
    )


def parse_args():
    """Parse command-line arguments."""
    import argparse
    parser = argparse.ArgumentParser(
        description="Generate and enable systemd refresh service and timer for SATPI.",
        epilog="""
WHAT THIS SCRIPT DOES:
  1. Reads scheduling configuration from config/config.ini
  2. Generates systemd service unit (satpi-refresh.service)
  3. Generates systemd timer unit (satpi-refresh.timer)
  4. Creates passwordless sudo rule for systemctl operations
  5. Links units into /etc/systemd/system/
  6. Enables and starts the timer

REFRESH SERVICE WORKFLOW:
  The timer triggers automatically on schedule (daily or weekly) and runs:
  • update_tle.py      - Download latest TLE (Two-Line Element) data
  • predict_passes.py  - Compute upcoming satellite passes
  • schedule_passes.py - Generate systemd units for each pass

CONFIGURATION:
  All settings are read from config/config.ini sections:
  • [systemd]    - service_user: User account for the service
  • [scheduling] - frequency, time, weekday for refresh schedule
  • [paths]      - Log directory and Python interpreter path

SUDOERS SETUP:
  This script automatically creates /etc/sudoers.d/satpi-systemctl
  allowing passwordless 'sudo systemctl' access for the service user.

EXAMPLE:
  python3 bin/generate_refresh_units.py
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.parse_args()
    # Script reads all configuration from config.ini via read_config.py


def main() -> int:
    """Main entry point."""
    parse_args()

    base_dir = str(Path(__file__).resolve().parent.parent)
    config_path = os.path.join(base_dir, "config", "config.ini")

    try:
        config = read_config(config_path)
    except ConfigError as e:
        print(f"[generate_refresh_units] CONFIG ERROR: {e}", file=sys.stderr)
        return 1

    setup_logging(config["paths"]["log_dir"])

    try:
        systemd_dir = os.path.join(base_dir, "systemd")
        os.makedirs(systemd_dir, exist_ok=True)

        service_path = os.path.join(systemd_dir, "satpi-refresh.service")
        timer_path = os.path.join(systemd_dir, "satpi-refresh.timer")

        service_user = config["systemd"]["service_user"]
        python_bin = config["paths"]["python_bin"]

        frequency = config["scheduling"]["frequency"].strip().upper()
        update_time = config["scheduling"]["time"].strip()
        weekday = config["scheduling"]["weekday"].strip().upper()

        on_calendar = build_on_calendar(frequency, update_time, weekday)

        logger.info("Generating refresh units")
        logger.info("base_dir=%s", base_dir)
        logger.info("service_user=%s", service_user)
        logger.info("python_bin=%s", python_bin)
        logger.info("on_calendar=%s", on_calendar)

        # Generate service and timer files
        write_file(service_path, make_service_content(base_dir, service_user, python_bin))
        write_file(timer_path, make_timer_content(on_calendar))

        logger.info("Wrote service unit: %s", service_path)
        logger.info("Wrote timer unit: %s", timer_path)

        # Setup sudoers rule
        setup_sudoers(service_user)

        # Link and enable units
        run(["sudo", "systemctl", "link", "--force", service_path])
        run(["sudo", "systemctl", "link", "--force", timer_path])
        run(["sudo", "systemctl", "daemon-reload"])
        run(["sudo", "systemctl", "enable", "--now", "satpi-refresh.timer"])

        logger.info("Refresh units linked and timer enabled")
        return 0

    except RuntimeError as e:
        logger.error("Setup failed: %s", e)
        return 1
    except Exception as e:
        logger.error("Unexpected error: %s", e, exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
