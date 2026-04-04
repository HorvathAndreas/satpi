#!/usr/bin/env python3
# satpi
# Creates and enables the refresh service and timer for periodic pass planning.
# This script manages the higher-level automation layer that regularly updates
# TLE data, predicts future passes and regenerates all per-pass systemd units.
# Its job is not to receive a pass directly, but to keep the full planning chain
# running automatically over time without manual intervention.
# Author: Andreas Horvath
# Project: Autonomous, Config-driven satellite reception pipeline for Raspberry Pi

import logging
import os
import subprocess

from load_config import load_config, ConfigError


logger = logging.getLogger("satpi.generate_refresh_units")


def setup_logging(log_dir):
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


def run(cmd):
    logger.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.stdout.strip():
        logger.info("stdout: %s", result.stdout.strip())
    if result.stderr.strip():
        logger.info("stderr: %s", result.stderr.strip())

    if result.returncode != 0:
        raise RuntimeError(f"Command failed ({result.returncode}): {' '.join(cmd)}")

    return result


def write_file(path, content):
    tmp_path = path + ".tmp"
    with open(tmp_path, "w") as f:
        f.write(content)
    os.replace(tmp_path, path)


def build_on_calendar(frequency, update_time, weekday):
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
    user_line = f"User={service_user}\n" if service_user else ""

    return f"""[Unit]
Description=SATPI refresh TLE, predict passes, and schedule timers
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
{user_line}WorkingDirectory={base_dir}
ExecStart={python_bin} {base_dir}/bin/update_tle.py
ExecStart={python_bin} {base_dir}/bin/predict_passes.py
ExecStart={python_bin} {base_dir}/bin/schedule_passes.py
"""


def make_timer_content(on_calendar):
    return f"""[Unit]
Description=Run SATPI refresh on schedule

[Timer]
OnCalendar={on_calendar}
Persistent=true
Unit=satpi-refresh.service

[Install]
WantedBy=timers.target
"""


def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(base_dir, "config", "config.ini")

    try:
        config = load_config(config_path)
    except ConfigError as e:
        print(f"[generate_refresh_units] CONFIG ERROR: {e}")
        return

    setup_logging(config["paths"]["log_dir"])

    systemd_dir = os.path.join(base_dir, "systemd")
    os.makedirs(systemd_dir, exist_ok=True)

    service_path = os.path.join(systemd_dir, "satpi-refresh.service")
    timer_path = os.path.join(systemd_dir, "satpi-refresh.timer")

    service_user = config["systemd"]["service_user"]
    python_bin = config["systemd"]["python_bin"]

    frequency = config["scheduling"]["frequency"].strip().upper()
    update_time = config["scheduling"]["time"].strip()
    weekday = config["scheduling"]["weekday"].strip().upper()

    on_calendar = build_on_calendar(frequency, update_time, weekday)

    logger.info("Generating refresh units")
    logger.info("base_dir=%s", base_dir)
    logger.info("service_user=%s", service_user)
    logger.info("python_bin=%s", python_bin)
    logger.info("on_calendar=%s", on_calendar)

    write_file(service_path, make_service_content(base_dir, service_user, python_bin))
    write_file(timer_path, make_timer_content(on_calendar))

    logger.info("Wrote service unit: %s", service_path)
    logger.info("Wrote timer unit: %s", timer_path)

    run(["sudo", "systemctl", "link", service_path])
    run(["sudo", "systemctl", "link", timer_path])
    run(["sudo", "systemctl", "daemon-reload"])
    run(["sudo", "systemctl", "enable", "--now", "satpi-refresh.timer"])

    logger.info("Refresh units linked and timer enabled")


if __name__ == "__main__":
    main()
