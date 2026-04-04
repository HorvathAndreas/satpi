#!/usr/bin/env python3
# satpi
# Generates systemd service and timer units for all relevant future passes.
# This script reads the predicted pass data, removes outdated generated units
# and creates one service and one timer for every pass that should still be
# received. Its role is to translate the abstract pass plan into concrete
# operating system jobs that systemd can execute automatically.
# Author: Andreas Horvath
# Project: Autonomous, Config-driven satellite reception pipeline for Raspberry Pi

import glob
import json
import logging
import os
import re
import subprocess
from datetime import datetime, timezone, timedelta

from load_config import load_config, ConfigError


logger = logging.getLogger("satpi.schedule")


def setup_logging(log_dir):
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "schedule_passes.log")

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


def parse_utc(ts):
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def isoformat_utc(dt):
    return (
        dt.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def systemd_time(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def sanitize_name(value):
    value = value.upper().replace(" ", "-").replace("_", "-")
    value = re.sub(r"[^A-Z0-9\-]", "-", value)
    value = re.sub(r"-+", "-", value).strip("-")
    return value


def load_passes(pass_file):
    if not os.path.exists(pass_file):
        raise FileNotFoundError(f"Pass file not found: {pass_file}")

    with open(pass_file, "r") as f:
        data = json.load(f)

    return data.get("passes", [])


def build_scheduled_passes(passes, pre_start_seconds, post_stop_seconds):
    scheduled = []

    for p in passes:
        start = parse_utc(p["start"])
        end = parse_utc(p["end"])

        entry = dict(p)
        entry["scheduled_start_dt"] = start - timedelta(seconds=pre_start_seconds)
        entry["scheduled_end_dt"] = end + timedelta(seconds=post_stop_seconds)
        scheduled.append(entry)

    return sorted(scheduled, key=lambda x: x["scheduled_start_dt"])


def future_passes_only(scheduled_passes, now):
    return [p for p in scheduled_passes if p["scheduled_end_dt"] > now]


def make_unit_base_name(pass_entry):
    sat = sanitize_name(pass_entry["satellite"])
    start = pass_entry["scheduled_start_dt"].strftime("%Y%m%dT%H%M%SZ")
    return f"satpi-pass-{start}-{sat}"

def make_service_content(pass_entry, receiver_script, python_bin, base_dir, service_user):
    user_line = f"User={service_user}\n" if service_user else ""

    scheduled_start = isoformat_utc(pass_entry["scheduled_start_dt"])
    scheduled_end = isoformat_utc(pass_entry["scheduled_end_dt"])

    return f"""[Unit]
Description=SATPI pass receiver for {pass_entry['satellite']} at {scheduled_start}
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
{user_line}WorkingDirectory={base_dir}
ExecStart={python_bin} {receiver_script} '{pass_entry["satellite"]}' '{pass_entry["frequency_hz"]}' '{pass_entry["bandwidth_hz"]}' '{pass_entry["pipeline"]}' '{pass_entry["start"]}' '{pass_entry["end"]}' '{scheduled_start}' '{scheduled_end}'
"""

def make_timer_content(service_name, pass_entry):
    return f"""[Unit]
Description=SATPI timer for {pass_entry['satellite']} at {isoformat_utc(pass_entry['scheduled_start_dt'])}

[Timer]
OnCalendar={systemd_time(pass_entry["scheduled_start_dt"])}
Persistent=true
Unit={service_name}

[Install]
WantedBy=timers.target
"""

def write_file(path, content):
    tmp_path = path + ".tmp"
    with open(tmp_path, "w") as f:
        f.write(content)
    os.replace(tmp_path, path)

def cleanup_existing_units(generated_units_dir):
    existing_services = sorted(glob.glob(os.path.join(generated_units_dir, "satpi-pass-*.service")))
    existing_timers = sorted(glob.glob(os.path.join(generated_units_dir, "satpi-pass-*.timer")))

    unit_names = [os.path.basename(p) for p in existing_services + existing_timers]

    if unit_names:
        logger.info("Stopping and disabling %d existing generated units", len(unit_names))
        for unit in unit_names:
            subprocess.run(["sudo", "systemctl", "disable", "--now", unit], capture_output=True, text=True)
            subprocess.run(["sudo", "systemctl", "reset-failed", unit], capture_output=True, text=True)

    for path in existing_services + existing_timers:
        logger.info("Removing old generated unit file: %s", path)
        os.remove(path)

def create_units(generated_units_dir, receiver_script, future_passes, python_bin, base_dir, service_user):
    created = []

    for p in future_passes:
        base_name = make_unit_base_name(p)
        service_name = f"{base_name}.service"
        timer_name = f"{base_name}.timer"

        service_path = os.path.join(generated_units_dir, service_name)
        timer_path = os.path.join(generated_units_dir, timer_name)

        write_file(
            service_path,
            make_service_content(p, receiver_script, python_bin, base_dir, service_user),
        )
        write_file(timer_path, make_timer_content(service_name, p))

        created.append((service_name, timer_name, service_path, timer_path))

    return created

def link_and_enable_units(created_units):
    for service_name, timer_name, service_path, timer_path in created_units:
        run(["sudo", "systemctl", "link", service_path])
        run(["sudo", "systemctl", "link", timer_path])

    run(["sudo", "systemctl", "daemon-reload"])

    for service_name, timer_name, service_path, timer_path in created_units:
        run(["sudo", "systemctl", "enable", "--now", timer_name])


def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(base_dir, "config", "config.ini")

    try:
        config = load_config(config_path)
    except ConfigError as e:
        print(f"[schedule] CONFIG ERROR: {e}")
        return

    setup_logging(config["paths"]["log_dir"])

    pass_file = config["paths"]["pass_file"]
    generated_units_dir = config["paths"]["generated_units_dir"]
    receiver_script = os.path.join(base_dir, "bin", "receive_pass.py")
    python_bin = config["systemd"]["python_bin"]
    service_user = config["systemd"]["service_user"]

    pre_start_seconds = config["scheduling"]["pre_start"]
    post_stop_seconds = config["scheduling"]["post_stop"]

    if not os.path.exists(receiver_script):
        raise FileNotFoundError(f"Receiver script not found: {receiver_script}")

    os.makedirs(generated_units_dir, exist_ok=True)

    passes = load_passes(pass_file)
    logger.info("Loaded %d passes from %s", len(passes), pass_file)

    scheduled_passes = build_scheduled_passes(passes, pre_start_seconds, post_stop_seconds)

    now = datetime.now(timezone.utc)
    future_passes = future_passes_only(scheduled_passes, now)
    logger.info("Keeping %d future passes", len(future_passes))

    cleanup_existing_units(generated_units_dir)

    if not future_passes:
        logger.info("No future passes to schedule")
        run(["sudo", "systemctl", "daemon-reload"])
        return

    created_units = create_units(
        generated_units_dir,
        receiver_script,
        future_passes,
        python_bin,
        base_dir,
        service_user,
    )

    logger.info("Created %d timer/service pairs", len(created_units))

    link_and_enable_units(created_units)
    logger.info("Scheduling complete")


if __name__ == "__main__":
    main()
