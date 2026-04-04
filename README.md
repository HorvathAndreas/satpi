# satpi

Autonomous, config-driven satellite reception pipeline for Raspberry Pi.

satpi is a headless workflow for automated weather satellite reception on Raspberry Pi systems. It downloads and filters TLE data, predicts passes, generates per-pass systemd timers, runs SatDump for live reception, decodes CADU data, uploads successful results, and sends notifications.

## Features

- autonomous end-to-end workflow
- config-driven setup
- headless operation
- per-satellite configuration
- Skyfield-based pass prediction
- systemd-based scheduling
- SatDump live reception
- automatic CADU decode
- optional upload via rclone
- optional notification via msmtp

## Workflow

satpi is split into small, focused components:

1. `update_tle.py`  
   Downloads and filters TLE data for the configured satellites.

2. `predict_passes.py`  
   Predicts upcoming passes and writes `passes.json`.

3. `schedule_passes.py`  
   Generates and schedules per-pass systemd timer and service units.

4. `receive_pass.py`  
   Executes one scheduled pass:
   - live reception with SatDump
   - CADU size check
   - image decode
   - upload
   - link generation
   - mail notification

5. `generate_refresh_units.py`  
   Generates the static systemd refresh units.

## Project Structure

```text
satpi/
├── bin/
│   ├── load_config.py
│   ├── update_tle.py
│   ├── predict_passes.py
│   ├── schedule_passes.py
│   ├── receive_pass.py
│   └── generate_refresh_units.py
├── config/
│   ├── config.ini
│   └── config.example.ini
├── docs/
│   ├── INSTALL_FOR_BEGINNERS.md
│   └── images/
├── logs/
├── results/
│   ├── captures/
│   └── passes/
├── scripts/
│   └── install_base.sh
├── systemd/
│   ├── satpi-refresh.service
│   ├── satpi-refresh.timer
│   └── generated/
├── tle/
│   └── weather.tle
└── README.md
```

## Requirements

- Raspberry Pi running Linux
- Python 3
- systemd
- SatDump
- Skyfield
- rclone
- msmtp
- RTL-SDR compatible receiver

## File overview

### `bin/load_config.py`
This module is responsible for reading the central `config.ini` file, parsing all required sections, converting values to the correct Python types and validating the resulting configuration. It acts as the common entry point for configuration handling across the whole project. If paths, required options or executable locations are missing or invalid, this module raises a configuration error early so that the other scripts fail fast and with a clearer message. In practice, this file defines the configuration contract for the entire satpi system.

### `bin/update_tle.py`
This script downloads current TLE data from the configured remote source, checks whether the download was successful and filters the result so that only the satellites relevant for satpi remain in the local TLE file. It is the first operational step in the planning chain, because accurate pass prediction depends on current orbital data. The script also performs basic connectivity checks in error situations and writes a runtime log so that download and filtering problems can be diagnosed later.

### `bin/predict_passes.py`
This script calculates upcoming satellite passes for the configured ground station position based on the filtered local TLE file. It uses the configured latitude, longitude, altitude, minimum elevation and scheduling window to determine which passes are relevant for reception. The result is written as structured pass data that can later be transformed into systemd jobs. In other words, this script converts orbital data into an actionable reception plan.

### `bin/schedule_passes.py`
This script reads the predicted pass data and generates one systemd service and one systemd timer for every future pass that should still be received. It is responsible for translating the abstract pass list into concrete operating system jobs. During that process, outdated generated units are removed, future passes are kept and the corresponding units are linked and enabled. This file therefore forms the bridge between pass prediction and automated execution.

### `bin/receive_pass.py`
This is the execution script for one scheduled pass. It is called by a generated systemd service and performs the actual operational workflow for a reception window. Depending on the configuration and the pass parameters, it prepares the output directory, starts SatDump with the correct settings, monitors the process until the scheduled stop time, triggers decoding, copies the results to the configured destination and optionally sends a notification email. This is the core runtime component of the project and the place where planning becomes a real recording and decode result.

### `bin/generate_refresh_units.py`
This script creates and enables the higher-level refresh service and timer that periodically update the overall planning state of the system. Its job is not to receive a satellite directly, but to make sure that the planning chain keeps running automatically in the background. The refresh workflow typically includes updating TLE data, predicting passes and regenerating all per-pass timers. This script therefore manages the recurring meta-schedule of the whole satpi installation.

### `config/config.example.ini`
This is the public example configuration file that documents the expected structure and available options for a satpi installation. It is intended as a template for new systems and should be copied to `config.ini` before the first real run. The file contains placeholder paths, comments and default values that explain how station settings, paths, hardware, satellites, scheduling, uploads and notifications are configured.

### `config/config.ini`
This is the active local configuration file used by the satpi scripts on a running system. It contains the real installation-specific values such as file paths, station coordinates, satellite definitions, hardware settings, upload targets and notification settings. Unlike `config.example.ini`, this file is meant to reflect the actual environment of one system and should normally not be committed with private values.

### `scripts/install_base.sh`
This interactive shell script prepares a Raspberry Pi system for satpi. It installs the required base packages, configures important operating system settings, prepares the directory structure, optionally builds SatDump and guides the user through the initial setup. Its purpose is to reduce the amount of manual system administration needed before satpi can be used. In practice, this script acts as the reproducible base installer for a fresh Raspberry Pi OS system.

### `systemd/satpi-refresh.service`
This systemd service defines the command that executes the periodic satpi refresh workflow. It is the service unit behind the recurring planning job and is typically triggered by `satpi-refresh.timer`. Its purpose is to run the update-and-reschedule chain in a controlled way through systemd rather than by manual execution.

### `systemd/satpi-refresh.timer`
This timer triggers `satpi-refresh.service` according to the schedule configured for the installation. It ensures that TLE updates, pass prediction and timer regeneration happen regularly without user intervention. This file is essential for keeping the automatic scheduling process alive over time.

### `systemd/generated/`
This directory contains the generated per-pass systemd service and timer files created by `schedule_passes.py`. These units are not static project files but dynamically produced runtime artifacts based on the currently predicted future passes. They represent the concrete execution plan for the next receptions known to the system.

### `tle/weather.tle`
This file stores the filtered TLE data used locally for pass prediction. It is created or updated by `update_tle.py` and should contain only the satellites relevant for the configured satpi installation. The file acts as the local orbital data source for the prediction step.

### `results/captures/`
This directory stores pass-specific reception results. Each pass usually gets its own subdirectory containing raw recording output, SatDump logs, decoded imagery and follow-up artifacts such as upload logs. It is the main archive location for actual reception results generated by the system.

### `results/passes/`
This directory stores generated pass data and planning-related output. Depending on the current project structure, this can include predicted pass lists or other intermediate scheduling artifacts used by the automation workflow. Its role is to separate planning data from actual reception captures.

### `logs/`
This directory contains log files written by the satpi scripts. These logs are important for diagnosing failures in configuration, TLE download, prediction, scheduling, reception, decoding, copying or notification delivery. For troubleshooting, this is usually the first place to inspect.

## Configuration

Copy the example configuration and adapt it to your system:

```bash
cp config/config.example.ini config/config.ini
```

Configure at least:

- station name and timezone
- QTH coordinates
- satellites
- frequencies and pipelines
- hardware settings
- paths
- copy target
- notifications
- systemd user and Python path

## systemd Integration

Generate the static refresh units:

```bash
python3 bin/generate_refresh_units.py
```

This creates and links:

- `satpi-refresh.service`
- `satpi-refresh.timer`

The refresh timer runs the full planning chain:

1. update TLE
2. predict passes
3. schedule pass timers

Generated per-pass timers and services are written to:

```text
systemd/generated/
```

## Typical Usage

### Update TLE manually

```bash
python3 bin/update_tle.py
```

### Predict passes manually

```bash
python3 bin/predict_passes.py
```

### Schedule all future passes manually

```bash
python3 bin/schedule_passes.py
```

### Generate refresh units

```bash
python3 bin/generate_refresh_units.py
```

## Output

For each successful pass, satpi creates a pass-specific output directory under `output/`.

Depending on signal quality and decode success, this may include:

- raw intermediate files
- `.soft`
- `.cadu`
- decoded image products
- `MSU-MR/`
- `satdump.log`
- `decode.log`
- `upload.log`
- pass metadata

## Upload and Notifications

If enabled in `config.ini`, satpi can:

- upload results via `rclone`
- create a share link
- send a notification mail via `msmtp`

The current implementation supports:

- `rclone` copy targets
- optional public/share link generation
- mail notifications after successful decode and upload

## Status

satpi is designed as a modular and transparent workflow for autonomous satellite reception. The current implementation covers the full pipeline from TLE update to decoded image delivery.

## Author

Andreas Horvath, info[at]andreas-horvath.ch WhatsApp +41 79 249 57 12

## Project

Autonomous, Config-driven satellite reception pipeline for Raspberry Pi
