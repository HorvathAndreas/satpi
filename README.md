# satpi

Autonomous, config-driven satellite reception pipeline for Raspberry Pi.

`satpi` is a headless workflow for automated weather satellite reception. It downloads and filters TLE data, predicts passes, generates per-pass systemd timers, runs SatDump for live reception, stores structured reception metadata, renders reception plots, imports pass metrics into SQLite, uploads successful results, and can send notification emails.

## Features

- Headless, autonomous workflow
- Config-driven setup
- Per-satellite configuration
- Skyfield-based pass prediction
- systemd-based scheduling
- SatDump live reception and decode
- Structured `reception.json` output per pass
- Automatic skyplot and time-series rendering
- SQLite-based reception database
- Reception analysis and optimization tools
- Optional upload via `rclone`
- Optional notification via `msmtp`

## Quick start

Clone the repository:

```bash
sudo apt update
sudo apt install -y git
git clone https://github.com/HorvathAndreas/satpi.git
cd satpi
```

Run the base installation script:

```bash
bash scripts/install_base.sh
```

Copy the example configuration and create the active local config:

```bash
cp config/config.example.ini config/config.ini
nano config/config.ini
```

Run the workflow manually once:

```bash
python3 bin/update_tle.py
python3 bin/predict_passes.py
python3 bin/schedule_passes.py
python3 bin/generate_refresh_units.py
```

Check active timers:

```bash
systemctl list-timers --all | grep satpi
```

## Requirements

### Hardware

- Raspberry Pi 4 or Raspberry Pi 5
- RTL-SDR compatible receiver
- Suitable antenna and RF setup for weather satellite reception

### Software

- Raspberry Pi OS Lite 64-bit
- Python 3
- systemd
- SatDump
- `python3-skyfield`
- `python3-numpy`
- `python3-matplotlib`
- `sqlite3`
- `rclone`
- `msmtp`

## Workflow

1. **`update_tle.py`**  
   Downloads fresh TLE data and filters it to the configured satellites.

2. **`predict_passes.py`**  
   Calculates upcoming passes for the configured ground station.

3. **`schedule_passes.py`**  
   Generates per-pass systemd service and timer units for all relevant future passes.

4. **`receive_pass.py`**  
   Executes one scheduled pass, starts SatDump, records structured reception data, renders plots, imports metrics into SQLite, decodes results, uploads output, and optionally sends a notification email.

5. **`generate_refresh_units.py`**  
   Creates the higher-level refresh service and timer that periodically updates the overall planning state of the system.

## Project structure

```text
satpi/
├── bin/
│   ├── export_reception_report_pdf.py
│   ├── generate_refresh_units.py
│   ├── import_reception_to_db.py
│   ├── init_reception_db.py
│   ├── load_config.py
│   ├── optimize_reception.py
│   ├── optimize_reception_ai.py
│   ├── plot_reception.py
│   ├── predict_passes.py
│   ├── query_reception_db.py
│   ├── receive_pass.py
│   ├── schedule_passes.py
│   └── update_tle.py
├── config/
│   ├── config.ini
│   └── config.example.ini
├── docs/
│   ├── INSTALL_FOR_BEGINNERS.md
│   └── images/
├── logs/
├── results/
│   ├── captures/
│   ├── database/
│   ├── optimization/
│   ├── passes/
│   ├── reports/
│   └── tle/
├── scripts/
│   └── install_base.sh
├── systemd/
│   ├── satpi-refresh.service
│   ├── satpi-refresh.timer
│   └── generated/
└── README.md
```

## File overview

### `bin/load_config.py`
Loads, parses, and validates the central `config.ini` file. It resolves relative project paths against `paths.base_dir` and converts configuration values into typed Python data structures.

### `bin/update_tle.py`
Downloads current TLE data from the configured source and filters it so that only the satellites used by this installation remain in the local TLE file.

### `bin/predict_passes.py`
Calculates upcoming satellite passes for the configured ground station based on the filtered local TLE file.

### `bin/schedule_passes.py`
Reads the predicted pass data and generates one systemd service and one systemd timer for every future pass that should still be received.

### `bin/receive_pass.py`
Executes one scheduled pass from start to finish. It prepares the pass-specific output directory, starts SatDump, records structured reception data into `reception.json`, renders plots, imports metrics into SQLite, triggers decode, copies the results, and optionally sends a notification email.

### `bin/plot_reception.py`
Creates a skyplot and a reception time-series plot from `reception.json`.

### `bin/init_reception_db.py`
Initializes the SQLite database schema used for reception history and analysis.

### `bin/import_reception_to_db.py`
Imports pass-level metrics and setup information from `reception.json` into the SQLite reception database.

### `bin/query_reception_db.py`
Queries the SQLite reception database for analysis and reporting.

### `bin/optimize_reception.py`
Analyzes recorded reception data from SQLite and compares geometrically similar passes to evaluate reception performance.

### `bin/optimize_reception_ai.py`
Builds on the optimizer output and produces an AI-assisted interpretation of reception quality trends.

### `bin/export_reception_report_pdf.py`
Exports reception analysis results into PDF format.

### `bin/generate_refresh_units.py`
Creates and enables the refresh service and timer that periodically run the higher-level planning chain.

### `config/config.example.ini`
Public example configuration file for new installations. Copy this file to `config/config.ini` before first use.

### `config/config.ini`
Active local configuration file used by the satpi scripts on a running system. This file is intentionally local and should not be committed.

### `scripts/install_base.sh`
Interactive base installation script for Raspberry Pi OS. It installs required packages, applies base operating system settings, prepares the directory structure, and builds the required SatDump binary.

### `systemd/satpi-refresh.service`
Systemd service that executes the periodic satpi refresh workflow.

### `systemd/satpi-refresh.timer`
Systemd timer that periodically triggers `satpi-refresh.service`.

### `systemd/generated/`
Contains the generated per-pass systemd service and timer files created by `schedule_passes.py`.

## Configuration

Create the active configuration file from the example:

```bash
cp config/config.example.ini config/config.ini
nano config/config.ini
```

### Minimum required adjustments

Before running satpi, at minimum you should review and adapt these settings in `config/config.ini`:

- **`[station]`**
  - `name`
  - `timezone`

- **`[qth]`**
  - `latitude`
  - `longitude`
  - `altitude_m`

- **`[paths]`**
  - `base_dir` if your installation path differs
  - `satdump_bin` if SatDump is installed elsewhere
  - `mail_bin` if `msmtp` is installed elsewhere
  - `python_bin` if your Python path differs

- **`[hardware]`**
  - `source_id`
  - `gain`
  - `sample_rate`
  - `bias_t`

- **`[satellite.*]`**
  - `enabled`
  - `min_elevation_deg`
  - `frequency_hz`
  - `bandwidth_hz`
  - `pipeline`

- **`[copytarget]`**
  - `enabled`
  - `rclone_remote`
  - `rclone_path`

- **`[notify]`**
  - `enabled`
  - `mail_to`
  - `mail_subject_prefix`

- **`[systemd]`**
  - `service_user`

- **`[reception_setup]`**
  - antenna, SDR, feedline, host, and power-supply description fields should match your real setup

- **`[optimize_reception_ai]`**
  - `enabled`
  - `api_key` if you want AI-assisted optimizer analysis

If you only want the basic reception pipeline first, the most important items are:

- station and QTH
- paths
- hardware
- satellite definitions
- systemd service user

## Path handling

All local project paths are configured in the `[paths]` section.

`base_dir` is the project root. Most project-specific paths in `config.ini` are stored relative to `base_dir`, for example:

- `results/passes/passes.json`
- `results/captures`
- `results/tle/weather.tle`
- `results/database/reception.db`

System binaries remain absolute paths:

- `satdump_bin`
- `mail_bin`
- `python_bin`

## Installation notes

The base installation script prepares the operating system and builds SatDump:

```bash
bash scripts/install_base.sh
```

After that, two manual integrations are typically still required.

### Configure rclone

```bash
rclone config
```

### Configure msmtp

Create the mail configuration file:

```bash
nano ~/.msmtprc
chmod 600 ~/.msmtprc
```

Example structure:

```ini
defaults
auth           on
tls            on
tls_trust_file /etc/ssl/certs/ca-certificates.crt
logfile        ~/.msmtp.log

account gmail
host smtp.gmail.com
port 587
from YOUR_GMAIL_ADDRESS
user YOUR_GMAIL_ADDRESS
password YOUR_APP_PASSWORD

account default : gmail
```

Test mail delivery:

```bash
printf "Subject: satpi test\n\nTest mail.\n" | /usr/bin/msmtp YOUR_GMAIL_ADDRESS
```

## systemd integration

Generate the refresh units:

```bash
python3 bin/generate_refresh_units.py
```

This creates and links:

- `satpi-refresh.service`
- `satpi-refresh.timer`

The refresh timer runs the planning chain:

1. update TLE
2. predict passes
3. schedule pass timers

Check the result:

```bash
systemctl list-timers --all | grep satpi
```

## Typical usage

Update TLE data manually:

```bash
python3 bin/update_tle.py
```

Predict passes manually:

```bash
python3 bin/predict_passes.py
```

Generate per-pass timers manually:

```bash
python3 bin/schedule_passes.py
```

Generate refresh units manually:

```bash
python3 bin/generate_refresh_units.py
```

Run reception optimizer manually:

```bash
python3 bin/optimize_reception.py --config config/config.ini
```

Run AI-based optimizer analysis manually:

```bash
python3 bin/optimize_reception_ai.py --config config/config.ini
```

## Output

### Pass results

Pass-specific reception results are written to:

```text
results/captures/
```

Each pass gets its own directory, for example:

```text
results/captures/2026-04-10_16-07-30_METEOR-M2_4/
```

A pass directory may contain:

- `reception.json`
- `*-skyplot.png`
- `*-timeseries.png`
- `.cadu`
- decoded image products
- `MSU-MR/`
- `dataset.json`
- `telemetry.json`

### Planning results

Prediction and planning artifacts are written to:

```text
results/passes/
```

This directory is intended only for planning-related files such as:

- `passes.json`

### Database

Reception history and derived pass metrics are stored in:

```text
results/database/reception.db
```

### Optimization output

Optimizer reports are written to:

```text
results/optimization/
```

### Runtime logs

General script logs are written to:

```text
logs/
```

## Structured reception data

For every recorded pass, `receive_pass.py` writes a `reception.json` file into the pass directory under `results/captures/`.

This file contains:

- pass identifiers and timing
- RF settings
- reception setup metadata
- time-stamped SNR / BER / sync-state samples
- azimuth and elevation samples

This JSON file is the basis for:

- plot generation
- database import
- optimizer analysis
- later report generation

## Upload and notifications

If enabled in `config.ini`, satpi can:

- upload results via `rclone`
- create a share link
- send a notification email via `msmtp`

## Documentation

Beginner-oriented setup notes are available here:

```text
docs/INSTALL_FOR_BEGINNERS.md
```

## Author

Andreas Horvath  
info[at]andreas-horvath.ch  
WhatsApp: +41 79 249 57 12

## Project

Autonomous, config-driven satellite reception pipeline for Raspberry Pi
