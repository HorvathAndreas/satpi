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
├── data/
│   ├── passes.json
│   └── state.json
├── logs/
├── output/
├── systemd/
│   ├── satpi-refresh.service
│   ├── satpi-refresh.timer
│   └── generated/
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

## Git Recommendations

Suggested `.gitignore` entries:

```gitignore
config/config.ini
logs/
output/
data/passes.json
data/state.json
systemd/generated/
__pycache__/
*.pyc
testdata/
```

## Status

satpi is designed as a modular and transparent workflow for autonomous satellite reception. The current implementation covers the full pipeline from TLE update to decoded image delivery.

## Author

Andreas Horvath

## Project

Autonomous, Config-driven satellite reception pipeline for Raspberry Pi
