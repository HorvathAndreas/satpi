#!/usr/bin/env bash
# satpi
# Installs the base software and system setup for satpi on Raspberry Pi 4 / 5.
# This script prepares a fresh Raspberry Pi OS system by installing required
# packages, applying basic operating system settings, preparing the directory
# structure and building the required SatDump binary. It serves as the standard
# base installation workflow for bringing a new satpi system into operation.
# Author: Andreas Horvath
# Project: Autonomous, Config-driven satellite reception pipeline for Raspberry Pi

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SATPI_DIR="${REPO_DIR}"
CONFIG_DIR="${SATPI_DIR}/config"
CONFIG_EXAMPLE="${CONFIG_DIR}/config.example.ini"
CONFIG_LOCAL="${CONFIG_DIR}/config.ini"

press_enter() {
    echo
    read -r -p "Press Enter to continue..."
    echo
}

section() {
    echo
    echo "============================================================"
    echo "$1"
    echo "============================================================"
    echo
}

info() {
    echo "[INFO] $1"
}

warn() {
    echo "[WARN] $1"
}

section "SATPI BASE INSTALLATION FOR RASPBERRY PI 4 / 5"

cat <<'EOF'
This script prepares a Raspberry Pi for satpi.

It will:
- update the system
- configure CPU performance mode
- configure locale
- disable services unneeded for a headless operation
- install required packages
- block DVB-T drivers for RTL-SDR
- prepare directories
- copy config.example.ini to config.ini if needed

It will NOT fully automate:
- rclone remote login
- msmtp account credentials
- SatDump installation path differences on custom systems

You should run this script on Raspberry Pi OS Lite 64-bit.
EOF

press_enter

section "UPDATE SYSTEM"

sudo apt update
sudo apt full-upgrade -y

press_enter

section "SET CPU GOVERNOR TO PERFORMANCE"

sudo tee /etc/systemd/system/cpu-performance.service >/dev/null <<'EOF'
[Unit]
Description=Set CPU governor to performance
After=multi-user.target

[Service]
Type=oneshot
ExecStart=/bin/bash -c 'for f in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do echo performance | tee "$f"; done'

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable cpu-performance.service
sudo systemctl start cpu-performance.service

press_enter

section "CONFIGURE LOCALE"

sudo sed -i 's/^# *en_GB.UTF-8 UTF-8/en_GB.UTF-8 UTF-8/' /etc/locale.gen
sudo locale-gen
sudo update-locale LANG=en_GB.UTF-8

sudo tee /etc/environment >/dev/null <<'EOF'
LANG=en_GB.UTF-8
LC_ALL=en_GB.UTF-8
EOF

sudo sed -i 's/^AcceptEnv LANG LC_/#AcceptEnv LANG LC_/g' /etc/ssh/sshd_config || true

press_enter

section "DISABLE SERVICES UNNEEDED FOR HEADLESS OPERATION"

sudo systemctl disable --now ModemManager.service || true
sudo systemctl disable --now getty@tty1.service || true
sudo systemctl mask serial-getty@ttyAMA10.service || true
sudo systemctl stop serial-getty@ttyAMA10.service || true

press_enter

section "INSTALL REQUIRED PACKAGES"

# prevent msmtp AppArmor dialog
echo 'msmtp msmtp/apparmor boolean false' | sudo debconf-set-selections

sudo apt install -y \
  git \
  cmake \
  build-essential \
  pkg-config \
  curl \
  wget \
  jq \
  python3 \
  python3-skyfield \
  python3-numpy \
  python3-pip \
  python3-venv \
  python3-openai  \
  python3-reportlab \
  sqlite3 \
  rtl-sdr \
  librtlsdr-dev \
  ffmpeg \
  libfftw3-dev \
  libvolk-dev \
  libzstd-dev \
  libpng-dev \
  libjpeg-dev \
  libtiff-dev \
  libcurl4-openssl-dev \
  libnng-dev \
  libsqlite3-dev \
  libglfw3-dev \
  libjemalloc-dev \
  libusb-1.0-0-dev \
  libdbus-1-dev \
  rclone \
  msmtp \
  rsync

press_enter

section "BLOCK DVB-T DRIVERS"

sudo tee /etc/modprobe.d/blacklist-rtl2832.conf >/dev/null <<'EOF'
blacklist dvb_usb_rtl28xxu
blacklist rtl2832
blacklist rtl2830
EOF

sudo udevadm control --reload-rules
sudo udevadm trigger

press_enter

section "CONFIGURE USB POWER (PI 4 — 1.2 A PER PORT)"

cat <<'EOT_INFO'
On Raspberry Pi 4, each USB port is limited to 600 mA by default. RTL-SDR
dongles (especially TCXO models like the Nooelec SMArTee XTR) can draw
more than that under load and may disconnect from the bus mid-operation.

Setting 'usb_max_current_enable=1' raises the per-port limit to 1.2 A.
Requires a 5V / 3A or better power supply for the Pi itself; with a
weaker PSU the Pi may show undervoltage warnings instead.

This setting has no effect on Raspberry Pi 5 (it is silently ignored).
Takes effect after the next reboot.
EOT_INFO

# Detect which config.txt path applies (Bookworm uses /boot/firmware/, older
# Pi OS releases use /boot/).
PI_CONFIG_TXT=""
for candidate in /boot/firmware/config.txt /boot/config.txt; do
    if [[ -f "$candidate" ]]; then
        PI_CONFIG_TXT="$candidate"
        break
    fi
done

if [[ -z "$PI_CONFIG_TXT" ]]; then
    warn "Neither /boot/firmware/config.txt nor /boot/config.txt found; skipping."
elif grep -qE '^[[:space:]]*usb_max_current_enable[[:space:]]*=' "$PI_CONFIG_TXT"; then
    info "usb_max_current_enable already set in ${PI_CONFIG_TXT}; skipping."
else
    info "Adding 'usb_max_current_enable=1' to ${PI_CONFIG_TXT}"
    sudo tee -a "$PI_CONFIG_TXT" >/dev/null <<'EOT_USBCFG'

# satpi: enable 1.2 A USB current (Pi 4) so RTL-SDR dongles don't
# disconnect under load. No effect on Pi 5.
usb_max_current_enable=1
EOT_USBCFG
    info "Done. A reboot is required for this to take effect."
fi

press_enter

section "PREPARE SOURCE DIRECTORY"

sudo mkdir -p /usr/local/src
sudo chown -R "$USER:$USER" /usr/local/src

press_enter

section "PREPARE SATPI DIRECTORY STRUCTURE"

mkdir -p "${SATPI_DIR}"/{bin,config,docs,logs,results,scripts,systemd}
mkdir -p "${SATPI_DIR}/results"/{captures,passes,tle}
mkdir -p "${SATPI_DIR}/systemd/generated"

if [[ -f "$CONFIG_LOCAL" ]]; then
    warn "config.ini already exists. It will not be overwritten."
else
    if [[ -f "$CONFIG_EXAMPLE" ]]; then
        cp "$CONFIG_EXAMPLE" "$CONFIG_LOCAL"
        info "Created ${CONFIG_LOCAL} from config.example.ini"
    else
        warn "config.example.ini not found: ${CONFIG_EXAMPLE}"
    fi
fi

press_enter

section "BUILD SATDUMP HEADLESS"

cat <<'EOF'
SatDump is required for satpi.

This script will:
- remove any previous /usr/local/src/SatDump tree (always start fresh, to
  avoid corruption from interrupted earlier clones)
- clone SatDump from upstream
- switch to stable version 1.2.2
- build a headless version
- install it to /usr/bin/satdump

If a previous SatDump build exists at /usr/local/src/SatDump it will be
deleted. The build runs in the current shell — if your SSH connection
is unstable, run this script inside tmux so the build survives drops.
EOF

press_enter

cd /usr/local/src

# Always start with a clean tree. Interrupted clones (especially over a flaky
# VPN or WiFi connection) leave a partially populated .git that breaks
# subsequent 'git fetch --tags' or 'git checkout' with errors like
# "fatal: bad object refs/heads/master" or "index file smaller than expected".
if [[ -d SatDump ]]; then
    info "Removing existing /usr/local/src/SatDump to start clean..."
    sudo rm -rf SatDump
fi

git clone https://github.com/SatDump/SatDump.git

cd SatDump
sudo chown -R "$USER:$USER" .
git fetch --all --tags
git checkout 1.2.2

rm -rf build
mkdir build
cd build

cmake .. \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX=/usr \
  -DSATDUMP_BUILD_UI=OFF \
  -DSATDUMP_BUILD_GUI=OFF \
  -DSATDUMP_BUILD_TESTS=OFF \
  -DCMAKE_C_FLAGS="-O3 -march=native -pipe" \
  -DCMAKE_CXX_FLAGS="-O3 -march=native -pipe" \
  -DCMAKE_EXE_LINKER_FLAGS="-s"

cmake --build . -j "$(nproc)"
sudo cmake --install .

info "SatDump installed."

press_enter

section "CHECK INSTALLED TOOLS"

for cmd in python3 git curl jq rclone msmtp cmake; do
    if command -v "$cmd" >/dev/null 2>&1; then
        echo "[OK] $cmd -> $(command -v "$cmd")"
    else
        echo "[MISSING] $cmd"
    fi
done

if command -v satdump >/dev/null 2>&1; then
    echo "[OK] satdump -> $(command -v satdump)"
else
    echo "[MISSING] satdump"
fi

section "CHECK INSTALLED TOOLS"

for cmd in python3 git curl jq rclone msmtp; do
    if command -v "$cmd" >/dev/null 2>&1; then
        echo "[OK] $cmd -> $(command -v "$cmd")"
    else
        echo "[MISSING] $cmd"
    fi
done

press_enter

section "REQUIRED MANUAL STEPS"

cat <<EOF
Manual steps still required:

1. Review and edit your config:
   nano "${CONFIG_LOCAL}"

2. Configure rclone:
   rclone config

3. Configure msmtp:
   nano ~/.msmtprc

4. Test mail setup:
   printf "Subject: satpi test\n\nTest mail.\n" | /usr/bin/msmtp you@example.com

5. Run the main workflow manually:
   cd "${SATPI_DIR}"
   python3 bin/update_tle.py
   python3 bin/predict_passes.py
   python3 bin/schedule_passes.py

6. Generate refresh units:
   cd "${SATPI_DIR}"
   python3 bin/generate_refresh_units.py
EOF

press_enter

section "BASE INSTALLATION COMPLETE"

info "satpi base setup finished."
info "Repository directory: ${REPO_DIR}"
info "Local satpi directory: ${SATPI_DIR}"
