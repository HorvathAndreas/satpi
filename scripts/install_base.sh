#!/usr/bin/env bash
# satpi
# Installs the base software and system setup for satpi on Raspberry Pi 4 / 5.
# Author: Andreas Horvath
# Project: Autonomous, Config-driven satellite reception pipeline for Raspberry Pi

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SATPI_DIR="${HOME}/satpi"
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
- disable some unneeded headless services
- disable Wi-Fi powersave
- install required packages
- block DVB-T drivers for RTL-SDR
- disable USB autosuspend
- prepare directories
- install Python dependency skyfield
- copy config.example.ini to config.ini if needed

It will NOT fully automate:
- WireGuard VPN secrets
- rclone remote login
- msmtp account credentials
- SatDump installation path differences on custom systems

You should run this script on Raspberry Pi OS Lite 64-bit.
EOF

press_enter

section "STEP 1 - UPDATE SYSTEM"

sudo apt update
sudo apt full-upgrade -y

press_enter

section "STEP 2 - SET CPU GOVERNOR TO PERFORMANCE"

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

section "STEP 3 - CONFIGURE LOCALE"

sudo sed -i 's/^# *en_GB.UTF-8 UTF-8/en_GB.UTF-8 UTF-8/' /etc/locale.gen
sudo locale-gen
sudo update-locale LANG=en_GB.UTF-8

sudo tee /etc/environment >/dev/null <<'EOF'
LANG=en_GB.UTF-8
LC_ALL=en_GB.UTF-8
EOF

sudo sed -i 's/^AcceptEnv LANG LC_/#AcceptEnv LANG LC_/g' /etc/ssh/sshd_config || true

press_enter

section "STEP 4 - DISABLE UNNEEDED HEADLESS SERVICES"

sudo systemctl disable --now ModemManager.service || true
sudo systemctl disable --now getty@tty1.service || true
sudo systemctl mask serial-getty@ttyAMA10.service || true
sudo systemctl stop serial-getty@ttyAMA10.service || true

press_enter

section "STEP 5 - DISABLE WI-FI POWERSAVE"

sudo mkdir -p /etc/NetworkManager/conf.d
sudo tee /etc/NetworkManager/conf.d/wifi-powersave.conf >/dev/null <<'EOF'
[connection]
wifi.powersave=2
EOF

press_enter

section "STEP 6 - INSTALL REQUIRED PACKAGES"

sudo apt install -y \
  git \
  cmake \
  build-essential \
  pkg-config \
  curl \
  wget \
  jq \
  python3 \
  python3-pip \
  python3-venv \
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
  wireguard \
  resolvconf \
  rclone \
  msmtp \
  rsync

press_enter

section "STEP 7 - BLOCK DVB-T DRIVERS"

sudo tee /etc/modprobe.d/blacklist-rtl2832.conf >/dev/null <<'EOF'
blacklist dvb_usb_rtl28xxu
blacklist rtl2832
blacklist rtl2830
EOF

sudo udevadm control --reload-rules
sudo udevadm trigger

press_enter

section "STEP 8 - DISABLE USB AUTOSUSPEND"

sudo tee /etc/modprobe.d/usb-autosuspend.conf >/dev/null <<'EOF'
options usbcore autosuspend=-1
EOF

press_enter

section "STEP 9 - PREPARE SOURCE DIRECTORY"

sudo mkdir -p /usr/local/src
sudo chown -R "$USER:$USER" /usr/local/src

press_enter

section "STEP 10 - INSTALL PYTHON DEPENDENCY"

python3 -m pip install --break-system-packages --user skyfield numpy

press_enter

section "STEP 11 - PREPARE SATPI DIRECTORY STRUCTURE"

mkdir -p "${SATPI_DIR}"/{bin,config,data,logs,output,systemd/generated,tle,scripts}

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

section "STEP 12 - OPTIONAL: BUILD SATDUMP HEADLESS"

cat <<'EOF'
You can now choose whether to build SatDump automatically.

Recommended default for this script:
- build stable SatDump 1.2.2 headless
- install to /usr/bin/satdump

If you prefer to install SatDump manually, answer 'n'.
EOF

read -r -p "Build SatDump now? [y/N]: " BUILD_SATDUMP
if [[ "${BUILD_SATDUMP:-N}" =~ ^[Yy]$ ]]; then
    cd /usr/local/src

    if [[ ! -d SatDump ]]; then
        git clone https://github.com/SatDump/SatDump.git
    fi

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
else
    warn "Skipping SatDump build."
fi

press_enter

section "STEP 13 - OPTIONAL: PREPARE WIREGUARD TEMPLATE"

cat <<'EOF'
satpi can use WireGuard during TLE download.

This script will NOT install real VPN secrets into your system.
If you want, it will create a template file:

  /etc/wireguard/proton.conf.example

You must fill in:
- PrivateKey
- Address
- Peer PublicKey
- Endpoint

before using it.
EOF

read -r -p "Create WireGuard template file? [y/N]: " CREATE_WG
if [[ "${CREATE_WG:-N}" =~ ^[Yy]$ ]]; then
    sudo mkdir -p /etc/wireguard
    sudo tee /etc/wireguard/proton.conf.example >/dev/null <<'EOF'
[Interface]
PrivateKey = REPLACE_ME
Address = 10.0.0.2/32
# Optional:
# PostUp = ip route add 192.168.0.0/24 via 192.168.0.1 dev wlan0
# PostDown = ip route del 192.168.0.0/24 via 192.168.0.1 dev wlan0

[Peer]
PublicKey = REPLACE_ME
AllowedIPs = 0.0.0.0/0, ::/0
Endpoint = REPLACE_ME:51820
PersistentKeepalive = 25
EOF
    info "Created /etc/wireguard/proton.conf.example"
fi

sudo rm -f /etc/resolv.conf
sudo tee /etc/resolv.conf >/dev/null <<'EOF'
nameserver 1.1.1.1
nameserver 1.0.0.1
nameserver 9.9.9.9
EOF

press_enter

section "STEP 14 - CHECK INSTALLED TOOLS"

for cmd in python3 pip3 git curl jq rclone msmtp; do
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

press_enter

section "STEP 15 - NEXT MANUAL STEPS"

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

5. If you use VPN for TLE downloads:
   copy and edit:
   sudo cp /etc/wireguard/proton.conf.example /etc/wireguard/proton.conf
   sudo nano /etc/wireguard/proton.conf
   sudo chmod 600 /etc/wireguard/proton.conf

6. Test the configuration:
   cd "${SATPI_DIR}"
   python3 bin/test_config.py

7. Run the main workflow manually:
   python3 bin/update_tle.py
   python3 bin/predict_passes.py
   python3 bin/schedule_passes.py

8. Generate refresh units:
   python3 bin/generate_refresh_units.py
EOF

press_enter

section "BASE INSTALLATION COMPLETE"

info "satpi base setup finished."
info "Repository directory: ${REPO_DIR}"
info "Local satpi directory: ${SATPI_DIR}"
