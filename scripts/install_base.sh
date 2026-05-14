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

# Note: Hardcoded to en_GB.UTF-8. Modify this section if a different locale is required.
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
  rsync \
  iw

press_enter

section "CONFIGURE SUDOERS FOR SYSTEMCTL OPERATIONS"

cat <<'EOT_SUDOERS'
satpi scripts require passwordless sudo for systemctl operations.
This section will add a sudoers rule allowing 'systemctl' calls
without a password prompt.
EOT_SUDOERS

# Create a temporary sudoers file
SUDOERS_TEMP=$(mktemp)
trap "rm -f $SUDOERS_TEMP" EXIT

# Add the rule for systemctl (system-wide for all users running satpi)
cat > "$SUDOERS_TEMP" <<EOF
# satpi: Allow passwordless systemctl for satellite pass scheduling
${USER} ALL=(root) NOPASSWD: /bin/systemctl
EOF

# Validate the sudoers file with visudo before applying
if sudo visudo -c -f "$SUDOERS_TEMP" >/dev/null 2>&1; then
    sudo install -o root -g root -m 0440 "$SUDOERS_TEMP" "/etc/sudoers.d/satpi-systemctl"
    info "sudoers rule added: ${USER} can run /bin/systemctl without password"
else
    warn "sudoers validation failed. Please configure manually:"
    warn "  sudo visudo"
    warn "  Add: ${USER} ALL=(root) NOPASSWD: /bin/systemctl"
fi

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

section "CONFIGURE RTL-SDR USB ACCESS"

cat <<'EOT_RTLSDR'
RTL-SDR dongles appear as USB devices and require group membership
for non-root access. This section configures udev rules to grant
automatic read/write access and adds the current user to the
plugdev group.

Without this, only root can access the RTL-SDR dongle.
EOT_RTLSDR

# Create udev rule for RTL-SDR access (multiple device IDs)
sudo tee /etc/udev/rules.d/99-rtlsdr.rules >/dev/null <<'EOF'
# RTL-SDR dongles (various manufacturer IDs)
SUBSYSTEMS=="usb", ATTRS{idVendor}=="0bda", ATTRS{idProduct}=="2838", MODE="0666"
SUBSYSTEMS=="usb", ATTRS{idVendor}=="0bda", ATTRS{idProduct}=="2832", MODE="0666"
SUBSYSTEMS=="usb", ATTRS{idVendor}=="0bda", ATTRS{idProduct}=="0129", MODE="0666"
SUBSYSTEMS=="usb", ATTRS{idVendor}=="16c0", ATTRS{idProduct}=="0296", MODE="0666"
EOF

sudo udevadm control --reload-rules
sudo udevadm trigger

# Add current user to plugdev group (for immediate access without reboot)
sudo usermod -a -G plugdev "$USER"

info "RTL-SDR USB access configured. User '$USER' added to plugdev group."
info "Note: A new login session (or 'newgrp plugdev') may be required for group membership to take effect."

press_enter


section "CONFIGURE USB POWER (RASPBERRY PI 4 / 5)"

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

section "DISABLE WIFI POWER SAVE (PI 5 BRCMFMAC HANG WORKAROUND)"

cat <<'EOT_WIFI_PS'
The Pi 4 / Pi 5 WiFi chip (BCM4345 via the brcmfmac driver) ships with
power save enabled by default. Under sustained load — especially during
WPA group rekey events on the 5 GHz band — this is known to cause the
kernel to silently hang: logs stop without any error message, network
goes dead, and the system requires a hard power-cycle to recover.

Symptom in journalctl -b -1:
  the last log line is a WiFi event (WPA rekey, avahi address record,
  or NTP timeout), and there is no shutdown or panic message after it.

Disabling power save on wlan0 reliably eliminates these hangs at the
cost of a small idle-power increase (a few milliwatts).
EOT_WIFI_PS

# 1. Apply the setting immediately for the current session.
sudo iw dev wlan0 set power_save off 2>/dev/null || \
    warn "Could not set power_save off now (wlan0 not present?). Persistent unit will still install."

# 2. Install a systemd oneshot so the setting survives every reboot.
sudo tee /etc/systemd/system/wifi-powersave-off.service >/dev/null <<'EOT_WIFI_UNIT'
[Unit]
Description=Disable WiFi power save on wlan0 (brcmfmac hang workaround)
After=multi-user.target

[Service]
Type=oneshot
ExecStart=/usr/sbin/iw dev wlan0 set power_save off
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOT_WIFI_UNIT

sudo systemctl daemon-reload
sudo systemctl enable --now wifi-powersave-off.service

CURRENT_PS="$(iw dev wlan0 get power_save 2>/dev/null | sed 's/^.*: //')"
info "wlan0 power_save is now: ${CURRENT_PS:-unknown}"

press_enter

section "ENABLE PERSISTENT SYSTEMD JOURNAL"

cat <<'EOT_JOURNAL_INFO'
By default, systemd-journald keeps logs in /run (RAM) only — they
disappear at every reboot. Enabling persistent storage in
/var/log/journal lets you debug crashes and reboot failures after
the fact, e.g. via:

    journalctl -b -1 -p err

Trade-off: continuous writes to the SD card. We cap journal size
at 200 MB and 2 weeks retention to limit wear. That is enough for
debugging while keeping flash wear low on long-running Pi setups.
EOT_JOURNAL_INFO

sudo mkdir -p /etc/systemd/journald.conf.d
sudo tee /etc/systemd/journald.conf.d/satpi.conf >/dev/null <<'EOT_JOURNALD_CONF'
[Journal]
Storage=persistent
SystemMaxUse=200M
SystemKeepFree=1G
MaxRetentionSec=2week
EOT_JOURNALD_CONF

sudo mkdir -p "/var/log/journal/$(cat /etc/machine-id)"
sudo systemd-tmpfiles --create --prefix /var/log/journal
sudo systemctl restart systemd-journald

info "Persistent journal enabled (cap: 200 MB, retention: 2 weeks)."

press_enter

section "ENABLE FAKE-HWCLOCK (CLOCK PERSISTENCE WITHOUT RTC BATTERY)"

cat <<'EOT_FHWC_INFO'
By default, the Pi 5 has no battery-backed real-time clock. Without
RTC, the system clock starts at 1970 at every boot until NTP syncs.
This causes confusing log timestamps and can trigger systemd timers
with Persistent=true to fire retroactively for many missed events.

fake-hwclock saves the last known time periodically and restores it
at boot, so the clock is approximately right even before NTP syncs.

This is a workaround. The proper fix is to attach a battery to the
J5 connector on the Pi 5 board (~5 EUR for the official accessory).
EOT_FHWC_INFO

sudo apt install -y fake-hwclock

# Modern Pi OS uses three split units (load/save service + save timer).
# The legacy fake-hwclock.service is masked by Pi OS in favour of those.
# Enable whichever variant is present.
if systemctl list-unit-files fake-hwclock-load.service >/dev/null 2>&1; then
    sudo systemctl enable --now fake-hwclock-load.service \
                                fake-hwclock-save.service \
                                fake-hwclock-save.timer || true
elif systemctl list-unit-files fake-hwclock.service >/dev/null 2>&1; then
    sudo systemctl enable --now fake-hwclock || true
fi

# Save current time once now, so the .data file is fresh.
sudo fake-hwclock save 2>/dev/null || true

info "fake-hwclock active. Saved time: $(cat /etc/fake-hwclock.data 2>/dev/null || echo 'unknown')"

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
elif [[ -f "$CONFIG_EXAMPLE" ]]; then
    cp "$CONFIG_EXAMPLE" "$CONFIG_LOCAL"
    info "Created ${CONFIG_LOCAL} from config.example.ini"
else
    warn "config.example.ini not found: ${CONFIG_EXAMPLE}"
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
if [[ -d /usr/local/src/SatDump ]]; then
    info "Removing existing /usr/local/src/SatDump to start clean..."
    sudo rm -rf /usr/local/src/SatDump
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

# Disable SatDump TLE auto-updates
sudo sed -i '/tle_update_interval/,/^[[:space:]]*},/{s/"value": "[^"]*"/"value": "Never"/}' /usr/share/satdump/satdump_cfg.json

info "SatDump installed."

press_enter

section "INITIALIZE RECEPTION DATABASE"

cd "${SATPI_DIR}"
python3 bin/init_reception_db.py

press_enter

section "VERIFY INSTALLATION"

cd "${SATPI_DIR}"
python3 bin/init_reception_db.py

press_enter

section "VERIFY INSTALLATION"

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
