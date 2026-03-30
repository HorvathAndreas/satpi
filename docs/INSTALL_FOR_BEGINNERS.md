# Install satpi on a Raspberry Pi (Beginner Guide)

This guide is written for beginners. It starts with the hardware, then shows how to prepare a Raspberry Pi with Raspberry Pi Imager on Windows or macOS, and finally how to install and configure satpi.

Raspberry Pi recommends Raspberry Pi Imager as the standard way to write Raspberry Pi OS to a microSD card, and the Imager can preconfigure hostname, user account, Wi-Fi, and SSH for a headless setup.  [oai_citation:0‡Raspberry Pi](https://www.raspberrypi.com/documentation/computers/getting-started.html?utm_source=chatgpt.com)

## 1. What you need

You need:

- a Raspberry Pi 4 or Raspberry Pi 5 (the latter with a cooling fan)
- a suitable power supply for your Raspberry Pi
- a microSD card
- a computer with Windows or macOS (to setup the SD Card initially)
- an internet connection
- an RTL-SDR compatible USB receiver
- an antenna suitable for weather satellite reception (best is a QFH antenna, but a V-dipol is fine for testing and you should be able to receive signals)
- optionally an LNA and Bias-T if your antenna setup needs it. With the Raspberry Pi installed close to the antenna (in a waterproof box), you can keep coax cables short and won't need an LNA.

## 2. Download Raspberry Pi Imager

On your Windows PC or Mac:

1. Open the Raspberry Pi software page.
2. Download **Raspberry Pi Imager** for your system.
3. Install it.

Raspberry Pi describes Raspberry Pi Imager as the quick and easy way to install Raspberry Pi OS to a microSD card.  [oai_citation:2‡Raspberry Pi](https://www.raspberrypi.com/software/operating-systems/?utm_source=chatgpt.com)

## 3. Install Raspberry Pi OS Lite 64-bit on the microSD card

1. Insert the microSD card into your computer.
2. Start **Raspberry Pi Imager**.
3. Click **Choose Device** and select your Raspberry Pi model.
4. Click **Choose OS**.
5. Select **Raspberry Pi OS Lite (64-bit)**. As it is a headless install and run, we don't need the raspberry pi gui
6. Click **Choose Storage** and select your microSD card.
7. Click **Next**.

## 4. Configure the image before writing it

When Raspberry Pi Imager asks whether you want to apply OS customisation settings, choose **Edit Settings**.

Set at least the following:

### General
- hostname, for example: `satpi`
- username, for example: `andreas`
- password
- Wi-Fi name
- Wi-Fi password
- Wi-Fi country

### Services
- enable **SSH**

Then save the settings and continue.

Raspberry Pi documents that Raspberry Pi Imager can preconfigure a hostname, user account, network connection, and SSH during imaging, which is ideal for headless setup.  [oai_citation:3‡Raspberry Pi](https://www.raspberrypi.com/documentation/computers/getting-started.html?utm_source=chatgpt.com)

## 5. Write the card and boot the Raspberry Pi

1. Click **Write** in Raspberry Pi Imager.
2. Wait until writing has finished.
3. Remove the card from your computer.
4. Insert the card into the Raspberry Pi.
5. Connect:
   - power
   - network, or rely on Wi-Fi if configured
   - RTL-SDR later, not yet required for first boot
6. Wait 1 to 2 minutes for the first boot.

## 6. Connect to the Raspberry Pi with SSH

From your computer, open a terminal.

### On macOS
Use Terminal.

### On Windows
Use PowerShell or PuTTY.

Connect with:

bash
ssh YOUR_USER@HOSTNAME.local

## Quick start after cloning

sudo apt update
sudo apt install -y git
git clone https://github.com/HorvathAndreas/satpi.git

cd ~/satpi
bash scripts/install_base.sh

Run the commands below after cloning the repository:

bash
cd ~/satpi

cp config/config.example.ini config/config.ini
nano config/config.ini
make all necessary changes in the config.ini file.

rclone config

printf "Subject: satpi test\n\nTest mail.\n" | /usr/bin/msmtp you@example.com

python3 bin/test_config.py
python3 bin/update_tle.py
python3 bin/predict_passes.py
python3 bin/schedule_passes.py
python3 bin/generate_refresh_units.py

systemctl list-timers --all | grep satpi

7. Wait and you should see mails arraving in your mailbox with the link to decoded weather pictures. Have fun!
