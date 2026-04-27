# Install satpi on a Raspberry Pi (Beginner Guide)

This guide is written for beginners. It starts with the hardware, then shows
how to prepare a Raspberry Pi with Raspberry Pi Imager on Windows or macOS,
and finally how to install and configure satpi.

Raspberry Pi recommends [Raspberry Pi Imager][rpi-imager] as the standard
way to write Raspberry Pi OS to a microSD card. The Imager can preconfigure
hostname, user account, Wi-Fi, and SSH for a headless setup
(see the [official getting-started guide][rpi-getting-started]).

## 1. What you need

You need:

- a Raspberry Pi 4 or Raspberry Pi 5 (the latter with a cooling fan)
- a suitable power supply for your Raspberry Pi
- a microSD card
- a computer with Windows or macOS (to set up the SD card initially)
- an internet connection
- an RTL-SDR compatible USB receiver
- an antenna suitable for weather satellite reception (best is a QFH antenna, but a V-dipole is fine for testing and you should still be able to receive signals)
- optionally an LNA and Bias-T if your antenna setup needs it; with the Raspberry Pi installed close to the antenna (in a waterproof box) you can keep coax cables short and may not need an LNA

## 2. Download Raspberry Pi Imager

On your Windows PC or Mac:

1. Open the [Raspberry Pi software page][rpi-software].
2. Download **Raspberry Pi Imager** for your system.
3. Install it.

Raspberry Pi describes Raspberry Pi Imager as the quick and easy way to
install Raspberry Pi OS to a microSD card.

## 3. Install Raspberry Pi OS Lite 64-bit on the microSD card

1. Insert the microSD card into your computer.
2. Start **Raspberry Pi Imager**.
3. Click **Choose Device** and select your Raspberry Pi model.
4. Click **Choose OS**.
5. Select **Raspberry Pi OS Lite (64-bit)**. This is a headless install, so the desktop GUI is not needed.
6. Click **Choose Storage** and select your microSD card.
7. Click **Next**.

## 4. Configure the image before writing it

When Raspberry Pi Imager asks whether you want to apply OS customisation
settings, choose **Edit Settings**.

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

Raspberry Pi Imager preconfigures the hostname, user account, network
connection, and SSH during imaging, which is ideal for a headless setup.

## 5. Write the card and boot the Raspberry Pi

1. Click **Write** in Raspberry Pi Imager.
2. Wait until writing has finished.
3. Remove the card from your computer.
4. Insert the card into the Raspberry Pi.
5. Connect:

    - power
    - network, or rely on Wi-Fi if configured
    - the RTL-SDR can be connected later — it is not required for the first boot

6. Wait 1 to 2 minutes for the first boot.

## 6. Connect to the Raspberry Pi with SSH

From your computer, open a terminal.

### On macOS

Use the **Terminal** app.

### On Windows

Use **PowerShell** or **PuTTY**.

Connect with:

```bash
ssh YOUR_USER@HOSTNAME.local
```

Replace `YOUR_USER` and `HOSTNAME` with the values you configured in
Raspberry Pi Imager.

## 7. Clone the satpi repository

On the Raspberry Pi, update the system and clone the repository:

```bash
sudo apt update
sudo apt install -y git
git clone https://github.com/HorvathAndreas/satpi.git
```

Or as a single copy-paste line:

```bash
sudo apt update && sudo apt install -y git && git clone https://github.com/HorvathAndreas/satpi.git
```

Then run the base installer:

```bash
cd ~/satpi
scripts/install_base.sh
```

## 8. Configure satpi

Copy the example configuration and edit it:

```bash
cd ~/satpi
cp config/config.example.ini config/config.ini
nano config/config.ini
```

Make all necessary changes in `config/config.ini`, then save and exit nano
(`Ctrl+O`, `Enter`, `Ctrl+X`).

Configure rclone for the cloud-storage upload:

```bash
rclone config
```

Send a test mail to confirm that msmtp is working
(replace `you@example.com` with your address):

```bash
printf "Subject: satpi test\n\nTest mail.\n" | /usr/bin/msmtp you@example.com
```

## 9. Run the initial pipeline scripts

```bash
python3 bin/test_config.py
python3 bin/update_tle.py
python3 bin/predict_passes.py
python3 bin/schedule_passes.py
python3 bin/generate_refresh_units.py
```

Or as a single copy-paste line:

```bash
python3 bin/test_config.py && \
python3 bin/update_tle.py && \
python3 bin/predict_passes.py && \
python3 bin/schedule_passes.py && \
python3 bin/generate_refresh_units.py
```

## 10. Verify scheduled timers

```bash
systemctl list-timers --all | grep satpi
```

You should see one timer per upcoming pass.

## 11. Wait for results

Wait, and you should see emails arriving in your mailbox with links to the
decoded weather pictures. Have fun!

---

[rpi-imager]: https://www.raspberrypi.com/software/
[rpi-getting-started]: https://www.raspberrypi.com/documentation/computers/getting-started.html
[rpi-software]: https://www.raspberrypi.com/software/operating-systems/
