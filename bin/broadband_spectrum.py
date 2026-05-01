#!/usr/bin/env python3
"""
broadband_spectrum.py — Record and plot a broadband RF spectrum using rtl_power.

Records a wideband spectrum scan using rtl-sdr hardware, then generates
a matplotlib visualization with band annotations and frequency markers.

Usage:
    python3 broadband_spectrum.py --fmin 80MHz --fmax 200MHz [options]

Examples:
    # Record 80-200 MHz for 5 minutes
    python3 broadband_spectrum.py --fmin 80MHz --fmax 200MHz --duration 300

    # Record METEOR band (137-138 MHz) for 2 minutes, custom output
    python3 broadband_spectrum.py --fmin 137MHz --fmax 139MHz --duration 120 -o meteor.png

    # Custom gain and title
    python3 broadband_spectrum.py --fmin 80MHz --fmax 200MHz --gain 40 --title "VHF Spectrum"

    # Mixed frequency units
    python3 broadband_spectrum.py --fmin 80000kHz --fmax 0.2GHz --duration 180

Options:
    --fmin FREQ         Start frequency (e.g., 80MHz, 80000kHz, 0.08GHz) [required]
    --fmax FREQ         End frequency (e.g., 200MHz, 200000kHz) [required]
    --gain GAIN         RTL-SDR gain in dB (default: 38.6)
    --duration SEC      Recording duration in seconds (default: 300)
    -o, --output FILE   Output PNG file (default: broadband_spectrum.png)
    --title TEXT        Custom plot title (default: auto-generated)
    -h, --help          Show this help message
"""

import argparse
import csv
import subprocess
import sys
import tempfile
import os
import numpy as np
import matplotlib
import matplotlib.patches as mpatches
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "lib"))
from read_config import read_config, ConfigError

BAND_ANNOTATIONS = [
    (80,   88,  '#2d4a1e', 'VHF Low'),
    (88,  108,  '#5c1a1a', 'FM Radio'),
    (108, 137,  '#1a3a5c', 'Aviation'),
    (137, 138,  '#2d5c1a', 'METEOR\n137–138'),
    (138, 144,  '#1a2d5c', 'VHF'),
    (144, 146,  '#3a1a5c', 'Amateur\n2m'),
    (146, 300,  '#1a2d3a', 'VHF/UHF'),
]

FREQ_MARKERS = [
    (100.0, 'FM peak'),
    (137.9, 'METEOR\n137.9 MHz'),
    (162.0, 'NOAA WX'),
]


def parse_frequency(freq_str: str) -> float:
    """Parse frequency string with units (Hz, kHz, MHz, GHz) into MHz.

    Examples:
        "137.9 MHz" → 137.9
        "137900 kHz" → 137.9
        "137900000 Hz" → 137.9
        "0.1379 GHz" → 137.9
        "80" → 80 (assumes MHz if no unit)

    Returns:
        Frequency in MHz (float)

    Raises:
        ValueError: If the frequency string cannot be parsed
    """
    freq_str = freq_str.strip().upper().replace(' ', '')

    # Try to extract value and unit
    for unit in ['GHZ', 'MHZ', 'KHZ', 'HZ']:
        if freq_str.endswith(unit):
            try:
                value = float(freq_str[:-len(unit)])
                if unit == 'GHZ':
                    return value * 1000.0
                elif unit == 'MHZ':
                    return value
                elif unit == 'KHZ':
                    return value / 1000.0
                elif unit == 'HZ':
                    return value / 1e6
            except ValueError:
                raise ValueError(f"Invalid frequency format: {freq_str}")

    # No unit found — assume MHz
    try:
        return float(freq_str)
    except ValueError:
        raise ValueError(f"Invalid frequency format: {freq_str}")


def parse_args():
    p = argparse.ArgumentParser(
        prog='broadband_spectrum.py',
        description='Record and plot a broadband RF spectrum using rtl_power.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument('--fmin', type=parse_frequency, required=True,
                   help='Start frequency (e.g., 80MHz, 80000kHz, 0.08GHz)')
    p.add_argument('--fmax', type=parse_frequency, required=True,
                   help='End frequency (e.g., 200MHz)')
    p.add_argument('--gain', type=float, default=38.6,
                   help='RTL-SDR gain in dB (default: 38.6)')
    p.add_argument('--duration', type=int, default=300, metavar='SECONDS',
                   help='Recording duration in seconds (default: 300 = 5 minutes)')
    p.add_argument('-o', '--output', default='broadband_spectrum.png',
                   help='Output PNG file (default: broadband_spectrum.png)')
    p.add_argument('--title', default=None,
                   help='Custom plot title (default: auto-generated)')
    return p.parse_args()


def record_spectrum(fmin_mhz: float, fmax_mhz: float, gain: float, duration: int, csv_path: str) -> bool:
    """Record broadband spectrum using rtl_power."""
    fmin_hz = int(fmin_mhz * 1e6)
    fmax_hz = int(fmax_mhz * 1e6)
    bin_width_hz = 10000  # 10 kHz bins

    freq_range = f"{fmin_hz}:{fmax_hz}:{bin_width_hz}"

    print(f"Recording {fmin_mhz:.1f}–{fmax_mhz:.1f} MHz for {duration} seconds ({duration//60}m {duration%60}s)...")
    print(f"  Frequency range: {freq_range}")
    print(f"  Gain: {gain} dB")

    try:
        cmd = [
            'timeout', str(duration),
            'rtl_power',
            '-f', freq_range,
            '-g', str(gain),
            '-i', '1',
            csv_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        # Exit code 124 means timeout terminated the process — that's expected
        if result.returncode not in [0, 124]:
            print(f"[ERROR] rtl_power failed: exit code {result.returncode}", file=sys.stderr)
            if result.stderr:
                print(f"stderr: {result.stderr}", file=sys.stderr)
            return False
        print(f"Recording complete: {csv_path}")
        return True
    except FileNotFoundError:
        print(f"[ERROR] rtl_power not found. Install with: sudo apt install rtl-sdr", file=sys.stderr)
        return False


def load_csv(path: str):
    """Parse rtl_power CSV into arrays of (freq_mhz, power_dbm)."""
    freqs, powers = [], []
    timestamp = None
    with open(path, errors='replace') as f:
        for row in csv.reader(f):
            row = [x.strip() for x in row]
            if len(row) < 7:
                continue
            try:
                if timestamp is None:
                    timestamp = f"{row[0].strip()} {row[1].strip()}"
                freq_low  = float(row[2])
                freq_high = float(row[3])
                pwr_values = [float(x) for x in row[6:]]
            except ValueError:
                continue
            # Skip edge bins (rtl_power artefacts)
            pwr_values = pwr_values[1:-1]
            n = len(pwr_values)
            if n == 0:
                continue
            for i, p in enumerate(pwr_values):
                f_hz = freq_low + (i + 0.5) * (freq_high - freq_low) / n
                freqs.append(f_hz / 1e6)
                powers.append(p)
    freqs = np.array(freqs)
    powers = np.array(powers)
    idx = np.argsort(freqs)
    return freqs[idx], powers[idx], timestamp


def plot_spectrum(freqs, powers, args, timestamp):
    import matplotlib.pyplot as plt

    fmin = args.fmin
    fmax = args.fmax
    mask = (freqs >= fmin) & (freqs <= fmax)

    p_visible = powers[mask]
    p_margin = (p_visible.max() - p_visible.min()) * 0.1 if p_visible.size else 5
    ymin = p_visible.min() - p_margin - 5
    ymax = p_visible.max() + p_margin + 3

    title = args.title or f'Broadband Spectrum {fmin:.0f}–{fmax:.0f} MHz\n{timestamp} UTC'

    fig, ax = plt.subplots(figsize=(16, 7))
    fig.patch.set_facecolor('#1a1a2e')
    ax.set_facecolor('#16213e')

    # Band shading
    for b_start, b_end, color, label in BAND_ANNOTATIONS:
        if b_end < fmin or b_start > fmax:
            continue
        ax.axvspan(max(b_start, fmin), min(b_end, fmax), alpha=0.3, color=color, zorder=0)
        mid = (max(b_start, fmin) + min(b_end, fmax)) / 2
        ax.text(mid, ymin + 0.5, label, color='#aaaaaa', fontsize=7,
                ha='center', va='bottom',
                rotation=0 if (b_end - b_start) > 10 else 90)

    # Raw trace
    ax.plot(freqs[mask], powers[mask], color='#334466', linewidth=0.4, alpha=0.5, zorder=1)

    # FM fill
    fm = mask & (freqs >= 88) & (freqs <= 108)
    if fm.any():
        ax.fill_between(freqs[fm], ymin, powers[fm], color='#ff4444', alpha=0.3)

    # METEOR fill
    meteor = mask & (freqs >= 137) & (freqs <= 138)
    if meteor.any():
        ax.fill_between(freqs[meteor], ymin, powers[meteor], color='#44ff88', alpha=0.4)

    # Frequency markers
    for f_mark, label in FREQ_MARKERS:
        if not (fmin <= f_mark <= fmax):
            continue
        idx_m = np.argmin(np.abs(freqs - f_mark))
        p_at = powers[idx_m]
        ax.annotate(label,
                    xy=(f_mark, p_at),
                    xytext=(f_mark + (fmax - fmin) * 0.03, p_at + (ymax - ymin) * 0.07),
                    color='#ffcc44', fontsize=8,
                    arrowprops=dict(arrowstyle='->', color='#ffcc44', lw=0.8))

    ax.set_xlabel('Frequency (MHz)', color='#cccccc', fontsize=12)
    ax.set_ylabel('Power (dBm)', color='#cccccc', fontsize=12)
    ax.set_title(title, color='#ffffff', fontsize=13, pad=10)
    ax.set_xlim(fmin, fmax)
    ax.set_ylim(ymin, ymax)
    ax.tick_params(colors='#aaaaaa')
    for spine in ('bottom', 'left'):
        ax.spines[spine].set_color('#445566')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.grid(True, color='#223344', linewidth=0.5, alpha=0.7)

    legend_elements = [
        mpatches.Patch(color='#ff4444', alpha=0.5, label='FM Radio (88–108 MHz, saturated)'),
        mpatches.Patch(color='#44ff88', alpha=0.6, label='METEOR band (137–138 MHz)'),
        mpatches.Patch(color='#1a3a5c', alpha=0.8, label='Aviation (108–137 MHz)'),
    ]
    ax.legend(handles=legend_elements, loc='upper right',
              facecolor='#0d1b2a', edgecolor='#445566',
              labelcolor='#cccccc', fontsize=9)

    plt.tight_layout()
    return fig

def main():
    args = parse_args()

    # Construct config path
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(base_dir, "config", "config.ini")

    # Load config
    try:
        config = read_config(config_path)
    except ConfigError as e:
        print(f"[ERROR] Config loading failed: {e}", file=sys.stderr)
        return 1

    # Determine output path
    if args.output == 'broadband_spectrum.png':
        output_dir = config["paths"]["output_dir"]
        output_path = os.path.join(output_dir, args.output)
        os.makedirs(output_dir, exist_ok=True)
    else:
        output_path = args.output
        parent_dir = os.path.dirname(output_path)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)

    args.output = output_path

    # Use non-interactive backend for batch processing
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    # Create temporary CSV file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp:
        csv_path = tmp.name

    try:
        # Record spectrum
        if not record_spectrum(args.fmin, args.fmax, args.gain, args.duration, csv_path):
            return 1

        # Load and plot
        print(f"Loading {csv_path}...")
        freqs, powers, timestamp = load_csv(csv_path)
        print(f"  {len(freqs)} data points, {freqs.min():.1f}–{freqs.max():.1f} MHz")

        fig = plot_spectrum(freqs, powers, args, timestamp)

        # Save plot
        fig.savefig(args.output, dpi=150, bbox_inches='tight', facecolor='#1a1a2e')
        print(f"Saved → {args.output}")

        return 0

    finally:
        # Clean up temporary CSV
        if os.path.exists(csv_path):
            os.unlink(csv_path)

if __name__ == '__main__':
    sys.exit(main())

