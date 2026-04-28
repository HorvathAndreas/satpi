#!/usr/bin/env python3
"""
broadband_spectrum.py — Plot a broadband spectrum from rtl_power CSV output.

Usage:
    python3 broadband_spectrum.py broadband.csv [options]

Options:
    -o, --output FILE       Save plot to FILE (default: broadband_spectrum.png)
    --title TEXT            Custom title (default: auto-generated from CSV timestamp)
    --fmin MHZ              Minimum frequency to display (default: auto)
    --fmax MHZ              Maximum frequency to display (default: auto)
    --ymin DBM              Y-axis minimum in dBm (default: auto)
    --ymax DBM              Y-axis maximum in dBm (default: auto)
    --smooth N              Moving-average window size (default: 3)
    --show                  Display plot interactively (requires display)
    -h, --help              Show this help message
"""

import argparse
import csv
import sys
import numpy as np
import matplotlib
import matplotlib.patches as mpatches

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


def parse_args():
    p = argparse.ArgumentParser(
        description='Plot broadband spectrum from rtl_power CSV.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument('csv', help='Input CSV file (rtl_power format)')
    p.add_argument('-o', '--output', default=None,
                   help='Output image file (default: <csv_stem>_spectrum.png)')
    p.add_argument('--title', default=None, help='Custom plot title')
    p.add_argument('--fmin', type=float, default=None, help='Min frequency (MHz)')
    p.add_argument('--fmax', type=float, default=None, help='Max frequency (MHz)')
    p.add_argument('--ymin', type=float, default=None, help='Y-axis min (dBm)')
    p.add_argument('--ymax', type=float, default=None, help='Y-axis max (dBm)')
    p.add_argument('--smooth', type=int, default=3, help='Smoothing window (default: 3)')
    p.add_argument('--show', action='store_true', help='Show interactive plot')
    return p.parse_args()


def load_csv(path):
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


def smooth(arr, window):
    if window < 2:
        return arr
    return np.convolve(arr, np.ones(window) / window, mode='same')


def plot_spectrum(freqs, powers, args, timestamp):
    import matplotlib.pyplot as plt

    powers_smooth = smooth(powers, args.smooth)

    fmin = args.fmin if args.fmin is not None else freqs.min()
    fmax = args.fmax if args.fmax is not None else freqs.max()
    mask = (freqs >= fmin) & (freqs <= fmax)

    p_visible = powers_smooth[mask]
    p_margin = (p_visible.max() - p_visible.min()) * 0.1 if p_visible.size else 5
    ymin = args.ymin if args.ymin is not None else p_visible.min() - p_margin - 5
    ymax = args.ymax if args.ymax is not None else p_visible.max() + p_margin + 3

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
    # Smoothed trace
    ax.plot(freqs[mask], powers_smooth[mask], color='#00d4ff', linewidth=1.2, zorder=2)

    # FM fill
    fm = mask & (freqs >= 88) & (freqs <= 108)
    if fm.any():
        ax.fill_between(freqs[fm], ymin, powers_smooth[fm], color='#ff4444', alpha=0.3)

    # METEOR fill
    meteor = mask & (freqs >= 137) & (freqs <= 138)
    if meteor.any():
        ax.fill_between(freqs[meteor], ymin, powers_smooth[meteor], color='#44ff88', alpha=0.4)

    # Frequency markers
    for f_mark, label in FREQ_MARKERS:
        if not (fmin <= f_mark <= fmax):
            continue
        idx_m = np.argmin(np.abs(freqs - f_mark))
        p_at = powers_smooth[idx_m]
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

    if args.show:
        matplotlib.use('TkAgg')
    else:
        matplotlib.use('Agg')

    import matplotlib.pyplot as plt

    print(f"Loading {args.csv} …")
    freqs, powers, timestamp = load_csv(args.csv)
    print(f"  {len(freqs)} data points, {freqs.min():.1f}–{freqs.max():.1f} MHz, timestamp: {timestamp}")

    fig = plot_spectrum(freqs, powers, args, timestamp)

    if args.output:
        out = args.output
    else:
        import os
        stem = os.path.splitext(os.path.basename(args.csv))[0]
        out = f"{stem}_spectrum.png"

    fig.savefig(out, dpi=150, bbox_inches='tight', facecolor='#1a1a2e')
    print(f"Saved → {out}")

    if args.show:
        plt.show()


if __name__ == '__main__':
    main()
