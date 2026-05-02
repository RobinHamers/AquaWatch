#!/usr/bin/env python3
"""
Weekend 2: Build water quality time series from all processed scenes.

Discovers all scenes with a complete clipped band set (B08_clipped.tif present),
computes water quality indices, extracts spatial statistics, saves CSV, and plots.

Usage:
    conda activate aquawatch
    python scripts/build_timeseries.py
"""

import logging
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from timeseries import build_timeseries, plot_timeseries

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(name)-20s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("build_timeseries")

PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
OUTPUT_CSV = PROJECT_ROOT / "outputs" / "timeseries" / "serre_poncon_wqi.csv"
OUTPUT_PLOT = PROJECT_ROOT / "outputs" / "timeseries" / "serre_poncon_wqi.png"

NDCI_ALERT_LOW = 0.2
NDCI_ALERT_MEDIUM = 0.3
NDCI_ALERT_HIGH = 0.4


def main() -> None:
    df = build_timeseries(
        processed_dir=PROCESSED_DIR,
        output_path=OUTPUT_CSV,
    )

    if df.empty:
        logger.error("No data — run download_all.py first")
        sys.exit(1)

    plot_timeseries(df=df, output_path=OUTPUT_PLOT)

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\nTime series: {len(df)} scenes  ({df['date'].min().date()} → {df['date'].max().date()})\n")

    if "ndci_water_mean" in df.columns:
        alerts_low = (df["ndci_water_mean"] > NDCI_ALERT_LOW).sum()
        alerts_med = (df["ndci_water_mean"] > NDCI_ALERT_MEDIUM).sum()
        alerts_high = (df["ndci_water_mean"] > NDCI_ALERT_HIGH).sum()
        print(f"Scenes with NDCI mean above threshold:")
        print(f"  LOW  (>{NDCI_ALERT_LOW}): {alerts_low}")
        print(f"  MED  (>{NDCI_ALERT_MEDIUM}): {alerts_med}")
        print(f"  HIGH (>{NDCI_ALERT_HIGH}): {alerts_high}")

        print(f"\nTop 10 scenes by NDCI p90:\n")
        cols = ["date", "scene_id", "ndci_water_mean", "ndci_water_p90", "ndci_water_n"]
        available = [c for c in cols if c in df.columns]
        top10 = df.nlargest(10, "ndci_water_p90")[available].copy()
        top10["scene_id"] = top10["scene_id"].str[:40]
        print(top10.to_string(index=False))

    print(f"\nCSV  → {OUTPUT_CSV.relative_to(PROJECT_ROOT)}")
    print(f"Plot → {OUTPUT_PLOT.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
