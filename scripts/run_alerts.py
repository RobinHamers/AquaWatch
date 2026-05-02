#!/usr/bin/env python3
"""
Weekend 3: Run anomaly detection on the Serre-Ponçon time series.

Usage:
    conda activate aquawatch
    python scripts/run_alerts.py
"""

import logging
import sys
from datetime import date
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from alerts import (
    compute_rolling_baseline,
    detect_alerts,
    save_alerts,
    summarize_alerts,
    validate_against_known_events,
    check_new_scene,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(name)-20s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("run_alerts")

TIMESERIES_CSV = PROJECT_ROOT / "outputs" / "timeseries" / "serre_poncon_wqi.csv"
ALERTS_DIR = PROJECT_ROOT / "outputs" / "alerts"
RESERVOIR_NAME = "serre_poncon"

# Known cyanobacteria bloom periods from field reports
KNOWN_EVENTS = [
    (date(2023, 7, 1),  date(2023, 8, 31),  "Summer 2023 bloom (Jul–Aug)"),
    (date(2024, 6, 1),  date(2024, 8, 31),  "Summer 2024 bloom (Jun–Aug)"),
]


def main() -> None:
    if not TIMESERIES_CSV.exists():
        logger.error("Time series CSV not found: %s", TIMESERIES_CSV)
        logger.error("Run scripts/build_timeseries.py first.")
        sys.exit(1)

    # ── 1. Load ───────────────────────────────────────────────────────────────
    df = pd.read_csv(TIMESERIES_CSV, parse_dates=["date"])
    df = df.set_index("date").sort_index()
    logger.info("Loaded %d scenes (%s → %s)", len(df), df.index.min().date(), df.index.max().date())

    # ── 2. Rolling baseline ───────────────────────────────────────────────────
    df = compute_rolling_baseline(df, window_days=30, min_periods=2)
    logger.info("Computed rolling 30-day NDCI baseline")

    # ── 3. Detect alerts ──────────────────────────────────────────────────────
    alerts = detect_alerts(
        df,
        absolute_threshold_low=0.2,
        absolute_threshold_medium=0.3,
        absolute_threshold_high=0.4,
        z_score_threshold=1.5,
    )

    # ── 4. Save ───────────────────────────────────────────────────────────────
    csv_path, json_path = save_alerts(alerts, ALERTS_DIR, RESERVOIR_NAME)
    print(f"\nSaved → {csv_path.relative_to(PROJECT_ROOT)}")
    print(f"Saved → {json_path.relative_to(PROJECT_ROOT)}")

    # ── 5. Summary ────────────────────────────────────────────────────────────
    summarize_alerts(alerts)

    # ── 6. Validation against known bloom events ──────────────────────────────
    passed = validate_against_known_events(alerts, KNOWN_EVENTS)
    if not passed:
        print("⚠  One or more known bloom periods had no MEDIUM/HIGH alert.")
        print("   Possible causes:")
        print("   - Dataset starts 2023-08-12 (missing July 2023 baseline context)")
        print("   - NDWI water mask (> 0) may exclude bloom-affected surface pixels")
        print("   - Bloom may have peaked before or after acquisition dates")

    # ── 7. check_new_scene demo ───────────────────────────────────────────────
    print("\n── check_new_scene() demo (using last scene as synthetic input) ──")
    last_row = df.iloc[-1]
    synthetic = {
        "date": str(df.index[-1].date()),
        "ndci_water_mean": float(last_row["ndci_water_mean"]),
        "ndci_water_p90": float(last_row["ndci_water_p90"]),
        "turbidity_water_mean": float(last_row["turbidity_water_mean"]),
        "ndci_water_n": int(last_row["ndci_water_n"]),
    }
    # Exclude the last scene from historical context
    historical_for_check = df.iloc[:-1]
    result = check_new_scene(
        new_scene_stats=synthetic,
        historical_df=historical_for_check,
        reservoir_name=RESERVOIR_NAME,
        z_score_threshold=2.0,
    )
    if result:
        print(f"  → ALERT: {result.severity}  NDCI={result.ndci_mean:.4f}  z={result.z_score:.2f}")
        print(f"     {result.notes}")
    else:
        print(f"  → All clear for {synthetic['date']}")


if __name__ == "__main__":
    main()
