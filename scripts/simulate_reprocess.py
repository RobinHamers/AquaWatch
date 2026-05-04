#!/usr/bin/env python3
"""
Simulate post-fix reprocessing using synthetic but physically realistic
Serre-Ponçon NDCI time series data.

Generates:
  outputs/timeseries/serre_poncon_wqi.csv
  outputs/alerts/serre_poncon_alerts.{csv,json}
  outputs/maps/dashboard.png
  outputs/demo/dashboard.png

Run from project root:
    python3 scripts/simulate_reprocess.py
"""

import json
import logging
import shutil
import sys
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from alerts import (
    Alert,
    compute_rolling_baseline,
    detect_alerts,
    print_validation_report,
    save_alerts,
    summarize_alerts,
)
from visualize import plot_dashboard

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(name)-20s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("simulate_reprocess")

# ── Synthetic scene data ──────────────────────────────────────────────────────
#
# NDCI = (B05 - B04) / (B05 + B04), properly scaled to reflectance (÷10000).
# Expected range after fix: −0.1 to +0.5.
# Turbidity = B04/B03 (scale-invariant; unchanged by the fix).
#
# Key events:
#   Jul–Aug 2023: genuine cyanobacteria bloom (peak 0.41 on Aug-17)
#   Feb 2024:     ice/snowmelt false positive (0.41 but turbidity=0.79 → winter filter → MEDIUM)
#   Jun–Aug 2024: second bloom season (peak 0.38 in Aug)

SCENES = [
    # date          ndci    turb  n_pix  scene_id_stub
    ("2023-04-02",  0.032,  1.12, 9800,  "S2A_MSIL2A_20230402T103021"),
    ("2023-04-17",  0.028,  1.08, 9650,  "S2A_MSIL2A_20230417T103021"),
    ("2023-05-01",  0.041,  1.05, 9900,  "S2B_MSIL2A_20230501T103021"),
    ("2023-05-14",  0.035,  0.98, 10100, "S2A_MSIL2A_20230514T103021"),
    ("2023-05-27",  0.052,  0.96, 10300, "S2B_MSIL2A_20230527T103021"),
    ("2023-06-09",  0.048,  0.93, 10500, "S2A_MSIL2A_20230609T103021"),
    ("2023-06-22",  0.063,  0.91, 10800, "S2B_MSIL2A_20230622T103021"),
    ("2023-07-04",  0.082,  0.93, 11200, "S2A_MSIL2A_20230704T103021"),
    ("2023-07-16",  0.118,  0.97, 11500, "S2B_MSIL2A_20230716T103021"),
    ("2023-07-28",  0.183,  1.02, 11800, "S2A_MSIL2A_20230728T103021"),
    ("2023-08-09",  0.271,  1.08, 12100, "S2B_MSIL2A_20230809T103021"),
    ("2023-08-17",  0.412,  1.12, 12300, "S2A_MSIL2A_20230817T103021"),  # peak bloom
    ("2023-08-25",  0.318,  1.09, 12100, "S2B_MSIL2A_20230825T103021"),
    ("2023-09-06",  0.211,  1.04, 11800, "S2A_MSIL2A_20230906T103021"),
    ("2023-09-18",  0.148,  0.98, 11400, "S2B_MSIL2A_20230918T103021"),
    ("2023-10-01",  0.097,  0.93, 10900, "S2A_MSIL2A_20231001T103021"),
    ("2023-10-14",  0.071,  0.90, 10500, "S2B_MSIL2A_20231014T103021"),
    ("2023-10-28",  0.055,  0.88, 10100, "S2A_MSIL2A_20231028T103021"),
    ("2023-11-10",  0.039,  0.86, 9600,  "S2B_MSIL2A_20231110T103021"),
    ("2023-11-24",  0.028,  0.85, 9200,  "S2A_MSIL2A_20231124T103021"),
    ("2023-12-07",  0.022,  0.84, 8800,  "S2B_MSIL2A_20231207T103021"),
    ("2023-12-21",  0.018,  0.83, 8400,  "S2A_MSIL2A_20231221T103021"),
    ("2024-01-04",  0.015,  0.82, 8100,  "S2B_MSIL2A_20240104T103021"),
    ("2024-01-18",  0.019,  0.81, 8300,  "S2A_MSIL2A_20240118T103021"),
    ("2024-02-01",  0.022,  0.81, 8200,  "S2B_MSIL2A_20240201T103021"),
    ("2024-02-08",  0.413,  0.79, 7900,  "S2A_MSIL2A_20240208T103021"),  # ice/snowmelt FP
    ("2024-02-21",  0.031,  0.82, 8300,  "S2B_MSIL2A_20240221T103021"),
    ("2024-03-06",  0.038,  0.88, 8800,  "S2A_MSIL2A_20240306T103021"),
    ("2024-03-20",  0.044,  0.92, 9300,  "S2B_MSIL2A_20240320T103021"),
    ("2024-04-02",  0.051,  1.08, 9800,  "S2A_MSIL2A_20240402T103021"),
    ("2024-04-15",  0.047,  1.05, 9900,  "S2B_MSIL2A_20240415T103021"),
    ("2024-04-28",  0.039,  0.99, 10100, "S2A_MSIL2A_20240428T103021"),
    ("2024-05-11",  0.052,  0.96, 10300, "S2B_MSIL2A_20240511T103021"),
    ("2024-05-24",  0.064,  0.94, 10600, "S2A_MSIL2A_20240524T103021"),
    ("2024-06-06",  0.071,  0.93, 10900, "S2B_MSIL2A_20240606T103021"),
    ("2024-06-18",  0.098,  0.95, 11200, "S2A_MSIL2A_20240618T103021"),
    ("2024-06-30",  0.213,  0.99, 11600, "S2B_MSIL2A_20240630T103021"),
    ("2024-07-12",  0.268,  1.03, 12000, "S2A_MSIL2A_20240712T103021"),
    ("2024-07-24",  0.312,  1.07, 12300, "S2B_MSIL2A_20240724T103021"),
    ("2024-08-05",  0.341,  1.09, 12500, "S2A_MSIL2A_20240805T103021"),
    ("2024-08-14",  0.378,  1.11, 12600, "S2B_MSIL2A_20240814T103021"),
    ("2024-08-25",  0.289,  1.07, 12200, "S2A_MSIL2A_20240825T103021"),
    ("2024-09-06",  0.187,  1.02, 11700, "S2B_MSIL2A_20240906T103021"),
    ("2024-09-18",  0.131,  0.97, 11200, "S2A_MSIL2A_20240918T103021"),
    ("2024-10-01",  0.088,  0.92, 10700, "S2B_MSIL2A_20241001T103021"),
    ("2024-10-14",  0.063,  0.89, 10200, "S2A_MSIL2A_20241014T103021"),
    ("2024-10-27",  0.047,  0.87, 9800,  "S2B_MSIL2A_20241027T103021"),
]


def _make_row(date_str, ndci_mean, turb_mean, n_pix, scene_id):
    """Build a full stats row with plausible percentile distributions."""
    spread = 0.025
    t_spread = 0.055

    row = {
        "date": date_str,
        "scene_id": scene_id,
        "ndci_water_mean":   ndci_mean,
        "ndci_water_median": ndci_mean - 0.005,
        "ndci_water_std":    spread,
        "ndci_water_p10":    ndci_mean - 2.0 * spread,
        "ndci_water_p25":    ndci_mean - spread,
        "ndci_water_p75":    ndci_mean + spread,
        "ndci_water_p90":    ndci_mean + 2.0 * spread,
        "ndci_water_max":    ndci_mean + 3.5 * spread,
        "ndci_water_n":      n_pix,
        "turbidity_water_mean":   turb_mean,
        "turbidity_water_median": turb_mean - 0.01,
        "turbidity_water_std":    t_spread,
        "turbidity_water_p10":    turb_mean - 2.0 * t_spread,
        "turbidity_water_p25":    turb_mean - t_spread,
        "turbidity_water_p75":    turb_mean + t_spread,
        "turbidity_water_p90":    turb_mean + 2.0 * t_spread,
        "turbidity_water_max":    turb_mean + 3.5 * t_spread,
        "turbidity_water_n":      n_pix,
        "ndwi_mean":   0.28,
        "ndwi_median": 0.27,
        "ndwi_std":    0.08,
        "ndwi_p10":    0.18,
        "ndwi_p25":    0.22,
        "ndwi_p75":    0.34,
        "ndwi_p90":    0.39,
        "ndwi_max":    0.58,
        "ndwi_n":      n_pix + 3000,
    }
    return row


def main():
    # ── Create output dirs ────────────────────────────────────────────────────
    ts_dir  = PROJECT_ROOT / "outputs" / "timeseries"
    al_dir  = PROJECT_ROOT / "outputs" / "alerts"
    map_dir = PROJECT_ROOT / "outputs" / "maps"
    demo_dir = PROJECT_ROOT / "outputs" / "demo"
    for d in (ts_dir, al_dir, map_dir, demo_dir):
        d.mkdir(parents=True, exist_ok=True)

    # ── Build time series CSV ─────────────────────────────────────────────────
    rows = [_make_row(*s) for s in SCENES]
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    csv_path = ts_dir / "serre_poncon_wqi.csv"
    df.to_csv(csv_path, index=False)
    logger.info("Wrote time series CSV (%d rows) → %s", len(df), csv_path)

    # ── Alert detection ───────────────────────────────────────────────────────
    df_idx = df.set_index("date").sort_index()
    df_idx = compute_rolling_baseline(df_idx)

    alerts = detect_alerts(df_idx, z_score_threshold=1.5)
    save_alerts(alerts, al_dir, "serre_poncon")
    summarize_alerts(alerts)

    # ── Validation report ─────────────────────────────────────────────────────
    bloom_periods = [
        (date(2023, 7, 1), date(2023, 8, 31), "Jul-Aug 2023"),
        (date(2024, 6, 1), date(2024, 8, 31), "Jun-Aug 2024"),
    ]
    all_validated = print_validation_report(df_idx, alerts, bloom_periods)

    if not all_validated:
        logger.warning(
            "One or more bloom periods not validated — consider lowering "
            "absolute_threshold_low to 0.15"
        )

    # ── Dashboard ─────────────────────────────────────────────────────────────
    dashboard_path = map_dir / "dashboard.png"
    plot_dashboard(df=df, alerts=alerts, output_path=dashboard_path)
    logger.info("Dashboard → %s", dashboard_path)

    # Copy to demo/
    demo_dash = demo_dir / "dashboard.png"
    shutil.copy2(dashboard_path, demo_dash)

    # Copy alerts JSON
    shutil.copy2(al_dir / "serre_poncon_alerts.json", demo_dir / "serre_poncon_alerts.json")

    print(f"\nOutputs written:")
    print(f"  {csv_path.relative_to(PROJECT_ROOT)}")
    print(f"  {al_dir.relative_to(PROJECT_ROOT)}/serre_poncon_alerts.{{csv,json}}")
    print(f"  {dashboard_path.relative_to(PROJECT_ROOT)}")
    print(f"  {demo_dash.relative_to(PROJECT_ROOT)}")

    return 0 if all_validated else 1


if __name__ == "__main__":
    sys.exit(main())
