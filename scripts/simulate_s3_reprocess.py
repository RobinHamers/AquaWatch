#!/usr/bin/env python3
"""
Simulate S2 + S3 fusion using synthetic but physically realistic data.

S3 OLCI observes Serre-Ponçon every 1–2 days (daily revisit, ~35% cloud loss).
S2 observes every ~5 days. At 300m resolution S3 NDCI values are ~10–20% lower
than S2 values (spatial averaging, mixed pixels at reservoir edges), but S3's
daily cadence means it crosses the LOW threshold several days before the next
S2 overpass arrives.

Generates:
  outputs/timeseries/serre_poncon_s3_wqi.csv
  outputs/timeseries/serre_poncon_fused.csv
  outputs/maps/dashboard_fused.png
  outputs/demo/dashboard_fused.png

Run from project root:
    python3 scripts/simulate_s3_reprocess.py
"""

import logging
import shutil
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from alerts import (
    compute_rolling_baseline,
    detect_alerts,
    apply_seasonal_filter,
    flag_isolated_spikes,
)
from fusion import build_fused_timeseries, detect_s3_precursor_alerts, print_fusion_report
from visualize import plot_fused_dashboard

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(name)-20s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("simulate_s3")

# ── Synthetic S3 signal model ─────────────────────────────────────────────────
#
# S3 NDCI is derived from S2 as the physical ground truth:
#   1. Linearly interpolate S2 NDCI to daily resolution
#   2. Scale by 0.85 — S3 300m pixels average out bloom with adjacent
#      non-bloom areas (shoreline mixing), reducing peak NDCI by ~15%
#   3. Add N(0, 0.012) noise — atmospheric correction + sensor differences
#   4. Drop ~35% of scenes as cloud (higher in winter for Alpine terrain)
#
# Precursor advantage arises naturally: S3's daily cadence crosses the 0.20
# LOW threshold on the exact day the true NDCI (interpolated) passes 0.20/0.85
# ≈ 0.235. The next S2 scene arrives up to 5 days later, so S3 fires first.

RNG = np.random.default_rng(42)


def _make_s3_scenes(s2_csv: Path) -> pd.DataFrame:
    """Generate synthetic daily S3 scenes derived from S2 as physical ground truth.

    S3 NDCI ≈ 0.85 × (S2 NDCI interpolated to daily) + N(0, 0.012).
    """
    s2 = pd.read_csv(s2_csv, parse_dates=["date"]).sort_values("date")
    s2_series = s2.set_index("date")["ndci_water_mean"]

    # Linear interpolation to daily resolution over the full S2 date range
    daily_idx = pd.date_range(s2_series.index.min(), s2_series.index.max(), freq="D")
    s2_daily  = s2_series.reindex(daily_idx).interpolate("linear")

    rows = []
    for ts, ndci_s2 in s2_daily.items():
        d = ts.date()
        month = d.month
        cloud_prob = 0.55 if month in (11, 12, 1, 2, 3) else 0.28
        if RNG.random() < cloud_prob:
            continue

        ndci_s3 = max(-0.05, float(ndci_s2) * 0.85 + float(RNG.normal(0, 0.012)))
        spread   = 0.016
        rows.append({
            "date":              d.isoformat(),
            "scene_id":          f"S3A_OL_2_WFR___{d.strftime('%Y%m%d')}T102030",
            "ndci_water_mean":   ndci_s3,
            "ndci_water_median": ndci_s3 - 0.003,
            "ndci_water_std":    spread,
            "ndci_water_p10":    ndci_s3 - 2.0 * spread,
            "ndci_water_p90":    ndci_s3 + 2.0 * spread,
            "ndci_water_n":      max(40, int(RNG.normal(220, 30))),
        })

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date").reset_index(drop=True)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    ts_dir   = PROJECT_ROOT / "outputs" / "timeseries"
    al_dir   = PROJECT_ROOT / "outputs" / "alerts"
    map_dir  = PROJECT_ROOT / "outputs" / "maps"
    demo_dir = PROJECT_ROOT / "outputs" / "demo"
    for d in (ts_dir, al_dir, map_dir, demo_dir):
        d.mkdir(parents=True, exist_ok=True)

    # ── Load existing S2 timeseries ───────────────────────────────────────────
    s2_csv = ts_dir / "serre_poncon_wqi.csv"
    if not s2_csv.exists():
        logger.error(
            "S2 timeseries not found: %s\n"
            "Run scripts/simulate_reprocess.py first.", s2_csv
        )
        return 1
    s2_df = pd.read_csv(s2_csv, parse_dates=["date"])

    # ── S3 timeseries ─────────────────────────────────────────────────────────
    s3_df = _make_s3_scenes(s2_csv)
    s3_csv = ts_dir / "serre_poncon_s3_wqi.csv"
    s3_df.to_csv(s3_csv, index=False)
    logger.info("S3 timeseries: %d scenes → %s", len(s3_df), s3_csv)

    # ── Build fused timeseries ────────────────────────────────────────────────
    fused_csv = ts_dir / "serre_poncon_fused.csv"
    fused_df = build_fused_timeseries(s2_csv, s3_csv, fused_csv)
    logger.info("Fused timeseries: %d rows", len(fused_df))

    # ── S2 alerts (reload from existing alert pipeline) ───────────────────────
    s2_df_idx = s2_df.set_index("date").sort_index()
    s2_df_idx = compute_rolling_baseline(s2_df_idx)
    alerts = detect_alerts(s2_df_idx, z_score_threshold=1.5)
    alerts = flag_isolated_spikes(alerts)
    alerts = apply_seasonal_filter(alerts)

    # ── Precursor analysis ────────────────────────────────────────────────────
    bloom_periods = [
        (date(2023, 7, 1), date(2023, 8, 31), "Jul–Aug 2023"),
        (date(2024, 6, 1), date(2024, 8, 31), "Jun–Aug 2024"),
    ]
    events = detect_s3_precursor_alerts(fused_df, bloom_periods, ndci_threshold=0.20)
    print_fusion_report(fused_df, events, len(s3_df), len(s2_df))

    # ── Fused dashboard ───────────────────────────────────────────────────────
    dash_path = map_dir / "dashboard_fused.png"
    plot_fused_dashboard(
        s2_df=s2_df,
        s3_df=s3_df,
        alerts=alerts,
        precursor_events=events,
        output_path=dash_path,
    )
    shutil.copy2(dash_path, demo_dir / "dashboard_fused.png")

    print(f"\nOutputs written:")
    print(f"  {s3_csv.relative_to(PROJECT_ROOT)}")
    print(f"  {fused_csv.relative_to(PROJECT_ROOT)}")
    print(f"  {dash_path.relative_to(PROJECT_ROOT)}")
    print(f"  {(demo_dir / 'dashboard_fused.png').relative_to(PROJECT_ROOT)}")

    # Both bloom periods detected by S3 is success; precursor advantage may vary by bloom
    both_detected = all(ev.s3_first_date is not None for ev in events)
    if not both_detected:
        logger.warning("One or more bloom periods not detected by S3.")
    return 0 if both_detected else 1


if __name__ == "__main__":
    sys.exit(main())
