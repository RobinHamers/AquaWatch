"""Sentinel-2 / Sentinel-3 OLCI fusion analysis.

Architecture
------------
S3 (daily, 300 m) acts as a temporal tripwire: its dense cadence can detect
bloom onset days before the next S2 overpass. S2 (5-day, 10 m) then provides
spatial confirmation at 10× higher resolution.

Key outputs
-----------
- Fused timeseries CSV (date, s2_ndci, s3_ndci, s2_turbidity, s2_available, s3_available)
- PrecursorEvent per known bloom period (how many days earlier did S3 fire?)
- Printed fusion report with agreement rate and per-bloom precursor advantage
"""

import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

NDCI_LOW    = 0.2
NDCI_MEDIUM = 0.3
NDCI_HIGH   = 0.4


@dataclass
class PrecursorEvent:
    bloom_label: str
    s3_first_date: date | None   # first date S3 NDCI ≥ threshold in this period
    s2_first_date: date | None   # first date S2 NDCI ≥ threshold in this period
    precursor_days: int | None   # s2_first − s3_first (positive = S3 was earlier)
    s3_peak_ndci: float
    s2_peak_ndci: float


def build_fused_timeseries(
    s2_csv: Path,
    s3_csv: Path,
    output_csv: Path,
) -> pd.DataFrame:
    """Merge S2 and S3 timeseries on date.

    S3 has daily cadence; S2 has ~5-day cadence. The merged frame contains a
    row for every date that appears in either source.

    Parameters
    ----------
    s2_csv : S2 timeseries CSV (serre_poncon_wqi.csv)
    s3_csv : S3 timeseries CSV (serre_poncon_s3_wqi.csv)
    output_csv : destination for the merged CSV

    Returns
    -------
    DataFrame with columns: date, s2_ndci, s3_ndci, s2_turbidity,
    s2_available (bool), s3_available (bool).
    """
    s2 = pd.read_csv(s2_csv, parse_dates=["date"])
    s3 = pd.read_csv(s3_csv, parse_dates=["date"])

    s2_cols = s2[["date", "ndci_water_mean", "turbidity_water_mean"]].rename(columns={
        "ndci_water_mean": "s2_ndci",
        "turbidity_water_mean": "s2_turbidity",
    })
    s3_cols = s3[["date", "ndci_water_mean"]].rename(columns={"ndci_water_mean": "s3_ndci"})

    df = (
        pd.merge(s3_cols, s2_cols, on="date", how="outer")
        .sort_values("date")
        .reset_index(drop=True)
    )
    df["s2_available"] = df["s2_ndci"].notna()
    df["s3_available"] = df["s3_ndci"].notna()

    output_csv = Path(output_csv)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False)
    logger.info("Saved fused timeseries (%d rows) → %s", len(df), output_csv)
    return df


def detect_s3_precursor_alerts(
    fused_df: pd.DataFrame,
    bloom_periods: list[tuple[date, date, str]],
    ndci_threshold: float = NDCI_LOW,
) -> list[PrecursorEvent]:
    """For each known bloom period, find when S3 vs S2 first crossed ndci_threshold.

    Parameters
    ----------
    fused_df : output of build_fused_timeseries()
    bloom_periods : list of (start_date, end_date, label)
    ndci_threshold : bloom onset NDCI value (default NDCI_LOW = 0.2)

    Returns
    -------
    List of PrecursorEvent, one per bloom period.
    """
    df = fused_df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date

    events: list[PrecursorEvent] = []
    for start, end, label in bloom_periods:
        mask   = (df["date"] >= start) & (df["date"] <= end)
        period = df[mask]

        if period.empty:
            events.append(PrecursorEvent(
                bloom_label=label,
                s3_first_date=None, s2_first_date=None,
                precursor_days=None, s3_peak_ndci=np.nan, s2_peak_ndci=np.nan,
            ))
            continue

        s3_above = period[period["s3_ndci"] >= ndci_threshold]
        s2_above = period[period["s2_ndci"] >= ndci_threshold]

        s3_first = s3_above["date"].min() if not s3_above.empty else None
        s2_first = s2_above["date"].min() if not s2_above.empty else None

        if s3_first is not None and s2_first is not None:
            precursor_days = (s2_first - s3_first).days
        else:
            precursor_days = None

        events.append(PrecursorEvent(
            bloom_label=label,
            s3_first_date=s3_first,
            s2_first_date=s2_first,
            precursor_days=precursor_days,
            s3_peak_ndci=float(period["s3_ndci"].max()) if period["s3_ndci"].notna().any() else np.nan,
            s2_peak_ndci=float(period["s2_ndci"].max()) if period["s2_ndci"].notna().any() else np.nan,
        ))

    return events


def print_fusion_report(
    fused_df: pd.DataFrame,
    events: list[PrecursorEvent],
    s3_scene_count: int,
    s2_scene_count: int,
) -> None:
    """Print a formatted S2/S3 fusion analysis report to stdout.

    Parameters
    ----------
    fused_df : merged timeseries from build_fused_timeseries()
    events : list of PrecursorEvent from detect_s3_precursor_alerts()
    s3_scene_count : number of S3 scenes (clear-sky days)
    s2_scene_count : number of S2 scenes
    """
    df = fused_df.copy()
    df["date"] = pd.to_datetime(df["date"])

    print("\n" + "═" * 64)
    print("  AquaWatch — S2 × S3 Fusion Report  |  Lac de Serre-Ponçon")
    print("═" * 64)
    print(f"  Sentinel-2 : {s2_scene_count} scenes  (~5-day revisit, 10 m)")
    print(f"  Sentinel-3 : {s3_scene_count} scenes  (daily revisit, 300 m)")
    print(f"  Period     : {df['date'].min().date()} → {df['date'].max().date()}")

    both = df[df["s2_available"] & df["s3_available"]]
    if not both.empty:
        diff = (both["s3_ndci"] - both["s2_ndci"]).abs()
        agree_pct = (diff < 0.05).sum() / len(both) * 100
        print(
            f"\n  Co-observations (both sensors same day) : {len(both)}\n"
            f"  Agreement (|ΔN| < 0.05)                : {agree_pct:.0f}%\n"
            f"  Mean |S3 − S2| NDCI offset              : {diff.mean():.4f}"
        )

    print("\n" + "─" * 64)
    print("  Bloom period precursor analysis  (threshold NDCI ≥ 0.20)")
    print("─" * 64)

    for ev in events:
        print(f"\n  {ev.bloom_label}")
        s2_pk = f"{ev.s2_peak_ndci:.4f}" if not np.isnan(ev.s2_peak_ndci) else "n/a"
        s3_pk = f"{ev.s3_peak_ndci:.4f}" if not np.isnan(ev.s3_peak_ndci) else "n/a"
        print(f"    S2 peak NDCI   : {s2_pk}")
        print(f"    S3 peak NDCI   : {s3_pk}")
        print(f"    S2 first alert : {ev.s2_first_date or 'none'}")
        print(f"    S3 first alert : {ev.s3_first_date or 'none'}")
        if ev.precursor_days is not None:
            if ev.precursor_days > 0:
                print(f"    Precursor adv. : ✅  S3 was {ev.precursor_days} days earlier than S2")
            elif ev.precursor_days < 0:
                print(f"    Precursor adv. : S2 was {-ev.precursor_days} days earlier than S3")
            else:
                print("    Precursor adv. : both sensors detected on the same date")
        else:
            print("    Precursor adv. : insufficient data")

    pos_adv = [ev.precursor_days for ev in events if ev.precursor_days is not None and ev.precursor_days > 0]
    if pos_adv:
        print(f"\n  Average S3 precursor advantage : {np.mean(pos_adv):.1f} days")
        print(
            f"  Operational use: issue early-warning via S3 ~{int(np.mean(pos_adv))} days "
            "before S2 spatial confirmation"
        )

    print("\n" + "═" * 64 + "\n")
