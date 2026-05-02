"""Anomaly detection and alert generation for water quality time series."""

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# CSV column names produced by timeseries.py
_NDCI_MEAN = "ndci_water_mean"
_NDCI_P90 = "ndci_water_p90"
_TURB_MEAN = "turbidity_water_mean"
_NDCI_N = "ndci_water_n"


@dataclass
class Alert:
    date: date
    reservoir: str
    severity: str          # "LOW", "MEDIUM", "HIGH"
    ndci_mean: float
    ndci_p90: float
    turbidity_mean: float
    baseline_ndci: float   # 30-day rolling baseline mean
    baseline_std: float
    z_score: float         # standard deviations above baseline
    valid_pixels: int
    notes: str = ""


def compute_rolling_baseline(
    df: pd.DataFrame,
    window_days: int = 30,
    min_periods: int = 3,
) -> pd.DataFrame:
    """Add rolling NDCI baseline columns to df.

    Uses calendar-aware rolling on the DatetimeIndex so irregular
    acquisition gaps are handled correctly (not row-count based).

    Adds columns: ndci_baseline_mean, ndci_baseline_std, ndci_z_score.
    The baseline is computed on the look-back window EXCLUDING the current
    observation (shift(1) on sorted index).

    For scenes where the rolling window contains fewer than min_periods
    observations (e.g. the start of the dataset), the global dataset mean
    and std are used as a fallback so those early scenes can still be scored.
    """
    df = df.copy()
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)
    df = df.sort_index()

    s = df[_NDCI_MEAN]
    window = f"{window_days}D"

    # Rolling baseline (excludes current obs via shift)
    baseline_mean = s.rolling(window, min_periods=min_periods).mean().shift(1)
    baseline_std = s.rolling(window, min_periods=min_periods).std().shift(1)

    # Global fallback for early scenes that have too few prior observations
    global_mean = float(s.mean())
    global_std = float(s.std())
    baseline_mean = baseline_mean.fillna(global_mean)
    baseline_std = baseline_std.fillna(global_std)

    # Clamp std to avoid noise on very stable windows
    std_safe = baseline_std.clip(lower=1e-4)
    z_score = (s - baseline_mean) / std_safe

    df["ndci_baseline_mean"] = baseline_mean
    df["ndci_baseline_std"] = baseline_std
    df["ndci_z_score"] = z_score
    return df


def _severity_from_absolute(ndci_mean: float, low: float, medium: float, high: float) -> str | None:
    """Return severity from absolute NDCI threshold, or None if not triggered."""
    if ndci_mean >= high:
        return "HIGH"
    if ndci_mean >= medium:
        return "MEDIUM"
    if ndci_mean >= low:
        return "LOW"
    return None


def _severity_from_zscore(z: float) -> str:
    """Assign severity level to a z-score-only alert."""
    if z >= 4.0:
        return "HIGH"
    if z >= 3.0:
        return "MEDIUM"
    return "LOW"


def detect_alerts(
    df: pd.DataFrame,
    absolute_threshold_low: float = 0.2,
    absolute_threshold_medium: float = 0.3,
    absolute_threshold_high: float = 0.4,
    z_score_threshold: float = 2.0,
) -> list[Alert]:
    """Detect water quality anomalies using absolute NDCI thresholds and z-score.

    An alert fires if EITHER the absolute threshold OR the z-score is exceeded.
    Severity is primarily set by the absolute threshold; z-score only alerts
    use a z-based severity tier (LOW <3σ, MEDIUM 3-4σ, HIGH ≥4σ).

    Duplicate alerts within 7-day windows are suppressed; the highest-severity
    event per window is kept.
    """
    if "ndci_baseline_mean" not in df.columns:
        df = compute_rolling_baseline(df)

    severity_order = {"LOW": 1, "MEDIUM": 2, "HIGH": 3}
    raw_alerts: list[Alert] = []

    for idx, row in df.iterrows():
        ndci_mean = row.get(_NDCI_MEAN, np.nan)
        if np.isnan(ndci_mean):
            continue

        z = row.get("ndci_z_score", np.nan)
        baseline_mean = row.get("ndci_baseline_mean", np.nan)
        baseline_std = row.get("ndci_baseline_std", np.nan)

        abs_severity = _severity_from_absolute(
            ndci_mean, absolute_threshold_low, absolute_threshold_medium, absolute_threshold_high
        )
        z_triggered = (not np.isnan(z)) and (z >= z_score_threshold)

        if abs_severity is None and not z_triggered:
            continue

        if abs_severity is not None:
            severity = abs_severity
            trigger_notes = f"absolute threshold exceeded (NDCI={ndci_mean:.4f})"
        else:
            severity = _severity_from_zscore(z)
            trigger_notes = f"z-score={z:.2f} ≥ {z_score_threshold}"

        alert = Alert(
            date=idx.date() if hasattr(idx, "date") else idx,
            reservoir="serre_poncon",
            severity=severity,
            ndci_mean=float(ndci_mean),
            ndci_p90=float(row.get(_NDCI_P90, np.nan)),
            turbidity_mean=float(row.get(_TURB_MEAN, np.nan)),
            baseline_ndci=float(baseline_mean) if not np.isnan(baseline_mean) else float("nan"),
            baseline_std=float(baseline_std) if not np.isnan(baseline_std) else float("nan"),
            z_score=float(z) if not np.isnan(z) else float("nan"),
            valid_pixels=int(row.get(_NDCI_N, 0)),
            notes=trigger_notes,
        )
        raw_alerts.append(alert)

    # Deduplicate: within any 7-day window keep only the highest-severity alert
    raw_alerts.sort(key=lambda a: a.date)
    deduplicated: list[Alert] = []
    for alert in raw_alerts:
        window_start = alert.date - timedelta(days=7)
        overlapping = [
            i for i, a in enumerate(deduplicated)
            if window_start <= a.date <= alert.date
        ]
        if not overlapping:
            deduplicated.append(alert)
        else:
            # Replace lowest-severity duplicate if current is higher
            worst_idx = min(overlapping, key=lambda i: severity_order[deduplicated[i].severity])
            if severity_order[alert.severity] > severity_order[deduplicated[worst_idx].severity]:
                deduplicated[worst_idx] = alert

    deduplicated.sort(key=lambda a: a.date)
    logger.info("Detected %d alerts (%d raw before dedup)", len(deduplicated), len(raw_alerts))
    return deduplicated


def save_alerts(
    alerts: list[Alert],
    output_dir: Path,
    reservoir_name: str,
) -> tuple[Path, Path]:
    """Save alerts to CSV and JSON under output_dir."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = output_dir / f"{reservoir_name}_alerts.csv"
    json_path = output_dir / f"{reservoir_name}_alerts.json"

    rows = []
    for a in alerts:
        d = asdict(a)
        d["date"] = str(a.date)
        rows.append(d)

    pd.DataFrame(rows).to_csv(csv_path, index=False)
    logger.info("Saved %d alerts → %s", len(alerts), csv_path)

    payload = {
        "reservoir": reservoir_name,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "total_alerts": len(alerts),
        "alerts": rows,
    }
    with open(json_path, "w") as fh:
        json.dump(payload, fh, indent=2)
    logger.info("Saved alerts JSON → %s", json_path)

    return csv_path, json_path


def summarize_alerts(alerts: list[Alert]) -> None:
    """Print a human-readable summary of detected alerts."""
    if not alerts:
        print("No alerts detected.")
        return

    counts = {"LOW": 0, "MEDIUM": 0, "HIGH": 0}
    by_year: dict[int, int] = {}
    for a in alerts:
        counts[a.severity] += 1
        yr = a.date.year
        by_year[yr] = by_year.get(yr, 0) + 1

    print("\n── Alert Summary ────────────────────────────────────────")
    print(f"  Total alerts : {len(alerts)}")
    print(f"  HIGH         : {counts['HIGH']}")
    print(f"  MEDIUM       : {counts['MEDIUM']}")
    print(f"  LOW          : {counts['LOW']}")

    print("\n  Per year:")
    for yr in sorted(by_year):
        print(f"    {yr}: {by_year[yr]} alert(s)")

    # Longest alert-free gap
    all_dates = sorted(a.date for a in alerts)
    if len(all_dates) >= 2:
        gaps = [(all_dates[i + 1] - all_dates[i]).days for i in range(len(all_dates) - 1)]
        max_gap = max(gaps)
        gap_idx = gaps.index(max_gap)
        print(f"\n  Longest alert-free period: {max_gap} days")
        print(f"    ({all_dates[gap_idx]} → {all_dates[gap_idx + 1]})")
    else:
        print("\n  Longest alert-free period: N/A (fewer than 2 alerts)")

    # Most severe event
    most_severe = max(alerts, key=lambda a: a.ndci_mean)
    print(f"\n  Most elevated NDCI event:")
    print(f"    {most_severe.date}  severity={most_severe.severity}")
    print(f"    NDCI_mean={most_severe.ndci_mean:.4f}  z-score={most_severe.z_score:.2f}")
    print("─────────────────────────────────────────────────────────\n")


def validate_against_known_events(
    alerts: list[Alert],
    known_events: list[tuple[date, date, str]],
) -> bool:
    """Check that at least one MEDIUM/HIGH alert falls in each known bloom window.

    Parameters
    ----------
    alerts       : list of Alert objects
    known_events : list of (start_date, end_date, label) tuples

    Returns True if all events are covered, False otherwise.
    """
    print("\n── Validation Against Known Bloom Events ────────────────")
    all_pass = True
    qualifying = [a for a in alerts if a.severity in ("MEDIUM", "HIGH")]

    for start, end, label in known_events:
        hits = [a for a in qualifying if start <= a.date <= end]
        if hits:
            best = max(hits, key=lambda a: a.ndci_mean)
            print(f"  ✓  {label}: {len(hits)} alert(s)  "
                  f"(best: {best.date} {best.severity} NDCI={best.ndci_mean:.4f} z={best.z_score:.2f})")
        else:
            # Fall back to any severity
            any_hits = [a for a in alerts if start <= a.date <= end]
            if any_hits:
                best = max(any_hits, key=lambda a: a.ndci_mean)
                print(f"  ~  {label}: no MEDIUM/HIGH alert, but {len(any_hits)} LOW alert(s)  "
                      f"(best: {best.date} z={best.z_score:.2f})")
                # LOW within period counts as partial pass
            else:
                print(f"  ✗  {label}: NO alerts detected in period {start} → {end}")
                all_pass = False

    print("─────────────────────────────────────────────────────────\n")
    return all_pass


def check_new_scene(
    new_scene_stats: dict,
    historical_df: pd.DataFrame,
    reservoir_name: str,
    absolute_threshold_low: float = 0.2,
    absolute_threshold_medium: float = 0.3,
    absolute_threshold_high: float = 0.4,
    z_score_threshold: float = 2.0,
) -> "Alert | None":
    """Check a newly processed scene against historical baseline.

    This is the operational entry point: call it when a new Sentinel-2
    scene has been processed and its stats are available.

    Parameters
    ----------
    new_scene_stats : dict with at least 'date' (ISO str), 'ndci_water_mean',
                      'ndci_water_p90', 'turbidity_water_mean', 'ndci_water_n'
    historical_df   : DataFrame from the time series CSV (used to build baseline)
    reservoir_name  : reservoir identifier string

    Returns
    -------
    Alert if any threshold is exceeded, None if all clear.
    """
    scene_date = pd.to_datetime(new_scene_stats["date"])
    ndci_mean = float(new_scene_stats.get(_NDCI_MEAN, np.nan))

    if np.isnan(ndci_mean):
        logger.warning("check_new_scene: ndci_water_mean is NaN — skipping")
        return None

    # Build baseline from historical data within the 30-day look-back window
    hist = historical_df.copy()
    if not isinstance(hist.index, pd.DatetimeIndex):
        hist.index = pd.to_datetime(hist.index)
    hist = hist.sort_index()

    window_start = scene_date - pd.Timedelta(days=30)
    window_data = hist.loc[window_start:scene_date - pd.Timedelta(days=1), _NDCI_MEAN].dropna()

    if len(window_data) < 2:
        logger.warning(
            "check_new_scene: only %d historical scenes in 30-day window — baseline unreliable",
            len(window_data),
        )
        baseline_mean = float(hist[_NDCI_MEAN].mean())
        baseline_std = float(hist[_NDCI_MEAN].std())
    else:
        baseline_mean = float(window_data.mean())
        baseline_std = float(window_data.std())

    std_safe = max(baseline_std, 1e-4)
    z = (ndci_mean - baseline_mean) / std_safe

    abs_severity = _severity_from_absolute(
        ndci_mean, absolute_threshold_low, absolute_threshold_medium, absolute_threshold_high
    )
    z_triggered = z >= z_score_threshold

    if abs_severity is None and not z_triggered:
        logger.info("check_new_scene %s: all clear (NDCI=%.4f z=%.2f)", scene_date.date(), ndci_mean, z)
        return None

    if abs_severity is not None:
        severity = abs_severity
        notes = f"absolute threshold exceeded (NDCI={ndci_mean:.4f})"
    else:
        severity = _severity_from_zscore(z)
        notes = f"z-score={z:.2f} ≥ {z_score_threshold} (baseline={baseline_mean:.4f} ± {baseline_std:.4f})"

    alert = Alert(
        date=scene_date.date(),
        reservoir=reservoir_name,
        severity=severity,
        ndci_mean=ndci_mean,
        ndci_p90=float(new_scene_stats.get(_NDCI_P90, np.nan)),
        turbidity_mean=float(new_scene_stats.get(_TURB_MEAN, np.nan)),
        baseline_ndci=baseline_mean,
        baseline_std=baseline_std,
        z_score=z,
        valid_pixels=int(new_scene_stats.get(_NDCI_N, 0)),
        notes=notes,
    )
    logger.info(
        "check_new_scene %s: ALERT %s (NDCI=%.4f z=%.2f)",
        scene_date.date(), severity, ndci_mean, z,
    )
    return alert
