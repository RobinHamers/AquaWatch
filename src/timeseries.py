"""Time series aggregation from per-scene index rasters."""

import logging
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import rasterio

logger = logging.getLogger(__name__)

NDCI_ALERT_LOW = 0.2
NDCI_ALERT_MEDIUM = 0.3

# Months outside the bloom window (May–Oct) — scenes in these months warrant caution
_WINTER_MONTHS = frozenset([11, 12, 1, 2, 3, 4])
_MIN_PIXEL_FRACTION = 0.4  # fraction of p75 below which a scene is 'low_pixels'


def add_quality_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Add a 'quality_flag' column based on pixel coverage and season.

    Flags:
      'good'              — ≥40% typical pixels and in bloom season (May–Oct)
      'low_pixels'        — <40% typical pixels, bloom season
      'winter'            — Nov–Apr, sufficient pixel coverage
      'low_pixels_winter' — Nov–Apr AND low pixel coverage

    The 'low_pixels' threshold is 40% of the 75th-percentile scene pixel count.
    Intended as an audit trail column; alert detection uses the same threshold.
    """
    df = df.copy()
    typical_n = float(df["ndci_water_n"].quantile(0.75))
    low_px = df["ndci_water_n"] < typical_n * _MIN_PIXEL_FRACTION
    winter = pd.to_datetime(df["date"]).dt.month.isin(_WINTER_MONTHS)

    conditions = [low_px & winter, low_px & ~winter, ~low_px & winter]
    choices    = ["low_pixels_winter", "low_pixels", "winter"]
    df["quality_flag"] = np.select(conditions, choices, default="good")
    return df
NDCI_ALERT_HIGH = 0.4


def extract_scene_stats(
    index_paths: dict[str, Path],
    scene_date: str,
) -> dict:
    """Compute spatial statistics over valid (non-NaN) pixels for one scene.

    Parameters
    ----------
    index_paths : mapping of index name → GeoTIFF path (ndci_water, turbidity_water, ndwi)
    scene_date  : ISO date string "YYYY-MM-DD"

    Returns
    -------
    Dict with date and per-index statistics (mean, median, std, p10, p25, p75, p90, max, n).
    """
    row: dict = {"date": scene_date}

    for name, path in index_paths.items():
        if not path.exists():
            logger.warning("Index file missing for %s on %s — skipping", name, scene_date)
            continue
        with rasterio.open(path) as src:
            data = src.read(1, masked=True).astype(np.float32)
        valid = data.compressed()
        valid = valid[np.isfinite(valid)]

        if valid.size == 0:
            for stat in ("mean", "median", "std", "p10", "p25", "p75", "p90", "max", "n"):
                row[f"{name}_{stat}"] = np.nan
            row[f"{name}_n"] = 0
            continue

        row[f"{name}_mean"] = float(np.mean(valid))
        row[f"{name}_median"] = float(np.median(valid))
        row[f"{name}_std"] = float(np.std(valid))
        row[f"{name}_p10"] = float(np.percentile(valid, 10))
        row[f"{name}_p25"] = float(np.percentile(valid, 25))
        row[f"{name}_p75"] = float(np.percentile(valid, 75))
        row[f"{name}_p90"] = float(np.percentile(valid, 90))
        row[f"{name}_max"] = float(np.max(valid))
        row[f"{name}_n"] = int(valid.size)

    return row


def build_timeseries(
    processed_dir: Path,
    output_path: Path,
) -> pd.DataFrame:
    """Build a time series CSV from all processed scenes in processed_dir.

    Discovers scenes by looking for subdirectories containing
    clipped/B08_clipped.tif (fully processed marker) and indices/.

    Parameters
    ----------
    processed_dir : parent directory containing per-scene subdirectories
    output_path   : where to write the CSV

    Returns
    -------
    DataFrame sorted by date with one row per scene.
    """
    from indices import compute_all_indices

    processed_dir = Path(processed_dir)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    scene_dirs = sorted(
        d for d in processed_dir.iterdir()
        if d.is_dir() and (d / "clipped" / "B08_clipped.tif").exists()
    )
    logger.info("Found %d fully processed scenes in %s", len(scene_dirs), processed_dir)

    rows = []
    for scene_dir in scene_dirs:
        # Parse date from directory name: S2X_MSIL2A_{sensing_dt}_...
        parts = scene_dir.name.split("_")
        sensing_dt = parts[2]  # e.g. "20230415T103021"
        scene_date = f"{sensing_dt[:4]}-{sensing_dt[4:6]}-{sensing_dt[6:8]}"

        clipped = scene_dir / "clipped"
        band_paths = {
            "B03": clipped / "B03_clipped.tif",
            "B04": clipped / "B04_clipped.tif",
            "B05": clipped / "B05_clipped.tif",
            "B08": clipped / "B08_clipped.tif",
        }

        missing = [k for k, v in band_paths.items() if not v.exists()]
        if missing:
            logger.warning("Scene %s missing bands %s — skipping", scene_dir.name, missing)
            continue

        index_dir = scene_dir / "indices"
        try:
            index_paths = compute_all_indices(band_paths=band_paths, output_dir=index_dir)
        except Exception:
            logger.exception("Index computation failed for %s", scene_dir.name)
            continue

        stats_inputs = {
            "ndci_water": index_paths["ndci_water"],
            "turbidity_water": index_paths["turbidity_water"],
            "ndwi": index_paths["ndwi"],
        }
        row = extract_scene_stats(stats_inputs, scene_date)
        row["scene_id"] = scene_dir.name
        rows.append(row)
        logger.info("Extracted stats for %s (%s)", scene_date, scene_dir.name[:40])

    if not rows:
        logger.error("No scenes processed — check that B08_clipped.tif exists in clipped dirs")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    df = add_quality_flags(df)
    col_order = ["date", "scene_id", "quality_flag"] + [
        c for c in df.columns if c not in ("date", "scene_id", "quality_flag")
    ]
    df = df[col_order]
    df.to_csv(output_path, index=False)
    logger.info("Saved time series (%d rows) → %s", len(df), output_path)
    return df


def plot_timeseries(
    df: pd.DataFrame,
    output_path: Path,
) -> None:
    """Plot NDCI and turbidity time series with alert thresholds.

    Parameters
    ----------
    df          : DataFrame from build_timeseries() with date column
    output_path : path for the output PNG
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    fig, axes = plt.subplots(2, 1, figsize=(14, 8), sharex=True)
    fig.suptitle("Serre-Ponçon — Water Quality Time Series (2023–2024)", fontsize=13, y=0.98)

    # ── Panel 1: NDCI ─────────────────────────────────────────────────────────
    ax1 = axes[0]
    if "ndci_water_mean" in df.columns:
        ax1.fill_between(
            df["date"], df.get("ndci_water_p25", np.nan), df.get("ndci_water_p75", np.nan),
            alpha=0.25, color="#2166ac", label="IQR (p25–p75)"
        )
        ax1.plot(df["date"], df["ndci_water_mean"], "o-", color="#2166ac",
                 linewidth=1.5, markersize=4, label="NDCI mean")
        ax1.plot(df["date"], df.get("ndci_water_p90", np.nan), "--", color="#4dac26",
                 linewidth=1, alpha=0.8, label="NDCI p90")

    ax1.axhline(NDCI_ALERT_LOW, color="#fdae61", linewidth=1.2, linestyle="--", label="LOW (0.2)")
    ax1.axhline(NDCI_ALERT_MEDIUM, color="#f46d43", linewidth=1.2, linestyle="--", label="MED (0.3)")
    ax1.axhline(NDCI_ALERT_HIGH, color="#d73027", linewidth=1.2, linestyle="--", label="HIGH (0.4)")
    ax1.set_ylabel("NDCI")
    ax1.legend(fontsize=8, ncol=3, loc="upper left")
    ax1.set_ylim(-0.3, 0.7)
    ax1.grid(axis="y", linestyle=":", alpha=0.5)

    # ── Panel 2: Turbidity ────────────────────────────────────────────────────
    ax2 = axes[1]
    if "turbidity_water_mean" in df.columns:
        ax2.fill_between(
            df["date"], df.get("turbidity_water_p25", np.nan), df.get("turbidity_water_p75", np.nan),
            alpha=0.25, color="#762a83", label="IQR (p25–p75)"
        )
        ax2.plot(df["date"], df["turbidity_water_mean"], "o-", color="#762a83",
                 linewidth=1.5, markersize=4, label="Turbidity mean")

    ax2.set_ylabel("Turbidity proxy (B04/B03)")
    ax2.legend(fontsize=8, loc="upper left")
    ax2.grid(axis="y", linestyle=":", alpha=0.5)

    # ── Summer shading ────────────────────────────────────────────────────────
    for year in (2023, 2024):
        for ax in axes:
            ax.axvspan(
                pd.Timestamp(f"{year}-06-01"), pd.Timestamp(f"{year}-09-30"),
                alpha=0.07, color="orange", label="_nolegend_"
            )

    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    fig.autofmt_xdate(rotation=30)

    plt.tight_layout()
    fig.savefig(str(output_path), dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Saved time series plot → %s", output_path)
