#!/usr/bin/env python3
"""
Weekend 4: Generate all spatial maps and the dashboard.

1. Alert map PNG for each detected alert
2. Dashboard PNG (hero image)
3. Before/during/after bloom comparison for the summer 2024 event
4. Assemble demo package in outputs/demo/

Usage:
    conda activate aquawatch
    python scripts/generate_maps.py
"""

import json
import logging
import shutil
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from alerts import Alert
from visualize import plot_alert_map, plot_bloom_comparison, plot_dashboard

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(name)-20s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("generate_maps")

ALERTS_JSON    = PROJECT_ROOT / "outputs" / "alerts" / "serre_poncon_alerts.json"
TIMESERIES_CSV = PROJECT_ROOT / "outputs" / "timeseries" / "serre_poncon_wqi.csv"
RESERVOIR_GJ   = PROJECT_ROOT / "data" / "reservoir" / "serre_poncon.geojson"
PROCESSED_DIR  = PROJECT_ROOT / "data" / "processed"
MAPS_DIR       = PROJECT_ROOT / "outputs" / "maps"
DEMO_DIR       = PROJECT_ROOT / "outputs" / "demo"


def load_alerts() -> list[Alert]:
    """Deserialise alerts from JSON."""
    with open(ALERTS_JSON) as fh:
        payload = json.load(fh)
    alerts = []
    for a in payload["alerts"]:
        alerts.append(Alert(
            date=date.fromisoformat(a["date"]),
            reservoir=a["reservoir"],
            severity=a["severity"],
            ndci_mean=a["ndci_mean"],
            ndci_p90=a["ndci_p90"],
            turbidity_mean=a["turbidity_mean"],
            baseline_ndci=a["baseline_ndci"],
            baseline_std=a["baseline_std"],
            z_score=a["z_score"],
            valid_pixels=a["valid_pixels"],
            notes=a.get("notes", ""),
        ))
    return alerts


def scene_ndci_path(scene_id: str) -> Path:
    return PROCESSED_DIR / scene_id / "indices" / "ndci_water.tif"


def date_to_scene_id(target_date: date, ts: pd.DataFrame) -> str | None:
    """Return the scene_id whose sensing date matches target_date."""
    match = ts[ts["date"].dt.date == target_date]
    if match.empty:
        return None
    return match["scene_id"].iloc[0]


def main() -> None:
    if not ALERTS_JSON.exists():
        logger.error("Alerts JSON not found. Run scripts/run_alerts.py first.")
        sys.exit(1)

    alerts = load_alerts()
    logger.info("Loaded %d alerts", len(alerts))

    ts = pd.read_csv(TIMESERIES_CSV, parse_dates=["date"])
    df = pd.read_csv(TIMESERIES_CSV, parse_dates=["date"])

    MAPS_DIR.mkdir(parents=True, exist_ok=True)
    DEMO_DIR.mkdir(parents=True, exist_ok=True)

    # ── 1. Alert maps ─────────────────────────────────────────────────────────
    best_alert = None
    best_alert_path = None
    sev_rank = {"LOW": 1, "MEDIUM": 2, "HIGH": 3}

    for alert in alerts:
        scene_id = date_to_scene_id(alert.date, ts)
        if scene_id is None:
            logger.warning("No scene_id found for alert date %s — skipping", alert.date)
            continue

        ndci_path = scene_ndci_path(scene_id)
        if not ndci_path.exists():
            logger.warning("NDCI raster not found: %s — skipping", ndci_path)
            continue

        out_path = MAPS_DIR / f"alert_{alert.date}_{alert.severity}.png"
        try:
            plot_alert_map(
                alert=alert,
                ndci_path=ndci_path,
                reservoir_geojson=RESERVOIR_GJ,
                output_path=out_path,
            )
        except Exception:
            logger.exception("Failed to generate alert map for %s", alert.date)
            continue

        if best_alert is None or sev_rank[alert.severity] >= sev_rank[best_alert.severity]:
            best_alert = alert
            best_alert_path = out_path

    # ── 2. Dashboard ──────────────────────────────────────────────────────────
    dashboard_path = MAPS_DIR / "dashboard.png"
    try:
        plot_dashboard(df=df, alerts=alerts, output_path=dashboard_path)
    except Exception:
        logger.exception("Dashboard generation failed")

    # ── 3. Bloom comparison (2024-08-21 event: before / during / after) ───────
    bloom_dates_str = ["2024-08-06", "2024-08-21", "2024-09-10"]
    bloom_dates = []
    bloom_paths = []
    for ds in bloom_dates_str:
        d = date.fromisoformat(ds)
        sid = date_to_scene_id(d, ts)
        if sid is None:
            logger.warning("No scene for bloom comparison date %s — skipping", ds)
            continue
        p = scene_ndci_path(sid)
        if not p.exists():
            logger.warning("NDCI file missing for %s — skipping", ds)
            continue
        bloom_dates.append(d)
        bloom_paths.append(p)

    bloom_path = MAPS_DIR / "bloom_comparison.png"
    if len(bloom_dates) >= 2:
        try:
            plot_bloom_comparison(
                dates=bloom_dates,
                ndci_paths=bloom_paths,
                reservoir_geojson=RESERVOIR_GJ,
                output_path=bloom_path,
                title="Serre-Ponçon — Summer 2024 Bloom Progression (Before / Peak / After)",
            )
        except Exception:
            logger.exception("Bloom comparison failed")
    else:
        logger.warning("Not enough dates for bloom comparison")

    # ── 4. Demo package ───────────────────────────────────────────────────────
    copies = [
        (dashboard_path, DEMO_DIR / "dashboard.png"),
        (ALERTS_JSON,    DEMO_DIR / "serre_poncon_alerts.json"),
        (bloom_path,     DEMO_DIR / "bloom_comparison.png"),
    ]
    if best_alert_path and best_alert_path.exists():
        copies.append((best_alert_path, DEMO_DIR / "best_alert_map.png"))

    for src, dst in copies:
        if src.exists():
            shutil.copy2(src, dst)
            logger.info("Demo: %s", dst.name)
        else:
            logger.warning("Demo file missing: %s", src)

    print(f"\nOutputs written to {MAPS_DIR.relative_to(PROJECT_ROOT)}/")
    print(f"Demo package:      {DEMO_DIR.relative_to(PROJECT_ROOT)}/")
    for f in sorted(DEMO_DIR.iterdir()):
        size_kb = f.stat().st_size // 1024
        print(f"  {f.name:<35} {size_kb:>5} KB")


if __name__ == "__main__":
    main()
