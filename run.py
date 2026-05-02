#!/usr/bin/env python3
"""
AquaWatch — command-line interface.

Usage:
    python run.py <command> [options]

Run `python run.py --help` for full command list.
"""

import argparse
import logging
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(name)-20s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("aquawatch")

RESERVOIR_GJ   = PROJECT_ROOT / "data" / "reservoir" / "serre_poncon.geojson"
RAW_DIR        = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR  = PROJECT_ROOT / "data" / "processed"
TIMESERIES_CSV = PROJECT_ROOT / "outputs" / "timeseries" / "serre_poncon_wqi.csv"
ALERTS_JSON    = PROJECT_ROOT / "outputs" / "alerts" / "serre_poncon_alerts.json"
MAPS_DIR       = PROJECT_ROOT / "outputs" / "maps"
BBOX           = [6.28, 44.49, 6.45, 44.62]
CLOUD_MAX      = 30.0
BANDS          = ["B03", "B04", "B05", "B08", "B8A", "SCL"]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _require_env() -> tuple[str, str]:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
    user = os.environ.get("CDSE_USERNAME", "")
    pwd  = os.environ.get("CDSE_PASSWORD", "")
    if not user or not pwd:
        logger.error("CDSE_USERNAME and CDSE_PASSWORD must be set in .env")
        sys.exit(1)
    return user, pwd


def _scene_is_done(scene_id: str) -> bool:
    return (PROCESSED_DIR / scene_id / "clipped" / "B08_clipped.tif").exists()


# ── Commands ──────────────────────────────────────────────────────────────────

def cmd_setup(args) -> None:
    """Create directory structure and validate credentials."""
    dirs = [
        PROJECT_ROOT / "data" / "raw",
        PROJECT_ROOT / "data" / "processed",
        PROJECT_ROOT / "data" / "reservoir",
        PROJECT_ROOT / "outputs" / "timeseries",
        PROJECT_ROOT / "outputs" / "alerts",
        PROJECT_ROOT / "outputs" / "maps" / "previews",
        PROJECT_ROOT / "outputs" / "demo",
        PROJECT_ROOT / "notebooks",
    ]
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)
        print(f"  {d.relative_to(PROJECT_ROOT)}/")

    env_file = PROJECT_ROOT / ".env"
    if not env_file.exists():
        template = PROJECT_ROOT / ".env.template"
        if template.exists():
            import shutil
            shutil.copy(template, env_file)
            print(f"\nCreated .env from template — fill in CDSE credentials.")
        else:
            env_file.write_text("CDSE_USERNAME=\nCDSEPASSWORD=\n")
            print(f"\nCreated empty .env — fill in CDSE credentials.")
    else:
        from dotenv import load_dotenv
        load_dotenv(env_file)
        user = os.environ.get("CDSE_USERNAME", "")
        print(f"\n.env found — CDSE_USERNAME={'set' if user else 'MISSING'}")

    print("\nSetup complete.")


def cmd_download(args) -> None:
    """Search and download Sentinel-2 scenes for a date range."""
    from download import search_sentinel2, download_scene
    username, password = _require_env()

    logger.info("Searching %s → %s  cloud ≤ %.0f%%", args.start, args.end, CLOUD_MAX)
    scenes = search_sentinel2(
        bbox=BBOX,
        date_start=args.start,
        date_end=args.end,
        cloud_cover_max=CLOUD_MAX,
        max_results=200,
    )
    if not scenes:
        logger.error("No scenes found.")
        sys.exit(1)

    to_download = [s for s in scenes if not _scene_is_done(s["name"].replace(".SAFE", ""))]
    print(f"\nFound {len(scenes)} scene(s) — {len(to_download)} to download.\n")

    for scene in to_download:
        scene_id = scene["name"].replace(".SAFE", "")
        try:
            download_scene(
                scene=scene,
                output_dir=RAW_DIR / scene_id,
                username=username,
                password=password,
                bands=BANDS,
            )
        except Exception:
            logger.exception("Download failed for %s", scene_id)


def cmd_process(args) -> None:
    """Apply cloud masking and clip all raw scenes not yet processed."""
    from preprocess import apply_cloud_mask, clip_to_reservoir

    raw_scenes = [d for d in RAW_DIR.iterdir() if d.is_dir()]
    to_process = [d for d in raw_scenes if not _scene_is_done(d.name)]
    print(f"{len(raw_scenes)} raw scene(s), {len(to_process)} to process.\n")

    for scene_dir in to_process:
        scene_id = scene_dir.name
        jp2s = {f.stem.split("_")[2]: f for f in scene_dir.glob("*.jp2")}
        if "SCL" not in jp2s:
            logger.warning("%s missing SCL — skipping", scene_id)
            continue

        scl_path = jp2s.pop("SCL")
        masked_dir = PROCESSED_DIR / scene_id / "masked"
        clipped_dir = PROCESSED_DIR / scene_id / "clipped"

        try:
            masked = apply_cloud_mask(
                band_paths={k: v for k, v in jp2s.items()},
                scl_path=scl_path,
                output_dir=masked_dir,
            )
            masked["SCL"] = scl_path
            clip_to_reservoir(
                band_paths=masked,
                reservoir_geojson=RESERVOIR_GJ,
                output_dir=clipped_dir,
                target_crs="EPSG:32632",
            )
            logger.info("Processed %s", scene_id[:50])
        except Exception:
            logger.exception("Processing failed for %s", scene_id)


def cmd_indices(args) -> None:
    """Compute water quality indices for all fully clipped scenes."""
    from indices import compute_all_indices

    scene_dirs = [
        d for d in PROCESSED_DIR.iterdir()
        if d.is_dir() and (d / "clipped" / "B08_clipped.tif").exists()
    ]
    print(f"{len(scene_dirs)} scene(s) eligible for index computation.\n")

    for scene_dir in scene_dirs:
        clipped = scene_dir / "clipped"
        band_paths = {b: clipped / f"{b}_clipped.tif" for b in ("B03", "B04", "B05", "B08")}
        if not all(p.exists() for p in band_paths.values()):
            logger.warning("%s missing clipped bands — skipping", scene_dir.name)
            continue
        index_dir = scene_dir / "indices"
        if (index_dir / "ndci_water.tif").exists():
            logger.debug("Indices already exist for %s", scene_dir.name[:40])
            continue
        try:
            compute_all_indices(band_paths=band_paths, output_dir=index_dir)
        except Exception:
            logger.exception("Index computation failed for %s", scene_dir.name)


def cmd_timeseries(args) -> None:
    """Build time series CSV from all processed scenes."""
    from timeseries import build_timeseries, plot_timeseries
    import pandas as pd

    output_csv = TIMESERIES_CSV
    df = build_timeseries(processed_dir=PROCESSED_DIR, output_path=output_csv)
    if df.empty:
        logger.error("No scenes processed — run download + process + indices first")
        sys.exit(1)

    plot_path = output_csv.with_suffix(".png")
    plot_timeseries(df=df, output_path=plot_path)
    print(f"\nTime series: {len(df)} scenes")
    print(f"CSV  → {output_csv.relative_to(PROJECT_ROOT)}")
    print(f"Plot → {plot_path.relative_to(PROJECT_ROOT)}")


def cmd_alerts(args) -> None:
    """Run anomaly detection and save alert CSV + JSON."""
    import pandas as pd
    from alerts import (
        compute_rolling_baseline, detect_alerts, save_alerts,
        summarize_alerts, validate_against_known_events,
    )
    from datetime import date as date_cls

    if not TIMESERIES_CSV.exists():
        logger.error("Run timeseries first: python run.py timeseries")
        sys.exit(1)

    df = pd.read_csv(TIMESERIES_CSV, parse_dates=["date"])
    df = df.set_index("date").sort_index()
    df = compute_rolling_baseline(df)

    alerts = detect_alerts(df, z_score_threshold=1.5)
    save_alerts(alerts, PROJECT_ROOT / "outputs" / "alerts", "serre_poncon")
    summarize_alerts(alerts)

    known = [
        (date_cls(2023, 7, 1),  date_cls(2023, 8, 31),  "Summer 2023 bloom"),
        (date_cls(2024, 6, 1),  date_cls(2024, 8, 31),  "Summer 2024 bloom"),
    ]
    validate_against_known_events(alerts, known)


def cmd_maps(args) -> None:
    """Generate spatial maps for all alerts and the dashboard."""
    import subprocess
    subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "scripts" / "generate_maps.py")],
        check=True,
    )


def cmd_dashboard(args) -> None:
    """Generate dashboard PNG only."""
    import json
    from datetime import date as date_cls
    import pandas as pd
    from alerts import Alert
    from visualize import plot_dashboard

    df = pd.read_csv(TIMESERIES_CSV, parse_dates=["date"])
    with open(ALERTS_JSON) as fh:
        payload = json.load(fh)
    alerts = [
        Alert(
            date=date_cls.fromisoformat(a["date"]),
            reservoir=a["reservoir"], severity=a["severity"],
            ndci_mean=a["ndci_mean"], ndci_p90=a["ndci_p90"],
            turbidity_mean=a["turbidity_mean"],
            baseline_ndci=a["baseline_ndci"], baseline_std=a["baseline_std"],
            z_score=a["z_score"], valid_pixels=a["valid_pixels"],
            notes=a.get("notes", ""),
        )
        for a in payload["alerts"]
    ]
    out = MAPS_DIR / "dashboard.png"
    MAPS_DIR.mkdir(parents=True, exist_ok=True)
    plot_dashboard(df=df, alerts=alerts, output_path=out)
    print(f"Dashboard → {out.relative_to(PROJECT_ROOT)}")


def cmd_check(args) -> None:
    """Check a specific date against the historical baseline."""
    import pandas as pd
    from datetime import date as date_cls
    from alerts import check_new_scene

    if not TIMESERIES_CSV.exists():
        logger.error("No time series CSV found. Run the full pipeline first.")
        sys.exit(1)

    target = date_cls.fromisoformat(args.date)
    df = pd.read_csv(TIMESERIES_CSV, parse_dates=["date"])
    df["_date"] = df["date"].dt.date

    # Find closest processed scene to requested date
    df["_gap"] = (df["_date"] - target).abs().apply(lambda x: x.days)
    closest = df.nsmallest(1, "_gap").iloc[0]
    gap_days = int(closest["_gap"])

    if gap_days > 10:
        print(f"\nNo processed scene within 10 days of {target}.")
        print(f"Closest is {closest['_date']} ({gap_days} days away).")
        print("Run: python run.py download --start <date-10d> --end <date+10d>")
        sys.exit(0)

    if gap_days > 0:
        print(f"No scene for {target} — using closest: {closest['_date']} ({gap_days}d away)")

    stats = {
        "date": str(closest["_date"]),
        "ndci_water_mean": closest.get("ndci_water_mean", float("nan")),
        "ndci_water_p90":  closest.get("ndci_water_p90",  float("nan")),
        "turbidity_water_mean": closest.get("turbidity_water_mean", float("nan")),
        "ndci_water_n": int(closest.get("ndci_water_n", 0)),
    }

    # Historical = all scenes strictly before the target date
    hist = df[df["_date"] < target].drop(columns=["_date", "_gap"]).set_index("date")

    alert = check_new_scene(
        new_scene_stats=stats,
        historical_df=hist,
        reservoir_name="serre_poncon",
        z_score_threshold=1.5,
    )

    print()
    if alert:
        print(f"  ⚠  ALERT — {alert.severity}")
        print(f"     Date     : {alert.date}")
        print(f"     NDCI     : {alert.ndci_mean:.4f}")
        print(f"     Z-score  : {alert.z_score:.2f}")
        print(f"     Baseline : {alert.baseline_ndci:.4f} ± {alert.baseline_std:.4f}")
        print(f"     Pixels   : {alert.valid_pixels:,}")
    else:
        print(f"  ✓  ALL CLEAR — {stats['date']}")
        print(f"     NDCI={stats['ndci_water_mean']:.4f}")
    print()


def cmd_run_all(args) -> None:
    """Run the full pipeline end to end."""
    print("=== AquaWatch — Full Pipeline ===\n")

    class NS:
        pass

    ns = NS()
    ns.start = args.start
    ns.end = args.end

    print("Step 1/6  download")
    cmd_download(ns)

    print("\nStep 2/6  process")
    cmd_process(ns)

    print("\nStep 3/6  indices")
    cmd_indices(ns)

    print("\nStep 4/6  timeseries")
    cmd_timeseries(ns)

    print("\nStep 5/6  alerts")
    cmd_alerts(ns)

    print("\nStep 6/6  maps")
    cmd_maps(ns)

    print("\n=== Done ===")
    print(f"  Time series : {TIMESERIES_CSV.relative_to(PROJECT_ROOT)}")
    print(f"  Alerts      : outputs/alerts/serre_poncon_alerts.{{csv,json}}")
    print(f"  Dashboard   : outputs/maps/dashboard.png")
    print(f"  Demo pack   : outputs/demo/")


# ── Argument parser ───────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python run.py",
        description="AquaWatch — Sentinel-2 water quality monitor for Serre-Ponçon",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run.py setup
  python run.py download --start 2024-06-01 --end 2024-08-31
  python run.py check --date 2024-08-21
  python run.py run-all --start 2023-04-01 --end 2024-10-31
        """,
    )
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")

    sub.add_parser("setup",      help="Create directory structure and validate credentials")
    sub.add_parser("process",    help="Cloud-mask and clip all downloaded raw scenes")
    sub.add_parser("indices",    help="Compute NDCI / NDWI / turbidity index rasters")
    sub.add_parser("timeseries", help="Aggregate scene statistics into a time series CSV")
    sub.add_parser("alerts",     help="Run anomaly detection and generate alert log")
    sub.add_parser("maps",       help="Generate spatial maps for all alerts")
    sub.add_parser("dashboard",  help="Generate summary dashboard PNG")

    dl = sub.add_parser("download", help="Download Sentinel-2 scenes for a date range")
    dl.add_argument("--start", required=True, metavar="YYYY-MM-DD", help="Start date")
    dl.add_argument("--end",   required=True, metavar="YYYY-MM-DD", help="End date")

    ra = sub.add_parser("run-all", help="Run full pipeline end to end")
    ra.add_argument("--start", required=True, metavar="YYYY-MM-DD")
    ra.add_argument("--end",   required=True, metavar="YYYY-MM-DD")

    ch = sub.add_parser("check",
                         help="Check a date against historical baseline (operational mode)")
    ch.add_argument("--date", required=True, metavar="YYYY-MM-DD",
                    help="Date to check (uses closest processed scene)")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    dispatch = {
        "setup":      cmd_setup,
        "download":   cmd_download,
        "process":    cmd_process,
        "indices":    cmd_indices,
        "timeseries": cmd_timeseries,
        "alerts":     cmd_alerts,
        "maps":       cmd_maps,
        "dashboard":  cmd_dashboard,
        "check":      cmd_check,
        "run-all":    cmd_run_all,
    }

    if args.command not in dispatch:
        parser.print_help()
        sys.exit(1)

    dispatch[args.command](args)


if __name__ == "__main__":
    main()
