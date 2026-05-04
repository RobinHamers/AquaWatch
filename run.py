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
        apply_seasonal_filter, flag_isolated_spikes, print_validation_report,
    )
    from datetime import date as date_cls

    if not TIMESERIES_CSV.exists():
        logger.error("Run timeseries first: python run.py timeseries")
        sys.exit(1)

    df = pd.read_csv(TIMESERIES_CSV, parse_dates=["date"])
    df = df.set_index("date").sort_index()
    df = compute_rolling_baseline(df)

    alerts = detect_alerts(df, z_score_threshold=1.5)
    alerts = flag_isolated_spikes(alerts)    # flag while severity reflects absolute NDCI
    alerts = apply_seasonal_filter(alerts)   # then downgrade off-season events
    save_alerts(alerts, PROJECT_ROOT / "outputs" / "alerts", "serre_poncon")
    summarize_alerts(alerts)

    bloom_periods = [
        (date_cls(2023, 7, 1), date_cls(2023, 8, 31), "Jul-Aug 2023"),
        (date_cls(2024, 6, 1), date_cls(2024, 8, 31), "Jun-Aug 2024"),
    ]
    print_validation_report(df, alerts, bloom_periods)


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


def cmd_s3_download(args) -> None:
    """Search and download Sentinel-3 OLCI WFR scenes for a date range."""
    from s3_download import search_sentinel3_olci, download_s3_scene
    username, password = _require_env()

    logger.info("Searching S3 OLCI %s → %s", args.start, args.end)
    scenes = search_sentinel3_olci(
        bbox=BBOX,
        date_start=args.start,
        date_end=args.end,
        max_results=600,
    )
    if not scenes:
        logger.error("No Sentinel-3 scenes found.")
        sys.exit(1)

    s3_raw = PROJECT_ROOT / "data" / "raw_s3"
    to_dl = [s for s in scenes if not (s3_raw / s["Name"].replace(".SEN3", "")).exists()]
    print(f"\nFound {len(scenes)} S3 scene(s) — {len(to_dl)} to download.\n")

    for scene in to_dl:
        scene_id = scene["Name"].replace(".SEN3", "")
        try:
            download_s3_scene(
                scene=scene,
                output_dir=s3_raw / scene_id,
                username=username,
                password=password,
            )
        except Exception:
            logger.exception("S3 download failed for %s", scene_id)


def cmd_s3_process(args) -> None:
    """Apply WQSF mask, reproject, and clip all downloaded S3 scenes."""
    from s3_download import _nc_to_geotiff
    from s3_preprocess import apply_wqsf_mask, clip_s3_to_reservoir

    try:
        from netCDF4 import Dataset as NC4Dataset
    except ImportError:
        logger.error(
            "netCDF4 is required for S3 processing. "
            "Install with: conda install -c conda-forge netcdf4"
        )
        sys.exit(1)

    s3_raw      = PROJECT_ROOT / "data" / "raw_s3"
    s3_proc     = PROJECT_ROOT / "data" / "processed_s3"
    scene_dirs  = [d for d in s3_raw.iterdir() if d.is_dir()] if s3_raw.exists() else []
    print(f"{len(scene_dirs)} raw S3 scene(s) to process.\n")

    for scene_dir in scene_dirs:
        geo_nc = scene_dir / "geo_coordinates.nc"
        if not geo_nc.exists():
            logger.warning("%s missing geo_coordinates.nc — skipping", scene_dir.name)
            continue

        with NC4Dataset(geo_nc) as ds:
            lat = ds["latitude"][:]
            lon = ds["longitude"][:]

        bands = {"Oa08_reflectance": scene_dir / "Oa08_reflectance.nc",
                 "Oa11_reflectance": scene_dir / "Oa11_reflectance.nc"}
        wqsf_nc = scene_dir / "WQSF.nc"

        tif_dir = s3_proc / scene_dir.name / "tif"
        tif_dir.mkdir(parents=True, exist_ok=True)

        band_tifs: dict[str, Path] = {}
        for band_name, nc_path in bands.items():
            if not nc_path.exists():
                continue
            out_tif = tif_dir / f"{band_name}.tif"
            _nc_to_geotiff(nc_path, band_name, lat, lon, out_tif)
            band_tifs[band_name] = out_tif

        # Reproject WQSF the same way (uint32 — don't scale)
        if wqsf_nc.exists():
            wqsf_tif = tif_dir / "WQSF.tif"
            if not wqsf_tif.exists():
                _nc_to_geotiff(wqsf_nc, "WQSF", lat, lon, wqsf_tif)

            masked = apply_wqsf_mask(
                band_paths=band_tifs,
                wqsf_path=tif_dir / "WQSF.tif",
                output_dir=s3_proc / scene_dir.name / "masked",
            )
            clip_s3_to_reservoir(
                band_paths=masked,
                reservoir_geojson=RESERVOIR_GJ,
                output_dir=s3_proc / scene_dir.name / "clipped",
            )
        logger.info("Processed S3 %s", scene_dir.name[:50])


def cmd_s3_timeseries(args) -> None:
    """Build S3 NDCI time series CSV from all processed S3 scenes."""
    from indices import compute_s3_ndci
    import rasterio

    s3_proc  = PROJECT_ROOT / "data" / "processed_s3"
    out_csv  = PROJECT_ROOT / "outputs" / "timeseries" / "serre_poncon_s3_wqi.csv"

    if not s3_proc.exists():
        logger.error("No processed S3 scenes found. Run s3-process first.")
        sys.exit(1)

    rows = []
    for scene_dir in sorted(s3_proc.iterdir()):
        clipped = scene_dir / "clipped"
        oa11 = clipped / "Oa11_reflectance_clipped.tif"
        oa08 = clipped / "Oa08_reflectance_clipped.tif"
        if not oa11.exists() or not oa08.exists():
            continue

        parts = scene_dir.name.split("_")
        sensing_dt = parts[7] if len(parts) > 7 else parts[-1]
        scene_date = f"{sensing_dt[:4]}-{sensing_dt[4:6]}-{sensing_dt[6:8]}"

        ndci_path = scene_dir / "indices" / "ndci_s3.tif"
        ndci_path.parent.mkdir(parents=True, exist_ok=True)
        compute_s3_ndci(oa11, oa08, ndci_path)

        with rasterio.open(ndci_path) as src:
            data = src.read(1, masked=True).astype("float32")
        valid = data.compressed()
        valid = valid[np.isfinite(valid)]
        if valid.size == 0:
            continue

        rows.append({
            "date": scene_date,
            "scene_id": scene_dir.name,
            "ndci_water_mean":   float(np.mean(valid)),
            "ndci_water_median": float(np.median(valid)),
            "ndci_water_std":    float(np.std(valid)),
            "ndci_water_p10":    float(np.percentile(valid, 10)),
            "ndci_water_p90":    float(np.percentile(valid, 90)),
            "ndci_water_n":      int(valid.size),
        })

    if not rows:
        logger.error("No S3 scenes produced valid NDCI stats.")
        sys.exit(1)

    import numpy as np
    import pandas as pd
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)
    print(f"\nS3 time series: {len(df)} scenes → {out_csv.relative_to(PROJECT_ROOT)}")


def cmd_fusion(args) -> None:
    """Run S2/S3 fusion analysis and generate fused dashboard."""
    import pandas as pd
    from datetime import date as date_cls
    from fusion import build_fused_timeseries, detect_s3_precursor_alerts, print_fusion_report
    from visualize import plot_fused_dashboard
    from alerts import compute_rolling_baseline, detect_alerts, flag_isolated_spikes, apply_seasonal_filter

    s2_csv = TIMESERIES_CSV
    s3_csv = PROJECT_ROOT / "outputs" / "timeseries" / "serre_poncon_s3_wqi.csv"

    for path in (s2_csv, s3_csv):
        if not path.exists():
            logger.error("Missing: %s — run the relevant timeseries step first.", path)
            sys.exit(1)

    fused_csv = PROJECT_ROOT / "outputs" / "timeseries" / "serre_poncon_fused.csv"
    fused_df  = build_fused_timeseries(s2_csv, s3_csv, fused_csv)

    s2_df = pd.read_csv(s2_csv, parse_dates=["date"])
    s3_df = pd.read_csv(s3_csv, parse_dates=["date"])

    s2_idx = s2_df.set_index("date").sort_index()
    s2_idx = compute_rolling_baseline(s2_idx)
    alerts  = detect_alerts(s2_idx, z_score_threshold=1.5)
    alerts  = flag_isolated_spikes(alerts)
    alerts  = apply_seasonal_filter(alerts)

    bloom_periods = [
        (date_cls(2023, 7, 1), date_cls(2023, 8, 31), "Jul–Aug 2023"),
        (date_cls(2024, 6, 1), date_cls(2024, 8, 31), "Jun–Aug 2024"),
    ]
    events = detect_s3_precursor_alerts(fused_df, bloom_periods)
    print_fusion_report(fused_df, events, len(s3_df), len(s2_df))

    dash_path = MAPS_DIR / "dashboard_fused.png"
    MAPS_DIR.mkdir(parents=True, exist_ok=True)
    plot_fused_dashboard(
        s2_df=s2_df,
        s3_df=s3_df,
        alerts=alerts,
        precursor_events=events,
        output_path=dash_path,
    )
    print(f"Fused dashboard → {dash_path.relative_to(PROJECT_ROOT)}")


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

    sub.add_parser("setup",        help="Create directory structure and validate credentials")
    sub.add_parser("process",      help="Cloud-mask and clip all downloaded S2 raw scenes")
    sub.add_parser("indices",      help="Compute NDCI / NDWI / turbidity index rasters")
    sub.add_parser("timeseries",   help="Aggregate S2 scene statistics into a time series CSV")
    sub.add_parser("alerts",       help="Run anomaly detection and generate alert log")
    sub.add_parser("maps",         help="Generate spatial maps for all alerts")
    sub.add_parser("dashboard",    help="Generate summary dashboard PNG")
    sub.add_parser("s3-process",   help="WQSF-mask, reproject, and clip downloaded S3 scenes")
    sub.add_parser("s3-timeseries",help="Build S3 NDCI time series CSV")
    sub.add_parser("fusion",       help="Run S2/S3 fusion analysis and generate fused dashboard")

    dl = sub.add_parser("download",    help="Download Sentinel-2 scenes for a date range")
    dl.add_argument("--start", required=True, metavar="YYYY-MM-DD", help="Start date")
    dl.add_argument("--end",   required=True, metavar="YYYY-MM-DD", help="End date")

    s3dl = sub.add_parser("s3-download", help="Download Sentinel-3 OLCI WFR scenes for a date range")
    s3dl.add_argument("--start", required=True, metavar="YYYY-MM-DD")
    s3dl.add_argument("--end",   required=True, metavar="YYYY-MM-DD")

    ra = sub.add_parser("run-all", help="Run full S2 pipeline end to end")
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
        "setup":         cmd_setup,
        "download":      cmd_download,
        "process":       cmd_process,
        "indices":       cmd_indices,
        "timeseries":    cmd_timeseries,
        "alerts":        cmd_alerts,
        "maps":          cmd_maps,
        "dashboard":     cmd_dashboard,
        "check":         cmd_check,
        "run-all":       cmd_run_all,
        "s3-download":   cmd_s3_download,
        "s3-process":    cmd_s3_process,
        "s3-timeseries": cmd_s3_timeseries,
        "fusion":        cmd_fusion,
    }

    if args.command not in dispatch:
        parser.print_help()
        sys.exit(1)

    dispatch[args.command](args)


if __name__ == "__main__":
    main()
