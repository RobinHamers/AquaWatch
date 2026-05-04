#!/usr/bin/env python3
"""
AquaWatch — command-line interface.

Usage:
    python run.py <command> [--reservoir NAME] [options]

Run `python run.py --help` for full command list.
Reservoir defaults to 'serre_poncon' when --reservoir is omitted.
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

CLOUD_MAX = 30.0
BANDS     = ["B03", "B04", "B05", "B08", "B8A", "SCL"]


# ── Reservoir config resolution ───────────────────────────────────────────────

def _resolve(args) -> dict:
    """Return per-reservoir path and config dict from parsed args."""
    from config import get_reservoir
    name = getattr(args, "reservoir", "serre_poncon") or "serre_poncon"
    cfg  = get_reservoir(name)

    raw_dir       = PROJECT_ROOT / "data" / "raw" / name
    processed_dir = PROJECT_ROOT / "data" / "processed" / name
    ts_csv        = PROJECT_ROOT / "outputs" / "timeseries" / f"{name}_wqi.csv"
    alerts_dir    = PROJECT_ROOT / "outputs" / "alerts"
    alerts_json   = alerts_dir / f"{name}_alerts.json"
    maps_dir      = PROJECT_ROOT / "outputs" / "maps" / name

    return {
        "name":          name,
        "display_name":  cfg["name"],
        "epsg":          cfg["epsg"],
        "bbox":          cfg["bbox"],
        "geojson":       Path(cfg["geojson"]),
        "known_blooms":  cfg["known_blooms"],
        "raw_dir":       raw_dir,
        "processed_dir": processed_dir,
        "ts_csv":        ts_csv,
        "alerts_dir":    alerts_dir,
        "alerts_json":   alerts_json,
        "maps_dir":      maps_dir,
        "s3_raw":        PROJECT_ROOT / "data" / "raw_s3" / name,
        "s3_proc":       PROJECT_ROOT / "data" / "processed_s3" / name,
        "s3_csv":        PROJECT_ROOT / "outputs" / "timeseries" / f"{name}_s3_wqi.csv",
        "fused_csv":     PROJECT_ROOT / "outputs" / "timeseries" / f"{name}_fused.csv",
    }


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


def _scene_done(processed_dir: Path, scene_id: str) -> bool:
    return (processed_dir / scene_id / "clipped" / "B08_clipped.tif").exists()


# ── Commands ──────────────────────────────────────────────────────────────────

def cmd_setup(args) -> None:
    """Create directory structure and validate credentials."""
    dirs = [
        PROJECT_ROOT / "data" / "reservoir",
        PROJECT_ROOT / "outputs" / "timeseries",
        PROJECT_ROOT / "outputs" / "alerts",
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
            print("\nCreated .env from template — fill in CDSE credentials.")
        else:
            env_file.write_text("CDSE_USERNAME=\nCDSE_PASSWORD=\n")
            print("\nCreated empty .env — fill in CDSE credentials.")
    else:
        from dotenv import load_dotenv
        load_dotenv(env_file)
        user = os.environ.get("CDSE_USERNAME", "")
        print(f"\n.env found — CDSE_USERNAME={'set' if user else 'MISSING'}")

    print("\nSetup complete.")


def cmd_download(args) -> None:
    """Search and download Sentinel-2 scenes for a date range."""
    from download import search_sentinel2, download_scene
    cfg = _resolve(args)
    username, password = _require_env()

    logger.info(
        "[%s] Searching %s → %s  cloud ≤ %.0f%%",
        cfg["name"], args.start, args.end, CLOUD_MAX,
    )
    scenes = search_sentinel2(
        bbox=cfg["bbox"],
        date_start=args.start,
        date_end=args.end,
        cloud_cover_max=CLOUD_MAX,
        max_results=200,
    )
    if not scenes:
        logger.error("No scenes found.")
        sys.exit(1)

    raw_dir = cfg["raw_dir"]
    proc_dir = cfg["processed_dir"]
    to_dl = [s for s in scenes if not _scene_done(proc_dir, s["name"].replace(".SAFE", ""))]
    print(f"\nFound {len(scenes)} scene(s) — {len(to_dl)} to download.\n")

    for scene in to_dl:
        scene_id = scene["name"].replace(".SAFE", "")
        try:
            download_scene(
                scene=scene,
                output_dir=raw_dir / scene_id,
                username=username,
                password=password,
                bands=BANDS,
            )
        except Exception:
            logger.exception("Download failed for %s", scene_id)


def cmd_process(args) -> None:
    """Apply cloud masking and clip all raw scenes not yet processed."""
    from preprocess import apply_cloud_mask, clip_to_reservoir
    cfg = _resolve(args)

    raw_dir  = cfg["raw_dir"]
    proc_dir = cfg["processed_dir"]

    if not raw_dir.exists():
        logger.error("No raw data for %s at %s", cfg["name"], raw_dir)
        sys.exit(1)

    raw_scenes = [d for d in raw_dir.iterdir() if d.is_dir()]
    to_process = [d for d in raw_scenes if not _scene_done(proc_dir, d.name)]
    print(f"{len(raw_scenes)} raw scene(s), {len(to_process)} to process.\n")

    for scene_dir in to_process:
        scene_id = scene_dir.name
        jp2s = {f.stem.split("_")[2]: f for f in scene_dir.glob("*.jp2")}
        if "SCL" not in jp2s:
            logger.warning("%s missing SCL — skipping", scene_id)
            continue

        scl_path = jp2s.pop("SCL")
        masked_dir  = proc_dir / scene_id / "masked"
        clipped_dir = proc_dir / scene_id / "clipped"

        try:
            masked = apply_cloud_mask(
                band_paths={k: v for k, v in jp2s.items()},
                scl_path=scl_path,
                output_dir=masked_dir,
            )
            masked["SCL"] = scl_path
            clip_to_reservoir(
                band_paths=masked,
                reservoir_geojson=cfg["geojson"],
                output_dir=clipped_dir,
                target_crs=cfg["epsg"],
            )
            logger.info("Processed %s", scene_id[:50])
        except Exception:
            logger.exception("Processing failed for %s", scene_id)


def cmd_indices(args) -> None:
    """Compute water quality indices for all fully clipped scenes."""
    from indices import compute_all_indices
    cfg = _resolve(args)

    proc_dir = cfg["processed_dir"]
    if not proc_dir.exists():
        logger.error("No processed data for %s at %s", cfg["name"], proc_dir)
        sys.exit(1)

    scene_dirs = [
        d for d in proc_dir.iterdir()
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
    cfg = _resolve(args)

    output_csv = cfg["ts_csv"]
    df = build_timeseries(processed_dir=cfg["processed_dir"], output_path=output_csv)
    if df.empty:
        logger.error("No scenes processed — run download + process + indices first")
        sys.exit(1)

    plot_path = output_csv.with_suffix(".png")
    plot_timeseries(df=df, output_path=plot_path)
    print(f"\nTime series [{cfg['display_name']}]: {len(df)} scenes")
    print(f"CSV  → {output_csv.relative_to(PROJECT_ROOT)}")
    print(f"Plot → {plot_path.relative_to(PROJECT_ROOT)}")


def cmd_alerts(args) -> None:
    """Run anomaly detection and save alert CSV + JSON."""
    import pandas as pd
    from alerts import (
        compute_rolling_baseline, detect_alerts, save_alerts,
        summarize_alerts, apply_seasonal_filter, flag_isolated_spikes,
        print_validation_report,
    )
    from datetime import date as date_cls
    cfg = _resolve(args)

    ts_csv = cfg["ts_csv"]
    if not ts_csv.exists():
        logger.error("Run timeseries first: python run.py timeseries --reservoir %s", cfg["name"])
        sys.exit(1)

    df = pd.read_csv(ts_csv, parse_dates=["date"])
    df = df.set_index("date").sort_index()
    df = compute_rolling_baseline(df)

    alerts = detect_alerts(df, z_score_threshold=1.5, reservoir_name=cfg["name"])
    alerts = flag_isolated_spikes(alerts)
    alerts = apply_seasonal_filter(alerts)
    save_alerts(alerts, cfg["alerts_dir"], cfg["name"])
    summarize_alerts(alerts)

    bloom_periods = [
        (date_cls.fromisoformat(b["start"]), date_cls.fromisoformat(b["end"]), b["label"])
        for b in cfg["known_blooms"]
    ]
    print_validation_report(df, alerts, bloom_periods, reservoir_name=cfg["display_name"])


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
    cfg = _resolve(args)

    ts_csv      = cfg["ts_csv"]
    alerts_json = cfg["alerts_json"]
    maps_dir    = cfg["maps_dir"]

    df = pd.read_csv(ts_csv, parse_dates=["date"])
    with open(alerts_json) as fh:
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
    out = maps_dir / "dashboard.png"
    maps_dir.mkdir(parents=True, exist_ok=True)
    plot_dashboard(df=df, alerts=alerts, output_path=out)
    print(f"Dashboard → {out.relative_to(PROJECT_ROOT)}")


def cmd_check(args) -> None:
    """Check a specific date against the historical baseline."""
    import pandas as pd
    from datetime import date as date_cls
    from alerts import check_new_scene
    cfg = _resolve(args)

    ts_csv = cfg["ts_csv"]
    if not ts_csv.exists():
        logger.error("No time series CSV found. Run the full pipeline first.")
        sys.exit(1)

    target = date_cls.fromisoformat(args.date)
    df = pd.read_csv(ts_csv, parse_dates=["date"])
    df["_date"] = df["date"].dt.date
    df["_gap"] = (df["_date"] - target).abs().apply(lambda x: x.days)
    closest = df.nsmallest(1, "_gap").iloc[0]
    gap_days = int(closest["_gap"])

    if gap_days > 10:
        print(f"\nNo processed scene within 10 days of {target}.")
        print(f"Closest is {closest['_date']} ({gap_days} days away).")
        sys.exit(0)

    if gap_days > 0:
        print(f"No scene for {target} — using closest: {closest['_date']} ({gap_days}d away)")

    stats = {
        "date": str(closest["_date"]),
        "ndci_water_mean":      closest.get("ndci_water_mean", float("nan")),
        "ndci_water_p90":       closest.get("ndci_water_p90",  float("nan")),
        "turbidity_water_mean": closest.get("turbidity_water_mean", float("nan")),
        "ndci_water_n":         int(closest.get("ndci_water_n", 0)),
    }
    hist = df[df["_date"] < target].drop(columns=["_date", "_gap"]).set_index("date")

    alert = check_new_scene(
        new_scene_stats=stats,
        historical_df=hist,
        reservoir_name=cfg["name"],
        z_score_threshold=1.5,
    )

    print()
    if alert:
        print(f"  ⚠  ALERT [{cfg['display_name']}] — {alert.severity}")
        print(f"     Date     : {alert.date}")
        print(f"     NDCI     : {alert.ndci_mean:.4f}")
        print(f"     Z-score  : {alert.z_score:.2f}")
        print(f"     Baseline : {alert.baseline_ndci:.4f} ± {alert.baseline_std:.4f}")
        print(f"     Pixels   : {alert.valid_pixels:,}")
    else:
        print(f"  ✓  ALL CLEAR [{cfg['display_name']}] — {stats['date']}")
        print(f"     NDCI={stats['ndci_water_mean']:.4f}")
    print()


def cmd_s3_download(args) -> None:
    """Search and download Sentinel-3 OLCI WFR scenes for a date range."""
    from s3_download import search_sentinel3_olci, download_s3_scene
    cfg = _resolve(args)
    username, password = _require_env()

    logger.info("[%s] Searching S3 OLCI %s → %s", cfg["name"], args.start, args.end)
    scenes = search_sentinel3_olci(
        bbox=cfg["bbox"],
        date_start=args.start,
        date_end=args.end,
        max_results=600,
    )
    if not scenes:
        logger.error("No Sentinel-3 scenes found.")
        sys.exit(1)

    s3_raw = cfg["s3_raw"]
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
    cfg = _resolve(args)

    try:
        from netCDF4 import Dataset as NC4Dataset
    except ImportError:
        logger.error(
            "netCDF4 is required for S3 processing. "
            "Install with: conda install -c conda-forge netcdf4"
        )
        sys.exit(1)

    s3_raw  = cfg["s3_raw"]
    s3_proc = cfg["s3_proc"]
    scene_dirs = [d for d in s3_raw.iterdir() if d.is_dir()] if s3_raw.exists() else []
    print(f"{len(scene_dirs)} raw S3 scene(s) to process.\n")

    for scene_dir in scene_dirs:
        geo_nc = scene_dir / "geo_coordinates.nc"
        if not geo_nc.exists():
            logger.warning("%s missing geo_coordinates.nc — skipping", scene_dir.name)
            continue

        with NC4Dataset(geo_nc) as ds:
            lat = ds["latitude"][:]
            lon = ds["longitude"][:]

        bands   = {"Oa08_reflectance": scene_dir / "Oa08_reflectance.nc",
                   "Oa11_reflectance": scene_dir / "Oa11_reflectance.nc"}
        wqsf_nc = scene_dir / "WQSF.nc"
        tif_dir = s3_proc / scene_dir.name / "tif"
        tif_dir.mkdir(parents=True, exist_ok=True)

        band_tifs: dict[str, Path] = {}
        for band_name, nc_path in bands.items():
            if not nc_path.exists():
                continue
            out_tif = tif_dir / f"{band_name}.tif"
            _nc_to_geotiff(nc_path, band_name, lat, lon, out_tif, target_crs=cfg["epsg"])
            band_tifs[band_name] = out_tif

        if wqsf_nc.exists():
            wqsf_tif = tif_dir / "WQSF.tif"
            if not wqsf_tif.exists():
                _nc_to_geotiff(wqsf_nc, "WQSF", lat, lon, wqsf_tif, target_crs=cfg["epsg"])
            masked = apply_wqsf_mask(
                band_paths=band_tifs,
                wqsf_path=wqsf_tif,
                output_dir=s3_proc / scene_dir.name / "masked",
            )
            clip_s3_to_reservoir(
                band_paths=masked,
                reservoir_geojson=cfg["geojson"],
                output_dir=s3_proc / scene_dir.name / "clipped",
            )
        logger.info("Processed S3 %s", scene_dir.name[:50])


def cmd_s3_timeseries(args) -> None:
    """Build S3 NDCI time series CSV from all processed S3 scenes."""
    import numpy as np
    import pandas as pd
    import rasterio
    from indices import compute_s3_ndci
    cfg = _resolve(args)

    s3_proc = cfg["s3_proc"]
    out_csv = cfg["s3_csv"]

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

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)
    print(f"\nS3 time series [{cfg['display_name']}]: {len(df)} scenes → {out_csv.relative_to(PROJECT_ROOT)}")


def cmd_fusion(args) -> None:
    """Run S2/S3 fusion analysis and generate fused dashboard."""
    import pandas as pd
    from datetime import date as date_cls
    from fusion import build_fused_timeseries, detect_s3_precursor_alerts, print_fusion_report
    from visualize import plot_fused_dashboard
    from alerts import compute_rolling_baseline, detect_alerts, flag_isolated_spikes, apply_seasonal_filter
    cfg = _resolve(args)

    s2_csv = cfg["ts_csv"]
    s3_csv = cfg["s3_csv"]
    for path in (s2_csv, s3_csv):
        if not path.exists():
            logger.error("Missing: %s", path)
            sys.exit(1)

    fused_df = build_fused_timeseries(s2_csv, s3_csv, cfg["fused_csv"])

    s2_df = pd.read_csv(s2_csv, parse_dates=["date"])
    s3_df = pd.read_csv(s3_csv, parse_dates=["date"])

    s2_idx = s2_df.set_index("date").sort_index()
    s2_idx = compute_rolling_baseline(s2_idx)
    alerts  = detect_alerts(s2_idx, z_score_threshold=1.5, reservoir_name=cfg["name"])
    alerts  = flag_isolated_spikes(alerts)
    alerts  = apply_seasonal_filter(alerts)

    bloom_periods = [
        (date_cls.fromisoformat(b["start"]), date_cls.fromisoformat(b["end"]), b["label"])
        for b in cfg["known_blooms"]
    ]
    events = detect_s3_precursor_alerts(fused_df, bloom_periods)
    print_fusion_report(fused_df, events, len(s3_df), len(s2_df))

    maps_dir = cfg["maps_dir"]
    maps_dir.mkdir(parents=True, exist_ok=True)
    dash_path = maps_dir / "dashboard_fused.png"
    plot_fused_dashboard(
        s2_df=s2_df, s3_df=s3_df,
        alerts=alerts, precursor_events=events,
        output_path=dash_path,
    )
    print(f"Fused dashboard → {dash_path.relative_to(PROJECT_ROOT)}")


def cmd_compare(args) -> None:
    """Generate cross-reservoir comparison dashboard."""
    import json
    from datetime import date as date_cls
    import pandas as pd
    from config import RESERVOIRS
    from alerts import Alert
    from visualize import plot_comparison_dashboard

    reservoirs_data = {}
    for res_name, res_cfg in RESERVOIRS.items():
        ts_csv    = PROJECT_ROOT / "outputs" / "timeseries" / f"{res_name}_wqi.csv"
        alerts_json = PROJECT_ROOT / "outputs" / "alerts" / f"{res_name}_alerts.json"
        if not ts_csv.exists() or not alerts_json.exists():
            logger.warning("Skipping %s — outputs not found", res_name)
            continue

        df = pd.read_csv(ts_csv, parse_dates=["date"])
        with open(alerts_json) as fh:
            payload = json.load(fh)
        alerts_list = [
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
        reservoirs_data[res_name] = {
            "timeseries": df,
            "alerts": alerts_list,
            "config": res_cfg,
        }

    if len(reservoirs_data) < 2:
        logger.error(
            "Need outputs for at least 2 reservoirs. "
            "Run: python run.py run-all --reservoir serre_poncon ... && "
            "python run.py run-all --reservoir entrepenhas ..."
        )
        sys.exit(1)

    out = PROJECT_ROOT / "outputs" / "demo" / "comparison_dashboard.png"
    out.parent.mkdir(parents=True, exist_ok=True)
    plot_comparison_dashboard(reservoirs=reservoirs_data, output_path=out)
    print(f"Comparison dashboard → {out.relative_to(PROJECT_ROOT)}")


def cmd_run_all(args) -> None:
    """Run the full S2 pipeline end to end for one reservoir."""
    cfg = _resolve(args)
    print(f"=== AquaWatch — Full Pipeline [{cfg['display_name']}] ===\n")

    class _NS:
        pass

    ns = _NS()
    ns.start    = args.start
    ns.end      = args.end
    ns.reservoir = cfg["name"]

    for step, fn in [
        ("download",   cmd_download),
        ("process",    cmd_process),
        ("indices",    cmd_indices),
        ("timeseries", cmd_timeseries),
        ("alerts",     cmd_alerts),
        ("maps",       cmd_maps),
    ]:
        print(f"\n── {step} ──")
        fn(ns)

    print("\n=== Done ===")
    print(f"  Time series : {cfg['ts_csv'].relative_to(PROJECT_ROOT)}")
    print(f"  Alerts      : outputs/alerts/{cfg['name']}_alerts.{{csv,json}}")
    print(f"  Dashboard   : outputs/maps/{cfg['name']}/dashboard.png")


# ── Argument parser ───────────────────────────────────────────────────────────

def _add_reservoir(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--reservoir", default="serre_poncon", metavar="NAME",
        help="Reservoir key from config.py (default: serre_poncon)",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python run.py",
        description="AquaWatch — multi-reservoir Sentinel-2 water quality monitor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run.py setup
  python run.py download --reservoir serre_poncon --start 2024-06-01 --end 2024-08-31
  python run.py run-all  --reservoir entrepenhas  --start 2022-04-01 --end 2023-10-31
  python run.py check    --reservoir serre_poncon --date 2024-08-21
  python run.py compare
        """,
    )
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")

    # Commands that don't need --reservoir
    sub.add_parser("setup",   help="Create directory structure and validate credentials")
    sub.add_parser("compare", help="Generate cross-reservoir comparison dashboard")

    # Commands that need --reservoir
    for name, helpstr in [
        ("process",       "Cloud-mask and clip all downloaded S2 raw scenes"),
        ("indices",       "Compute NDCI / NDWI / turbidity index rasters"),
        ("timeseries",    "Aggregate S2 scene statistics into a time series CSV"),
        ("alerts",        "Run anomaly detection and generate alert log"),
        ("maps",          "Generate spatial maps for all alerts"),
        ("dashboard",     "Generate summary dashboard PNG"),
        ("s3-process",    "WQSF-mask, reproject, and clip downloaded S3 scenes"),
        ("s3-timeseries", "Build S3 NDCI time series CSV"),
        ("fusion",        "Run S2/S3 fusion analysis and generate fused dashboard"),
    ]:
        p = sub.add_parser(name, help=helpstr)
        _add_reservoir(p)

    dl = sub.add_parser("download", help="Download Sentinel-2 scenes for a date range")
    _add_reservoir(dl)
    dl.add_argument("--start", required=True, metavar="YYYY-MM-DD")
    dl.add_argument("--end",   required=True, metavar="YYYY-MM-DD")

    s3dl = sub.add_parser("s3-download", help="Download Sentinel-3 OLCI WFR scenes for a date range")
    _add_reservoir(s3dl)
    s3dl.add_argument("--start", required=True, metavar="YYYY-MM-DD")
    s3dl.add_argument("--end",   required=True, metavar="YYYY-MM-DD")

    ra = sub.add_parser("run-all", help="Run full S2 pipeline end to end")
    _add_reservoir(ra)
    ra.add_argument("--start", required=True, metavar="YYYY-MM-DD")
    ra.add_argument("--end",   required=True, metavar="YYYY-MM-DD")

    ch = sub.add_parser("check", help="Check a date against historical baseline (operational mode)")
    _add_reservoir(ch)
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
        "compare":       cmd_compare,
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
