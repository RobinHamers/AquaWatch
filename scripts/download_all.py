#!/usr/bin/env python3
"""
Weekend 2: Download all Sentinel-2 L2A scenes for Serre-Ponçon, 2023-04-01 to 2024-10-31.

Skips scenes where data/processed/{scene_id}/clipped/B08_clipped.tif already exists.
Adds B08 to the band set (required for NDWI) and re-runs preprocessing for scenes
that were downloaded in Weekend 1 without B08.

Usage:
    conda activate aquawatch
    python scripts/download_all.py
"""

import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from tqdm import tqdm

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from download import search_sentinel2, download_scene
from preprocess import apply_cloud_mask, clip_to_reservoir, count_valid_pixels, MIN_VALID_FRACTION

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(name)-20s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("download_all")

BBOX = [6.28, 44.49, 6.45, 44.62]
DATE_START = "2023-04-01"
DATE_END = "2024-10-31"
CLOUD_MAX = 30.0
BANDS = ["B03", "B04", "B05", "B08", "B8A", "SCL"]


def scene_is_done(processed_dir: Path, scene_id: str) -> bool:
    """Return True if B08_clipped.tif exists (fully processed marker)."""
    return (processed_dir / scene_id / "clipped" / "B08_clipped.tif").exists()


def process_scene(
    scene: dict,
    raw_dir: Path,
    processed_dir: Path,
    reservoir_geojson: Path,
    username: str,
    password: str,
) -> bool:
    """Download, mask, and clip one scene. Returns True on success."""
    scene_id = scene["name"].replace(".SAFE", "")
    scene_raw_dir = raw_dir / scene_id

    cloud_str = f"{scene['cloud_cover']:.1f}%" if scene["cloud_cover"] is not None else "N/A"
    logger.info("── %s  cloud=%s  %s MB", scene["date"], cloud_str, f"{scene['size_mb']:.0f}")

    try:
        band_paths = download_scene(
            scene=scene,
            output_dir=scene_raw_dir,
            username=username,
            password=password,
            bands=BANDS,
        )
    except Exception:
        logger.exception("Download failed for %s", scene_id)
        return False

    scl_path = band_paths.pop("SCL")

    masked_dir = processed_dir / scene_id / "masked"
    try:
        masked_paths = apply_cloud_mask(
            band_paths=band_paths,
            scl_path=scl_path,
            output_dir=masked_dir,
        )
        masked_paths["SCL"] = scl_path
    except Exception:
        logger.exception("Cloud masking failed for %s", scene_id)
        return False

    clipped_dir = processed_dir / scene_id / "clipped"
    try:
        clipped_paths = clip_to_reservoir(
            band_paths=masked_paths,
            reservoir_geojson=reservoir_geojson,
            output_dir=clipped_dir,
            target_crs="EPSG:32632",
        )
    except Exception:
        logger.exception("Clipping failed for %s", scene_id)
        return False

    polygon_pixels, _ = count_valid_pixels(clipped_paths["SCL"])
    cloud_free_counts = []
    for band in ("B03", "B04"):
        valid, _ = count_valid_pixels(clipped_paths[band])
        frac = valid / polygon_pixels if polygon_pixels > 0 else 0.0
        cloud_free_counts.append((band, frac))

    min_frac = min(f for _, f in cloud_free_counts)
    if min_frac < MIN_VALID_FRACTION:
        logger.warning(
            "Scene %s has too few cloud-free pixels (min %.1f%%) — kept but flagged",
            scene_id, 100 * min_frac,
        )

    logger.info(
        "Done: %s  cloud-free B03=%.1f%% B04=%.1f%%",
        scene_id[:40], 100 * cloud_free_counts[0][1], 100 * cloud_free_counts[1][1],
    )
    return True


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env")
    username = os.environ.get("CDSE_USERNAME", "")
    password = os.environ.get("CDSE_PASSWORD", "")
    if not username or not password:
        logger.error("CDSE_USERNAME and CDSE_PASSWORD must be set in .env")
        sys.exit(1)

    reservoir_geojson = PROJECT_ROOT / "data" / "reservoir" / "serre_poncon.geojson"
    raw_dir = PROJECT_ROOT / "data" / "raw"
    processed_dir = PROJECT_ROOT / "data" / "processed"

    logger.info("Searching %s → %s  cloud ≤ %.0f%%", DATE_START, DATE_END, CLOUD_MAX)
    scenes = search_sentinel2(
        bbox=BBOX,
        date_start=DATE_START,
        date_end=DATE_END,
        cloud_cover_max=CLOUD_MAX,
        max_results=200,
    )

    if not scenes:
        logger.error("No scenes found. Check credentials and date range.")
        sys.exit(1)

    print(f"\nFound {len(scenes)} scene(s) in catalogue\n")

    to_process = []
    already_done = 0
    for scene in scenes:
        scene_id = scene["name"].replace(".SAFE", "")
        if scene_is_done(processed_dir, scene_id):
            already_done += 1
        else:
            to_process.append(scene)

    print(f"  Already done : {already_done}")
    print(f"  To process   : {len(to_process)}")
    print()

    if not to_process:
        print("All scenes already processed.")
        return

    success = 0
    failed = 0
    for scene in tqdm(to_process, desc="Scenes", unit="scene"):
        ok = process_scene(
            scene=scene,
            raw_dir=raw_dir,
            processed_dir=processed_dir,
            reservoir_geojson=reservoir_geojson,
            username=username,
            password=password,
        )
        if ok:
            success += 1
        else:
            failed += 1

    print(f"\nFinished: {success} succeeded, {failed} failed")
    total_done = already_done + success
    print(f"Total scenes ready for time series: {total_done}")


if __name__ == "__main__":
    main()
