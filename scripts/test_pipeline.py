#!/usr/bin/env python3
"""
Weekend 1 integration test: search → download → cloud mask → clip → preview.

Usage:
    conda activate aquawatch
    python scripts/test_pipeline.py
"""

import logging
import os
import sys
from pathlib import Path

import numpy as np
import matplotlib.pyplot as plt
import rasterio
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
logger = logging.getLogger("test_pipeline")

BBOX = [6.28, 44.49, 6.45, 44.62]
DATE_START = "2024-06-01"
DATE_END = "2024-08-31"
CLOUD_MAX = 30.0
BANDS = ["B03", "B04", "B05", "B8A", "SCL"]
N_SCENES = 3


def save_rgb_preview(
    clipped_paths: dict[str, Path],
    output_path: Path,
    percentile: int = 2,
) -> None:
    """Save a percentile-stretched RGB PNG (R=B04, G=B03, B=B03).

    B02 is not downloaded in this test, so B03 is used for both G and B
    channels, producing a red-edge-enhanced pseudo-colour image.
    """
    with rasterio.open(clipped_paths["B04"]) as src:
        red = src.read(1, masked=True).astype(float)
    with rasterio.open(clipped_paths["B03"]) as src:
        green = src.read(1, masked=True).astype(float)
    blue = green.copy()

    def stretch(arr: np.ma.MaskedArray) -> np.ndarray:
        valid = arr.compressed()
        if valid.size == 0:
            return np.zeros(arr.shape, dtype=float)
        lo, hi = np.nanpercentile(valid, [percentile, 100 - percentile])
        return np.clip((arr - lo) / max(hi - lo, 1e-9), 0, 1).filled(0)

    rgb = np.dstack([stretch(red), stretch(green), stretch(blue)])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.imsave(str(output_path), rgb)
    logger.info("Saved preview → %s", output_path.relative_to(PROJECT_ROOT))


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env")
    username = os.environ.get("CDSE_USERNAME", "")
    password = os.environ.get("CDSE_PASSWORD", "")
    if not username or not password:
        logger.error(
            "CDSE_USERNAME and CDSE_PASSWORD must be set in .env — "
            "copy .env.template to .env and fill in your credentials."
        )
        sys.exit(1)

    reservoir_geojson = PROJECT_ROOT / "data" / "reservoir" / "serre_poncon.geojson"
    raw_dir = PROJECT_ROOT / "data" / "raw"
    processed_dir = PROJECT_ROOT / "data" / "processed"
    preview_dir = PROJECT_ROOT / "outputs" / "maps" / "previews"

    # ── 1. Catalogue search ────────────────────────────────────────────────
    logger.info("Searching for scenes over Serre-Ponçon (%s → %s) …", DATE_START, DATE_END)
    scenes = search_sentinel2(
        bbox=BBOX,
        date_start=DATE_START,
        date_end=DATE_END,
        cloud_cover_max=CLOUD_MAX,
    )

    if not scenes:
        logger.error("No scenes found. Check credentials and date range.")
        sys.exit(1)

    print(f"\nFound {len(scenes)} scene(s):\n")
    for s in scenes:
        cloud_str = f"{s['cloud_cover']:.1f}%" if s["cloud_cover"] is not None else "N/A"
        print(f"  {s['date']}  cloud={cloud_str:>6}  {s['size_mb']:>7.0f} MB  {s['name']}")

    # ── 2. Process first N scenes ──────────────────────────────────────────
    target_scenes = scenes[:N_SCENES]
    print(f"\nProcessing {len(target_scenes)} scene(s) …\n")

    for scene in tqdm(target_scenes, desc="Scenes", unit="scene"):
        scene_id = scene["name"].replace(".SAFE", "")
        cloud_str = f"{scene['cloud_cover']:.1f}%" if scene["cloud_cover"] is not None else "N/A"
        logger.info("── Scene: %s  (cloud=%s)", scene_id, cloud_str)

        # 2a. Download bands
        scene_raw_dir = raw_dir / scene_id
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
            continue

        scl_path = band_paths.pop("SCL")
        band_paths_no_scl = band_paths  # B03, B04, B05, B8A

        # 2b. Cloud masking
        masked_dir = processed_dir / scene_id / "masked"
        try:
            masked_paths = apply_cloud_mask(
                band_paths=band_paths_no_scl,
                scl_path=scl_path,
                output_dir=masked_dir,
            )
            masked_paths["SCL"] = scl_path
        except Exception:
            logger.exception("Cloud masking failed for %s", scene_id)
            continue

        # 2c. Clip to reservoir + resample 20m → 10m
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
            continue

        # 2d. Valid pixel report
        # SCL is not cloud-masked so its non-NaN count = total pixels inside the
        # reservoir polygon. Use it as the denominator for the cloud-free fraction.
        polygon_pixels, _ = count_valid_pixels(clipped_paths["SCL"])

        print(f"\n  {scene_id}:")
        scene_usable = True
        for band in sorted(clipped_paths):
            valid, total_bbox = count_valid_pixels(clipped_paths[band])
            if band == "SCL":
                print(f"    SCL: {valid}/{total_bbox} in bbox  ({valid/total_bbox:.1%} polygon coverage)")
                continue
            frac = valid / polygon_pixels if polygon_pixels > 0 else 0.0
            flag = "" if frac >= MIN_VALID_FRACTION else "  ⚠ BELOW THRESHOLD"
            print(f"    {band}: {valid:>6}/{polygon_pixels} cloud-free ({frac:.1%}){flag}")
            if band in ("B03", "B04") and frac < MIN_VALID_FRACTION:
                scene_usable = False

        if not scene_usable:
            logger.warning("Scene %s has too many cloudy pixels over reservoir — skipping preview", scene_id)
            continue

        # 2e. RGB preview
        preview_path = preview_dir / f"{scene_id}_preview.png"
        try:
            save_rgb_preview(
                clipped_paths={k: v for k, v in clipped_paths.items() if k in ("B03", "B04")},
                output_path=preview_path,
            )
        except Exception:
            logger.warning("Preview generation failed for %s", scene_id, exc_info=True)

    print(f"\nDone. Preview images (if any) saved to {preview_dir.relative_to(PROJECT_ROOT)}/")


if __name__ == "__main__":
    main()
