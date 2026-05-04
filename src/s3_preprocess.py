"""Sentinel-3 OLCI WFR preprocessing: WQSF masking and reservoir clipping."""

import json
import logging
from pathlib import Path

import numpy as np
import rasterio
from rasterio.mask import mask as rio_mask

logger = logging.getLogger(__name__)

# WQSF bitmask: keep pixels where WATER (bit 1) or INLAND_WATER (bit 2) is set,
# and INVALID (bit 0) / CLOUD (bit 25) / CLOUD_AMBIGUOUS (bit 26) / CLOUD_MARGIN (bit 27) are clear.
_WATER_BITS   = (1 << 1) | (1 << 2)
_REJECT_BITS  = (1 << 0) | (1 << 25) | (1 << 26) | (1 << 27)


def apply_wqsf_mask(
    band_paths: dict[str, Path],
    wqsf_path: Path,
    output_dir: Path,
) -> dict[str, Path]:
    """Apply WQSF water quality flag mask to Sentinel-3 band GeoTIFFs.

    Keeps only pixels classified as water (WATER or INLAND_WATER bits set)
    and not flagged as invalid, cloud, or cloud-margin.

    Parameters
    ----------
    band_paths : mapping of band name → GeoTIFF path (already reprojected)
    wqsf_path  : WQSF flag GeoTIFF (uint32)
    output_dir : directory for masked outputs

    Returns
    -------
    Dict of band name → masked GeoTIFF path.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    with rasterio.open(wqsf_path) as src:
        wqsf = src.read(1).astype(np.uint32)

    water_mask = ((wqsf & _WATER_BITS) > 0) & ((wqsf & _REJECT_BITS) == 0)

    masked_paths: dict[str, Path] = {}
    for band_name, band_path in band_paths.items():
        out_path = output_dir / f"{band_name}_masked.tif"
        if out_path.exists():
            logger.debug("Already masked: %s", out_path.name)
            masked_paths[band_name] = out_path
            continue

        with rasterio.open(band_path) as src:
            data = src.read(1).astype(np.float32)
            profile = src.profile.copy()

        masked = np.where(water_mask, data, np.nan)
        profile.update(dtype="float32", nodata=np.nan)
        with rasterio.open(out_path, "w", **profile) as dst:
            dst.write(masked, 1)

        masked_paths[band_name] = out_path
        logger.info(
            "Masked %s → %s  (%.1f%% water pixels)",
            band_name, out_path.name, 100.0 * water_mask.mean(),
        )

    return masked_paths


def clip_s3_to_reservoir(
    band_paths: dict[str, Path],
    reservoir_geojson: Path,
    output_dir: Path,
) -> dict[str, Path]:
    """Clip Sentinel-3 GeoTIFFs to the reservoir polygon.

    The polygon is reprojected to match each raster's CRS before clipping.
    At 300m resolution the reservoir (~28 km²) may span only ~3–4 pixels
    across its narrowest extent; statistics must be interpreted accordingly.

    Parameters
    ----------
    band_paths        : mapping of band name → masked GeoTIFF path
    reservoir_geojson : reservoir polygon (any CRS)
    output_dir        : directory for clipped outputs

    Returns
    -------
    Dict of band name → clipped GeoTIFF path.
    """
    import pyproj
    from shapely.geometry import shape, mapping
    from shapely.ops import transform as shapely_transform

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(reservoir_geojson) as fh:
        gj = json.load(fh)

    features = gj["features"] if gj["type"] == "FeatureCollection" else [gj]
    geoms_wgs84 = [shape(f["geometry"]) for f in features]

    clipped_paths: dict[str, Path] = {}
    for band_name, band_path in band_paths.items():
        out_path = output_dir / f"{band_name}_clipped.tif"
        if out_path.exists():
            logger.debug("Already clipped: %s", out_path.name)
            clipped_paths[band_name] = out_path
            continue

        with rasterio.open(band_path) as src:
            raster_crs = src.crs.to_string()

        proj = pyproj.Transformer.from_crs(
            "EPSG:4326", raster_crs, always_xy=True
        ).transform
        shapes = [mapping(shapely_transform(proj, g)) for g in geoms_wgs84]

        with rasterio.open(band_path) as src:
            try:
                out_data, out_transform = rio_mask(src, shapes, crop=True, filled=False)
            except Exception:
                logger.warning(
                    "Clip failed for S3 %s — reservoir may be outside swath", band_name
                )
                continue

            arr = out_data[0].astype(np.float32)
            arr = np.where(out_data.mask[0], np.nan, arr)
            profile = src.profile.copy()
            profile.update(
                driver="GTiff", dtype="float32", nodata=np.nan,
                height=arr.shape[0], width=arr.shape[1],
                transform=out_transform, count=1,
            )

        with rasterio.open(out_path, "w", **profile) as dst:
            dst.write(arr, 1)

        logger.info("Clipped S3 %s → %s", band_name, out_path.name)
        clipped_paths[band_name] = out_path

    return clipped_paths
