"""Cloud masking, spatial clipping, and resampling for Sentinel-2 L2A bands."""

import logging
from pathlib import Path

import numpy as np
import geopandas as gpd
import rasterio
from rasterio.crs import CRS
from rasterio.mask import mask as rio_mask
from rasterio.warp import reproject, Resampling
from shapely.geometry import mapping

logger = logging.getLogger(__name__)

# SCL classes that indicate unusable pixels (cloud, shadow, snow, saturated)
INVALID_SCL_CLASSES: frozenset[int] = frozenset({0, 1, 3, 8, 9, 10, 11})

# Skip a scene if fewer than this fraction of reservoir pixels are valid
MIN_VALID_FRACTION: float = 0.5

# Bands at 20m that need resampling to 10m
BANDS_20M: frozenset[str] = frozenset({"B05", "B8A", "B11", "B12", "SCL"})


def apply_cloud_mask(
    band_paths: dict[str, Path],
    scl_path: Path,
    output_dir: Path,
) -> dict[str, Path]:
    """Apply SCL-based cloud mask to all bands.

    Pixels in SCL classes 0, 1, 3, 8, 9, 10, 11 are set to NaN.
    Input JP2 files are read as uint16; outputs are float32 GeoTIFFs.

    Parameters
    ----------
    band_paths : mapping of band name → JP2 file path (excludes SCL)
    scl_path : path to the SCL band JP2
    output_dir : directory for masked output files

    Returns
    -------
    Dict mapping band name → masked GeoTIFF path.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    with rasterio.open(scl_path) as src:
        scl = src.read(1)
        scl_transform = src.transform
        scl_crs = src.crs
        scl_shape = src.shape

    valid_mask = ~np.isin(scl, list(INVALID_SCL_CLASSES))
    logger.debug(
        "SCL valid pixels: %d / %d (%.1f%%)",
        valid_mask.sum(),
        valid_mask.size,
        100 * valid_mask.mean(),
    )

    result: dict[str, Path] = {}

    for band, path in band_paths.items():
        with rasterio.open(path) as src:
            data = src.read(1).astype(np.float32)
            profile = src.profile.copy()
            band_transform = src.transform

        # Resample validity mask to band pixel grid if resolutions differ
        if data.shape != scl_shape:
            mask_float = valid_mask.astype(np.float32)
            resampled_mask = np.empty(data.shape, dtype=np.float32)
            reproject(
                source=mask_float,
                destination=resampled_mask,
                src_transform=scl_transform,
                src_crs=scl_crs,
                dst_transform=band_transform,
                dst_crs=scl_crs,
                resampling=Resampling.nearest,
            )
            band_valid = resampled_mask > 0.5
        else:
            band_valid = valid_mask

        data[~band_valid] = np.nan

        profile.update(dtype="float32", nodata=np.nan, driver="GTiff")
        out_path = output_dir / (path.stem + "_masked.tif")
        with rasterio.open(out_path, "w", **profile) as dst:
            dst.write(data, 1)

        result[band] = out_path
        logger.debug("Cloud-masked %s → %s", band, out_path.name)

    return result


def clip_to_reservoir(
    band_paths: dict[str, Path],
    reservoir_geojson: Path,
    output_dir: Path,
    target_crs: str = "EPSG:32632",
) -> dict[str, Path]:
    """Clip all bands to the reservoir extent and resample 20m bands to 10m.

    The reservoir polygon is reprojected from EPSG:4326 to target_crs before
    clipping. 20m bands are resampled to 10m to align with B03/B04/B08.
    SCL uses nearest-neighbour resampling; all other bands use bilinear.

    Parameters
    ----------
    band_paths : mapping of band name → masked GeoTIFF path
    reservoir_geojson : path to reservoir polygon (EPSG:4326)
    output_dir : directory for clipped output files
    target_crs : expected CRS of input rasters (EPSG:32632 for Serre-Ponçon)

    Returns
    -------
    Dict mapping band name → clipped GeoTIFF path.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Detect actual CRS from the first band (tiles near UTM zone boundaries may be
    # in EPSG:32631 instead of 32632, e.g. T31TGK covering Serre-Ponçon)
    first_path = next(iter(band_paths.values()))
    with rasterio.open(first_path) as src:
        actual_crs = src.crs
    actual_epsg = actual_crs.to_epsg()
    if actual_epsg != int(target_crs.split(":")[1]):
        logger.info(
            "Raster CRS is EPSG:%d (not %s) — reprojecting reservoir polygon to match",
            actual_epsg,
            target_crs,
        )
        target_crs = f"EPSG:{actual_epsg}"

    target_epsg = int(target_crs.split(":")[1])

    gdf = gpd.read_file(reservoir_geojson)
    gdf_proj = gdf.to_crs(target_crs)
    geom = [mapping(gdf_proj.geometry.iloc[0])]

    # First pass: clip all bands and store clipped arrays + profiles
    clipped: dict[str, tuple[np.ndarray, dict]] = {}

    for band, path in band_paths.items():
        with rasterio.open(path) as src:
            # Use filled=False to get a masked array — avoids the float NaN / int
            # incompatibility when the source file is still an integer JP2.
            data, clip_transform = rio_mask(src, geom, crop=True, filled=False)
            arr = data[0].astype(np.float32)
            arr[data[0].mask] = np.nan
            profile = src.profile.copy()
            profile.update(
                driver="GTiff",
                dtype="float32",
                nodata=np.nan,
                height=arr.shape[0],
                width=arr.shape[1],
                transform=clip_transform,
                count=1,
            )
        clipped[band] = (arr, profile)

    # Determine the reference 10m transform/shape from any 10m band
    ref_profile: dict | None = None
    for band in ("B03", "B04", "B08", "B02"):
        if band in clipped:
            ref_profile = clipped[band][1]
            break

    result: dict[str, Path] = {}

    for band, (data, profile) in clipped.items():
        if band in BANDS_20M and ref_profile is not None:
            # Resample 20m clipped data to match 10m reference grid
            resampling_method = Resampling.nearest if band == "SCL" else Resampling.bilinear
            destination = np.full(
                (ref_profile["height"], ref_profile["width"]),
                np.nan,
                dtype=np.float32,
            )
            reproject(
                source=data,
                destination=destination,
                src_transform=profile["transform"],
                src_crs=CRS.from_epsg(target_epsg),
                dst_transform=ref_profile["transform"],
                dst_crs=CRS.from_epsg(target_epsg),
                resampling=resampling_method,
                src_nodata=np.nan,
                dst_nodata=np.nan,
            )
            out_data = destination
            out_profile = {**profile, **{
                "height": ref_profile["height"],
                "width": ref_profile["width"],
                "transform": ref_profile["transform"],
            }}
        else:
            out_data = data
            out_profile = profile

        out_path = output_dir / f"{band}_clipped.tif"

        with rasterio.open(out_path, "w", **out_profile) as dst:
            dst.write(out_data, 1)

        result[band] = out_path
        logger.info("Clipped %s → shape (%d, %d)", band, out_data.shape[0], out_data.shape[1])

    return result


def count_valid_pixels(band_path: Path) -> tuple[int, int]:
    """Count valid (non-NaN / non-nodata) pixels in a raster.

    Returns
    -------
    (valid_pixel_count, total_pixel_count)
    """
    with rasterio.open(band_path) as src:
        data = src.read(1, masked=True)
    valid = int(np.count_nonzero(~data.mask))
    total = int(data.size)
    return valid, total
