"""Water quality index computation for Sentinel-2 L2A clipped rasters."""

import logging
from pathlib import Path

import numpy as np
import rasterio
from rasterio.crs import CRS

logger = logging.getLogger(__name__)


def validate_reflectance(array: np.ndarray, band_name: str) -> None:
    mean_val = np.nanmean(array)
    if mean_val > 1.0:
        logger.warning(
            "%s: mean value %.1f suggests unscaled DN. Expected range 0.0-1.0 after /10000.",
            band_name, mean_val,
        )


def _read_band(path: Path, scale: bool = True) -> tuple[np.ndarray, dict]:
    with rasterio.open(path) as src:
        data = src.read(1, masked=True).astype(np.float32)
        profile = src.profile.copy()
    arr = np.where(data.mask, np.nan, data.data)
    if scale:
        arr = np.where(arr == 0, np.nan, arr)   # DN=0 is nodata in S2 L2A
        arr = arr / 10000.0
        validate_reflectance(arr, path.name)
    return arr, profile


def _write_index(
    data: np.ndarray,
    profile: dict,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out_profile = {**profile, "dtype": "float32", "nodata": np.nan, "driver": "GTiff", "count": 1}
    with rasterio.open(output_path, "w", **out_profile) as dst:
        dst.write(data, 1)
    return output_path


def compute_ndci(
    b05_path: Path,
    b04_path: Path,
    output_path: Path,
) -> Path:
    """Compute NDCI = (B05 - B04) / (B05 + B04).

    Primary cyanobacteria bloom indicator. Range [-1, 1]; threshold > 0.2 = alert.
    NaN where both inputs are NaN or denominator is zero.
    """
    if output_path.exists():
        logger.debug("NDCI already exists: %s", output_path.name)
        return output_path

    b05, profile = _read_band(b05_path)
    b04, _ = _read_band(b04_path)

    with np.errstate(invalid="ignore", divide="ignore"):
        denom = b05 + b04
        ndci = np.where(denom != 0, (b05 - b04) / denom, np.nan)

    ndci = np.where(np.isnan(b05) | np.isnan(b04), np.nan, ndci)
    _write_index(ndci.astype(np.float32), profile, output_path)
    logger.info("Computed NDCI → %s", output_path.name)
    return output_path


def compute_ndwi(
    b03_path: Path,
    b08_path: Path,
    output_path: Path,
) -> Path:
    """Compute NDWI = (B03 - B08) / (B03 + B08).

    Water detection index. Pixels > 0 are classified as water.
    """
    if output_path.exists():
        logger.debug("NDWI already exists: %s", output_path.name)
        return output_path

    b03, profile = _read_band(b03_path)
    b08, _ = _read_band(b08_path)

    with np.errstate(invalid="ignore", divide="ignore"):
        denom = b03 + b08
        ndwi = np.where(denom != 0, (b03 - b08) / denom, np.nan)

    ndwi = np.where(np.isnan(b03) | np.isnan(b08), np.nan, ndwi)
    _write_index(ndwi.astype(np.float32), profile, output_path)
    logger.info("Computed NDWI → %s", output_path.name)
    return output_path


def compute_turbidity(
    b04_path: Path,
    b03_path: Path,
    output_path: Path,
) -> Path:
    """Compute turbidity proxy = B04 / B03.

    Higher ratio indicates more turbid water. NaN where B03 is zero or NaN.
    """
    if output_path.exists():
        logger.debug("Turbidity already exists: %s", output_path.name)
        return output_path

    b04, profile = _read_band(b04_path)
    b03, _ = _read_band(b03_path)

    with np.errstate(invalid="ignore", divide="ignore"):
        turb = np.where(b03 != 0, b04 / b03, np.nan)

    turb = np.where(np.isnan(b04) | np.isnan(b03), np.nan, turb)
    _write_index(turb.astype(np.float32), profile, output_path)
    logger.info("Computed turbidity → %s", output_path.name)
    return output_path


def apply_water_mask(
    index_path: Path,
    ndwi_path: Path,
    output_path: Path,
    ndwi_threshold: float = 0.1,
) -> Path:
    """Mask index to water pixels only (NDWI > threshold).

    Non-water pixels are set to NaN. Preserves existing NaN coverage.
    Threshold of 0.1 excludes mixed shoreline pixels; use lower values for more permissive masking.
    """
    if output_path.exists():
        logger.debug("Water-masked index already exists: %s", output_path.name)
        return output_path

    index, profile = _read_band(index_path, scale=False)
    ndwi, _ = _read_band(ndwi_path, scale=False)

    water_mask = ndwi > ndwi_threshold
    masked = np.where(water_mask, index, np.nan)
    _write_index(masked.astype(np.float32), profile, output_path)
    logger.info("Applied water mask → %s (%.1f%% water)", output_path.name, 100 * water_mask.mean())
    return output_path


def compute_all_indices(
    band_paths: dict[str, Path],
    output_dir: Path,
) -> dict[str, Path]:
    """Compute NDCI, NDWI, turbidity, and water-masked variants for one scene.

    Parameters
    ----------
    band_paths : mapping of band name → clipped GeoTIFF path.
                 Must contain B03, B04, B05, B08.
    output_dir : directory for index GeoTIFFs

    Returns
    -------
    Dict mapping index name → output Path. Keys: ndci, ndwi, turbidity,
    ndci_water, turbidity_water.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    required = {"B03", "B04", "B05", "B08"}
    missing = required - set(band_paths)
    if missing:
        raise ValueError(f"Missing bands for index computation: {missing}")

    ndci_path = compute_ndci(
        b05_path=band_paths["B05"],
        b04_path=band_paths["B04"],
        output_path=output_dir / "ndci.tif",
    )
    ndwi_path = compute_ndwi(
        b03_path=band_paths["B03"],
        b08_path=band_paths["B08"],
        output_path=output_dir / "ndwi.tif",
    )
    turb_path = compute_turbidity(
        b04_path=band_paths["B04"],
        b03_path=band_paths["B03"],
        output_path=output_dir / "turbidity.tif",
    )
    ndci_water = apply_water_mask(
        index_path=ndci_path,
        ndwi_path=ndwi_path,
        output_path=output_dir / "ndci_water.tif",
    )
    turb_water = apply_water_mask(
        index_path=turb_path,
        ndwi_path=ndwi_path,
        output_path=output_dir / "turbidity_water.tif",
    )

    return {
        "ndci": ndci_path,
        "ndwi": ndwi_path,
        "turbidity": turb_path,
        "ndci_water": ndci_water,
        "turbidity_water": turb_water,
    }
