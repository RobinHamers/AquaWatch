"""Sentinel-3 OLCI WFR download from CDSE (Copernicus Data Space Ecosystem)."""

import logging
from pathlib import Path

import numpy as np
import requests

logger = logging.getLogger(__name__)

CDSE_SEARCH   = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
CDSE_TOKEN    = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
CDSE_DOWNLOAD = "https://download.dataspace.copernicus.eu/odata/v1"

# S3 OLCI WFR bands needed: Oa08 (Red 665nm), Oa11 (Red Edge 709nm), WQSF flags
S3_BANDS = ["Oa08_reflectance", "Oa11_reflectance", "WQSF"]


def _get_token(username: str, password: str) -> str:
    resp = requests.post(
        CDSE_TOKEN,
        data={
            "grant_type": "password",
            "username": username,
            "password": password,
            "client_id": "cdse-public",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def search_sentinel3_olci(
    bbox: list[float],
    date_start: str,
    date_end: str,
    max_results: int = 600,
) -> list[dict]:
    """Search CDSE catalogue for Sentinel-3 OLCI WFR products.

    Parameters
    ----------
    bbox : [lon_min, lat_min, lon_max, lat_max] in WGS84
    date_start, date_end : "YYYY-MM-DD" strings
    max_results : maximum number of products to return

    Returns
    -------
    List of product metadata dicts with 'Id' and 'Name' keys.
    """
    lon_min, lat_min, lon_max, lat_max = bbox
    footprint = (
        f"POLYGON(({lon_min} {lat_min},{lon_max} {lat_min},"
        f"{lon_max} {lat_max},{lon_min} {lat_max},{lon_min} {lat_min}))"
    )
    params = {
        "$filter": (
            "Collection/Name eq 'SENTINEL-3' and "
            "Attributes/OData.CSC.StringAttribute/any("
            "att:att/Name eq 'productType' and "
            "att/OData.CSC.StringAttribute/Value eq 'OL_2_WFR___') and "
            f"ContentDate/Start gt {date_start}T00:00:00.000Z and "
            f"ContentDate/Start lt {date_end}T23:59:59.000Z and "
            f"OData.CSC.Intersects(area=geography'SRID=4326;{footprint}')"
        ),
        "$top": max_results,
        "$orderby": "ContentDate/Start asc",
    }
    resp = requests.get(CDSE_SEARCH, params=params, timeout=60)
    resp.raise_for_status()
    products = resp.json().get("value", [])
    logger.info("Found %d Sentinel-3 OLCI WFR products", len(products))
    return products


def download_s3_scene(
    scene: dict,
    output_dir: Path,
    username: str,
    password: str,
) -> Path:
    """Download Sentinel-3 WFR band files via CDSE OData Nodes() API.

    Downloads Oa08_reflectance.nc, Oa11_reflectance.nc, WQSF.nc, and
    geo_coordinates.nc from the .SEN3 product directory.

    Parameters
    ----------
    scene : product dict from search_sentinel3_olci()
    output_dir : directory to write downloaded .nc files
    username, password : CDSE credentials

    Returns
    -------
    Path to the output directory containing downloaded files.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    token = _get_token(username, password)
    product_id = scene["Id"]
    product_name = scene["Name"]  # e.g. S3A_OL_2_WFR____20230817T...SEN3

    headers = {"Authorization": f"Bearer {token}"}

    node_url = f"{CDSE_DOWNLOAD}/Products({product_id})/Nodes({product_name})/Nodes"
    resp = requests.get(node_url, headers=headers, timeout=60)
    resp.raise_for_status()
    nodes = resp.json().get("result", [])

    want = {"geo_coordinates.nc"} | {f"{b}.nc" for b in S3_BANDS}
    for node in nodes:
        fname = node["Name"]
        if fname not in want:
            continue
        out_path = output_dir / fname
        if out_path.exists():
            logger.debug("Already downloaded: %s", fname)
            continue

        dl_url = (
            f"{CDSE_DOWNLOAD}/Products({product_id})"
            f"/Nodes({product_name})/Nodes({fname})/$value"
        )
        logger.info("Downloading %s …", fname)
        with requests.get(dl_url, headers=headers, stream=True, timeout=120,
                          allow_redirects=False) as r:
            if r.status_code in (301, 302, 307, 308):
                r2 = requests.get(r.headers["Location"], headers=headers,
                                  stream=True, timeout=120)
                r2.raise_for_status()
                _stream_to_file(r2, out_path)
            else:
                r.raise_for_status()
                _stream_to_file(r, out_path)
        logger.info("Saved %s", out_path)

    return output_dir


def _stream_to_file(response: requests.Response, path: Path) -> None:
    with open(path, "wb") as fh:
        for chunk in response.iter_content(chunk_size=65536):
            fh.write(chunk)


def _nc_to_geotiff(
    nc_path: Path,
    var_name: str,
    lat_array: np.ndarray,
    lon_array: np.ndarray,
    output_path: Path,
    target_crs: str = "EPSG:32631",
) -> Path:
    """Reproject a variable from a NetCDF4 swath file to a UTM GeoTIFF.

    S3 WFR products use irregular lat/lon swath grids (not projected).
    This function regrids to a regular UTM grid at 300m resolution via
    nearest-neighbour interpolation.

    Parameters
    ----------
    nc_path : path to the .nc band file
    var_name : variable name inside the file
    lat_array, lon_array : 2D swath coordinate arrays from geo_coordinates.nc
    output_path : destination GeoTIFF path
    target_crs : output projection (must match S2 tile CRS)

    Returns
    -------
    Path to the written GeoTIFF.
    """
    try:
        from netCDF4 import Dataset as NC4Dataset
    except ImportError as exc:
        raise ImportError(
            "netCDF4 is required for real Sentinel-3 processing. "
            "Install with: conda install -c conda-forge netcdf4"
        ) from exc

    import pyproj
    from scipy.interpolate import griddata
    import rasterio
    from rasterio.transform import from_bounds
    from rasterio.crs import CRS as RioCRS

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with NC4Dataset(nc_path) as ds:
        raw = ds[var_name][:]
        data = np.ma.filled(raw.astype(np.float32), np.nan)
    if data.ndim == 3:
        data = data[0]

    transformer = pyproj.Transformer.from_crs("EPSG:4326", target_crs, always_xy=True)
    x_utm, y_utm = transformer.transform(lon_array.ravel(), lat_array.ravel())

    flat_data = data.ravel()
    valid = np.isfinite(flat_data) & np.isfinite(x_utm) & np.isfinite(y_utm)
    x_v, y_v, d_v = x_utm[valid], y_utm[valid], flat_data[valid]

    x_res = y_res = 300.0
    x_min, x_max = float(x_v.min()), float(x_v.max())
    y_min, y_max = float(y_v.min()), float(y_v.max())
    x_grid = np.arange(x_min, x_max, x_res)
    y_grid = np.arange(y_max, y_min, -y_res)
    xx, yy = np.meshgrid(x_grid, y_grid)

    grid = griddata((x_v, y_v), d_v, (xx, yy), method="nearest").astype(np.float32)

    transform = from_bounds(x_min, y_min, x_max, y_max, len(x_grid), len(y_grid))
    profile = {
        "driver": "GTiff", "dtype": "float32", "nodata": np.nan,
        "width": len(x_grid), "height": len(y_grid), "count": 1,
        "crs": RioCRS.from_string(target_crs),
        "transform": transform,
        "compress": "deflate",
    }
    with rasterio.open(output_path, "w", **profile) as dst:
        dst.write(grid, 1)

    logger.info("Converted %s/%s → %s", nc_path.name, var_name, output_path.name)
    return output_path
