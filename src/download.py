"""CDSE catalogue search and Sentinel-2 band download."""

import logging
import time
from pathlib import Path
from typing import Optional

import requests
from tqdm import tqdm

CATALOGUE_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1"
TOKEN_URL = (
    "https://identity.dataspace.copernicus.eu"
    "/auth/realms/CDSE/protocol/openid-connect/token"
)
DOWNLOAD_URL = "https://download.dataspace.copernicus.eu/odata/v1"

# Band → (resolution folder, resolution suffix)
BAND_RESOLUTION: dict[str, tuple[str, str]] = {
    "B02": ("R10m", "10m"),
    "B03": ("R10m", "10m"),
    "B04": ("R10m", "10m"),
    "B08": ("R10m", "10m"),
    "B05": ("R20m", "20m"),
    "B8A": ("R20m", "20m"),
    "B11": ("R20m", "20m"),
    "B12": ("R20m", "20m"),
    "SCL": ("R20m", "20m"),
}

logger = logging.getLogger(__name__)


def get_access_token(username: str, password: str) -> str:
    """Obtain an OAuth2 bearer token from the CDSE identity service."""
    data = {
        "client_id": "cdse-public",
        "grant_type": "password",
        "username": username,
        "password": password,
    }
    response = requests.post(TOKEN_URL, data=data, timeout=30)
    response.raise_for_status()
    return response.json()["access_token"]


def search_sentinel2(
    bbox: list[float],
    date_start: str,
    date_end: str,
    cloud_cover_max: float = 30.0,
    max_results: int = 100,
) -> list[dict]:
    """Search the CDSE OData catalogue for Sentinel-2 L2A scenes.

    Parameters
    ----------
    bbox : [west, south, east, north] in EPSG:4326
    date_start : ISO date string, e.g. "2024-06-01"
    date_end : ISO date string, e.g. "2024-08-31"
    cloud_cover_max : maximum scene cloud cover percentage (0–100)
    max_results : OData $top limit

    Returns
    -------
    List of dicts with keys: id, name, date, cloud_cover, size_mb, s3_path
    Sorted by date descending.
    """
    w, s, e, n = bbox
    polygon_wkt = f"POLYGON(({w} {s},{e} {s},{e} {n},{w} {n},{w} {s}))"

    filter_str = (
        "Collection/Name eq 'SENTINEL-2'"
        " and Attributes/OData.CSC.StringAttribute/any("
        "att:att/Name eq 'productType'"
        " and att/OData.CSC.StringAttribute/Value eq 'S2MSI2A')"
        f" and OData.CSC.Intersects(area=geography'SRID=4326;{polygon_wkt}')"
        f" and ContentDate/Start gt {date_start}T00:00:00.000Z"
        f" and ContentDate/Start lt {date_end}T23:59:59.000Z"
        f" and Attributes/OData.CSC.DoubleAttribute/any("
        f"att:att/Name eq 'cloudCover'"
        f" and att/OData.CSC.DoubleAttribute/Value le {cloud_cover_max})"
    )

    params = {
        "$filter": filter_str,
        "$orderby": "ContentDate/Start desc",
        "$top": str(max_results),
        "$expand": "Attributes",
    }

    response = _get_with_retry(f"{CATALOGUE_URL}/Products", params=params)
    items = response.json().get("value", [])

    results = []
    for item in items:
        cloud = next(
            (
                a["Value"]
                for a in item.get("Attributes", [])
                if a.get("Name") == "cloudCover"
            ),
            None,
        )
        results.append(
            {
                "id": item["Id"],
                "name": item["Name"],
                "date": item["ContentDate"]["Start"][:10],
                "cloud_cover": cloud,
                "size_mb": item.get("ContentLength", 0) / 1e6,
                "s3_path": item.get("S3Path", ""),
            }
        )

    logger.info(
        "Found %d scenes between %s and %s (cloud ≤ %.0f%%)",
        len(results),
        date_start,
        date_end,
        cloud_cover_max,
    )
    return results


def download_scene(
    scene: dict,
    output_dir: Path,
    username: str,
    password: str,
    bands: list[str] | tuple[str, ...] = ("B03", "B04", "B05", "B8A", "SCL"),
) -> dict[str, Path]:
    """Download specific bands for a Sentinel-2 L2A scene via OData Nodes().

    Parameters
    ----------
    scene : dict from search_sentinel2() with keys 'id' and 'name'
    output_dir : local directory to save .jp2 files
    username : CDSE username
    password : CDSE password
    bands : band names to download

    Returns
    -------
    Dict mapping band name to local Path.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    token = get_access_token(username, password)
    session = requests.Session()
    session.headers["Authorization"] = f"Bearer {token}"

    product_id = scene["id"]
    safe_name = scene["name"]
    if not safe_name.endswith(".SAFE"):
        safe_name = safe_name + ".SAFE"

    # Parse tile ID and sensing datetime from product name
    # Format: S2A_MSIL2A_{sensing_dt}_N{baseline}_R{orbit}_T{tile}_{proc_dt}.SAFE
    parts = safe_name.replace(".SAFE", "").split("_")
    sensing_dt = parts[2]  # e.g. "20240610T102021"
    tile_id = parts[5]     # e.g. "T32TLQ"

    logger.info("Downloading scene %s (%s)", tile_id, sensing_dt[:8])

    granule_dir = _get_granule_dir(session, product_id, safe_name)
    logger.debug("Granule directory: %s", granule_dir)

    result: dict[str, Path] = {}

    for band in tqdm(list(bands), desc=f"{tile_id} {sensing_dt[:8]}", unit="band"):
        if band not in BAND_RESOLUTION:
            raise ValueError(f"Unknown band: {band}. Known bands: {list(BAND_RESOLUTION)}")

        res_folder, res_suffix = BAND_RESOLUTION[band]
        filename = f"{tile_id}_{sensing_dt}_{band}_{res_suffix}.jp2"
        local_path = output_dir / filename

        if local_path.exists():
            logger.debug("Already present: %s", filename)
            result[band] = local_path
            continue

        nodes_url = (
            f"{DOWNLOAD_URL}/Products({product_id})"
            f"/Nodes({safe_name})"
            f"/Nodes(GRANULE)"
            f"/Nodes({granule_dir})"
            f"/Nodes(IMG_DATA)"
            f"/Nodes({res_folder})"
            f"/Nodes({filename})"
            f"/$value"
        )

        _stream_download(session, nodes_url, local_path, filename)
        logger.info(
            "Downloaded %s (%.1f MB)", filename, local_path.stat().st_size / 1e6
        )
        result[band] = local_path

    return result


def _get_granule_dir(
    session: requests.Session,
    product_id: str,
    safe_name: str,
) -> str:
    """Return the single GRANULE subdirectory name for a product."""
    url = (
        f"{DOWNLOAD_URL}/Products({product_id})"
        f"/Nodes({safe_name})/Nodes(GRANULE)/Nodes"
    )
    resp = _get_with_retry(url, session=session)
    body = resp.json()
    # CDSE Nodes() response uses "result" key
    nodes = body.get("result", body.get("value", []))
    granule_dirs = [n["Name"] for n in nodes if n.get("Name", "").startswith("L2A_")]
    if not granule_dirs:
        raise ValueError(
            f"No L2A_ GRANULE subdirectory found for {safe_name}. "
            f"Response keys: {list(body.keys())}, nodes: {nodes[:3]}"
        )
    return granule_dirs[0]


def _get_with_retry(
    url: str,
    session: Optional[requests.Session] = None,
    max_retries: int = 5,
    **kwargs,
) -> requests.Response:
    """GET with exponential backoff on 429/503 responses."""
    client = session or requests
    wait = 2
    for attempt in range(max_retries):
        resp = client.get(url, timeout=60, **kwargs)
        if resp.status_code in (429, 503):
            logger.warning(
                "Rate limited (attempt %d/%d). Waiting %ds.",
                attempt + 1,
                max_retries,
                wait,
            )
            time.sleep(wait)
            wait = min(wait * 2, 120)
            continue
        resp.raise_for_status()
        return resp
    raise RuntimeError(f"Failed after {max_retries} retries: {url}")


def _stream_download(
    session: requests.Session,
    url: str,
    local_path: Path,
    label: str,
) -> None:
    """Stream a file download with tqdm progress bar.

    Follows redirects manually to preserve the Authorization header
    across cross-domain redirects issued by CDSE storage backends.
    """
    resp = session.get(url, allow_redirects=False, stream=True, timeout=120)

    # Follow redirects manually to keep auth header
    hops = 0
    while resp.status_code in (301, 302, 303, 307, 308) and hops < 5:
        redirect_url = resp.headers["Location"]
        resp = session.get(redirect_url, allow_redirects=False, stream=True, timeout=120)
        hops += 1

    if resp.status_code in (301, 302, 303, 307, 308):
        raise RuntimeError(f"Too many redirects for {label}")

    resp.raise_for_status()
    total = int(resp.headers.get("Content-Length", 0))

    with open(local_path, "wb") as fh:
        with tqdm(
            total=total or None,
            unit="B",
            unit_scale=True,
            desc=label,
            leave=False,
        ) as pbar:
            for chunk in resp.iter_content(chunk_size=256 * 1024):
                fh.write(chunk)
                pbar.update(len(chunk))
