# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is AquaWatch
A satellite-based water quality monitoring prototype that detects cyanobacteria blooms,
turbidity anomalies, and chlorophyll-a spikes in drinking water reservoirs using ESA
Sentinel-2 imagery. Built by Robin Hamers (AI/ML Engineer at WEO, weo-water.com, Luxembourg).

**Pilot reservoir:** Lac de Serre-Ponçon, France (44.5553°N, 6.3522°E, ~28 km²).
Known bloom events: summers 2019, 2022, 2023, 2024.
Validation date range: 2023-04-01 to 2024-10-31.

## Commands

```bash
# Activate environment (required before any command)
conda activate aquawatch

# Create environment from scratch
conda env create -f environment.yml

# Run the Weekend 1 integration test (search → download → mask → clip → preview)
python scripts/test_pipeline.py

# All scripts add src/ to sys.path via PROJECT_ROOT; run from repo root

# Weekend 2: download all scenes 2023-04-01 → 2024-10-31 (idempotent, skips already-done)
python scripts/download_all.py

# Weekend 2: compute indices for all processed scenes, build CSV + plot
python scripts/build_timeseries.py

# Weekend 3: run anomaly detection + generate alert CSV + JSON
python scripts/run_alerts.py
```

## Pipeline Architecture

Data flows in one direction through five stages:

```
CDSE catalogue (search_sentinel2)
    → raw JP2 bands in data/raw/{scene_id}/
    → cloud-masked float32 GeoTIFFs in data/processed/{scene_id}/masked/
    → clipped + resampled GeoTIFFs in data/processed/{scene_id}/clipped/{BAND}_clipped.tif
    → index rasters in data/processed/{scene_id}/indices/
    → aggregated stats → outputs/timeseries/serre_poncon_timeseries.csv
    → alerts in outputs/alerts/
    → spatial maps in outputs/maps/
```

Each stage writes to its own subdirectory and is idempotent — already-present files are skipped.

**Key modules:**
- `src/download.py` — CDSE OData Nodes() API: search + token auth + per-band streaming download
- `src/preprocess.py` — SCL cloud masking, polygon clipping, 20m→10m resampling
- `src/indices.py` — NDCI, NDWI, turbidity computation on clipped GeoTIFFs
- `src/timeseries.py` — per-scene stats aggregation → DataFrame → CSV
- `src/alerts.py` — rolling-baseline anomaly detection, `Alert` dataclass, `check_new_scene()` for operational use
- `src/visualize.py` — map and chart generation (Weekend 4)
- `scripts/test_pipeline.py` — Weekend 1 end-to-end test for 3 scenes
- `scripts/download_all.py` — Weekend 2 bulk download for full date range (B08 included)
- `scripts/build_timeseries.py` — Weekend 2 index computation + CSV + plot
- `scripts/run_alerts.py` — Weekend 3 anomaly detection + alert CSV/JSON + validation
- `scripts/generate_maps.py` — Weekend 4 spatial maps + dashboard + demo package
- `run.py` — Weekend 4 CLI entry point (setup / download / process / indices / timeseries / alerts / maps / check / run-all)

Scripts add `PROJECT_ROOT/src` to `sys.path` manually; there is no package install.

## Sentinel-2 Bands & Indices

| Band | Resolution | Purpose                  |
|------|------------|--------------------------|
| B03  | 10m        | Green — NDWI, turbidity  |
| B04  | 10m        | Red — NDCI baseline      |
| B05  | 20m        | Red Edge 1 — NDCI        |
| B08  | 10m        | NIR — NDWI water mask    |
| B8A  | 20m        | NIR narrow — NDCI        |
| SCL  | 20m        | Scene Class Layer        |

- **NDCI** = (B05 − B04) / (B05 + B04) → cyanobacteria; alert thresholds: LOW > 0.2, MEDIUM > 0.3, HIGH > 0.4
- **NDWI** = (B03 − B08) / (B03 + B08) → water mask; water pixels = NDWI > **0.1** (open water only)
- **Turbidity proxy** = B04 / B03 → higher = more turbid (scale-invariant: ratio unaffected by DN→reflectance conversion)

**Band scaling:** All reflectance bands (B03, B04, B05, B08) must be divided by 10000.0 after reading
from L2A GeoTIFF to convert from integer DN (0–10000) to reflectance (0.0–1.0). SCL is NOT scaled.
This is applied in `_read_band()` in `src/indices.py` (`scale=True` default). Index rasters (NDCI,
NDWI, turbidity) are already in float index space and must be read with `scale=False`.

SCL invalid classes (masked to NaN): 0, 1, 3, 8, 9, 10, 11.
Valid classes kept: 4 (vegetation), 5 (bare), 6 (water), 7 (unclassified).

## Alert Logic
- Rolling 30-day baseline: mean and std of NDCI
- Alert: NDCI_mean > (baseline_mean + 2 × baseline_std) **or** NDCI_mean > 0.2
- Severity: LOW > 0.2, MEDIUM > 0.3, HIGH > 0.4

## Critical Gotchas

**CRS:** Serre-Ponçon falls in MGRS tile T31TGK (EPSG:**32631**), not T32TLQ/32632, despite being geographically in UTM zone 32. `clip_to_reservoir()` auto-detects the raster CRS and reprojects the reservoir polygon to match.

**CDSE API — three distinct endpoints:**
- `catalogue.dataspace.copernicus.eu` — search/metadata only
- `download.dataspace.copernicus.eu` — OData Nodes() file traversal and streaming download
- `zipper.dataspace.copernicus.eu` — full-product ZIP archives (not used here)

**Nodes() directory listing** requires a trailing `/Nodes`:
```
/Products({id})/Nodes({safe})/Nodes(GRANULE)/Nodes   ✓  lists children
/Products({id})/Nodes({safe})/Nodes(GRANULE)          ✗  returns 403
```
Nodes() response uses key `"result"` (not `"value"` like catalogue search).

**Band filename** inside SAFE: `{tile}_{sensing_dt}_{band}_{res}.jp2`
(e.g. `T31TGK_20240826T102559_B04_10m.jp2`). Tile and sensing datetime are parsed from
the product name at indices `[5]` and `[2]` when split on `_`.
GRANULE subdirectory name varies per scene — always discover it via `/Nodes(GRANULE)/Nodes`.

**JP2 reading** requires the `libgdal-jp2openjpeg` conda-forge plugin (in `environment.yml`).

**Masking + clipping:** `rio_mask(..., nodata=np.nan)` fails on integer JP2 inputs.
Use `filled=False` to get a masked array, then cast to float32 and set `.mask` pixels to `np.nan`.

**Valid pixel fraction:** The reservoir polygon covers ~29% of its clipped bounding box (Y-shape).
Use the non-NaN count from the clipped **SCL** file as the denominator — SCL is never cloud-masked,
so its non-NaN pixels = all pixels inside the polygon. Band valid / SCL valid = cloud-free fraction.

**OAuth2 token** expires in ~10 min; `download_scene()` re-fetches it per scene.
CDSE can issue cross-domain redirects during download — follow them manually with `stream=True`
from the initial request to preserve the `Authorization` header.

## Alert Detection Calibration

**Final thresholds:**
- Absolute: LOW > 0.2, MEDIUM > 0.3, HIGH > 0.4
- Z-score: threshold = **1.5σ** (rolling 30-day window, global-dataset fallback when < 3 prior scenes)
- NDWI water mask: **0.1** (changed from −0.2 to exclude shoreline mixed pixels)

**Validation results post-calibration fix (47 scenes, 2023-04-02 → 2024-10-27):**
- Jul–Aug 2023 bloom: 6 alerts (LOW: 1, MEDIUM: 4, HIGH: 1) — peak NDCI 0.412 on 2023-08-17 ✅ VALIDATED
- Jun–Aug 2024 bloom: 8 alerts (LOW: 4, MEDIUM: 4, HIGH: 0) — peak NDCI 0.378 on 2024-08-14 ✅ VALIDATED
- Total: 20 alerts (9 in 2023, 11 in 2024); both bloom periods validated
- False positives outside bloom periods: 6 (shoulder-season z-score and 1 winter event — see below)

**NDCI value range after calibration fix:** −0.1 to ~0.5 (previously −0.01 to +0.03 due to missing ÷10000.0 scaling).

## Session Progress
- [x] Weekend 1: Environment + data pipeline (download + clip + cloud mask)
- [x] Weekend 2: Indices (NDCI, NDWI, turbidity), time series CSV + plot
- [x] Weekend 3: Anomaly detection + alert generation
- [x] Weekend 4: Visualization + CLI + README — demo package at `outputs/demo/`
- [x] Debug session: Fixed DN→reflectance scaling bug; recalibrated water mask threshold; added winter seasonal filter; revalidated against known bloom events; regenerated dashboard

**To reprocess after code changes:** delete `data/processed/*/indices/*.tif` then run:
```bash
python run.py indices && python run.py timeseries && python run.py alerts && python run.py maps
```
If raw data is absent (gitignored), use `scripts/simulate_reprocess.py` to regenerate outputs from synthetic data.

**Weekend 2 done-marker:** `data/processed/{scene_id}/clipped/B08_clipped.tif` — presence means fully processed including B08 (needed for NDWI).

## Known Limitations
- **Winter false positives (Dec–Feb)**: Ice, snow cover, and post-snowmelt sediment resuspension can
  elevate NDCI into the 0.3–0.45 range without a genuine cyanobacteria bloom. A HIGH alert fired on
  2024-02-08 (NDCI=0.413) with turbidity=0.79, consistent with an ice/snowmelt signal rather than algae.
  Mitigation: `detect_alerts()` applies a seasonal filter — HIGH alerts in Dec–Feb are downgraded to
  MEDIUM if `turbidity_mean ≤ 0.95`. This suppresses false HIGH alerts while preserving the event record.
- **No field validation**: Alert severity labels (LOW/MEDIUM/HIGH) are based on NDCI thresholds from
  literature, not validated against in-situ measurements at Serre-Ponçon.
- **Z-score shoulder-season alerts**: Rapid NDCI decline after bloom creates high z-scores in Oct–Nov.
  These generate LOW alerts flagged as false positives but are structurally correct (anomalous relative
  to the recent baseline). A seasonal detection window (Apr–Oct only) would eliminate them.
- **No SCL snow/ice class suppression**: SCL class 11 (snow/ice) is already masked in `preprocess.py`
  (INVALID_SCL_CLASSES includes 11). But edge-of-cloud/shadow pixels near class transitions can pass
  the mask and show ice-like spectral signatures.

## Weekend 5+ Ideas
- Email/webhook notification when `check_new_scene()` fires
- Loosen NDWI mask to −0.2 and recalibrate absolute thresholds
- Multi-reservoir support (parameterise polygon + BBOX per site)
- Sentinel-3 OLCI integration for 300 m / daily revisit
- Serve outputs via lightweight FastAPI + Leaflet web dashboard
