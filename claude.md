# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is AquaWatch
A satellite-based water quality monitoring prototype that detects cyanobacteria blooms,
turbidity anomalies, and chlorophyll-a spikes in drinking water reservoirs using ESA
Sentinel-2 imagery. Built by Robin Hamers (AI/ML Engineer at WEO, weo-water.com, Luxembourg).

**Pilot reservoir:** Lac de Serre-Ponçon, France (44.5553°N, 6.3522°E, ~28 km²).
Known bloom events: summers 2019, 2022, 2023, 2024.
Validation date range: 2023-04-01 to 2024-10-31.

**Second reservoir (Weekend 6):** Embalse de Entrepeñas, Spain (40.55°N, 2.69°W, ~80 km²).
Known bloom events: Jul–Sep 2022 and Jul–Sep 2023.
Validation date range: 2022-04-05 to 2023-10-18. Both bloom periods validated with no threshold retuning.

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

# Weekend 5: simulate S3 fusion (no real data needed)
python scripts/simulate_s3_reprocess.py   # requires simulate_reprocess.py to have run first

# Weekend 5: CLI commands for real S3 data (requires netCDF4 + CDSE credentials)
python run.py s3-download --start 2023-04-01 --end 2024-10-31
python run.py s3-process
python run.py s3-timeseries
python run.py fusion

# Weekend 6: second reservoir validation
python scripts/simulate_entrepenhas.py    # synthetic S2 scenes + full alert pipeline for Entrepeñas
python run.py compare                     # comparison dashboard (requires both reservoirs to have outputs)

# Weekend 6: multi-reservoir CLI (use --reservoir flag for all commands)
python run.py download --reservoir entrepenhas --start 2022-04-01 --end 2023-10-31
python run.py process  --reservoir entrepenhas
python run.py indices  --reservoir entrepenhas
python run.py timeseries --reservoir entrepenhas
python run.py alerts   --reservoir entrepenhas
```

## Pipeline Architecture

### Sentinel-2 (primary: 10m spatial, 5-day revisit)
```
CDSE catalogue (search_sentinel2)
    → raw JP2 bands in data/raw/{scene_id}/
    → cloud-masked float32 GeoTIFFs in data/processed/{scene_id}/masked/
    → clipped + resampled GeoTIFFs in data/processed/{scene_id}/clipped/{BAND}_clipped.tif
    → index rasters in data/processed/{scene_id}/indices/
    → aggregated stats → outputs/timeseries/serre_poncon_wqi.csv
    → alerts in outputs/alerts/
    → spatial maps in outputs/maps/
```

### Sentinel-3 OLCI (tripwire: 300m, daily revisit)
```
CDSE catalogue (search_sentinel3_olci)
    → band NetCDF4 files in data/raw_s3/{scene_id}/ (Oa08, Oa11, WQSF, geo_coordinates)
    → reprojected + WQSF-masked GeoTIFFs in data/processed_s3/{scene_id}/masked/
    → clipped GeoTIFFs in data/processed_s3/{scene_id}/clipped/
    → NDCI raster in data/processed_s3/{scene_id}/indices/ndci_s3.tif
    → aggregated stats → outputs/timeseries/serre_poncon_s3_wqi.csv
    → fused with S2 → outputs/timeseries/serre_poncon_fused.csv
    → fused dashboard → outputs/maps/dashboard_fused.png
```

Each stage writes to its own subdirectory and is idempotent — already-present files are skipped.

**Key modules:**
- `src/download.py` — CDSE OData Nodes() API: search + token auth + per-band streaming download
- `src/preprocess.py` — SCL cloud masking, polygon clipping, 20m→10m resampling
- `src/indices.py` — NDCI, NDWI, turbidity (S2) + `compute_s3_ndci()` (S3, scale=False)
- `src/timeseries.py` — per-scene stats aggregation → DataFrame → CSV
- `src/alerts.py` — rolling-baseline anomaly detection, `Alert` dataclass, `check_new_scene()` for operational use
- `src/visualize.py` — map and chart generation; `plot_fused_dashboard()` (Weekend 5)
- `src/s3_download.py` — S3 OLCI WFR search + download + `_nc_to_geotiff()` (needs netCDF4)
- `src/s3_preprocess.py` — WQSF masking, reservoir clipping for S3
- `src/fusion.py` — `build_fused_timeseries()`, `detect_s3_precursor_alerts()`, `print_fusion_report()`
- `scripts/test_pipeline.py` — Weekend 1 end-to-end test for 3 scenes
- `scripts/download_all.py` — Weekend 2 bulk download for full date range (B08 included)
- `scripts/build_timeseries.py` — Weekend 2 index computation + CSV + plot
- `scripts/run_alerts.py` — Weekend 3 anomaly detection + alert CSV/JSON + validation
- `scripts/generate_maps.py` — Weekend 4 spatial maps + dashboard + demo package
- `scripts/simulate_s3_reprocess.py` — Weekend 5 synthetic S3 simulation + fusion
- `run.py` — CLI entry point (all commands including s3-download, s3-process, s3-timeseries, fusion)

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
- Min valid pixels: **40% of 75th-percentile scene** — scenes below this are skipped as cloud-contaminated

**Alert post-processing pipeline (order matters):**
1. `detect_alerts()` — raw detection + Dec–Feb HIGH suppression + pixel-count filter
2. `flag_isolated_spikes()` — mark HIGH/MEDIUM with no neighbor in ±15 days as low-confidence
3. `apply_seasonal_filter()` — downgrade alerts outside bloom season (May–Oct): HIGH→MEDIUM, MEDIUM→LOW

**Quality flags in timeseries CSV (`quality_flag` column):**
- `good` — ≥40% typical pixels and May–Oct
- `winter` — Nov–Apr, sufficient pixels (use with caution)
- `low_pixels` — <40% typical pixels, bloom season
- `low_pixels_winter` — both conditions (highest suspicion)

**Validation results post-false-positive fixes (47 scenes, 2023-04-02 → 2024-10-27):**
- Jul–Aug 2023 bloom: 6 alerts (LOW: 1, MEDIUM: 4, HIGH: 1) — peak NDCI 0.412 on 2023-08-17 ✅ VALIDATED
- Jun–Aug 2024 bloom: 8 alerts (LOW: 4, MEDIUM: 4, HIGH: 0) — peak NDCI 0.378 on 2024-08-14 ✅ VALIDATED
- Total: 19 alerts (9 in 2023, 10 in 2024); both bloom periods validated
- False positives outside bloom periods: 5 (shoulder-season z-score × 4 + 1 winter LOW)

**NDCI value range after calibration fix:** −0.1 to ~0.5 (previously −0.01 to +0.03 due to missing ÷10000.0 scaling).

## Session Progress
- [x] Weekend 1: Environment + data pipeline (download + clip + cloud mask)
- [x] Weekend 2: Indices (NDCI, NDWI, turbidity), time series CSV + plot
- [x] Weekend 3: Anomaly detection + alert generation
- [x] Weekend 4: Visualization + CLI + README — demo package at `outputs/demo/`
- [x] Debug session 1: Fixed DN→reflectance scaling bug; recalibrated water mask threshold; added winter seasonal filter; revalidated against known bloom events; regenerated dashboard
- [x] Debug session 2: Added min-pixel filter (40% of p75); spike isolation flag; broad seasonal filter (May–Oct bloom window); quality_flag column in timeseries CSV; false positive rate documented
- [x] Weekend 5: Sentinel-3 OLCI integration — `src/s3_download.py`, `src/s3_preprocess.py`, `src/fusion.py`; `compute_s3_ndci()` in indices.py; `plot_fused_dashboard()` in visualize.py; simulate_s3_reprocess.py; fusion CLI commands; dashboard_fused.png
- [x] Weekend 6: Second reservoir validation — `src/config.py` RESERVOIRS registry; `run.py` multi-reservoir refactor (`--reservoir` flag, `_resolve()` helper); `data/reservoir/entrepenhas.geojson`; `plot_comparison_dashboard()` in visualize.py; `print_validation_report()` generalised to any reservoir name; `scripts/simulate_entrepenhas.py` (synthetic S2 + alert pipeline for Entrepeñas); 4/4 bloom periods validated across 2 reservoirs with no threshold retuning

**To reprocess after code changes:** delete `data/processed/{reservoir}/indices/*.tif` then run:
```bash
python run.py indices && python run.py timeseries && python run.py alerts && python run.py maps
# with --reservoir flag for non-default reservoir:
python run.py indices --reservoir entrepenhas && ...
```
If raw data is absent (gitignored), use `scripts/simulate_reprocess.py` to regenerate outputs from synthetic data.

**Weekend 2 done-marker:** `data/processed/{reservoir}/{scene_id}/clipped/B08_clipped.tif` — presence means fully processed including B08 (needed for NDWI).

## Known Limitations and False Positive Analysis

**March/April cloud-contaminated spike (root cause: low pixel count):**
  2024-03-20 showed apparent NDCI=0.388 with only 3100 valid pixels (~27% of typical summer coverage).
  Cause: partial cloud cover left too few clean water pixels; the remaining fraction was dominated by
  cloud-shadow edge pixels with anomalous spectral response. Filtered by `MIN_VALID_PIXEL_FRACTION=0.4`
  in `detect_alerts()`. Quality flag: `low_pixels_winter`. No alert generated.

**February 2024 alert (root cause: ice/snowmelt sediment):**
  2024-02-08 NDCI=0.413, turbidity=0.79. The low turbidity rules out algae (blooms correlate with
  elevated turbidity ≥ 1.0). Suspected cause: ice edge reflectance or snowmelt sediment resuspension.
  Triple-downgraded via: Dec–Feb HIGH→MEDIUM suppression in `detect_alerts()`, then `[isolated_spike]`
  flag (no neighboring alerts in ±15 days), then `apply_seasonal_filter()` → final severity LOW.
  Full notes trace: `[downgraded: winter HIGH without elevated turbidity] [isolated_spike - low confidence]
  [outside_bloom_season - possible sediment or optical artifact]`.

**False positive rate (post-fix):** 5 alerts outside bloom season out of 19 total = 26%.
  All 5 are LOW severity; 4 are shoulder-season z-score triggers (NDCI 0.05–0.07), 1 is the Feb event.
  Summer bloom alerts (HIGH + MEDIUM) occur exclusively within the May–Oct bloom window.

- **No field validation**: Alert severity labels are based on NDCI thresholds from literature, not
  validated against in-situ measurements at Serre-Ponçon.
- **Z-score shoulder-season alerts**: Rapid NDCI decline post-bloom creates high z-scores in Oct–Nov.
  These generate LOW alerts that are structurally correct but ecologically insignificant. A hard
  bloom-season detection window (May–Oct only) would eliminate them.
- **No SCL snow/ice class suppression**: SCL class 11 (snow/ice) is masked in `preprocess.py`
  (INVALID_SCL_CLASSES includes 11). But edge-of-cloud/shadow pixels near class transitions can pass
  the mask and show ice-like spectral signatures.

## Sentinel-3 OLCI Integration (Weekend 5)

**Product:** `S3_OL_2_WFR` (Water Full Resolution)
**Resolution:** 300 m, ~daily revisit (Sentinel-3A + 3B)
**Format:** NetCDF4 per band, irregular lat/lon swath grid → must reproject to UTM via `_nc_to_geotiff()`
**Bands used:** Oa08 (665 nm Red) + Oa11 (709 nm Red Edge) → `compute_s3_ndci()` in `src/indices.py`
**Already in reflectance:** S3 WFR values are pre-scaled; always read with `scale=False`
**WQSF mask:** keep WATER (bit 1) and INLAND_WATER (bit 2), reject INVALID (bit 0), CLOUD (bit 25–27)

**Fusion analysis results (simulation, 2023-04-01 → 2024-10-31):**
- S3: 383 scenes (clear-sky), S2: 47 scenes
- Co-observation agreement (|S3 − S2 NDCI| < 0.05): 97% of 30 joint scenes
- Mean |S3 − S2| offset: 0.021 (S3 slightly lower due to 300m spatial averaging)
- Jul–Aug 2023 bloom: S3 detected 6 days before S2 ✅ (S3 Aug 3 vs S2 Aug 9)
- Jun–Aug 2024 bloom: S2 3 days before S3 (2024 bloom onset barely crossed LOW threshold)
- Note: S3 NDCI values are ~15% lower than S2 (300m pixels average bloom with surrounding water)
- `netCDF4` Python library required for real S3 data; not in current `environment.yml`

**To use real S3 data:**
1. Add `netcdf4` to `environment.yml`
2. Run `python run.py s3-download --start 2023-04-01 --end 2024-10-31`
3. Run `python run.py s3-process && python run.py s3-timeseries && python run.py fusion`

## Multi-Reservoir Config (Weekend 6)

All reservoir metadata lives in `src/config.py` — adding a new reservoir requires only a new entry in `RESERVOIRS`, no code changes. Each entry holds: `name`, `country`, `geojson` path, `epsg`, `bbox`, `area_km2`, `known_blooms`.

`run.py` calls `_resolve(args)` at the start of each command to derive all paths from the reservoir key:
- Raw data: `data/raw/{reservoir}/`
- Processed: `data/processed/{reservoir}/`
- Timeseries: `outputs/timeseries/{reservoir}_wqi.csv`
- Alerts: `outputs/alerts/{reservoir}_alerts.{csv,json}`
- Maps: `outputs/maps/{reservoir}/`

Default reservoir is `serre_poncon` (backward-compatible with all pre-Weekend 6 workflows).

**Generalisation validation (Weekend 6):**
- Serre-Ponçon (France, 28 km², EPSG:32631): 2/2 bloom periods validated, 5 false positives
- Entrepeñas (Spain, 80 km², EPSG:32630) — SIMULATION: 2/2 validated (synthetic data, peaks 0.38–0.43)
- Entrepeñas — REAL DATA (115 real S2 scenes, 2022–2023): max NDCI 0.026, 1/2 bloom periods validated
  - NDCI is NEGATIVE across the main reservoir body (B04 > B05) — sediment/DOC dominated optical signal
  - Turbidity proxy flat at 0.74-0.88 all year (no seasonal bloom enhancement)
  - Weak positive NDCI (0.02-0.04) only in narrow tributary arms (<5% of surface)
  - Spatial analysis: outputs/demo/entrepenhas_spatial_analysis.png
  - Root cause: sediment/DOC overwhelms phytoplankton spectral signal at B04/B05
  - Alternatives: Sentinel-3 OLCI (620nm phycocyanin band), turbidity-corrected index, targeted tributary polygons
  - Conclusion: NDCI from S2 B04/B05 requires optically clear reservoir — does NOT work for turbid/sediment-dominated systems

**Known processing environment requirements:**
- `s3_interp` conda env: for `process` step (needs working PROJ — ml_env has PROJ db version conflict)
- `ml_env` conda env: for `timeseries`, `alerts`, `maps` (needs matplotlib — s3_interp missing cycler)
- JP2 CRS fallback: ml_env's GDAL JP2 driver doesn't embed CRS in JP2 metadata; fixed via `fallback_crs=cfg["epsg"]` in `apply_cloud_mask()` and `clip_to_reservoir()`

## Weekend 7+ Ideas
- Email/webhook notification when `check_new_scene()` fires
- Serve outputs via lightweight FastAPI + Leaflet web dashboard
- Loosen NDWI mask to −0.2 and recalibrate absolute thresholds (only if field data available)
- Add a third Mediterranean reservoir to test drier-climate generalisation
