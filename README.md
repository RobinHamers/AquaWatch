# AquaWatch

Satellite-based water quality monitoring for drinking water reservoirs. AquaWatch downloads Sentinel-2 imagery, computes cyanobacteria and turbidity indices, detects anomalies against a rolling baseline, and generates spatial alert maps — all from the command line.

**Pilot site:** Lac de Serre-Ponçon, France (44.55°N, 6.35°E, ~28 km²). Known bloom events detected in summers 2023 and 2024.

---

## Quick Start

**1. Create the conda environment**
```bash
conda env create -f environment.yml
conda activate aquawatch
```

**2. Add CDSE credentials**

Register for free at [dataspace.copernicus.eu](https://dataspace.copernicus.eu), then:
```bash
cp .env.template .env
# Edit .env and fill in CDSE_USERNAME and CDSE_PASSWORD
```

**3. Run the full pipeline**
```bash
python run.py run-all --start 2023-04-01 --end 2024-10-31
```

**4. Check a specific date (operational mode)**
```bash
python run.py check --date 2024-08-21
#   ⚠  ALERT — HIGH
#      NDCI=0.0289  Z-score=8.47
```

Outputs land in `outputs/`:
- `timeseries/serre_poncon_wqi.csv` — per-scene water quality statistics
- `alerts/serre_poncon_alerts.{csv,json}` — detected anomaly log
- `maps/dashboard.png` — summary dashboard
- `demo/` — 4-file shareable demo package

---

## Pipeline Overview

Each stage is idempotent — already-processed files are skipped on re-run.

```
CDSE catalogue search
    ↓  download_scene()               data/raw/{scene_id}/*.jp2
    ↓  apply_cloud_mask()             data/processed/{scene_id}/masked/
    ↓  clip_to_reservoir()            data/processed/{scene_id}/clipped/
    ↓  compute_all_indices()          data/processed/{scene_id}/indices/
    ↓  build_timeseries()             outputs/timeseries/serre_poncon_wqi.csv
    ↓  detect_alerts()                outputs/alerts/serre_poncon_alerts.json
    ↓  plot_alert_map() / dashboard   outputs/maps/
```

**Weekend 1 — Data Pipeline** (`src/download.py`, `src/preprocess.py`)  
Downloads individual JP2 band files from CDSE via the OData Nodes() API — only the 5–6 needed bands (~35 MB per scene vs. 500–900 MB for a full L2A ZIP). Cloud masking uses the SCL band; scenes are clipped to the reservoir polygon and 20 m bands are resampled to 10 m.

**Weekend 2 — Indices & Time Series** (`src/indices.py`, `src/timeseries.py`)  
Three indices computed per scene over water pixels (NDWI > −0.2):
- **NDCI** = (B05 − B04) / (B05 + B04) — cyanobacteria proxy
- **NDWI** = (B03 − B08) / (B03 + B08) — water extent mask
- **Turbidity** = B04 / B03 — suspended sediment proxy

Per-scene statistics (mean, median, std, p10–p90, n) are aggregated into a CSV time series.

**Weekend 3 — Anomaly Detection** (`src/alerts.py`)  
A 30-day calendar-aware rolling baseline is computed for each scene. Alerts are triggered when NDCI mean exceeds 0.2 / 0.3 / 0.4 (absolute LOW/MEDIUM/HIGH) **or** is ≥ 1.5 σ above the rolling baseline (z-score). Duplicates within 7-day windows are suppressed. `check_new_scene()` is the operational entry point for near-real-time monitoring.

**Weekend 4 — Visualization & CLI** (`src/visualize.py`, `run.py`)  
Spatial NDCI maps with severity badge, scale bar, and north arrow; before/during/after bloom comparison panels; single-page dashboard with time series, alert markers, and monthly bar chart.

---

## Example Output

![Dashboard](outputs/maps/dashboard.png)

The dashboard shows the full 2023–2024 time series with alert triangles (yellow = LOW, orange = MEDIUM, red = HIGH), the 30-day rolling baseline band, and a monthly alert count chart.

---

## Technical Approach

| Band | Resolution | Role |
|------|-----------|------|
| B03  | 10 m | Green — NDWI, turbidity denominator |
| B04  | 10 m | Red — NDCI denominator |
| B05  | 20 m | Red Edge — NDCI numerator (cyanobacteria) |
| B08  | 10 m | NIR — NDWI water mask |
| B8A  | 20 m | NIR narrow — alternative NDCI |
| SCL  | 20 m | Scene Class Layer — cloud mask |

**Water mask:** NDWI > −0.2 threshold captures surface pixels including bloom-affected areas while excluding land. Stricter thresholds (NDWI > 0) exclude ~76% of polygon pixels in drought years when the reservoir is at reduced capacity and shorelines are exposed.

**Alert logic:** z-score detection relative to a 30-day rolling baseline is more robust than absolute thresholds for this dataset because the NDCI range is narrow (−0.03 to +0.03 over water pixels). A global-dataset fallback is used when fewer than 3 prior scenes fall within the 30-day window (e.g. the start of the archive).

---

## Validation Results

**Dataset:** 49 scenes, 2023-04-09 → 2024-10-30  
**Known bloom events:** summer 2023 (Jul–Aug), summer 2024 (Jun–Aug)

| Event | Alert Date | Severity | NDCI mean | Z-score |
|-------|-----------|---------|-----------|---------|
| Summer 2023 | 2023-08-17 | **HIGH** | 0.029 | 8.47 |
| Summer 2024 | 2024-08-21 | LOW | −0.002 | 2.64 |

**Total alerts:** 9 (5 in 2023, 4 in 2024). False positive rate not yet estimated against field data.

The 2023 bloom produces a strong HIGH alert (z = 8.47) once April–July pre-season data is included to establish a local baseline. The 2024 signal is weaker (z = 2.64), which may reflect a genuinely milder bloom or a higher summer baseline from the looser water mask.

---

## Data Sources

- **Imagery:** ESA Sentinel-2 L2A, provided free of charge via the [Copernicus Data Space Ecosystem](https://dataspace.copernicus.eu)
- **Reservoir polygon:** Manually digitised from Sentinel-2 RGB preview; approximate boundary of Lac de Serre-Ponçon at maximum operating level
- **Bloom reference events:** Agence de l'Eau Rhône-Méditerranée-Corse public advisories (summers 2019, 2022, 2023, 2024)

---

## Roadmap

| | Goal |
|-|------|
| Next | Notification layer — email/webhook when `check_new_scene()` fires |
| | Multi-reservoir support — parameterise polygon + BBOX per site |
| | Field validation — compare against in-situ chlorophyll-a measurements |
| | Sentinel-3 OLCI integration — 300 m ocean colour, daily revisit |
| | Web dashboard — FastAPI + Leaflet map viewer |

---

## Project Structure

```
AquaWatch/
├── run.py                    # CLI entry point
├── environment.yml           # conda environment
├── .env.template             # credential template
├── src/
│   ├── download.py           # CDSE API + streaming download
│   ├── preprocess.py         # cloud masking, clipping, resampling
│   ├── indices.py            # NDCI, NDWI, turbidity
│   ├── timeseries.py         # stats aggregation → CSV
│   ├── alerts.py             # anomaly detection + Alert dataclass
│   └── visualize.py          # maps + dashboard
├── scripts/                  # one-off and batch scripts
├── data/
│   ├── raw/                  # downloaded JP2 files (gitignored)
│   ├── processed/            # clipped + indexed rasters per scene
│   └── reservoir/            # reservoir polygon GeoJSON
└── outputs/
    ├── timeseries/           # CSV + time series plot
    ├── alerts/               # alert CSV + JSON
    ├── maps/                 # spatial maps + dashboard
    └── demo/                 # 4-file shareable demo package
```

---

*Built by Robin Hamers (AI/ML Engineer, [WEO](https://weo-water.com), Luxembourg) as a weekend prototype.*
