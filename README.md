# Data Construction Pipeline: PSKC Panel × SIGUNGU Centers × KMA Weather Matching

This repository reproduces the data construction pipeline for my project
**“The Impact of Early-life Sunlight Exposure on Child Obesity”** using:

- **PSKC** (Panel Study on Korean Children) microdata
- **Korean administrative boundaries** (SIGUNGU polygons)
- **Korea Meteorological Administration** (KMA) station metadata + daily sunshine (일조시간) records

> **Data note:** PSKC and KMA raw files are **not** redistributed in this repository.  
> The pipeline can be reproduced by downloading the public data from the official portals (registration, acceptance of the providers’ terms, submitting a proposal and such may be required) and placing the inputs in data/raw/ as described below.”

---

## What this repo produces

### Core steps
1. **Construct an analysis panel (Stata)**  
   - keep children who do not move through Wave 3  
   - drop foster children  
2. **Compute region (SIGUNGU) “center” coordinates (Python)** from polygon boundaries  
   - exports both **centroids** (geometric centers) and **representative points** (guaranteed inside polygons)  
   - constructs a merge key `resid_area` (e.g., `서울특별시/강동구`, `경상북도/포항시/남구`)  
3. **Match each SIGUNGU to the nearest *active* weather station by day (Python)**  
   - distances computed in **meters** under **EPSG:5179 (UTM-K)**  
   - station eligibility varies by day (station openings / closures / relocations are handled via station META segments)  
4. **Merge PSKC and weather-based exposure measures (Stata)**

---

## Repository layout
<details>
<summary><b>Repository structure</b></summary>

```text
code/
  stata/        Stata do-files (panel construction, merges, exposure variables)
  python/       Python scripts (SIGUNGU centers, station matching)
data/
  raw/          Raw inputs (NOT committed)
  derived/      Intermediate outputs (NOT committed)
  processed/    Final analysis-ready datasets (NOT committed)
outputs/
  logs/         Logs (NOT committed)

---

## Inputs (not committed)

Place the following under `data/raw/`:

### 1) PSKC raw survey files
- Obtain PSKC files from the official PSKC website.
- Ensure filenames match the conventions expected by the Stata scripts in `code/stata/`.

### 2) SIGUNGU boundary polygons (2010)
- Obtain SIGUNGU boundary polygons from the official SGIS portal.
- Example filename: `bnd_sigungu_00_2010_4Q.zip`

### 3) KMA weather data
- Daily sunshine file (example): `Sunlight_(Jun2007-Aug2011).csv`
- Station metadata (example): `META_weather_station_data.csv`

---

## Outputs

Intermediate (examples):
- `data/derived/PSKC_long_w1to7_nomove_until_w3_dropfoster.dta'
- `data/derived/sigungu2010_centers_UTMK.csv`
- `data/derived/sigungu2010_centers_UTMK.dta`
- `data/derived/sigungu_daily_sunlight_20070601_20110831.csv`
- `data/derived/sigungu_station_assignment_intervals_20070601_20110831.csv`

Final (example):
- `data/processed/analysis_ready_panel.dta`

---

## Software requirements

- **Stata** (recommended: 17+)
- **Python** 3.9+ with:
  - `pandas`, `numpy`, `pyproj`
  - `shapely`, `pyshp` (for polygon handling)

> Reproducibility tip: add an `environment.yml` (conda) or `requirements.txt` (pip) once you finalize your environment.

---

## How to run

### 0) Create expected folders
Create (if they do not exist):
- `data/raw/`, `data/derived/`, `data/processed/`, `outputs/logs/`

### 1) Stata -- build the analysis panel
do "code/stata/01_build_panel.do"

### 2) Python -- compute SIGUNGU centers
python "code/python/sigungu_center.py"

### 3) Python -- daily nearest-station assignment + sunshine attachment
python "code/python/assign_nearest_station_dynamic_200706_201108.py"

### 4) Stata -- merge PSKC with weather outputs and build exposure measures
do "code/stata/02_merge_weather.do"
