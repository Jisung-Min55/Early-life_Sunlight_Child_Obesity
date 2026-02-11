# Reproducible pipeline: PSKC panel + SIGUNGU centers + weather matching

This repository reproduces the data construction pipeline for my project, "The Impact of Early-life Sunlight Exposure on Child Obesity" using the Panel Study on Korean Children data, district boundary data, and meteorological data.

## What this repository does
1) **Build a long PSKC panel (in Stata)** by keeping non-movers through Wave 3 and dropping foster kids from the dataset.  
2) **Compute region (SIGUNGU) “center” coordinates (in Python)** from administrative boundary polygons  
   - outputs both **centroids** (geographical centers) and **representative points** (points guaranteed inside polygons)
   - constructs a merge key `resid_area` (e.g., `서울특별시/강동구`, `경상북도/포항시/남구`)
3) **Match each SIGUNGU to its nearest weather station for each day (in Python)** (meters-based CRS, EPSG:5179)
4) **Merge the PSKC panel and the weather dataset (in Stata)**

## Repository layout
- `code/stata/` : Stata scripts (panel construction, merges, exposure variables)
- `code/python/` : Python scripts (SIGUNGU centers, station matching)
- `data/raw/` : raw inputs (NOT committed)
- `data/derived/` : intermediate outputs (NOT committed)
- `data/processed/` : final analysis-ready datasets (NOT committed)
- `outputs/logs/` : logs (NOT committed)

## Data inputs (not committed)
Place the following in `data/raw/`:

### PSKC raw survey files
- Download PSKC raw data from the official website (requires access).
- Rename files to a consistent convention used by the Stata scripts (see `code/stata/`).

### SIGUNGU boundary zip (2010)
- Example expected filename: `bnd_sigungu_00_2010_4Q.zip`

### Weather station list
- `stations.csv` (must include station id + coordinates; see `docs/data_notes.md`)

## Outputs
Intermediate outputs:
- `data/derived/sigungu2010_centers_UTMK.csv`
- `data/derived/sigungu2010_centers_UTMK.dta`

Final output (example):
- `data/processed/analysis_ready_panel.dta`

## How to run (recommended order)

### 0) Set up folders
Create:
- `data/raw/`, `data/derived/`, `data/processed/`, `outputs/logs/`

### 1) Stata: build panel and keep non-movers
From Stata:
```stata
do "code/stata/01_build_panel.do"
