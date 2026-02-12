# -*- coding: utf-8 -*-
"""
Baseline code: assign each region (시/군/구) to the nearest *ACTIVE* weather station EACH DAY and attach daily sunshine hours (일조시간).

This handles three concerns:
1) Stations built mid-window (e.g., station number 174 (순천) starts April 1, 2011):
   - Before the start date, the station simply is not eligible (no station-day record in the candidate set).
2) Stations built after the window (e.g., station number 181 (서청주) in 2022):
   - Never appear as candidates during the window of our interest, so effectively dropped.
3) Stations that move/rename:
   - META has multiple rows per station with start date/end date (시작일/종료일) and coordinates; we attach the correct segment for each day.

Inputs
- sigungu2010_centers_UTMK.csv
- Sunlight_(Jun2007-Aug2011).csv
- META_weather_station_data.csv

Outputs
- sigungu_daily_sunlight_20070601_20110831.csv                 (CSV, UTF-8)
- sigungu_daily_sunlight_20070601_20110831.dta        (DTA, ASCII-only vars)
- sigungu_station_assignment_intervals_20070601_20110831.csv   (CSV)
- sigungu_monthly_sunlight_200706_201108.csv                   (CSV)

IMPORTANT NOTES:
- The raw datasets (items 2 and 3 of "Inputs") can be accessed via the Korea Meteorological Administration (KMA) Open MET Data Portal
- Distances are computed in meters using UTM-K (EPSG:5179). (Regions should be already in UTM-K in your "centers" file.)
- A station is a candidate only if it has a sunshine record on that day (prevents avoidable missingness).
- Missing sunshine is never treated as 0.

"""

from __future__ import annotations

import os
import warnings
from typing import Tuple

import numpy as np
import pandas as pd
from pyproj import Transformer


# =========================
# PATHS
# =========================
CENTERS_PATH = r"C:\Users\jaspe\OneDrive\Desktop\Research\Projects\Sunlight_ChildObesity\derived\sigungu2010_centers_UTMK.csv"
SUNLIGHT_PATH = r"C:\Users\jaspe\OneDrive\Desktop\Data\Weather\Sunlight_(Jun2007-Aug2011).csv"
STATION_META_PATH = r"C:\Users\jaspe\OneDrive\Desktop\Data\Weather\META_weather_station_data.csv"

OUT_DIR = r"C:\Users\jaspe\OneDrive\Desktop\Research\Projects\Sunlight_ChildObesity\derived"

# Analysis window (inclusive; date format follows YYYY-MM-DD)
WINDOW_START = "2007-06-01"
WINDOW_END = "2011-08-31"


def read_csv_smart(path: str, dtype=None) -> pd.DataFrame:
    """Read CSV using common encodings."""
    for enc in ("utf-8-sig", "utf-8", "cp949", "euc-kr"):
        try:
            return pd.read_csv(path, encoding=enc, dtype=dtype)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, dtype=dtype)


def prepare_meta(meta: pd.DataFrame, window_start: pd.Timestamp, window_end: pd.Timestamp) -> pd.DataFrame:
    """Parse meta segments, clip to window, and resolve overlaps."""
    meta = meta.copy()

    meta["지점"] = pd.to_numeric(meta["지점"], errors="coerce").astype("Int64")
    meta = meta.dropna(subset=["지점"]).copy()
    meta["지점"] = meta["지점"].astype(int)

    meta["시작일"] = pd.to_datetime(meta["시작일"], errors="coerce")
    meta["종료일"] = pd.to_datetime(meta["종료일"], errors="coerce")

    meta["seg_start"] = meta["시작일"].fillna(window_start)
    meta["seg_end"] = meta["종료일"].fillna(window_end)

    # keep only segments that overlap the window
    meta = meta[(meta["seg_end"] >= window_start) & (meta["seg_start"] <= window_end)].copy()
    meta["seg_start"] = meta["seg_start"].clip(lower=window_start)
    meta["seg_end"] = meta["seg_end"].clip(upper=window_end)

    meta["위도"] = pd.to_numeric(meta["위도"], errors="coerce")
    meta["경도"] = pd.to_numeric(meta["경도"], errors="coerce")
    meta = meta.dropna(subset=["위도", "경도", "seg_start", "seg_end"])

    # resolve overlaps within station: if seg_end >= next seg_start, set seg_end = day before next seg_start
    meta = meta.sort_values(["지점", "seg_start", "seg_end"]).reset_index(drop=True)
    meta["next_start"] = meta.groupby("지점")["seg_start"].shift(-1)
    overlap = meta["next_start"].notna() & (meta["seg_end"] >= meta["next_start"])
    if overlap.any():
        meta.loc[overlap, "seg_end"] = meta.loc[overlap, "next_start"] - pd.Timedelta(days=1)
    meta = meta.drop(columns=["next_start"])

    return meta


def attach_segment_to_sun(sun: pd.DataFrame, meta: pd.DataFrame) -> pd.DataFrame:
    """Attach correct (lon,lat) for each station-day from META segments."""
    m = sun.merge(
        meta[["지점", "지점명", "seg_start", "seg_end", "위도", "경도"]],
        on="지점",
        how="left",
        suffixes=("", "_meta")
    )

    in_seg = (m["일시"] >= m["seg_start"]) & (m["일시"] <= m["seg_end"])
    m = m.loc[in_seg].copy()

    # If multiple segments match, keep the latest seg_start
    m = m.sort_values(["지점", "일시", "seg_start"], ascending=[True, True, False])
    m = m.drop_duplicates(subset=["지점", "일시"], keep="first")

    return m


def nearest_station_for_day(
    rx: np.ndarray, ry: np.ndarray, stx: np.ndarray, sty: np.ndarray
) -> Tuple[np.ndarray, np.ndarray]:
    """Return (idx, dist_m) for each region among candidate stations."""
    dx = rx[:, None] - stx[None, :]
    dy = ry[:, None] - sty[None, :]
    dist = np.hypot(dx, dy)
    idx = dist.argmin(axis=1)
    dist_m = dist[np.arange(dist.shape[0]), idx]
    return idx, dist_m


def make_intervals(region_day: pd.DataFrame, station_col: str, dist_col: str) -> pd.DataFrame:
    """Compress region-day station assignments into intervals."""
    out = []
    gcols = ["SIGUNGU_CD", "resid_area", "date", station_col, dist_col]
    for area, g in region_day[gcols].sort_values(["resid_area", "date"]).groupby("resid_area"):
        st = g[station_col].to_numpy()
        d = g[dist_col].to_numpy()
        dates = pd.to_datetime(g["date"]).to_numpy()
        code = g["SIGUNGU_CD"].iloc[0]

        change = np.ones(len(g), dtype=bool)
        change[1:] = (st[1:] != st[:-1])

        starts = np.where(change)[0]
        ends = np.r_[starts[1:] - 1, len(g) - 1]

        for s, e in zip(starts, ends):
            st_id = st[s]
            if pd.isna(st_id):
                continue
            out.append({
                "SIGUNGU_CD": code,
                "resid_area": area,
                "station_id": int(st_id),
                "start_date": pd.to_datetime(dates[s]).date().isoformat(),
                "end_date": pd.to_datetime(dates[e]).date().isoformat(),
                "mean_distance_m": float(np.nanmean(d[s:e+1])),
                "n_days": int(e - s + 1),
            })
    return pd.DataFrame(out)


def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)

    # ----------------------
    # Window
    # ----------------------
    w_start = pd.to_datetime(WINDOW_START)
    w_end = pd.to_datetime(WINDOW_END)

    # ----------------------
    # Load region centers
    # ----------------------
    centers = read_csv_smart(CENTERS_PATH, dtype={"SIGUNGU_CD": str})
    need = {"SIGUNGU_CD", "resid_area", "centroid_x_utmK", "centroid_y_utmK", "rep_x_utmK", "rep_y_utmK"}
    missing = need - set(centers.columns)
    if missing:
        raise ValueError(f"Centers file missing columns: {sorted(missing)}")

    centers["SIGUNGU_CD"] = centers["SIGUNGU_CD"].astype(str).str.zfill(5)
    centers["resid_area"] = centers["resid_area"].astype(str).str.replace(r"\s+", "", regex=True)

    rx_c = centers["centroid_x_utmK"].astype(float).to_numpy()
    ry_c = centers["centroid_y_utmK"].astype(float).to_numpy()
    rx_r = centers["rep_x_utmK"].astype(float).to_numpy()
    ry_r = centers["rep_y_utmK"].astype(float).to_numpy()

    # ----------------------
    # Load sunshine data
    # ----------------------
    sun = read_csv_smart(SUNLIGHT_PATH)
    sun["지점"] = pd.to_numeric(sun["지점"], errors="coerce").astype("Int64")
    sun["일시"] = pd.to_datetime(sun["일시"], errors="coerce")
    sun = sun.dropna(subset=["지점", "일시"]).copy()
    sun["지점"] = sun["지점"].astype(int)

    sun = sun[(sun["일시"] >= w_start) & (sun["일시"] <= w_end)].copy()
    if sun.empty:
        raise ValueError("Sunlight data is empty after window filtering. Check WINDOW_START/END and file contents.")

    sun_col = "합계 일조시간(hr)"
    if sun_col not in sun.columns:
        raise ValueError(f"Sunlight file missing '{sun_col}'. Columns: {list(sun.columns)}")
    sun[sun_col] = pd.to_numeric(sun[sun_col], errors="coerce")

    print(f"Using sunshine window: {sun['일시'].min().date()} to {sun['일시'].max().date()}")

    # ----------------------
    # Load station META and prepare segments
    # ----------------------
    meta = read_csv_smart(STATION_META_PATH)
    meta = prepare_meta(meta, sun["일시"].min().normalize(), sun["일시"].max().normalize())

    # ----------------------
    # Attach correct station coords (segment-valid) to each station-day sunshine record
    # ----------------------
    sun_seg = attach_segment_to_sun(
        sun[["지점", "지점명", "일시", sun_col]].copy(),
        meta
    )

    # Convert station lon/lat -> UTM-K (EPSG:5179) for meter distances
    to_utmK = Transformer.from_crs(4326, 5179, always_xy=True)
    stx, sty = to_utmK.transform(sun_seg["경도"].to_numpy(), sun_seg["위도"].to_numpy())
    sun_seg["st_x_utmK"] = stx
    sun_seg["st_y_utmK"] = sty

    # ----------------------
    # Compute nearest station per region per day
    # ----------------------
    out_rows = []
    for day, g in sun_seg.groupby(sun_seg["일시"].dt.normalize()):
        st_ids = g["지점"].to_numpy()
        stx = g["st_x_utmK"].to_numpy()
        sty = g["st_y_utmK"].to_numpy()
        sun_hr = g[sun_col].to_numpy()

        # nearest for centroid / representative point
        idx_c, dist_c = nearest_station_for_day(rx_c, ry_c, stx, sty)
        idx_r, dist_r = nearest_station_for_day(rx_r, ry_r, stx, sty)

        out_rows.append(pd.DataFrame({
            "SIGUNGU_CD": centers["SIGUNGU_CD"].to_numpy(),
            "resid_area": centers["resid_area"].to_numpy(),
            "date": pd.to_datetime(day).date().isoformat(),

            "station_id_centroid": st_ids[idx_c],
            "dist_m_centroid": dist_c,
            "sun_hr_centroid": sun_hr[idx_c],

            "station_id_rep": st_ids[idx_r],
            "dist_m_rep": dist_r,
            "sun_hr_rep": sun_hr[idx_r],
        }))

    region_day = pd.concat(out_rows, ignore_index=True)

    # ----------------------
    # Save daily CSV (UTF-8)
    # ----------------------
    daily_csv = os.path.join(OUT_DIR, "sigungu_daily_sunlight_20070601_20110831.csv")
    region_day.to_csv(daily_csv, index=False, encoding="utf-8-sig")
    print("Wrote:", daily_csv)

    # Daily DTA without Korean strings (safe for pandas -> Stata)
    daily_dta = os.path.join(OUT_DIR, "sigungu_daily_sunlight_20070601_20110831.dta")
    try:
        region_day_noK = region_day.drop(columns=["resid_area"])
        region_day_noK.to_stata(daily_dta, write_index=False, version=118)
        print("Wrote:", daily_dta)
    except Exception as e:
        warnings.warn(f"DTA export failed ({e!r}). Use the CSV and import in Stata instead.")

    # ----------------------
    # Intervals (compressed mapping)
    # ----------------------
    intervals_c = make_intervals(region_day, "station_id_centroid", "dist_m_centroid")
    intervals_c["method"] = "centroid"
    intervals_r = make_intervals(region_day, "station_id_rep", "dist_m_rep")
    intervals_r["method"] = "rep"
    intervals = pd.concat([intervals_c, intervals_r], ignore_index=True)

    intervals_csv = os.path.join(OUT_DIR, "sigungu_station_assignment_intervals_20070601_20110831.csv")
    intervals.to_csv(intervals_csv, index=False, encoding="utf-8-sig")
    print("Wrote:", intervals_csv)

    # ----------------------
    # Monthly sums (region-month)
    # ----------------------
    region_day["ym"] = pd.to_datetime(region_day["date"]).dt.to_period("M").astype(str)
    monthly = (region_day
               .groupby(["SIGUNGU_CD", "resid_area", "ym"], as_index=False)
               .agg(
                   sun_hr_centroid_sum=("sun_hr_centroid", "sum"),
                   sun_hr_rep_sum=("sun_hr_rep", "sum"),
                   n_days=("date", "count")
               ))

    monthly_csv = os.path.join(OUT_DIR, "sigungu_monthly_sunlight_200706_201108.csv")
    monthly.to_csv(monthly_csv, index=False, encoding="utf-8-sig")
    print("Wrote:", monthly_csv)


if __name__ == "__main__":
    main()
