# This code computes the "central" coordinates (centroid & representative point) of each region (시/군/구),
# # and builds a merge key (resid_area) that matches the main dataset style (e.g., 서울특별시/강동구, 경상북도/포항시/남구)
#
# Raw boundary data source can be downloaded via https://sgis.mods.go.kr/view/pss/openDataIntrcn
# Coordinate system: UTM-K (GRS80)  -> EPSG:5179

import os
import re
import zipfile

import shapefile  # if necessary: pip install pyshp
import pandas as pd
from shapely.geometry import shape as shapely_shape  # if necessary: pip install shapely
from pyproj import Transformer  # if necessary: pip install pyproj


# ======================
# 0. Paths
# ======================
zip_path = r"C:\Users\jaspe\OneDrive\Desktop\Data\Admin_Regions\bnd_sigungu_00_2010_4Q.zip"
extract_dir = r"C:\Users\jaspe\OneDrive\Desktop\Research\Projects\Sunlight_ChildObesity\derived\_sigungu2010_extract"

# Output filenames
out_csv_name = "sigungu2010_centers_UTMK.csv"
out_dta_name = "sigungu2010_centers_UTMK.dta"


# ============================================================
# 1) Unzip
# ============================================================
os.makedirs(extract_dir, exist_ok=True)
with zipfile.ZipFile(zip_path, "r") as z:
    z.extractall(extract_dir)

# Auto-find a .shp file
shp_candidates = []
for root, _, files in os.walk(extract_dir):
    for f in files:
        if f.lower().endswith(".shp"):
            shp_candidates.append(os.path.join(root, f))

if not shp_candidates:
    raise FileNotFoundError(f"No .shp file found under: {extract_dir}")

# Prefer a file that looks like bnd_sigungu_00_2010 if multiple exist
preferred = [p for p in shp_candidates if "bnd_sigungu_00_2010" in os.path.basename(p).lower()]
shp_path = preferred[0] if preferred else shp_candidates[0]
print("Using shapefile:", shp_path)

# =======================================
# 2) Read shapefile (DBF is Korean-encoded)
# =======================================
r = shapefile.Reader(shp_path, encoding="cp949")
print("DBF fields:", [f[0] for f in r.fields[1:]])

# Expecting: BASE_YEAR, SIGUNGU_CD, SIGUNGU_NM
# If the file changes for whatever reason, this print helps adjust safely.

# =======================================================
# 3) Coordinate transformer: UTM-K(GRS80) -> WGS84 lon/lat
# =======================================================
to_wgs84 = Transformer.from_crs(5179, 4326, always_xy=True)

# ==========================================
# 4) Compute centroid & representative point
# ==========================================
rows = []
for sr in r.iterShapeRecords():
    base_year, code, name = sr.record  # BASE_YEAR, SIGUNGU_CD, SIGUNGU_NM
    geom = shapely_shape(sr.shape.__geo_interface__)

    cent = geom.centroid                # geometric centroid (may be outside for concave shapes)
    rp = geom.representative_point()    # guaranteed inside polygon

    lon_cent, lat_cent = to_wgs84.transform(cent.x, cent.y)
    lon_rp, lat_rp = to_wgs84.transform(rp.x, rp.y)

    rows.append({
        "BASE_YEAR": int(base_year),
        "SIGUNGU_CD": str(code),
        "SIGUNGU_NM": str(name),

        # UTM-K (meters)
        "centroid_x_utmK": float(cent.x),
        "centroid_y_utmK": float(cent.y),
        "rep_x_utmK": float(rp.x),
        "rep_y_utmK": float(rp.y),

        # WGS84 — convenient for maps / sanity checks
        "centroid_lon_wgs84": float(lon_cent),
        "centroid_lat_wgs84": float(lat_cent),
        "rep_lon_wgs84": float(lon_rp),
        "rep_lat_wgs84": float(lat_rp),
    })

df = pd.DataFrame(rows).sort_values("SIGUNGU_CD").reset_index(drop=True)

# ============================================================
# 5) Build resid_area-style merge key and apply name "updates"
# ============================================================

# Clean up strings (remove whitespace)
df["SIGUNGU_CD"] = df["SIGUNGU_CD"].astype(str).str.zfill(5)
df["SIGUNGU_NM"] = df["SIGUNGU_NM"].astype(str).str.replace(r"\s+", "", regex=True)

# Map prefix in THIS boundary file to province/city names
# (This matches the common boundary-file convention where 11=Seoul, 21=Busan, 31=Gyeonggi, etc. Ensured to match the main PSKC dataset)
sido_map = {
    "11": "서울특별시",
    "21": "부산광역시",
    "22": "대구광역시",
    "23": "인천광역시",
    "24": "광주광역시",
    "25": "대전광역시",
    "26": "울산광역시",
    "31": "경기도",
    "32": "강원도",
    "33": "충청북도",
    "34": "충청남도",
    "35": "전라북도",
    "36": "전라남도",
    "37": "경상북도",
    "38": "경상남도",
    "39": "제주특별자치도",
}

df["SIDO_CD"] = df["SIGUNGU_CD"].str[:2]
df["SIDO_NM"] = df["SIDO_CD"].map(sido_map)

if df["SIDO_NM"].isna().any():
    bad = df.loc[df["SIDO_NM"].isna(), ["SIGUNGU_CD", "SIGUNGU_NM"]].head(20)
    raise ValueError("Unmapped SIDO_CD found. Examples:\n" + bad.to_string(index=False))

# Split patterns like "포항시남구" -> "포항시/남구"
pat_city_gu = re.compile(r"^(.*?시)(.*구)$")

def make_resid_area_2010(sido_nm: str, sigungu_nm: str) -> str:
    m = pat_city_gu.match(sigungu_nm)
    if m:
        return f"{sido_nm}/{m.group(1)}/{m.group(2)}"
    return f"{sido_nm}/{sigungu_nm}"

df["resid_area_2010"] = [
    make_resid_area_2010(a, b) for a, b in zip(df["SIDO_NM"], df["SIGUNGU_NM"])
]

# 1-to-1 rename crosswalk to match the main dataset labels

rename_map = {
    "충청남도/당진군": "충청남도/당진시",
    "경기도/여주군": "경기도/여주시",
}

df["resid_area"] = df["resid_area_2010"].replace(rename_map)

# Uniqueness check (critical because names like "중구" exist in multiple places)
if not df["resid_area"].is_unique:
    dup = df[df["resid_area"].duplicated(keep=False)][["SIGUNGU_CD", "resid_area"]].sort_values("resid_area")
    raise ValueError("resid_area key is not unique. Duplicates:\n" + dup.to_string(index=False))

print("Example keys:\n", df[["SIGUNGU_CD", "SIGUNGU_NM", "resid_area"]].head(10).to_string(index=False))

# ==================
# 6) Export the files
# ==================
out_dir = os.path.dirname(extract_dir)
out_csv = os.path.join(out_dir, out_csv_name)
out_dta = os.path.join(out_dir, out_dta_name)

df.to_csv(out_csv, index=False, encoding="utf-8-sig")

# Note: pandas build doesn't accept encoding=... for to_stata()
df.to_stata(out_dta, write_index=False, version=118)

print("Wrote:", out_csv)
print("Wrote:", out_dta)
