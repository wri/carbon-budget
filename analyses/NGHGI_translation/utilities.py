import pandas as pd
from pathlib import Path

########################################################################################################################
#STEP 1: MANAGED LAND PROXY RECLASSIFICATION
########################################################################################################################
#Makes sure that the JRC managed land proxy codes are standardized (i.e numbers are ints and letters are lowercased)
def standardize_jrc_code(val):
    if pd.isna(val):
        return None
    s = str(val).strip()
    try:
        f = float(s)
        i = int(f)
        if f == i:
            return i
    except Exception:
        pass
    return s.lower()

# Reclassifies the JRC managed land proxy codes into new codes that determine which methods to use for the NGHGI translation
def jrc_to_wri(normalized):
    if normalized in ("3a", "3b"):
        return "1"
    if normalized == 1:
        return "2a"
    if normalized in {2, 4, 5, 6}:
        return "2b"
    return None

# Updates the managed land proxy dataframe with a reclassified "gfw_code" column used for the NGHGI translation
def update_managed_land_proxy_df(df, jrc_col, wri_col):

    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"must be a pandas DataFrame")

    # Find the jrc column (case-insensitive)
    col_map = {c.lower(): c for c in df.columns}
    if jrc_col.lower() not in col_map:
        raise KeyError(f"'{jrc_col}' column not found. Available columns: {list(df.columns)}")
    jrc_actual = col_map[jrc_col.lower()]

    out = df.copy()
    normalized = out[jrc_actual].map(standardize_jrc_code)
    out[wri_col] = normalized.map(jrc_to_wri)
    print("Reclassified JRC managed land proxy codes into GFW managed land proxy codes")
    return out

########################################################################################################################
#STEP 2: REMOVALS TRANSLATION
########################################################################################################################