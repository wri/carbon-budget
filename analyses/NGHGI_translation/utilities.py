import pandas as pd
import numpy as np
from pathlib import Path
import constants as cn

########################################################################################################################
#STEP 1: MANAGED LAND PROXY RECLASSIFICATION
########################################################################################################################
def check_pandas_df(df):
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"must be a pandas DataFrame")

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
    check_pandas_df(df)
    df.columns = df.columns.str.strip().str.lower()     #make sure columns are all lowercase
    col_map = {c for c in df.columns}

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
def standardize_bool(s):
    return s.astype(str).str.strip().str.lower().map({"true": True, "false": False, "1": True, "0": False, "nan": np.nan}).astype("boolean")

def translate_removals(translated_removals_df, gfw_removals_df):
    out = translated_removals_df.copy()
    gfw = gfw_removals_df.copy()

    #Make sure the removals column is numeric
    gfw[cn.gfw_annual_removals_col] = pd.to_numeric(gfw[cn.gfw_annual_removals_col], errors="coerce")

    #Coerce is is__intact_primary_forest and is__umd_tree_cover_loss into boolean for translation rules
    gfw[cn.is_ifl_col] = standardize_bool(gfw[cn.is_ifl_col])
    gfw[cn.is_tcl_col] = standardize_bool(gfw[cn.is_tcl_col])

    #Translate removals into those considered to be "anthropogenic" using the IFL/ primary forest managed land proxy.
    # This includes removals where:
        # "is__intact_primary_forest" == FALSE OR
        # "is__intact_primary_forest" == TRUE AND is__umd_tree_cover_loss == TRUE AND driver_of_tree_cover_loss IN ('Shifting cultivation', 'Logging')
    # Note: Removals associated with TCL in intact or primary forests due to shifting cultivation or logging
    # are considered "anthropogenic", since regrowth can occur after unmanaged forest is converted to managed forest.
    nifl_mask = ((~gfw[cn.is_ifl_col]) |
                 (gfw[cn.is_ifl_col] & gfw[cn.is_tcl_col] & gfw[cn.driver_col].isin([
                     "Shifting cultivation", "Logging"])))

    #Translate removals into those considered to be "non-anthropogenic" using the IFL/ primary forest managed land proxy.
    # This includes removals where:
        # ("is__intact_primary_forest" == TRUE AND is__umd_tree_cover_loss == FALSE) OR
        # ("is__intact_primary_forest" == TRUE AND is__umd_tree_cover_loss == TRUE AND driver_of_tree_cover_loss IN
        # ('Permanent agriculture', 'Hard commodities', 'Wildfire', 'Settlements & infrastructure', 'Other natural disturbances', 'No driver'))
    # Note: Removals associated with TCL in intact or primary forests due to deforestation (permanent ag, commodites, and settlements)
    # are assumed to occur before deforestation and thus occured before unmanged forest was converted to managed land.
    # Note: TCL in intact or primary forests due to non-anthropogenic causes (wildfire, natural disturbances, no driver) do not result
    # in the conversion of unmanaged forest to managed forest and thus these removals are considered to be non-anthropogenic.
    ifl_mask = ((gfw[cn.is_ifl_col] & ~gfw[cn.is_tcl_col]) |
                (gfw[cn.is_ifl_col] & gfw[cn.is_tcl_col] & gfw[cn.driver_col].isin([
                    "Permanent agriculture", "Hard commodities", "Settlements & Infrastructure",
                    "Wildfire", "Other natural disturbances", "Unknown"])))

    #Sum by iso
    gross_annual_removals = gfw.groupby(cn.iso_col, dropna=False)[cn.gfw_annual_removals_col].sum(min_count=1)
    nifl_removals = gfw.loc[nifl_mask].groupby(cn.iso_col, dropna=False)[cn.gfw_annual_removals_col].sum(min_count=1)
    ifl_removals = gfw.loc[ifl_mask].groupby(cn.iso_col, dropna=False)[cn.gfw_annual_removals_col].sum(min_count=1)

    #Write sums into translated_removals_df
    out[cn.gross_removals_col] = out[cn.iso_col].map(gross_annual_removals)
    out['nifl_proxy_removals'] = out[cn.iso_col].map(nifl_removals)
    out['ifl_proxy_removals'] = out[cn.iso_col].map(ifl_removals)

    # Check that nifl_proxy + ifl_proxy removals equals gross annual removals in out df
    check = out['nifl_proxy_removals'].fillna(0) + out['ifl_proxy_removals'].fillna(0)
    gross = out[cn.gross_removals_col].fillna(0)
    if not np.allclose(check.values, gross.values, atol=1e-6, rtol=0.0):
        raise ValueError("Non-ifl/primary proxy + ifl/primary proxy removals do not equal gross removals for at least one country.")

    #Use the GFW managed land proxy decision tree code to decide which removals are "anthropogenic" and which are "non-anthropogenic"
    # Initialize target columns
    out[cn.anthro_removals_col] = np.nan
    out[cn.nonanthro_removals_col] = np.nan

    mlp_1 = out[cn.gfw_code_col].astype(str).str.lower().eq("1")
    mlp_2a = out[cn.gfw_code_col].astype(str).str.lower().eq("2a")
    mlp_2b = out[cn.gfw_code_col].astype(str).str.lower().eq("2b")

    # Case 1: All removals are anthropogenic, no removals are non-anthropogenic
    out.loc[mlp_1, cn.anthro_removals_col] = out.loc[mlp_1, cn.gross_removals_col]
    out.loc[mlp_1, cn.nonanthro_removals_col] = 0.0

    # Case 2a: Managed land polygons determine anthropogenic (managed) vs non-anthropogenic (unmanaged) removals
    # (leave NaN placeholders; fill later when polygon-derived removals are available)
    # out.loc[mlp_2a, cn.anthro_removals_col] = fill from geotrellis spreadsheet
    # out.loc[mlp_2a, cn.nonanthro_removals_col] = fill from geotrellis spreadsheet
    # TODO: Update with managed land polygons

    # Case 2b: IFL/primary forest proxy determines anthropogenic (non-ifl/primary proxy) vs non-anthropogenic (ifl/primary proxy) removals
    out.loc[mlp_2b, cn.anthro_removals_col] = out.loc[mlp_2b, "nifl_proxy_removals"]
    out.loc[mlp_2b, cn.nonanthro_removals_col] = out.loc[mlp_2b, "ifl_proxy_removals"]

    # Check that anthro + non-anthro removals equals gross annual removals in out df
    # check = out[cn.anthro_removals_col].fillna(0) + out[cn.nonanthro_removals_col].fillna(0)
    # gross = out[cn.gross_removals_col].fillna(0)
    # if not np.allclose(check.values, gross.values, atol=1e-6, rtol=0.0):
    #     raise ValueError("Anthropogenic + non-anthropoegnic removals do not equal gross removals for at least one country.")
    #TODO: Make sure managed land polygon total == country totals

    # Delete intermediate columns if keep_inter_cols is not True in constants.py
    if not cn.keep_inter_cols:
        out = out.drop(columns=['nifl_proxy_removals', 'ifl_proxy_removals'], errors='ignore')

    return out
