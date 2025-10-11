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

    #Make sure the value column is numeric
    gfw[cn.gfw_annual_removals_col] = pd.to_numeric(gfw[cn.gfw_annual_removals_col], errors="coerce")

    #Coerce into boolean for translation rules
    gfw[cn.is_ifl_col] = standardize_bool(gfw[cn.is_ifl_col])
    gfw[cn.is_tcl_col] = standardize_bool(gfw[cn.is_tcl_col])

    #Translate removals into those considered to be "anthropogenic" using the IFL/ primary forest managed land proxy.
    # This includes removals where:
        # "is__intact_primary_forest" == FALSE OR
        # "is__intact_primary_forest" == TRUE AND is__umd_tree_cover_loss == TRUE AND driver_of_tree_cover_loss IN ('Shifting cultivation', 'Logging')
    # Note: Removals associated with TCL in intact or primary forests due to shifting cultivation or logging
    # are considered "anthropogenic", since regrowth occurs after unmanaged forest is converted to managed land.
    anthro_mask = (~gfw[cn.is_ifl_col]) | (gfw[cn.is_ifl_col] & gfw[cn.is_tcl_col] & gfw[cn.driver_col].isin(["Shifting cultivation", "Logging"]))

    #Translate removals into those considered to be "non-anthropogenic" using the IFL/ primary forest managed land proxy.
    # This includes removals where:
        # ("is__intact_primary_forest" == TRUE AND is__umd_tree_cover_loss == FALSE) OR
        # ("is__intact_primary_forest" == TRUE AND is__umd_tree_cover_loss == TRUE AND driver_of_tree_cover_loss IN
        # ('Permanent agriculture', 'Hard commodities', 'Wildfire', 'Settlements & infrastructure', 'Other natural disturbances', 'No driver'))
    # Note: Removals associated with TCL in intact or primary forests due to deforestation (permanent ag, commodites, and settlements)
    # are assumed to occur before deforestation and thus occured before unmanged forest was converted to managed land.
    # TCL in intact or primary forests due to non-anthropogenic causes (wildfire, natural disturbances, NA) do not result in
    # the conversion of unmanaged forest to managed land and thus these removals are considered to be non-anthropogenic
    nonanthro_mask = (gfw[cn.is_ifl_col] & ~gfw[cn.is_tcl_col]) | (gfw[cn.is_ifl_col] & gfw[cn.is_tcl_col] & gfw[cn.driver_col].isin([
        "Permanent agriculture", "Hard commodities", "Wildfire", "Settlements & infrastructure", "Other natural disturbances", "No driver"]))

    #Sum by iso
    gross_annual_removals = gfw.groupby(cn.iso_col, dropna=False)[cn.gfw_annual_removals_col].sum(min_count=1)
    anthro_removals = gfw.loc[anthro_mask].groupby(cn.iso_col, dropna=False)[cn.gfw_annual_removals_col].sum(min_count=1)
    nonanthro_removals = gfw.loc[nonanthro_mask].groupby(cn.iso_col, dropna=False)[cn.gfw_annual_removals_col].sum(min_count=1)

    #Write sums into translated_removals_df
    out["gross_annual_removal__Mg_CO2_yr-1"] = out[cn.iso_col].map(gross_annual_removals)
    out["anthro_annual_removal__Mg_CO2_yr-1"] = out[cn.iso_col].map(anthro_removals)
    out["non_anthro_annual_removal__Mg_CO2_yr-1"] = out[cn.iso_col].map(nonanthro_removals)

    # Check that anthro + nonanthro equals gross annual removals in output df
    check = out["anthro_annual_removal__Mg_CO2_yr-1"].fillna(0) + out["non_anthro_annual_removal__Mg_CO2_yr-1"].fillna(0)
    gross = out["gross_annual_removal__Mg_CO2_yr-1"].fillna(0)

    if not np.allclose(check.values, gross.values, atol=1e-6, rtol=0.0):
        raise ValueError("Anthropogenic + non-anthropogenic removals does not equal gross removals for at least one country.")

    #TODO: MANAGED LAND PROXY CODE IMPLEMENTATION


    return out
