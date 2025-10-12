import pandas as pd
import numpy as np
from pathlib import Path
import constants as cn

########################################################################################################################
# STEP 1: MANAGED LAND PROXY RECLASSIFICATION
########################################################################################################################
def check_pandas_df(df):
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"must be a pandas DataFrame")

# Makes sure that the JRC managed land proxy codes are standardized (i.e numbers are ints and letters are lowercased)
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
# STEP 2: REMOVALS TRANSLATION
########################################################################################################################
def standardize_bool(s):
    return s.astype(str).str.strip().str.lower().map({"true": True, "false": False, "1": True, "0": False, "nan": np.nan}).astype("boolean")

# Core function to translate GFW forest removals into anthropogenic forest and non-anthropogenic forest removals.
def translate_removals(keep_col_df, gfw_removals_df):
    out = keep_col_df.copy()
    gfw = gfw_removals_df.copy()

    # Make sure the removals column is numeric
    gfw[cn.gfw_annual_removals_col] = pd.to_numeric(gfw[cn.gfw_annual_removals_col], errors="coerce")

    # Coerce primary/ifl and tcl columns into boolean for translation rules
    gfw[cn.is_ifl_col] = standardize_bool(gfw[cn.is_ifl_col])
    gfw[cn.is_tcl_col] = standardize_bool(gfw[cn.is_tcl_col])

    # Translate GFW removals into "anthropogenic forest" removals using the primary/IFL forest managed land proxy.
    # This includes the following removals:
        # All removals in secondary forests
        # Removals associated with loss of primary/intact forests due to shifting cultivation or logging because
        # regrowth can occur after unmanaged forest is converted to managed forest.
    anthro_forest_mask = ((~gfw[cn.is_ifl_col]) |
                          (gfw[cn.is_ifl_col] & gfw[cn.is_tcl_col] & gfw[cn.driver_col].isin(['Shifting cultivation', 'Logging'])))

    # Translate GFW removals into "non-anthropogenic forest" removals using the primary/IFL forest managed land proxy.
    # This includes removals where:
        # Removals from primary/intact forests without any tree cover loss.
        # Removals from primary/intact forests with tree cover loss due to "non-anthropogenic" causes (wildfire, natural disturbances, unknown)
        # Removals from primary/intact forests with tree cover loss resulting in "deforestation" (permanent ag, commodites, and settlements). Removals
        # are assumed to have occured before deforestation occured and thus before unmanged forest was converted to managed land.
    nonanthro_forest_mask = ((gfw[cn.is_ifl_col] & ~gfw[cn.is_tcl_col]) |
                (gfw[cn.is_ifl_col] & gfw[cn.is_tcl_col] & gfw[cn.driver_col].isin(['Wildfire', 'Other natural disturbances', 'Unknown'])) |
                (gfw[cn.is_ifl_col] & gfw[cn.is_tcl_col] & gfw[cn.driver_col].isin(['Permanent agriculture', 'Hard commodities', 'Settlements & Infrastructure'])))

    # Sum by iso
    gross_annual_removals = gfw.groupby(cn.iso_col, dropna=False)[cn.gfw_annual_removals_col].sum(min_count=1)
    anthro_forest_removals = gfw.loc[anthro_forest_mask].groupby(cn.iso_col, dropna=False)[cn.gfw_annual_removals_col].sum(min_count=1)
    nonanthro_forest_removals = gfw.loc[nonanthro_forest_mask].groupby(cn.iso_col, dropna=False)[cn.gfw_annual_removals_col].sum(min_count=1)

    # Write sums into translated_removals_df
    out[cn.gross_removals_col] = out[cn.iso_col].map(gross_annual_removals)
    out['anthro_forest_removals_proxy'] = out[cn.iso_col].map(anthro_forest_removals)
    out['nonanthro_forest_removals_proxy'] = out[cn.iso_col].map(nonanthro_forest_removals)

    # Check that estimates for "anthropogenic forest" + "non-anthropogenic forest" using the managed land proxy equals gross annual removals
    check = out['anthro_forest_removals_proxy'].fillna(0) + out['nonanthro_forest_removals_proxy'].fillna(0)
    gross = out[cn.gross_removals_col].fillna(0)
    if not np.allclose(check.values, gross.values, atol=1e-6, rtol=0.0):
        raise ValueError("Managed land proxies for anthropogenic + non-anthropogenic removals do not equal gross removals for at least one country.")

    # Use the GFW managed land proxy code to determine which removals to assign as "anthropogenic forest" and "non-anthropogenic forest"
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

    # Case 2b: Primary/IFL forest proxy determines anthropogenic vs non-anthropogenic removals (rules outlined above)
    out.loc[mlp_2b, cn.anthro_removals_col] = out.loc[mlp_2b, "anthro_forest_removals_proxy"]
    out.loc[mlp_2b, cn.nonanthro_removals_col] = out.loc[mlp_2b, "nonanthro_forest_removals_proxy"]

    # Check that anthro + non-anthro removals equals gross annual removals in out df
    # check = out[cn.anthro_removals_col].fillna(0) + out[cn.nonanthro_removals_col].fillna(0)
    # gross = out[cn.gross_removals_col].fillna(0)
    # if not np.allclose(check.values, gross.values, atol=1e-6, rtol=0.0):
    #     raise ValueError("Anthropogenic + non-anthropoegnic removals do not equal gross removals for at least one country.")
    #TODO: Make sure managed land polygon total == country totals

    # Delete intermediate columns if keep_inter_cols is not True in constants.py
    if not cn.keep_inter_cols:
        out = out.drop(columns=['anthro_forest_removals_proxy', 'nonanthro_forest_removals_proxy'], errors='ignore')

    return out

########################################################################################################################
# STEP 3: EMISSIONS TRANSLATION
########################################################################################################################
# Core function to translate GFW forest emissions into "anthropogenic deforestation", "anthropogenic forest", and "non-anthropogenic forest" emissions.
def translate_emissions(keep_col_df, gfw_emissions_df):
    out_base = keep_col_df.copy()
    gfw = gfw_emissions_df.copy()

    # Make sure the emissions column is numeric and primary/intact column is boolean for translation rules
    gfw[cn.gfw_emissions_col] = pd.to_numeric(gfw[cn.gfw_emissions_col], errors="coerce")
    gfw[cn.is_ifl_col] = standardize_bool(gfw[cn.is_ifl_col])

    # Since it is often not clear which NGHGI category emissions from tree cover loss in secondary forests associated
    # with shifting cultivation cycles are reported in, we generated two scenarios for our emissions reclassification:
        # 'forest': one where these emissions are reported as "anthropogenic forest" and
        # 'deforest': one where they are reported as "anthropogenic deforestation" emissions.

    # Translate GFW emissions into "anthropogenic deforestation" using the primary/IFL forest managed land proxy.
    # This includes the following emissions:
        # All emissions from permanent ag, commodites, and settlements in both primary/intact forests and secondary forests.
        # Always emissions from shifting cultivation in primary/intact forests because it is a permanent change from forest to a non-forest land use.
        # Optional: Emissions from shifting cultivation in secondary forest can also be included.
    if cn.secondary_shift_cult_cat == 'forest':
        mask_anthro_def = ((gfw[cn.driver_col].isin(['Permanent agriculture', 'Hard commodities', 'Settlements & Infrastructure'])) |
                           (gfw[cn.is_ifl_col] & gfw[cn.driver_col].isin(['Shifting cultivation'])))
    elif cn.secondary_shift_cult_cat == 'deforestation':
        mask_anthro_def = ((gfw[cn.driver_col].isin(['Permanent agriculture', 'Hard commodities', 'Settlements & Infrastructure'])) |
                           (gfw[cn.is_ifl_col] & gfw[cn.driver_col].isin(['Shifting cultivation'])) |
                           ((~gfw[cn.is_ifl_col]) & gfw[cn.driver_col].isin(['Shifting cultivation'])))

    # Translate GFW emissions into "anthropogenic forest" using the primary/IFL forest managed land proxy.
    # This includes the following emissions:
        # All emissions from logging in both primary/intact forests and secondary forests.
        # Emissions from "non-anthropogenic" causes (wildfire, natural disturbances, unknown) in secondary forests only.
        # Optional: Emissions from shifting cultivation in secondary forest only can also be included.
    if cn.secondary_shift_cult_cat == 'forest':
        mask_anthro_for = ((gfw[cn.driver_col].isin(['Logging'])) |
                          ((~gfw[cn.is_ifl_col]) & gfw[cn.driver_col].isin(['Wildfire', 'Other natural disturbances', 'Unknown'])) |
                          ((~gfw[cn.is_ifl_col]) & gfw[cn.driver_col].isin(['Shifting cultivation'])))

    elif cn.secondary_shift_cult_cat == 'deforestation':
        mask_anthro_for = ((gfw[cn.driver_col].isin(['Logging'])) |
                          ((~gfw[cn.is_ifl_col]) & gfw[cn.driver_col].isin(['Wildfire', 'Other natural disturbances', 'Unknown'])))

    # Translate GFW emissions into "non-anthropogenic forest" using the primary/IFL forest managed land proxy.
    # This includes the following emissions:
        # Emissions from "non-anthropogenic" causes (wildfire, natural disturbances, unknown) in primary/intact forests only.
    mask_nonanthro_for = (gfw[cn.is_ifl_col]) & gfw[cn.driver_col].isin(['Wildfire', 'Other natural disturbances', 'Unknown'])

    # Sum by iso x tree cover loss year
    group_keys = [cn.iso_col, cn.tcl_year_col]
    gross_emissions = gfw.groupby(group_keys, dropna=False)[cn.gfw_emissions_col].sum(min_count=1)
    anthro_def_emis = gfw.loc[mask_anthro_def].groupby(group_keys, dropna=False)[cn.gfw_emissions_col].sum(min_count=1)
    anthro_for_emis = gfw.loc[mask_anthro_for].groupby(group_keys, dropna=False)[cn.gfw_emissions_col].sum(min_count=1)
    nonanthro_for_emis = gfw.loc[mask_nonanthro_for].groupby(group_keys, dropna=False)[cn.gfw_emissions_col].sum(min_count=1)

    # Create the iso x year index
    uniq_pairs = gfw[group_keys].drop_duplicates()
    keep_cols = out_base[[cn.iso_col, cn.country_col, cn.gfw_code_col]].drop_duplicates()
    out = uniq_pairs.merge(keep_cols, on=cn.iso_col, how="left")

    # Write sums into translated_emissions_df
    out[cn.gross_emissions_col] = list(map(lambda r: gross_emissions.get((r[cn.iso_col], r[cn.tcl_year_col]), np.nan), out.to_dict("records")))
    out['anthro_deforest_emissions_proxy'] = list(map(lambda r: anthro_def_emis.get((r[cn.iso_col], r[cn.tcl_year_col]), np.nan), out.to_dict("records")))
    out['anthro_forest_emissions_proxy'] = list(map(lambda r: anthro_for_emis.get((r[cn.iso_col], r[cn.tcl_year_col]), np.nan), out.to_dict("records")))
    out['nonanthro_forest_emissions_proxy'] = list(map(lambda r: nonanthro_for_emis.get((r[cn.iso_col], r[cn.tcl_year_col]), np.nan), out.to_dict("records")))

    # Check that estimates for "anthropogenic deforestation" + "anthropogenic forest" + "non-anthropogenic forest" using
    # the managed land proxy equals gross emissions for each year of tree cover loss.
    check = (out['anthro_deforest_emissions_proxy'].fillna(0) + 
             out['anthro_forest_emissions_proxy'].fillna(0) + 
             out['nonanthro_forest_emissions_proxy'].fillna(0))
    gross = out[cn.gross_emissions_col].fillna(0)
    if not np.allclose(check.values, gross.values, atol=1e-6, rtol=0.0):
        raise ValueError("Managed land proxies for anthropogenic + non-anthropogenic emissions do not equal gross emissions for at least one country.")

    # Use the GFW managed land proxy code to determine which emissions to assign as
    # "anthropogenic deforestation", "anthropogenic forest" and "non-anthropogenic forest"
    out[cn.anthro_deforest_emissions_col] = np.nan
    out[cn.anthro_forest_emissions_col] = np.nan
    out[cn.nonanthro_forest_emissions_col] = np.nan

    mlp_1 = out[cn.gfw_code_col].astype(str).str.lower().eq("1")
    mlp_2a = out[cn.gfw_code_col].astype(str).str.lower().eq("2a")
    mlp_2b = out[cn.gfw_code_col].astype(str).str.lower().eq("2b")

    # Case 1: All forest emissions are anthropogenic, no emissions are non-anthropogenic.
    out.loc[mlp_1, cn.anthro_deforest_emissions_col] = out.loc[mlp_1, 'anthro_deforest_emissions_proxy']
    out.loc[mlp_1, cn.anthro_forest_emissions_col] = (out.loc[mlp_1, ['anthro_forest_emissions_proxy',
                                                                      'nonanthro_forest_emissions_proxy']].sum(axis=1, min_count=1))
    out.loc[mlp_1, cn.nonanthro_forest_emissions_col] = 0.0
    
    # Case 2a: Managed land polygons determine anthropgogenic vs non-anthropogenic emissions. 
    # (leave NaN placeholders; fill later when polygon-derived emissions are available)
    # out.loc[mlp_2a, cn.anthro_deforest_emissions_col] = fill from geotrellis spreadsheet
    # out.loc[mlp_2a, cn.anthro_forest_emissions_col] = fill from geotrellis spreadsheet
    # out.loc[mlp_2a, cn.nonanthro_forest_emissions_col] = fill from geotrellis spreadsheet
    # TODO: Update with managed land polygons

    # Case 2b: Primary/IFL forest proxy determines anthropogenic vs non-anthropogenic removals (rules outlined above)
    out.loc[mlp_2b, cn.anthro_deforest_emissions_col] = out.loc[mlp_2b, 'anthro_deforest_emissions_proxy']
    out.loc[mlp_2b, cn.anthro_forest_emissions_col] = out.loc[mlp_2b, 'anthro_forest_emissions_proxy'] 
    out.loc[mlp_2b, cn.nonanthro_forest_emissions_col] = out.loc[mlp_2b, 'nonanthro_forest_emissions_proxy']

    # Check that anthro + non-anthro emissions equals gross emissions in out
    # check = (out[cn.anthro_deforest_emissions_col].fillna(0) + 
    #          out[cn.anthro_forest_emissions_col].fillna(0) +
    #          out[cn.nonanthro_forest_emissions_col].fillna(0))
    # gross = out[cn.gross_emissions_col].fillna(0)
    # if not np.allclose(check.values, gross.values, atol=1e-6, rtol=0.0):
    #     raise ValueError("Anthropogenic + non-anthropoegnic emissions do not equal gross emissions for at least one country.")
    # TODO: Make sure managed land polygon total == country totals

    # Delete intermediate columns if keep_inter_cols is not True in constants.py
    if not cn.keep_inter_cols:
        out = out.drop(columns=['anthro_deforest_emissions_proxy', 'anthro_forest_emissions_proxy', 'nonanthro_forest_emissions_proxy'], errors='ignore')

    return out