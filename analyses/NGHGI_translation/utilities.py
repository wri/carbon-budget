import pandas as pd
import numpy as np
from pathlib import Path
import constants as cn


#-----------------------------------------------------------------------------------------------------------------------
# STEP 1: MANAGED LAND PROXY RECLASSIFICATION
#-----------------------------------------------------------------------------------------------------------------------
# Ensures that the JRC managed land proxy codes are standardized (i.e numbers are ints and letters are lowercased)
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

# Reclassifies the JRC managed land proxy codes into new codes that determine which methods to use for NGHGI translation
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
    # Ensure all column names are lowercased
    df.columns = df.columns.str.strip().str.lower()
    column_names = {c for c in df.columns}

    # Selects the column with the JRC managed land proxy codes
    if jrc_col.lower() not in column_names:
        raise KeyError(f"'{jrc_col}' column not found. Available columns: {list(df.columns)}")
    jrc_codes = column_names[jrc_col.lower()]

    # Creates a new copy of the dataframe and adds a column with the reclassified WRI codes based on the JRC codes
    out = df.copy()
    normalized = out[jrc_codes].map(standardize_jrc_code)
    out[wri_col] = normalized.map(jrc_to_wri)
    print("Reclassified JRC managed land proxy codes into GFW managed land proxy codes")
    return out


#-----------------------------------------------------------------------------------------------------------------------
# STEP 2: REMOVALS TRANSLATION
#-----------------------------------------------------------------------------------------------------------------------

# Makes the true, false, and nan values in the dataframe to boolean types
def standardize_bool(s):
    return s.astype(str).str.strip().str.lower().map({"true": True, "false": False, "nan": np.nan}).astype("boolean")

# Translates GFW forest removals into anthropogenic forest and non-anthropogenic forest removals.
def translate_removals(keep_col_df, gfw_removals_df, managed_polygons_df):
    out = keep_col_df.copy()
    gfw = gfw_removals_df.copy()
    managed = managed_polygons_df.copy()

    # Ensure removals columns are numeric and coerce primary/ifl and tcl columns into boolean for translation rules
    gfw[cn.gfw_annual_removals_col] = pd.to_numeric(gfw[cn.gfw_annual_removals_col], errors="coerce")
    gfw[cn.is_ifl_prim_col] = standardize_bool(gfw[cn.is_ifl_prim_col])
    gfw[cn.is_tcl_col] = standardize_bool(gfw[cn.is_tcl_col])

    # Divide gross geotrellis removals by number of years and multiply by -1 to get average annual removals
    managed[cn.gfw_annual_removals_col] = pd.to_numeric(managed[cn.geotrellis_gross_removals_col], errors="coerce").div(cn.n_years) * -1
    managed[cn.is_tcl_col] = standardize_bool(managed[cn.is_tcl_col])

    # Group countries by GFW managed land proxy code
    mlp_1 = out[cn.gfw_code_col].astype(str).str.lower().eq("1")
    mlp_2a = out[cn.gfw_code_col].astype(str).str.lower().eq("2a")
    mlp_2b = out[cn.gfw_code_col].astype(str).str.lower().eq("2b")

    # Add gross average annual removals (Mg CO2 per year) column to out df
    gross_annual_removals = gfw.groupby(cn.iso_col, dropna=False)[cn.gfw_annual_removals_col].sum(min_count=1)
    gross_removals_avg = managed.groupby(cn.iso_col, dropna=False)[cn.gfw_annual_removals_col].sum(min_count=1)
    out[cn.gross_removals_col] = np.where(mlp_2a, out[cn.iso_col].map(gross_removals_avg), out[cn.iso_col].map(gross_annual_removals))

    # Use the managed land proxy code to assign "anthropogenic forest" and "non-anthropogenic forest" removals below
    out[cn.anthro_removals_col] = np.nan
    out[cn.nonanthro_removals_col] = np.nan

    #-------------------------------------------------------------------------------------------------------------------
    # Case 1: All removals are anthropogenic, no removals are non-anthropogenic
    #-------------------------------------------------------------------------------------------------------------------
    out.loc[mlp_1, cn.anthro_removals_col] = out.loc[mlp_1, cn.gross_removals_col]
    out.loc[mlp_1, cn.nonanthro_removals_col] = 0.0

    #-------------------------------------------------------------------------------------------------------------------
    # Case 2a: Managed land polygons determine anthropogenic (managed) vs non-anthropogenic (unmanaged) removals
    #-------------------------------------------------------------------------------------------------------------------
    # GFW removals are translated into "anthropogenic forest" removals using managed polygon delineation.
    # This includes the following removals:
    #   1) All removals in managed polygons
    #   2) Removals in unmanaged land polygons associated with tree cover loss due to shifting cultivation or logging
    #      because regrowth can occur after unmanaged forest is converted to managed forest.
    managed_forest_mask = (managed[cn.class_col].eq("managed")|
                          (managed[cn.class_col].eq("unmanaged") & managed[cn.is_tcl_col] & managed[cn.driver_col].isin(["Shifting cultivation", "Logging"])))

    # GFW removals are translated into "non-anthropogenic forest" removals unmanaged polygon delineation.
    # This includes the following removals:
    #     1) Removals in unmanaged polygons without any tree cover loss.
    #     2) Removals in unmanaged polygons with tree cover loss due to "non-anthropogenic" causes (wildfire, natural disturbances, unknown)
    #     3) Removals in unmanaged polygons with tree cover loss resulting in "deforestation" (permanent ag, commodities, and settlements).
    #        They are assumed to have occured before deforestation and thus before unmanaged forest was converted to managed land.
    unmanaged_forest_mask = ((managed[cn.class_col].eq("unmanaged") & ~managed[cn.is_tcl_col]) |
                             (managed[cn.class_col].eq("unmanaged") & managed[cn.is_tcl_col] & managed[cn.driver_col].isin(['Wildfire', 'Other natural disturbances', 'Unknown'])) |
                             (managed[cn.class_col].eq("unmanaged") & managed[cn.is_tcl_col] & managed[cn.driver_col].isin(['Permanent agriculture', 'Hard commodities', 'Settlements & Infrastructure'])))

    # Sum translated removals by iso code
    managed_forest_removals = managed.loc[managed_forest_mask].groupby(cn.iso_col, dropna=False)[cn.gfw_annual_removals_col].sum(min_count=1)
    unmanaged_forest_removals = managed.loc[unmanaged_forest_mask].groupby(cn.iso_col, dropna=False)[cn.gfw_annual_removals_col].sum(min_count=1)

    # Write translated removals from managed polygons for '2a' countries only
    out.loc[mlp_2a, cn.anthro_removals_col] = out.loc[mlp_2a, cn.iso_col].map(managed_forest_removals)
    out.loc[mlp_2a, cn.nonanthro_removals_col] = out.loc[mlp_2a, cn.iso_col].map(unmanaged_forest_removals)

    #-------------------------------------------------------------------------------------------------------------------
    # Case 2b: Primary/IFL forest proxy determines anthropogenic vs non-anthropogenic removals
    #-------------------------------------------------------------------------------------------------------------------
    # GFW removals are translated into "anthropogenic forest" removals using a non-primary/non-IFL forest proxy for managed forest.
    # This includes the following removals:
    #     1) All removals in secondary forests
    #     2) Removals associated with loss of primary/intact forests due to shifting cultivation or logging because
    #        regrowth can occur after unmanaged forest is converted to managed forest.
    anthro_forest_mask = ((~gfw[cn.is_ifl_prim_col]) |
                          (gfw[cn.is_ifl_prim_col] & gfw[cn.is_tcl_col] & gfw[cn.driver_col].isin(['Shifting cultivation', 'Logging'])))

    # GFW removals are translated into "non-anthropogenic forest" removals using a primary/IFL forest proxy for unmanaged forests.
    # This includes the following removals:
    #     1) Removals from primary/intact forests without any tree cover loss.
    #     2) Removals from primary/intact forests with tree cover loss due to "non-anthropogenic" causes (wildfire, natural disturbances, unknown)
    #     3) Removals from primary/intact forests with tree cover loss resulting in "deforestation" (permanent ag, commodities, and settlements).
    #        They are assumed to have occured before deforestation and thus before unmanaged forest was converted to managed land.
    nonanthro_forest_mask = ((gfw[cn.is_ifl_prim_col] & ~gfw[cn.is_tcl_col]) |
                             (gfw[cn.is_ifl_prim_col] & gfw[cn.is_tcl_col] & gfw[cn.driver_col].isin(['Wildfire', 'Other natural disturbances', 'Unknown'])) |
                             (gfw[cn.is_ifl_prim_col] & gfw[cn.is_tcl_col] & gfw[cn.driver_col].isin(['Permanent agriculture', 'Hard commodities', 'Settlements & Infrastructure'])))

    # Sum translated removals by iso code
    anthro_forest_removals = gfw.loc[anthro_forest_mask].groupby(cn.iso_col, dropna=False)[cn.gfw_annual_removals_col].sum(min_count=1)
    nonanthro_forest_removals = gfw.loc[nonanthro_forest_mask].groupby(cn.iso_col, dropna=False)[cn.gfw_annual_removals_col].sum(min_count=1)

    # Write translated removals from primary/IFL forest proxy for '2b' countries only
    out.loc[mlp_2b, cn.anthro_removals_col] = out.loc[mlp_2b, cn.iso_col].map(anthro_forest_removals)
    out.loc[mlp_2b, cn.nonanthro_removals_col] = out.loc[mlp_2b, cn.iso_col].map(nonanthro_forest_removals)

    # Check that anthro + non-anthro removals equals gross annual removals in out df
    check = out[cn.anthro_removals_col].fillna(0) + out[cn.nonanthro_removals_col].fillna(0)
    gross = out[cn.gross_removals_col].fillna(0)
    if not np.allclose(check.values, gross.values, atol=1e-6, rtol=0.0):
        raise ValueError("Anthropogenic + non-anthropogenic removals do not equal gross removals for at least one country.")

    return out


#-----------------------------------------------------------------------------------------------------------------------
# STEP 3: EMISSIONS TRANSLATION
#-----------------------------------------------------------------------------------------------------------------------

# Core function to translate GFW forest emissions into "anthropogenic deforestation", "anthropogenic forest", and "non-anthropogenic forest" emissions.
def translate_emissions(keep_col_df, gfw_emissions_df, usa_df, canada_df, brazil_df):
    out_base = keep_col_df.copy()
    gfw = gfw_emissions_df.copy()

    # Make sure the emissions column is numeric and primary/intact column is boolean for translation rules
    gfw[cn.gfw_emissions_col] = pd.to_numeric(gfw[cn.gfw_emissions_col], errors="coerce")
    gfw[cn.is_ifl_prim_col] = standardize_bool(gfw[cn.is_ifl_prim_col])

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
                           (gfw[cn.is_ifl_prim_col] & gfw[cn.driver_col].isin(['Shifting cultivation'])))
    elif cn.secondary_shift_cult_cat == 'deforestation':
        mask_anthro_def = ((gfw[cn.driver_col].isin(['Permanent agriculture', 'Hard commodities', 'Settlements & Infrastructure'])) |
                           (gfw[cn.is_ifl_prim_col] & gfw[cn.driver_col].isin(['Shifting cultivation'])) |
                           ((~gfw[cn.is_ifl_prim_col]) & gfw[cn.driver_col].isin(['Shifting cultivation'])))

    # Translate GFW emissions into "anthropogenic forest" using the primary/IFL forest managed land proxy.
    # This includes the following emissions:
        # All emissions from logging in both primary/intact forests and secondary forests.
        # Emissions from "non-anthropogenic" causes (wildfire, natural disturbances, unknown) in secondary forests only.
        # Optional: Emissions from shifting cultivation in secondary forest only can also be included.
    if cn.secondary_shift_cult_cat == 'forest':
        mask_anthro_for = ((gfw[cn.driver_col].isin(['Logging'])) |
                           ((~gfw[cn.is_ifl_prim_col]) & gfw[cn.driver_col].isin(['Wildfire', 'Other natural disturbances', 'Unknown'])) |
                           ((~gfw[cn.is_ifl_prim_col]) & gfw[cn.driver_col].isin(['Shifting cultivation'])))

    elif cn.secondary_shift_cult_cat == 'deforestation':
        mask_anthro_for = ((gfw[cn.driver_col].isin(['Logging'])) |
                           ((~gfw[cn.is_ifl_prim_col]) & gfw[cn.driver_col].isin(['Wildfire', 'Other natural disturbances', 'Unknown'])))

    # Translate GFW emissions into "non-anthropogenic forest" using the primary/IFL forest managed land proxy.
    # This includes the following emissions:
        # Emissions from "non-anthropogenic" causes (wildfire, natural disturbances, unknown) in primary/intact forests only.
    mask_nonanthro_for = (gfw[cn.is_ifl_prim_col]) & gfw[cn.driver_col].isin(['Wildfire', 'Other natural disturbances', 'Unknown'])

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
    # if not cn.keep_inter_cols:
    #     out = out.drop(columns=['anthro_deforest_emissions_proxy', 'anthro_forest_emissions_proxy', 'nonanthro_forest_emissions_proxy'], errors='ignore')

    return out