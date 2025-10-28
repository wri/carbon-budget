import pandas as pd
import numpy as np
from pathlib import Path
import constants as cn
import re


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

# Makes the true, false, and nan values in the dataframe boolean types
def standardize_bool(s):
    return s.astype(str).str.strip().str.lower().map({"true": True, "false": False, "nan": np.nan}).astype("boolean")

# Translate GFW forest removals into "anthropogenic forest" and "non-anthropogenic forest" removals.
def translate_removals(keep_col_df, gfw_removals_df, managed_polygons_df):
    out = keep_col_df.copy()
    gfw = gfw_removals_df.copy()
    managed = managed_polygons_df.copy()

    # Ensure removals columns are numeric and coerce primary/ifl and tcl columns into boolean for translation rules
    gfw[cn.gfw_annual_removals_col] = pd.to_numeric(gfw[cn.gfw_annual_removals_col], errors="coerce")
    gfw[cn.is_ifl_prim_col] = standardize_bool(gfw[cn.is_ifl_prim_col])
    gfw[cn.is_tcl_col] = standardize_bool(gfw[cn.is_tcl_col])

    # Divide managed polygon removals by number of years and multiply by -1 to get average annual removals
    managed[cn.gfw_annual_removals_col] = pd.to_numeric(managed[cn.geotrellis_gross_removals_col], errors="coerce").div(cn.n_years) * -1
    managed[cn.is_tcl_col] = standardize_bool(managed[cn.is_tcl_col])

    # Group countries by GFW managed land proxy code
    mlp_1 = out[cn.gfw_code_col].astype(str).str.lower().eq("1")
    mlp_2a = out[cn.gfw_code_col].astype(str).str.lower().eq("2a")
    mlp_2b = out[cn.gfw_code_col].astype(str).str.lower().eq("2b")

    # Add gross average annual removals (Mg CO2 per year) column to out df for QC, fill nan with 0
    gross_annual_removals = gfw.groupby(cn.iso_col)[cn.gfw_annual_removals_col].sum()
    gross_removals_avg = managed.groupby(cn.iso_col)[cn.gfw_annual_removals_col].sum()
    out[cn.gross_removal_col] = np.where(mlp_2a, out[cn.iso_col].map(gross_removals_avg), out[cn.iso_col].map(gross_annual_removals))
    out[cn.gross_removal_col] = np.nan_to_num(out[cn.gross_removal_col], nan=0.0)

    # Use the managed land proxy code to assign "anthropogenic forest" and "non-anthropogenic forest" removals below
    out[cn.anthro_removal_col] = 0.0
    out[cn.nonanthro_removal_col] = 0.0

    #-------------------------------------------------------------------------------------------------------------------
    # Case 1: All removals are anthropogenic, no removals are non-anthropogenic
    #-------------------------------------------------------------------------------------------------------------------
    out.loc[mlp_1, cn.anthro_removal_col] = out.loc[mlp_1, cn.gross_removal_col]
    out.loc[mlp_1, cn.nonanthro_removal_col] = 0.0

    #-------------------------------------------------------------------------------------------------------------------
    # Case 2a: Managed land polygons determine anthropogenic (managed) vs non-anthropogenic (unmanaged) removals
    #-------------------------------------------------------------------------------------------------------------------
    # GFW removals are translated into "anthropogenic forest" removals using managed polygons.
    # This includes the following removals:
    #   1) All removals in managed polygons
    #   2) Removals in unmanaged land polygons associated with tree cover loss due to shifting cultivation or logging
    #      because regrowth can occur after unmanaged forest is converted to managed forest.
    managed_forest_mask = (managed[cn.class_col].eq("managed")|
                          (managed[cn.class_col].eq("unmanaged") & managed[cn.is_tcl_col] & managed[cn.driver_col].isin(["Shifting cultivation", "Logging"])))

    # GFW removals are translated into "non-anthropogenic forest" removals using unmanaged polygons.
    # This includes the following removals:
    #     1) Removals in unmanaged polygons without any tree cover loss.
    #     2) Removals in unmanaged polygons with tree cover loss due to "non-anthropogenic" causes (wildfire, natural disturbances, unknown).
    #     3) Removals in unmanaged polygons with tree cover loss resulting in "deforestation" (permanent ag, commodities, and settlements).
    #        They are assumed to have occured before deforestation and thus before unmanaged forest was converted to managed land.
    unmanaged_forest_mask = ((managed[cn.class_col].eq("unmanaged") & ~managed[cn.is_tcl_col]) |
                             (managed[cn.class_col].eq("unmanaged") & managed[cn.is_tcl_col] & managed[cn.driver_col].isin(['Wildfire', 'Other natural disturbances', 'Unknown'])) |
                             (managed[cn.class_col].eq("unmanaged") & managed[cn.is_tcl_col] & managed[cn.driver_col].isin(['Permanent agriculture', 'Hard commodities', 'Settlements & Infrastructure'])))

    # Sum translated removals by iso code
    managed_forest_removals = managed.loc[managed_forest_mask].groupby(cn.iso_col)[cn.gfw_annual_removals_col].sum()
    unmanaged_forest_removals = managed.loc[unmanaged_forest_mask].groupby(cn.iso_col)[cn.gfw_annual_removals_col].sum()

    # Write translated removals from managed polygons for '2a' countries only
    out.loc[mlp_2a, cn.anthro_removal_col] = out.loc[mlp_2a, cn.iso_col].map(managed_forest_removals)
    out.loc[mlp_2a, cn.nonanthro_removal_col] = out.loc[mlp_2a, cn.iso_col].map(unmanaged_forest_removals)

    #-------------------------------------------------------------------------------------------------------------------
    # Case 2b: Primary/IFL forest proxy determines anthropogenic vs non-anthropogenic removals
    #-------------------------------------------------------------------------------------------------------------------
    # GFW removals are translated into "anthropogenic forest" removals using a non-primary/non-IFL approximation for managed forest.
    # This includes the following removals:
    #     1) All removals in secondary forests
    #     2) Removals associated with loss of primary/intact forests due to shifting cultivation or logging because
    #        regrowth can occur after unmanaged forest is converted to managed forest.
    anthro_forest_mask = ((~gfw[cn.is_ifl_prim_col]) |
                          (gfw[cn.is_ifl_prim_col] & gfw[cn.is_tcl_col] & gfw[cn.driver_col].isin(['Shifting cultivation', 'Logging'])))

    # GFW removals are translated into "non-anthropogenic forest" removals using a primary/IFL forest approximation for unmanaged forests.
    # This includes the following removals:
    #     1) Removals from primary/intact forests without any tree cover loss.
    #     2) Removals from primary/intact forests with tree cover loss due to "non-anthropogenic" causes (wildfire, natural disturbances, unknown)
    #     3) Removals from primary/intact forests with tree cover loss resulting in "deforestation" (permanent ag, commodities, and settlements).
    #        They are assumed to have occured before deforestation and thus before unmanaged forest was converted to managed land.
    nonanthro_forest_mask = ((gfw[cn.is_ifl_prim_col] & ~gfw[cn.is_tcl_col]) |
                             (gfw[cn.is_ifl_prim_col] & gfw[cn.is_tcl_col] & gfw[cn.driver_col].isin(['Wildfire', 'Other natural disturbances', 'Unknown'])) |
                             (gfw[cn.is_ifl_prim_col] & gfw[cn.is_tcl_col] & gfw[cn.driver_col].isin(['Permanent agriculture', 'Hard commodities', 'Settlements & Infrastructure'])))

    # Sum translated removals by iso code
    anthro_forest_removals = gfw.loc[anthro_forest_mask].groupby(cn.iso_col)[cn.gfw_annual_removals_col].sum()
    nonanthro_forest_removals = gfw.loc[nonanthro_forest_mask].groupby(cn.iso_col)[cn.gfw_annual_removals_col].sum()

    # Write translated removals from primary/IFL forest proxy for '2b' countries only
    out.loc[mlp_2b, cn.anthro_removal_col] = out.loc[mlp_2b, cn.iso_col].map(anthro_forest_removals)
    out.loc[mlp_2b, cn.nonanthro_removal_col] = out.loc[mlp_2b, cn.iso_col].map(nonanthro_forest_removals)

    out[cn.gross_removal_col] = (out[cn.gross_removal_col].fillna(0.0))
    out[cn.anthro_removal_col] = (out[cn.anthro_removal_col].fillna(0.0))
    out[cn.nonanthro_removal_col] = (out[cn.nonanthro_removal_col].fillna(0.0))

    # Check that anthro + non-anthro removals equals gross annual removals in out df
    check = out[cn.anthro_removal_col] + out[cn.nonanthro_removal_col]
    gross = out[cn.gross_removal_col]
    if not np.allclose(check.values, gross.values, atol=1e-6, rtol=0.0):
        raise ValueError("Anthropogenic + non-anthropogenic removals do not equal gross removals for at least one country.")

    return out


#-----------------------------------------------------------------------------------------------------------------------
# STEP 3: EMISSIONS TRANSLATION
#-----------------------------------------------------------------------------------------------------------------------
# Reformat managed polygon emissions timeseries from columns (geotrellis) into rows (API) for translated results
def col_to_row_emis(df_rows, name):
    year_axis = pd.Index(cn.geotrellis_annual_emission_cols).str.extract(r'(\d{4})')[0].astype("string")
    series = (df_rows.groupby(cn.iso_col)[cn.geotrellis_annual_emission_cols].sum()
              .set_axis(year_axis, axis=1).stack().rename(name))
    series.index = series.index.set_names([cn.iso_col, cn.tcl_year_col])
    return series

# Translate GFW forest emissions into "anthropogenic deforestation", "anthropogenic forest", and "non-anthropogenic forest" emissions.
def translate_emissions(keep_col_df, gfw_emissions_df, managed_polygons_df):
    gfw = gfw_emissions_df.copy()
    managed = managed_polygons_df.copy()

    # Dataframe for translated results that has a TCL year column with all years between 2001 and the final year of timeseries
    years = pd.DataFrame({cn.tcl_year_col: pd.Series(range(cn.start_year, int(cn.end_year)+1), dtype="string")})
    out = keep_col_df.copy().merge(years, how="cross").sort_values([cn.iso_col, cn.tcl_year_col], ignore_index=True)

    # Standardize columns for translation rules
    gfw[cn.gfw_emissions_col] = pd.to_numeric(gfw[cn.gfw_emissions_col], errors="coerce")
    gfw[cn.is_ifl_prim_col] = standardize_bool(gfw[cn.is_ifl_prim_col])
    gfw[cn.tcl_year_col] = gfw[cn.tcl_year_col].astype("string")

    for col in cn.geotrellis_annual_emission_cols:
        managed[col] = pd.to_numeric(managed[col], errors="coerce")
    managed[cn.is_prim_col] = standardize_bool(managed[cn.is_prim_col])
    managed[cn.is_ifl_col] = standardize_bool(managed[cn.is_ifl_col])
    managed[cn.is_ifl_prim_col] = (managed[cn.is_ifl_col] | managed[cn.is_prim_col])  #combine ifl and primary bools for managed polygons

    # Group countries by GFW managed land proxy code
    mlp_1 = out[cn.gfw_code_col].astype(str).str.lower().eq("1")
    mlp_2a = out[cn.gfw_code_col].astype(str).str.lower().eq("2a")
    mlp_2b = out[cn.gfw_code_col].astype(str).str.lower().eq("2b")

    # Reformat managed polygon emissions timeseries from columns (geotrellis) into rows (API) for translated results
    # year_axis = pd.Index(cn.geotrellis_annual_emission_cols).str.extract(r'(\d{4})')[0].astype("string")
    # managed_emis_cols = (managed.groupby(cn.iso_col)[cn.geotrellis_annual_emission_cols]
    #     .set_axis(year_axis, axis=1).stack().rename("managed"))
    #gfw_emis_rows = gfw.groupby([cn.iso_col, cn.tcl_year_col])[cn.gfw_emissions_col].rename("gfw")

    # out_idx = pd.MultiIndex.from_frame(out[[cn.iso_col, cn.tcl_year_col]])
    # managed_annual_emissions = managed_emis_cols.reindex(out_idx)
    #gfw_annual_emissions = gfw_emis_rows.reindex(out_idx)

    # Add gross annual CO2 emissions (Mg CO2 per year) column to out df for QC, fill nan with 0
    # out[cn.gross_emissions_col] = np.where(mlp_2a, managed_annual_emissions, gfw_annual_emissions)
    # out[cn.gross_emissions_col] = np.nan_to_num(out[cn.gross_emissions_col], nan=0.0)

    # Use the managed land proxy code to assign "anthropogenic deforestation", "anthropogenic forest" and "non-anthropogenic forest" emissions
    out[cn.gross_emis_col] = 0.0
    out[cn.anthro_deforest_emis_col] = 0.0
    out[cn.anthro_forest_emis_col] = 0.0
    out[cn.nonanthro_forest_emis_col] = 0.0

    # -------------------------------------------------------------------------------------------------------------------
    # Managed land polygons: Brazil, Canada, and the United States
    # -------------------------------------------------------------------------------------------------------------------
    # GFW emissions are translated into "anthropogenic deforestation" using managed polygons + driver of tree cover loss
    # This includes the following emissions:
    #   1) All emissions from permanent agriculture, hard commodities, and settlements & infrastructure.
    #   2) Emissions from shifting cultivation in primary/ifl forests because it is considered a permanent change from forest to non-forest land use.
    #   3) Optional: Emissions from shifting cultivation in secondary forests can also be considered "deforestation" (see below).
        # Since it is often not clear which NGHGI category emissions from tree cover loss in secondary forests associated with
        # shifting cultivation are reported in, we make it possible to generate two emissions reclassification scenarios:
        # 'forest': one where these emissions are reported as "anthropogenic forest" and
        # 'deforest': one where they are reported as "anthropogenic deforestation" emissions.
    if cn.secondary_shift_cult_cat == 'forest':
        mask_anthro_def = ((managed[cn.driver_col].isin(['Permanent agriculture', 'Hard commodities', 'Settlements & Infrastructure'])) |
                           (managed[cn.is_ifl_prim_col] & managed[cn.driver_col].isin(['Shifting cultivation'])))
    elif cn.secondary_shift_cult_cat == 'deforestation':
        mask_anthro_def = ((managed[cn.driver_col].isin(['Permanent agriculture', 'Hard commodities', 'Settlements & Infrastructure'])) |
                           (managed[cn.is_ifl_prim_col] & managed[cn.driver_col].isin(['Shifting cultivation'])) |
                           ((~managed[cn.is_ifl_prim_col]) & managed[cn.driver_col].isin(['Shifting cultivation'])))
    else:
        raise ValueError("Emissions associated with shifting cultivation in secondary forests must be assigned as forest or deforestation")

    # GFW emissions are translated into "anthropogenic forest" using managed polygons + driver of tree cover loss
    # This includes the following emissions:
    #   1) All emissions from logging
    #   2) Emissions from "non-anthropogenic" causes (wildfire, natural disturbances, unknown) in managed polygons
    #   3) Optional: Emissions from shifting cultivation in secondary forests only can also be considered.
    if cn.secondary_shift_cult_cat == 'forest':
        mask_anthro_for = ((managed[cn.driver_col].isin(['Logging'])) |
                           (managed[cn.class_col].eq("managed") & managed[cn.driver_col].isin(['Wildfire', 'Other natural disturbances', 'Unknown'])) |
                           ((~managed[cn.is_ifl_prim_col]) & managed[cn.driver_col].isin(['Shifting cultivation'])))
    elif cn.secondary_shift_cult_cat == 'deforestation':
        mask_anthro_for = ((managed[cn.driver_col].isin(['Logging'])) |
                           (managed[cn.class_col].eq("managed") & managed[cn.driver_col].isin(['Wildfire', 'Other natural disturbances', 'Unknown'])))
    else:
        raise ValueError("Emissions associated with shifting cultivation in secondary forests must be assigned as forest or deforestation")

    # GFW emissions are translated into "non-anthropogenic forest" using unmanaged polygons + driver of tree cover loss
    # This includes the following emissions:
    #   1) Emissions from "non-anthropogenic" causes (wildfire, natural disturbances, unknown) in unmanaged polygons only.
    mask_nonanthro_for = (managed[cn.class_col].eq("unmanaged") & managed[cn.driver_col].isin(['Wildfire', 'Other natural disturbances', 'Unknown']))

    # Sum translated emissions by iso x tree cover loss year
    gross_emis = col_to_row_emis(managed, "gross_emiss")
    anthro_def_emis = col_to_row_emis(managed.loc[mask_anthro_def], "anthro_def")
    anthro_for_emis = col_to_row_emis(managed.loc[mask_anthro_for], "anthro_for")
    nonanthro_for_emis = col_to_row_emis(managed.loc[mask_nonanthro_for], "nonanthro_for")

    # -------------------------------------------------------------------------------------------------------------------
    # Case 2a: Managed land polygons determine anthropogenic (managed) vs non-anthropogenic (unmanaged) emissions
    # -------------------------------------------------------------------------------------------------------------------
    # Write sums into translated_emissions_df for Case 2a countries
    out_idx_2a = pd.MultiIndex.from_frame(out.loc[mlp_2a, [cn.iso_col, cn.tcl_year_col]])
    out.loc[mlp_2a, cn.gross_emis_col] = gross_emis.reindex(out_idx_2a).to_numpy()
    out.loc[mlp_2a, cn.anthro_deforest_emis_col] = anthro_def_emis.reindex(out_idx_2a).to_numpy()
    out.loc[mlp_2a, cn.anthro_forest_emis_col] = anthro_for_emis.reindex(out_idx_2a).to_numpy()
    out.loc[mlp_2a, cn.nonanthro_forest_emis_col] = nonanthro_for_emis.reindex(out_idx_2a).to_numpy()


    # -------------------------------------------------------------------------------------------------------------------
    # Managed land proxy using primary/ifl forest extent
    # -------------------------------------------------------------------------------------------------------------------
    # GFW emissions are translated into "anthropogenic deforestation" using a secondary forest approximation for managed forests.
    # This includes the following emissions:
    #   1) All emissions from permanent ag, commodites, and settlements in both primary/intact forests and secondary forests.
    #   2) Emissions from shifting cultivation in primary/ifl because it is a permanent change from forest to non-forest land use.
    #   3) Optional: Emissions from shifting cultivation in secondary forest can also be included.
    if cn.secondary_shift_cult_cat == 'forest':
        mask_anthro_def = ((gfw[cn.driver_col].isin(['Permanent agriculture', 'Hard commodities', 'Settlements & Infrastructure'])) |
                           (gfw[cn.is_ifl_prim_col] & gfw[cn.driver_col].isin(['Shifting cultivation'])))
    elif cn.secondary_shift_cult_cat == 'deforestation':
        mask_anthro_def = ((gfw[cn.driver_col].isin(['Permanent agriculture', 'Hard commodities', 'Settlements & Infrastructure'])) |
                           (gfw[cn.is_ifl_prim_col] & gfw[cn.driver_col].isin(['Shifting cultivation'])) |
                           ((~gfw[cn.is_ifl_prim_col]) & gfw[cn.driver_col].isin(['Shifting cultivation'])))
    else:
        raise ValueError("Emissions associated with shifting cultivation in secondary forests must be assigned as forest or deforestation")

    # GFW emissions are translated into "anthropogenic forest" using a secondary forest approximation for managed forests.
    # This includes the following emissions:
        #   1) All emissions from logging in both primary/intact forests and secondary forests.
        #   2) Emissions from "non-anthropogenic" causes (wildfire, natural disturbances, unknown) in secondary forests only.
        #   3) Optional: Emissions from shifting cultivation in secondary forest only can also be included.
    if cn.secondary_shift_cult_cat == 'forest':
        mask_anthro_for = ((gfw[cn.driver_col].isin(['Logging'])) |
                           ((~gfw[cn.is_ifl_prim_col]) & gfw[cn.driver_col].isin(['Wildfire', 'Other natural disturbances', 'Unknown'])) |
                           ((~gfw[cn.is_ifl_prim_col]) & gfw[cn.driver_col].isin(['Shifting cultivation'])))
    elif cn.secondary_shift_cult_cat == 'deforestation':
        mask_anthro_for = ((gfw[cn.driver_col].isin(['Logging'])) |
                           ((~gfw[cn.is_ifl_prim_col]) & gfw[cn.driver_col].isin(['Wildfire', 'Other natural disturbances', 'Unknown'])))
    else:
        raise ValueError(
            "Emissions associated with shifting cultivation in secondary forests must be assigned as forest or deforestation")

    # GFW emissions are translated into "non-anthropogenic forest" using a primary/IFL approximation for unmanaged forests.
    # This includes the following emissions:
        #   1) Emissions from "non-anthropogenic" causes (wildfire, natural disturbances, unknown) in primary/intact forests only.
    mask_nonanthro_for = (gfw[cn.is_ifl_prim_col]) & gfw[cn.driver_col].isin(['Wildfire', 'Other natural disturbances', 'Unknown'])

    # Sum translated emissions by iso x tree cover loss year
    group_keys = [cn.iso_col, cn.tcl_year_col]
    gross_emis = gfw.groupby(group_keys, dropna=False)[cn.gfw_emissions_col].sum().rename("gross_emiss")
    anthro_def_emis = gfw.loc[mask_anthro_def].groupby(group_keys, dropna=False)[cn.gfw_emissions_col].sum().rename("anthro_def")
    anthro_for_emis = gfw.loc[mask_anthro_for].groupby(group_keys, dropna=False)[cn.gfw_emissions_col].sum().rename("anthro_for")
    nonanthro_for_emis = gfw.loc[mask_nonanthro_for].groupby(group_keys, dropna=False)[cn.gfw_emissions_col].sum().rename("nonanthro_for")

    # -------------------------------------------------------------------------------------------------------------------
    # Case 1: All emissions are anthropogenic, no emissions are non-anthropogenic
    # -------------------------------------------------------------------------------------------------------------------
    # Write sums into translated_emissions_df for Case 1 countries
    out_idx_1 = pd.MultiIndex.from_frame(out.loc[mlp_1, [cn.iso_col, cn.tcl_year_col]])
    out.loc[mlp_1, cn.gross_emis_col] = gross_emis.reindex(out_idx_1).to_numpy()
    out.loc[mlp_1, cn.anthro_deforest_emis_col] = anthro_def_emis.reindex(out_idx_1).to_numpy()
    anthro_forest_sum = anthro_for_emis.add(nonanthro_for_emis, fill_value=0)
    out.loc[mlp_1, cn.anthro_forest_emis_col] = anthro_forest_sum.reindex(out_idx_1).to_numpy()
    out.loc[mlp_1, cn.nonanthro_forest_emis_col] = 0.0

    # -------------------------------------------------------------------------------------------------------------------
    # Case 2b: Primary/IFL forest proxy determines anthropogenic vs non-anthropogenic emissions
    # -------------------------------------------------------------------------------------------------------------------
    # Write sums into translated_emissions_df for Case 2b countries
    out_idx_2b = pd.MultiIndex.from_frame(out.loc[mlp_2b, [cn.iso_col, cn.tcl_year_col]])
    out.loc[mlp_2b, cn.gross_emis_col] = gross_emis.reindex(out_idx_2b).to_numpy()
    out.loc[mlp_2b, cn.anthro_deforest_emis_col] = anthro_def_emis.reindex(out_idx_2b).to_numpy()
    out.loc[mlp_2b, cn.anthro_forest_emis_col] = anthro_for_emis.reindex(out_idx_2b).to_numpy()
    out.loc[mlp_2b, cn.nonanthro_forest_emis_col] = nonanthro_for_emis.reindex(out_idx_2b).to_numpy()

    out[cn.gross_emis_col] = (out[cn.gross_emis_col].fillna(0.0))
    out[cn.anthro_deforest_emis_col] = (out[cn.anthro_deforest_emis_col].fillna(0.0))
    out[cn.anthro_forest_emis_col] = (out[cn.anthro_forest_emis_col].fillna(0.0))
    out[cn.nonanthro_forest_emis_col] = (out[cn.nonanthro_forest_emis_col].fillna(0.0))

    # Check that anthro + non-anthro emissions equals gross emissions in out
    check = (out[cn.anthro_deforest_emis_col] + out[cn.anthro_forest_emis_col] + out[cn.nonanthro_forest_emis_col])
    gross = out[cn.gross_emis_col]
    if not np.allclose(check.values, gross.values, atol=1e-6, rtol=0.0):
        raise ValueError("Anthropogenic + non-anthropoegnic emissions do not equal gross emissions for at least one country.")

    return out

#-----------------------------------------------------------------------------------------------------------------------
# STEP 4: TRANSLATED FLUX RESULTS
#-----------------------------------------------------------------------------------------------------------------------

# Flip annual emission results from rows to columns
def pivot_emis(df, value_col):
    year_col = cn.tcl_year_col
    prefix = value_col.split("__", 1)[0]
    
    ds = (df[[cn.iso_col, year_col, value_col]].copy())
    ds[year_col] = ds[year_col].astype(int)
    ds = ds.pivot_table(index=cn.iso_col, columns=year_col, values=value_col, aggfunc="sum")
    ds = ds.reindex(columns=cn.years)
    ds.columns = [f"{prefix}_{y}__Mg_CO2" for y in ds.columns]

    return ds.reset_index()

# Combine translated emissions and removals data into the three categories:
# anthropogenic deforestation emissions, anthropogenic forest flux, and non-anthropogenic forest flux
def make_flux_tables(managed_land_proxy_codes_df, translated_removals_df, translated_emissions_df):

    # -------------------------------------------------------------------------------------------------------------------
    # 1) Anthropogenic deforestation (emissions-only)
    # -------------------------------------------------------------------------------------------------------------------
    anthro_deforest_pivot = pivot_emis(translated_emissions_df, cn.anthro_deforest_emis_col)
    anthro_deforestation_emissions_df = managed_land_proxy_codes_df.merge(anthro_deforest_pivot, on=cn.iso_col, how="left")

    # -------------------------------------------------------------------------------------------------------------------
    # 2) Anthropogenic forest flux
    # -------------------------------------------------------------------------------------------------------------------
    anthro_forest_pivot = pivot_emis(translated_emissions_df, cn.anthro_forest_emis_col)
    anthro_forest_flux_df = (managed_land_proxy_codes_df
                             .merge(translated_removals_df[[cn.iso_col, cn.anthro_removal_col]], on=cn.iso_col, how="left")
                             .merge(anthro_forest_pivot, on=cn.iso_col, how="left"))

    # Calculate annual anthro forest flux timeseries
    for y in cn.years:
        emis_pattern = cn.anthro_forest_emis_col.split("__", 1)[0]
        emis_col = f"{emis_pattern}_{y}__Mg_CO2"
        flux_col = f"{cn.anthro_forest_flux_pattern}_{y}__Mg_CO2"
        anthro_forest_flux_df[flux_col] = (anthro_forest_flux_df[emis_col].fillna(0)
                                           + anthro_forest_flux_df[cn.anthro_removal_col].fillna(0))

    # -------------------------------------------------------------------------------------------------------------------
    #  3) Non-anthropogenic forest flux
    # -------------------------------------------------------------------------------------------------------------------
    nonanthro_pivot = pivot_emis(translated_emissions_df, cn.nonanthro_forest_emis_col)
    nonanthro_forest_flux_df = (managed_land_proxy_codes_df
                            .merge(translated_removals_df[[cn.iso_col, cn.nonanthro_removal_col]], on=cn.iso_col, how="left")
                            .merge(nonanthro_pivot, on=cn.iso_col, how="left"))

    # Calculate annual non-anthro forest flux timeseries
    for y in cn.years:
        emis_pattern = cn.nonanthro_forest_emis_col.split("__", 1)[0]
        emis_col = f"{emis_pattern}_{y}__Mg_CO2"
        flux_col = f"{cn.nonanthro_forest_flux_pattern}_{y}__Mg_CO2"
        nonanthro_forest_flux_df[flux_col] = (nonanthro_forest_flux_df[emis_col].fillna(0)
                                              + nonanthro_forest_flux_df[cn.nonanthro_removal_col].fillna(0))

    return anthro_deforestation_emissions_df, anthro_forest_flux_df, nonanthro_forest_flux_df