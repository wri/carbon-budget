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
    if normalized == 1:
        return "1"
    if normalized == 2:
        return "2a"
    if normalized in {3, 4}:
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
    jrc_codes = jrc_col.lower()

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
def col_to_row_emis(df_rows, annual_cols, name):
    year_axis = pd.Index(annual_cols).str.extract(r'(\d{4})')[0].astype("string")
    series = (df_rows.groupby(cn.iso_col)[annual_cols].sum()
              .set_axis(year_axis, axis=1).stack().rename(name))
    series.index = series.index.set_names([cn.iso_col, cn.tcl_year_col])
    return series

# Translate GFW forest emissions into "anthropogenic deforestation", "anthropogenic forest", and "non-anthropogenic forest" emissions.
# The same translation rules (masks) are applied to every gas in cn.gases (CO2, CH4, N2O).
def translate_emissions(keep_col_df, gfw_emissions_df, managed_polygons_df):
    gfw = gfw_emissions_df.copy()
    managed = managed_polygons_df.copy()

    # Dataframe for translated results that has a TCL year column with all years between 2001 and the final year of timeseries
    years = pd.DataFrame({cn.tcl_year_col: pd.Series(range(cn.start_year, int(cn.end_year)+1), dtype="string")})
    out = keep_col_df.copy().merge(years, how="cross").sort_values([cn.iso_col, cn.tcl_year_col], ignore_index=True)

    # Standardize columns for translation rules
    for gas in cn.gases:
        gfw[cn.gfw_emissions_cols[gas]] = pd.to_numeric(gfw[cn.gfw_emissions_cols[gas]], errors="coerce")
    gfw[cn.is_ifl_prim_col] = standardize_bool(gfw[cn.is_ifl_prim_col])
    gfw[cn.tcl_year_col] = gfw[cn.tcl_year_col].astype("string")

    for gas in cn.gases:
        for col in cn.geotrellis_annual_emission_cols[gas]:
            managed[col] = pd.to_numeric(managed[col], errors="coerce")
    managed[cn.is_prim_col] = standardize_bool(managed[cn.is_prim_col])
    managed[cn.is_ifl_col] = standardize_bool(managed[cn.is_ifl_col])
    managed[cn.is_ifl_prim_col] = (managed[cn.is_ifl_col] | managed[cn.is_prim_col])  #combine ifl and primary bools for managed polygons

    # Group countries by GFW managed land proxy code
    mlp_1 = out[cn.gfw_code_col].astype(str).str.lower().eq("1")
    mlp_2a = out[cn.gfw_code_col].astype(str).str.lower().eq("2a")
    mlp_2b = out[cn.gfw_code_col].astype(str).str.lower().eq("2b")

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
        managed_mask_anthro_def = ((managed[cn.driver_col].isin(['Permanent agriculture', 'Hard commodities', 'Settlements & Infrastructure'])) |
                                   (managed[cn.is_ifl_prim_col] & managed[cn.driver_col].isin(['Shifting cultivation'])))
    elif cn.secondary_shift_cult_cat == 'deforestation':
        managed_mask_anthro_def = ((managed[cn.driver_col].isin(['Permanent agriculture', 'Hard commodities', 'Settlements & Infrastructure'])) |
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
        managed_mask_anthro_for = ((managed[cn.driver_col].isin(['Logging'])) |
                                   (managed[cn.class_col].eq("managed") & managed[cn.driver_col].isin(['Wildfire', 'Other natural disturbances', 'Unknown'])) |
                                   ((~managed[cn.is_ifl_prim_col]) & managed[cn.driver_col].isin(['Shifting cultivation'])))
    elif cn.secondary_shift_cult_cat == 'deforestation':
        managed_mask_anthro_for = ((managed[cn.driver_col].isin(['Logging'])) |
                                   (managed[cn.class_col].eq("managed") & managed[cn.driver_col].isin(['Wildfire', 'Other natural disturbances', 'Unknown'])))
    else:
        raise ValueError("Emissions associated with shifting cultivation in secondary forests must be assigned as forest or deforestation")

    # GFW emissions are translated into "non-anthropogenic forest" using unmanaged polygons + driver of tree cover loss
    # This includes the following emissions:
    #   1) Emissions from "non-anthropogenic" causes (wildfire, natural disturbances, unknown) in unmanaged polygons only.
    managed_mask_nonanthro_for = (managed[cn.class_col].eq("unmanaged") & managed[cn.driver_col].isin(['Wildfire', 'Other natural disturbances', 'Unknown']))

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

    # Index of output rows for each managed land proxy case
    out_idx_1 = pd.MultiIndex.from_frame(out.loc[mlp_1, [cn.iso_col, cn.tcl_year_col]])
    out_idx_2a = pd.MultiIndex.from_frame(out.loc[mlp_2a, [cn.iso_col, cn.tcl_year_col]])
    out_idx_2b = pd.MultiIndex.from_frame(out.loc[mlp_2b, [cn.iso_col, cn.tcl_year_col]])
    group_keys = [cn.iso_col, cn.tcl_year_col]

    # -------------------------------------------------------------------------------------------------------------------
    # Apply the same translation rules to each gas
    # -------------------------------------------------------------------------------------------------------------------
    for gas in cn.gases:
        gross_col = cn.emis_col(cn.gross_emis_base, gas)
        def_col = cn.emis_col(cn.anthro_deforest_emis_base, gas)
        for_col = cn.emis_col(cn.anthro_forest_emis_base, gas)
        nonanthro_col = cn.emis_col(cn.nonanthro_forest_emis_base, gas)
        for col in [gross_col, def_col, for_col, nonanthro_col]:
            out[col] = 0.0

        # Sum translated managed polygon emissions by iso x tree cover loss year
        annual_cols = cn.geotrellis_annual_emission_cols[gas]
        gross_emis = col_to_row_emis(managed, annual_cols, "gross_emiss")
        anthro_def_emis = col_to_row_emis(managed.loc[managed_mask_anthro_def], annual_cols, "anthro_def")
        anthro_for_emis = col_to_row_emis(managed.loc[managed_mask_anthro_for], annual_cols, "anthro_for")
        nonanthro_for_emis = col_to_row_emis(managed.loc[managed_mask_nonanthro_for], annual_cols, "nonanthro_for")

        # ---------------------------------------------------------------------------------------------------------------
        # Case 2a: Managed land polygons determine anthropogenic (managed) vs non-anthropogenic (unmanaged) emissions
        # ---------------------------------------------------------------------------------------------------------------
        out.loc[mlp_2a, gross_col] = gross_emis.reindex(out_idx_2a).to_numpy()
        out.loc[mlp_2a, def_col] = anthro_def_emis.reindex(out_idx_2a).to_numpy()
        out.loc[mlp_2a, for_col] = anthro_for_emis.reindex(out_idx_2a).to_numpy()
        out.loc[mlp_2a, nonanthro_col] = nonanthro_for_emis.reindex(out_idx_2a).to_numpy()

        # Sum translated emissions (primary/IFL proxy) by iso x tree cover loss year
        value_col = cn.gfw_emissions_cols[gas]
        gross_emis = gfw.groupby(group_keys, dropna=False)[value_col].sum().rename("gross_emiss")
        anthro_def_emis = gfw.loc[mask_anthro_def].groupby(group_keys, dropna=False)[value_col].sum().rename("anthro_def")
        anthro_for_emis = gfw.loc[mask_anthro_for].groupby(group_keys, dropna=False)[value_col].sum().rename("anthro_for")
        nonanthro_for_emis = gfw.loc[mask_nonanthro_for].groupby(group_keys, dropna=False)[value_col].sum().rename("nonanthro_for")

        # ---------------------------------------------------------------------------------------------------------------
        # Case 1: All emissions are anthropogenic, no emissions are non-anthropogenic
        # ---------------------------------------------------------------------------------------------------------------
        out.loc[mlp_1, gross_col] = gross_emis.reindex(out_idx_1).to_numpy()
        out.loc[mlp_1, def_col] = anthro_def_emis.reindex(out_idx_1).to_numpy()
        anthro_forest_sum = anthro_for_emis.add(nonanthro_for_emis, fill_value=0)
        out.loc[mlp_1, for_col] = anthro_forest_sum.reindex(out_idx_1).to_numpy()
        out.loc[mlp_1, nonanthro_col] = 0.0

        # ---------------------------------------------------------------------------------------------------------------
        # Case 2b: Primary/IFL forest proxy determines anthropogenic vs non-anthropogenic emissions
        # ---------------------------------------------------------------------------------------------------------------
        out.loc[mlp_2b, gross_col] = gross_emis.reindex(out_idx_2b).to_numpy()
        out.loc[mlp_2b, def_col] = anthro_def_emis.reindex(out_idx_2b).to_numpy()
        out.loc[mlp_2b, for_col] = anthro_for_emis.reindex(out_idx_2b).to_numpy()
        out.loc[mlp_2b, nonanthro_col] = nonanthro_for_emis.reindex(out_idx_2b).to_numpy()

        for col in [gross_col, def_col, for_col, nonanthro_col]:
            out[col] = out[col].fillna(0.0)

        # Check that anthro + non-anthro emissions equals gross emissions in out
        check = out[def_col] + out[for_col] + out[nonanthro_col]
        if not np.allclose(check.values, out[gross_col].values, atol=1e-6, rtol=0.0):
            raise ValueError(f"Anthropogenic + non-anthropogenic {gas} emissions do not equal gross {gas} emissions for at least one country.")

    # -------------------------------------------------------------------------------------------------------------------
    # Add gas group totals (non_CO2 = CH4 + N2O; CO2e = CO2 + CH4 + N2O). The CO2 group is the CO2 columns themselves.
    # -------------------------------------------------------------------------------------------------------------------
    for group, group_gases in cn.gas_groups.items():
        if group in cn.gases:
            continue
        for base in cn.emis_bases:
            out[cn.emis_col(base, group)] = out[[cn.emis_col(base, gas) for gas in group_gases]].sum(axis=1)

    return out

#-----------------------------------------------------------------------------------------------------------------------
# STEP 4: TRANSLATED FLUX RESULTS
#-----------------------------------------------------------------------------------------------------------------------

# Flip annual emission results from rows to columns
def pivot_emis(df, value_col, prefix, unit):
    year_col = cn.tcl_year_col

    ds = (df[[cn.iso_col, year_col, value_col]].copy())
    ds[year_col] = ds[year_col].astype(int)
    ds = ds.pivot_table(index=cn.iso_col, columns=year_col, values=value_col, aggfunc="sum")
    ds = ds.reindex(columns=cn.years)
    ds.columns = [f"{prefix}_{y}__{unit}" for y in ds.columns]

    return ds.reset_index()

# Combine translated emissions and removals data into the three categories:
# anthropogenic deforestation emissions, anthropogenic forest flux, and non-anthropogenic forest flux.
# `group` is a key of cn.gas_groups ("CO2", "non_CO2", "CO2e"). Removals are CO2 only, so they are set to 0 for groups without CO2.
def make_flux_tables(managed_land_proxy_codes_df, translated_removals_df, translated_emissions_df, group="CO2"):
    unit = cn.unit(group)
    span = f"{cn.start_year}_{cn.end_year}"
    include_removals = "CO2" in cn.gas_groups[group]

    removals = translated_removals_df[[cn.iso_col, cn.anthro_removal_col, cn.nonanthro_removal_col]].copy()
    if not include_removals:
        removals[[cn.anthro_removal_col, cn.nonanthro_removal_col]] = 0.0

    # -------------------------------------------------------------------------------------------------------------------
    # 1) Anthropogenic deforestation (emissions-only)
    # -------------------------------------------------------------------------------------------------------------------
    emis_pattern = cn.tagged(cn.anthro_deforest_emis_base, group)
    anthro_deforest_pivot = pivot_emis(translated_emissions_df, cn.emis_col(cn.anthro_deforest_emis_base, group), emis_pattern, unit)
    anthro_deforestation_emissions_df = managed_land_proxy_codes_df.merge(anthro_deforest_pivot, on=cn.iso_col, how="left")

    # Add gross deforestation emissions columns
    deforest_emis_cols = [f"{emis_pattern}_{y}__{unit}" for y in cn.years]
    anthro_deforestation_emissions_df[f"{cn.tagged('gross_deforestation_emissions', group)}_{span}__{unit}"] = (
        anthro_deforestation_emissions_df[deforest_emis_cols].sum(axis=1, skipna=True))

    # -------------------------------------------------------------------------------------------------------------------
    # 2) Anthropogenic forest flux
    # -------------------------------------------------------------------------------------------------------------------
    emis_pattern = cn.tagged(cn.anthro_forest_emis_base, group)
    flux_pattern = cn.tagged(cn.anthro_forest_flux_pattern, group)
    anthro_forest_pivot = pivot_emis(translated_emissions_df, cn.emis_col(cn.anthro_forest_emis_base, group), emis_pattern, unit)
    anthro_forest_flux_df = (managed_land_proxy_codes_df
                             .merge(removals[[cn.iso_col, cn.anthro_removal_col]], on=cn.iso_col, how="left")
                             .merge(anthro_forest_pivot, on=cn.iso_col, how="left"))

    # Calculate annual anthropogenic forest flux timeseries
    for y in cn.years:
        emis_col = f"{emis_pattern}_{y}__{unit}"
        flux_col = f"{flux_pattern}_{y}__{unit}"
        anthro_forest_flux_df[flux_col] = (anthro_forest_flux_df[emis_col].fillna(0)
                                           + anthro_forest_flux_df[cn.anthro_removal_col].fillna(0))

    # Add gross anthropogenic forest removals, emissions, and net flux
    anthro_emis_cols = [f"{emis_pattern}_{y}__{unit}" for y in cn.years]
    rem_total = f"{cn.tagged('gross_anthro_forest_removals', group)}_{span}__{unit}"
    emis_total = f"{cn.tagged('gross_anthro_forest_emissions', group)}_{span}__{unit}"
    flux_total = f"{cn.tagged('gross_anthro_forest_flux', group)}_{span}__{unit}"
    anthro_forest_flux_df[rem_total] = anthro_forest_flux_df[cn.anthro_removal_col] * cn.n_years
    anthro_forest_flux_df[emis_total] = anthro_forest_flux_df[anthro_emis_cols].sum(axis=1, skipna=True)
    anthro_forest_flux_df[flux_total] = anthro_forest_flux_df[rem_total] + anthro_forest_flux_df[emis_total]

    # -------------------------------------------------------------------------------------------------------------------
    #  3) Non-anthropogenic forest flux
    # -------------------------------------------------------------------------------------------------------------------
    emis_pattern = cn.tagged(cn.nonanthro_forest_emis_base, group)
    flux_pattern = cn.tagged(cn.nonanthro_forest_flux_pattern, group)
    nonanthro_pivot = pivot_emis(translated_emissions_df, cn.emis_col(cn.nonanthro_forest_emis_base, group), emis_pattern, unit)
    nonanthro_forest_flux_df = (managed_land_proxy_codes_df
                            .merge(removals[[cn.iso_col, cn.nonanthro_removal_col]], on=cn.iso_col, how="left")
                            .merge(nonanthro_pivot, on=cn.iso_col, how="left"))

    # Calculate annual non-anthro forest flux timeseries
    for y in cn.years:
        emis_col = f"{emis_pattern}_{y}__{unit}"
        flux_col = f"{flux_pattern}_{y}__{unit}"
        nonanthro_forest_flux_df[flux_col] = (nonanthro_forest_flux_df[emis_col].fillna(0)
                                              + nonanthro_forest_flux_df[cn.nonanthro_removal_col].fillna(0))

    # Add gross non-anthropogenic forest removals, emissions, and net flux
    nonanthro_emis_cols = [f"{emis_pattern}_{y}__{unit}" for y in cn.years]
    rem_total = f"{cn.tagged('gross_non_anthro_forest_removals', group)}_{span}__{unit}"
    emis_total = f"{cn.tagged('gross_non_anthro_forest_emissions', group)}_{span}__{unit}"
    flux_total = f"{cn.tagged('gross_non_anthro_forest_flux', group)}_{span}__{unit}"
    nonanthro_forest_flux_df[rem_total] = nonanthro_forest_flux_df[cn.nonanthro_removal_col] * cn.n_years
    nonanthro_forest_flux_df[emis_total] = nonanthro_forest_flux_df[nonanthro_emis_cols].sum(axis=1, skipna=True)
    nonanthro_forest_flux_df[flux_total] = nonanthro_forest_flux_df[rem_total] + nonanthro_forest_flux_df[emis_total]

    return anthro_deforestation_emissions_df, anthro_forest_flux_df, nonanthro_forest_flux_df

#-----------------------------------------------------------------------------------------------------------------------
# STEP 5: DATA HUB TIMESERIES (layout of timeseries_GFW_translated_4.0.0.csv)
#-----------------------------------------------------------------------------------------------------------------------
# Adds WRD / EU27 totals (cn.datahub_regions) to a long Data Hub table. Regional rows have no Source.
def add_datahub_regions(df, managed_land_proxy_codes_df):
    regions = []
    for label, flag_col in cn.datahub_regions.items():
        flag = pd.to_numeric(managed_land_proxy_codes_df[flag_col], errors="coerce").eq(1)
        members = managed_land_proxy_codes_df.loc[flag, cn.iso_col]
        reg = df[df["ISO3"].isin(members)].groupby("Year", as_index=False)[["emissions", "removals", "netflux"]].sum(min_count=1)
        reg["ISO3"] = label
        reg["Source"] = np.nan
        regions.append(reg)
    return pd.concat([df] + regions, ignore_index=True)

# Builds one long table per category in cn.datahub_categories x gas group (CO2, non_CO2, CO2e), in Mt.
#   FOREST:        emissions = anthropogenic forest emissions, removals = anthropogenic forest removals, netflux = sum
#   DEFORESTATION: emissions = deforestation emissions, removals = 0, netflux = emissions
#   HWP:           CO2 only (HWP_CO2), netflux from the sum_deltaCO2 tab of the HWP spreadsheet (see make_hwp_table)
# Countries are listed in managed land proxy order, followed by the regional totals in cn.datahub_regions (no Source).
def make_datahub_tables(managed_land_proxy_codes_df, translated_removals_df, translated_emissions_df, hwp_path):
    countries = managed_land_proxy_codes_df[cn.iso_col].tolist()
    emis = translated_emissions_df.copy()
    emis["Year"] = emis[cn.tcl_year_col].astype(int)
    emis = emis.set_index([cn.iso_col, "Year"])
    removals = translated_removals_df.set_index(cn.iso_col)
    idx = pd.MultiIndex.from_product([countries, cn.years], names=[cn.iso_col, "Year"])

    tables = {}
    for category in cn.datahub_categories:
        # Harvested wood products are CO2 only
        if category == "HWP":
            tables[f"{category}_CO2"] = make_hwp_table(hwp_path, managed_land_proxy_codes_df)
            continue

        for group, group_gases in cn.gas_groups.items():
            if category == "FOREST":
                e = emis[cn.emis_col(cn.anthro_forest_emis_base, group)].reindex(idx).fillna(0.0)
                if "CO2" in group_gases:
                    r = pd.Series(idx.get_level_values(0).map(removals[cn.anthro_removal_col]), index=idx).fillna(0.0)
                else:
                    r = pd.Series(0.0, index=idx)
            elif category == "DEFORESTATION":
                e = emis[cn.emis_col(cn.anthro_deforest_emis_base, group)].reindex(idx).fillna(0.0)
                r = pd.Series(0.0, index=idx)
            else:
                raise ValueError(f"Unknown Data Hub category: {category}")

            df = pd.DataFrame({"emissions": e.to_numpy(), "removals": r.to_numpy()}, index=idx).div(cn.datahub_unit_divisor)
            df["netflux"] = df["emissions"] + df["removals"]
            df = df.reset_index().rename(columns={cn.iso_col: "ISO3"})
            df["Source"] = cn.datahub_source

            # Regional totals
            df = add_datahub_regions(df, managed_land_proxy_codes_df)

            df["Version"] = cn.datahub_version
            df["Gas"] = group
            df["Category"] = category
            tables[f"{category}_{group}"] = df[["ISO3", "Version", "Source", "Gas", "Year", "Category", "emissions", "removals", "netflux"]]

    return tables

#-----------------------------------------------------------------------------------------------------------------------
# STEP 6: HARVESTED WOOD PRODUCTS (HWP)
#-----------------------------------------------------------------------------------------------------------------------
# Reads annual CO2 from HWP in use (t CO2, + emission / - removal) from the sum_deltaCO2 tab of the HWP spreadsheet and
# returns it in the Data Hub layout (Mt CO2, netflux only), for managed land proxy countries and years >= cn.start_year.
# Only years present in the HWP spreadsheet are included. Countries with no HWP data are left blank.
def make_hwp_table(hwp_path, managed_land_proxy_codes_df):
    hwp = pd.read_excel(hwp_path, sheet_name=cn.hwp_sheet, header=0)

    # First column is the ISO code; year columns have numeric headers (the averages columns at the end are dropped)
    hwp = hwp.rename(columns={hwp.columns[0]: "ISO3"})
    hwp["ISO3"] = hwp["ISO3"].astype("string").str.strip()
    hwp = hwp[hwp["ISO3"].notna()]
    year_cols = [c for c in hwp.columns if isinstance(c, (int, float)) and not pd.isna(c) and int(c) >= cn.start_year]
    hwp = hwp[["ISO3"] + year_cols].rename(columns={c: int(c) for c in year_cols})

    # Wide -> long, keep managed land proxy countries only (in managed land proxy order)
    countries = managed_land_proxy_codes_df[cn.iso_col].tolist()
    idx = pd.MultiIndex.from_product([countries, sorted(int(c) for c in year_cols)], names=["ISO3", "Year"])
    netflux = (hwp.melt(id_vars="ISO3", var_name="Year", value_name="netflux")
               .astype({"Year": int})
               .set_index(["ISO3", "Year"])["netflux"]
               .pipe(pd.to_numeric, errors="coerce")
               .reindex(idx))

    df = pd.DataFrame({"emissions": np.nan, "removals": np.nan,
                       "netflux": netflux.to_numpy() / cn.datahub_unit_divisor}, index=idx).reset_index()
    df["Source"] = cn.datahub_source
    df = add_datahub_regions(df, managed_land_proxy_codes_df)

    missing = sorted(set(countries) - set(hwp["ISO3"]))
    if missing:
        print(f"No HWP data for: {missing}")

    df["Version"] = cn.datahub_version
    df["Gas"] = "CO2"
    df["Category"] = "HWP"
    return df[["ISO3", "Version", "Source", "Gas", "Year", "Category", "Emissions_MtCO2e", "Removals_MtCO2", "Netflux_MtCO2e"]]