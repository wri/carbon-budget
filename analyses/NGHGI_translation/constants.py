"""
TODO: 
- Replace local file paths with files in s3
- Save output locally and in s3
"""

# Date used for the final output spreadsheet
run_date = '20261006'

# Number of years
start_year = 2001
end_year = 2025
n_years = float(25)
years = list(range(start_year, end_year + 1))

# Whether to keep the untranslated GFW forest flux data in the final output spreadsheet
keep_raw_data = False

# What category emissions from shifting cultivation in secondary forests should be assigned to (options: forest or deforestation)
secondary_shift_cult_cat = 'forest'

# -----------------------------------------------------------------------------------------------------------------------
# Greenhouse gases
# -----------------------------------------------------------------------------------------------------------------------
# Individual GHG gases that are translated
gases = ["CO2", "CH4", "N2O"]

# Gas groups reported in the outputs. Removals are CO2 only, so they are only included in groups that contain CO2.
gas_groups = {
    "CO2": ["CO2"],
    "non_CO2": ["CH4", "N2O"],
    "CO2e": ["CO2", "CH4", "N2O"],
}

# -----------------------------------------------------------------------------------------------------------------------
# Input GFW data spreadsheets
# -----------------------------------------------------------------------------------------------------------------------
# Input files
in_sheet = r"C:\Users\Melissa.Rose\OneDrive - World Resources Institute\Documents\Projects\NGHGI_translation\2025_TCL_translation\inputs\JRC_datahub_2025_update_input_20260902.xlsx"
hwp_in_sheet = r"C:\Users\Melissa.Rose\OneDrive - World Resources Institute\Documents\Projects\NGHGI_translation\2025_TCL_translation\inputs\HWP_1990-2024_20260624.xlsx"

# Note: All column names are lowercased, and spaces are replaced with "_" so make sure that is reflected here
# 1. Managed land proxy sheet and column names
managed_land_proxy_sheet = "managed_land_proxy"
iso_col = "iso"
country_col = "country"
jrc_code_col = "jrc_code"
gfw_code_col = "gfw_code"
world_col = "world"
eu_col = "eu27+uk"

# 2. Average annual removals (Mg CO2 per year) sheet and column names
gfw_removals_sheet = "removals"
is_tcl_col = "is__umd_tree_cover_loss"
is_ifl_prim_col = "is__intact_primary_forest"
driver_col = "tcl_driver__class"
gfw_annual_removals_col = "average_annual_removal__mg_co2_yr-1"

# 3. Timeseries of annual GHG emissions per year sheet and column names
gfw_emissions_sheet = "emissions_timeseries"
tcl_year_col = "umd_tree_cover_loss__year"
gfw_emissions_cols = {
    "CO2": "emissions_co2_only_biomass_soil__mg_co2",
    "CH4": "emissions_ch4_only_biomass_soil__mg_co2e",
    "N2O": "emissions_n2o_only_biomass_soil__mg_co2e",
}
gfw_emissions_col = gfw_emissions_cols["CO2"]

# 4. United States, Canada, and Brazil managed polygon sheets and column names
usa_sheet = "USA"
canada_sheet = "CAN"
brazil_sheet = "BRA"
class_col = "class"
is_prim_col = "is__umd_regional_primary_forest_2001"
is_ifl_col = "is__intact_forest_landscapes_2000"
geotrellis_gross_removals_col = f"gfw_forest_carbon_gross_removals_{start_year}_{end_year}__mg_co2"
geotrellis_annual_emission_cols = {gas: [f"gfw_forest_carbon_gross_emissions_{gas.lower()}_{year}__mg_co2e" for year in range(start_year, end_year + 1)] for gas in gases}

#5. Harvested wood products (HWP) spreadsheet: annual CO2 from HWP from FAO
hwp_sheet = "sum_deltaCO2"

# -----------------------------------------------------------------------------------------------------------------------
# NGHGI translated data spreadsheet
# -----------------------------------------------------------------------------------------------------------------------
# Removals (CO2 only)
gross_removal_col = "annual_removals__Mg_CO2_yr-1"
anthro_removal_col = "anthro_forest_removals__Mg_CO2_yr-1"
nonanthro_removal_col = "non_anthro_forest_removals__Mg_CO2_yr-1"

# Emissions
# Column names are built per gas / gas group. CO2 keeps the original names (e.g. "gross_emissions__Mg_CO2");
# every other gas or group is tagged and in CO2e (e.g. "gross_emissions_CH4__Mg_CO2e", "gross_emissions_non_CO2__Mg_CO2e").
def unit(tag):
    return "Mg_CO2" if tag == "CO2" else "Mg_CO2e"

def tagged(base, tag):
    return base if tag == "CO2" else f"{base}_{tag}"

def emis_col(base, tag):
    return f"{tagged(base, tag)}__{unit(tag)}"

gross_emis_base = "gross_emissions"
anthro_deforest_emis_base = "deforestation_emissions"
anthro_forest_emis_base = "anthro_forest_emissions"
nonanthro_forest_emis_base = "non_anthro_forest_emissions"
emis_bases = [gross_emis_base, anthro_deforest_emis_base, anthro_forest_emis_base, nonanthro_forest_emis_base]

# CO2 names, kept for backwards compatibility
gross_emis_col = emis_col(gross_emis_base, "CO2")
anthro_deforest_emis_col = emis_col(anthro_deforest_emis_base, "CO2")
anthro_forest_emis_col = emis_col(anthro_forest_emis_base, "CO2")
nonanthro_forest_emis_col = emis_col(nonanthro_forest_emis_base, "CO2")

# Flux
anthro_forest_flux_pattern = "anthro_forest_flux"
nonanthro_forest_flux_pattern = "non_anthro_forest_flux"

# Final results for WRI (includes non-anthro totals for QC)
out_sheet = rf"C:\Users\Melissa.Rose\OneDrive - World Resources Institute\Documents\Projects\NGHGI_translation\2025_TCL_translation\outputs\JRC_datahub_2025_update__all_GHGs__for_WRI__{run_date}.xlsx"
nghgi_removals_sheet = "translated_removals"
nghgi_emissions_sheet = "translated_emissions"
anthro_deforest_emis_sheet = "deforestation_emissions"
anthro_forest_flux_sheet = "anthro_forest_flux"
nonanthro_forest_flux_sheet = "non-anthro_forest_flux"

# Final results for JRC (includes HWP and formatted for webmasters)
# One tab per category x gas group (FOREST_CO2, FOREST_non_CO2, FOREST_CO2e, DEFORESTATION_CO2, etc)
datahub_version = "V4.0.0 (NGHGI 2026)"
datahub_source = "GNW 2025 v1.4.3"
datahub_unit_divisor = 1e6  # Mg -> Mt
datahub_categories = ["FOREST", "DEFORESTATION", "HWP"]
datahub_regions = {"WRD": world_col, "EU27": eu_col}
datahub_out_xlsx = rf"C:\Users\Melissa.Rose\OneDrive - World Resources Institute\Documents\Projects\NGHGI_translation\2025_TCL_translation\outputs\JRC_datahub_2025_update__all_GHGs__for_JRC__{run_date}.xlsx"
