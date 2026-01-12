# Date used for the final output spreadsheet
run_date = '20258888'

#Number of years
start_year = 2001
end_year = 2024
n_years = float(24)
years = list(range(start_year, end_year + 1))

# Whether to keep the untranslated GFW forest flux data in the final output spreadsheet
keep_raw_data = False

# What category emissions from shifting cultivation in secondary forests should be assigned to (options: forest or deforestation)
secondary_shift_cult_cat = 'forest'

#-----------------------------------------------------------------------------------------------------------------------
# Input GFW data spreadsheets
#-----------------------------------------------------------------------------------------------------------------------
#Input file
in_sheet = r"C:\Users\Melissa.Rose\OneDrive - World Resources Institute\Documents\Projects\NGHGI_translation\JRC_datahub_2024_update_20251010_copy.xlsx"

#Note: All column names are lowercased, and spaces are replaced with "_" so make sure that is reflected here
#1. Managed land proxy sheet and column names
managed_land_proxy_sheet = "managed_land_proxy"
iso_col = "iso"
country_col = "country"
jrc_code_col = "jrc_code"
gfw_code_col = "gfw_code"

#2. Average annual removals (Mg CO2 per year) sheet and column names
gfw_removals_sheet = "removals"
is_tcl_col = "is__umd_tree_cover_loss"
is_ifl_prim_col = "is__intact_primary_forest"
driver_col = "tcl_driver__class"
gfw_annual_removals_col = "average_annual_removal__mg_co2_yr-1"

#3. Timeseries of annual emissions (Mg CO2 per year) sheet and column names
gfw_emissions_sheet = "emissions_timeseries"
tcl_year_col = "umd_tree_cover_loss__year"
gfw_emissions_col = "emissions_co2_only_biomass_soil__mg_co2"

#4. United States, Canada, and Brazil managed polygon sheets and column names
usa_sheet = "USA"
canada_sheet = "CAN"
brazil_sheet = "BRA"
class_col = "class"
is_prim_col = "is__umd_regional_primary_forest_2001"
is_ifl_col = "is__intact_forest_landscapes_2000"
geotrellis_gross_removals_col = f"gfw_forest_carbon_gross_removals_{start_year}_{end_year}__mg_co2"
geotrellis_annual_emission_cols = [f"gfw_forest_carbon_gross_emissions_co2_{year}__mg_co2e"for year in range(start_year, end_year+1)]


#-----------------------------------------------------------------------------------------------------------------------
# NGHGI translated data spreadsheet
#-----------------------------------------------------------------------------------------------------------------------
#Removals
gross_removal_col = "annual_removals__Mg_CO2_yr-1"
anthro_removal_col = "anthro_forest_removals__Mg_CO2_yr-1"
nonanthro_removal_col = "non_anthro_forest_removals__Mg_CO2_yr-1"

#Emissions
gross_emis_col = "gross_emissions__Mg_CO2"
anthro_deforest_emis_col = "deforestation_emissions__Mg_CO2"
anthro_forest_emis_col = "anthro_forest_emissions__Mg_CO2"
nonanthro_forest_emis_col = "non_anthro_forest_emissions__Mg_CO2"

#Flux
anthro_forest_flux_pattern = "anthro_forest_flux"
nonanthro_forest_flux_pattern = "non_anthro_forest_flux"

#Final results
out_sheet = rf"C:\Users\Melissa.Rose\OneDrive - World Resources Institute\Documents\Projects\NGHGI_translation\JRC_datahub_2024_update_{run_date}.xlsx"
nghgi_removals_sheet = "translated_removals"
nghgi_emissions_sheet = "translated_emissions"
anthro_deforest_emis_sheet = "deforestation_emissions"
anthro_forest_flux_sheet =  "anthro_forest_flux"
nonanthro_forest_flux_sheet = "non-anthro_forest_flux"