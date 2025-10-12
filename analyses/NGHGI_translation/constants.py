# Used for final output spreadsheet
run_date = '20258888'

# Whether to keep intermediate columns in the final output spreadsheet. Useful for QCing.
keep_inter_cols = True

# Whether to keep the untranslated GFW forest flux data in the final output spreadsheet
keep_raw_data = False

# What category emissions from shifting cultivation in secondary forests should be assigned to (options: forest or deforestation)
secondary_shift_cult_cat = 'forest'

########################################################################################################################
#Input GFW data spreadsheet
########################################################################################################################
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
is_ifl_col = "is__intact_primary_forest"
driver_col = "driver_of_tree_cover_loss"
gfw_annual_removals_col = "average_annual_removal__mg_co2_yr-1"

#3. Timeseries of annual emissions (Mg CO2 per year) sheet and column names
gfw_emissions_sheet = "emissions_timeseries"
tcl_year_col = "umd_tree_cover_loss__year"
gfw_emissions_col = "emissions_co2_only_biomass_soil__mg_co2"

########################################################################################################################
#NGHGI translated data spreadsheet
########################################################################################################################
#Removals
gross_removals_col = "gross_annual_removal__Mg_CO2_yr-1"
anthro_removals_col = "anthro_forest_annual_removals__Mg_CO2_yr-1"
nonanthro_removals_col = "non_anthro_forest_annual_removals__Mg_CO2_yr-1"

#Emissions
gross_emissions_col = "gross_annual_emissions__Mg_CO2"
anthro_deforest_emissions_col = "anthro_deforestation_emissions__Mg_CO2"
anthro_forest_emissions_col = "anthro_forest_emissions__Mg_CO2"
nonanthro_forest_emissions_col = "non_anthro_forest_emissions__Mg_CO2"

#Final results
out_sheet = rf"C:\Users\Melissa.Rose\OneDrive - World Resources Institute\Documents\Projects\NGHGI_translation\JRC_datahub_2024_update_{run_date}.xlsx"
nghgi_removals_sheet = "translated_removals"
nghgi_emissions_sheet = "translated_emissions"