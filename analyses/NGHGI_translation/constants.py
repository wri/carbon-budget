#Used for final output spreadsheet
run_date = '20259999'

#Whether to keep intermediate columns. Useful for QCing.
keep_inter_cols = True

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

########################################################################################################################
#NGHGI translated data spreadsheet
########################################################################################################################
#Removals
gross_removals_col = "gross_annual_removal__mg_co2_yr-1"
anthro_removals_col = "anthro_annual_removal__mg_co2_yr-1"
nonanthro_removals_col = "non_anthro_annual_removal__mg_co2_yr-1"




#Final results
out_sheet = rf"C:\Users\Melissa.Rose\OneDrive - World Resources Institute\Documents\Projects\NGHGI_translation\JRC_datahub_2024_update_{run_date}.xlsx"
nghgi_removals_sheet = "translated_removals"