#Input file
in_sheet = r"C:\Users\Melissa.Rose\OneDrive - World Resources Institute\Documents\Projects\NGHGI_translation\JRC_datahub_2024_update_20251010.xlsx"
run_date = '20259999'

#Input GFW data sheet names
managed_land_proxy_sheet = "managed_land_proxy"
iso_col = "iso"
country_col = "country"
jrc_code_col = "jrc_code"
gfw_code_col = "gfw_code"

gfw_removals_sheet = "removals"
is_tcl_col = "is__umd_tree_cover_loss"
is_ifl_col = "is__intact_primary_forest"
driver_col = "driver_of_tree_cover_loss"
gfw_annual_removals_col = "average_annual_removal__Mg_CO2_yr-1"

gfw_emissions_sheet = "emissions_timeseries"


#Final results
out_sheet = rf"C:\Users\Melissa.Rose\OneDrive - World Resources Institute\Documents\Projects\NGHGI_translation\JRC_datahub_2024_update_{run_date}.xlsx"
