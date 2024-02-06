import arcpy
import os

#####################################################################################
# USER INPUTS
#####################################################################################
# Set the working directory to the folder which contains the AOIS subfolder
working_directory = r"C:\GIS\carbon_model\CarbonFlux_QA_v1.3.1"

# Whether you want to overwrite previous arcpy outputs
overwrite_arcgis_output = True

# With each model update, change loss years and model_run_date
    # loss_years = number of years of tree cover loss (if input loss raster is changed, this must be changed, too)
    # model_run_date = s3 folder where per-pixel outputs from most recent model run are located
loss_years = 22
model_run_date = '20231114'

# List of tile_ids to process (change according to which tiles overlap with your AOIS shapefiles)
tile_list = ['00N_110E', '20N_020W']

# Dictionary to cross-reference countries to their tile_id and GADM boundaries
tile_dictionary = {"IDN": "00N_110E",
                   "GMB": "20N_020W"}

# Choose which extent to use for emission, removal, and net flux zonal stats
    # options = 'forest', 'full', or 'both'
extent = 'full'

# List of tree cover density thresholds to mask by
tcd_threshold = [0, 30, 75]
gain = True

# Flag to save intermediate masks during create_masks()
save_intermediates = False

#####################################################################################
# DEFAULT INPUTS
#####################################################################################

# Setting the arcpy environ workspace
arcpy.env.workspace = working_directory
arcpy.env.overwriteOutput = overwrite_arcgis_output

# Directories to be created/ checked
aois_folder = os.path.join(arcpy.env.workspace,"AOIS")
input_folder = os.path.join(arcpy.env.workspace,"Input")
mask_folder = os.path.join(arcpy.env.workspace,"Mask")
mask_input_folder = os.path.join(arcpy.env.workspace,"Mask", "Inputs")
mask_output_folder = os.path.join(arcpy.env.workspace,"Mask", "Mask")
gain_folder = os.path.join(mask_input_folder, "Gain")
mangrove_folder = os.path.join(mask_input_folder, "Mangrove")
plantations_folder = os.path.join(mask_input_folder, "Pre_2000_Plantations")
tcd_folder = os.path.join(mask_input_folder, "TCD")
whrc_folder = os.path.join(mask_input_folder, "WHRC")
outputs_folder = os.path.join(arcpy.env.workspace, "Outputs")
csv_folder = os.path.join(outputs_folder, "CSV")
annual_folder = os.path.join(outputs_folder, "Annual")
tcl_folder = os.path.join(arcpy.env.workspace, "TCL")
tcl_input_folder = os.path.join(tcl_folder, "Inputs")
tcl_clip_folder = os.path.join(tcl_folder, "Clip")

# Filepath prefix for tile download step
s3_base_dir = 's3://gfw2-data/climate/carbon_model/'

## Input folder s3 filepath informaiton
# Gross emissions per pixel in forest extent
gross_emis_forest_extent_s3_path = os.path.join(s3_base_dir, f'gross_emissions/all_drivers/all_gases/biomass_soil/standard/forest_extent/per_pixel/{model_run_date}/')
gross_emis_forest_extent_s3_pattern = f'gross_emis_all_gases_all_drivers_Mg_CO2e_pixel_biomass_soil_forest_extent_2001_{loss_years}'

# Gross emissions per pixel in all pixels
gross_emis_full_extent_s3_path = os.path.join(s3_base_dir, f'gross_emissions/all_drivers/all_gases/biomass_soil/standard/full_extent/per_pixel/{model_run_date}/')
gross_emis_full_extent_s3_pattern = f'gross_emis_all_gases_all_drivers_Mg_CO2e_pixel_biomass_soil_full_extent_2001_{loss_years}'

# Gross removals per pixel in forest extent
gross_removals_forest_extent_s3_path = os.path.join(s3_base_dir, f'gross_removals_AGCO2_BGCO2_all_forest_types/standard/forest_extent/per_pixel/{model_run_date}/')
gross_removals_forest_extent_s3_pattern = f'gross_removals_AGCO2_BGCO2_Mg_pixel_all_forest_types_forest_extent_2001_{loss_years}'

# Gross removals per pixel in all pixels
gross_removals_full_extent_s3_path = os.path.join(s3_base_dir, f'gross_removals_AGCO2_BGCO2_all_forest_types/standard/full_extent/per_pixel/{model_run_date}/')
gross_removals_full_extent_s3_pattern = f'gross_removals_AGCO2_BGCO2_Mg_pixel_all_forest_types_full_extent_2001_{loss_years}'

# Net flux per pixel in forest extent
netflux_forest_extent_s3_path = os.path.join(s3_base_dir, f'net_flux_all_forest_types_all_drivers/biomass_soil/standard/forest_extent/per_pixel/{model_run_date}/')
netflux_forest_extent_s3_pattern = f'net_flux_Mg_CO2e_pixel_biomass_soil_forest_extent_2001_{loss_years}'

# Net flux per pixel in all pixels
netflux_full_extent_s3_path = os.path.join(s3_base_dir, f'net_flux_all_forest_types_all_drivers/biomass_soil/standard/full_extent/per_pixel/{model_run_date}/')
netflux_full_extent_s3_pattern = f'net_flux_Mg_CO2e_pixel_biomass_soil_full_extent_2001_{loss_years}'

## Mask, Inputs folder s3 filepath informaiton
# Hansen removals tiles based on canopy height (2000-2020)
gain_s3_path = 's3://gfw-data-lake/umd_tree_cover_gain_from_height/v202206/raster/epsg-4326/10/40000/gain/geotiff/'
gain_s3_pattern = ''
gain_local_pattern = 'tree_cover_gain_2000_2020'

# Woods Hole aboveground biomass 2000 version 4 tiles
whrc_s3_path = 's3://gfw2-data/climate/WHRC_biomass/WHRC_V4/Processed/'
whrc_s3_pattern = "t_aboveground_biomass_ha_2000"

# Tree cover density 2000 tiles
tcd_s3_path = 's3://gfw2-data/forest_cover/2000_treecover/'
tcd_s3_pattern = 'Hansen_GFC2014_treecover2000'

# Processed mangrove aboveground biomass in the year 2000
mangrove_s3_path = os.path.join(s3_base_dir, 'mangrove_biomass/processed/standard/20190220/')
mangrove_s3_pattern = 'mangrove_agb_t_ha_2000'

# Pre-2000 plantations
plantation_s3_path = os.path.join(s3_base_dir, 'other_emissions_inputs/IDN_MYS_plantation_pre_2000/processed/20200724/')
plantation_s3_pattern = 'plantation_2000_or_earlier_processed'

# Annual Hansen loss tiles (2001-2022)
loss_s3_path = 's3://gfw2-data/forest_change/hansen_2022/'
loss_s3_pattern = 'GFW2022'