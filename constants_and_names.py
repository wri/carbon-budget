import os
import multiprocessing
import universal_util as uu
import datetime

########     ########
##### Constants #####
########     ########

# Model version
version = '1.2.3'
version_filename = version.replace('.', '_')


# Global variables that can be modified by the command line
global NO_UPLOAD
NO_UPLOAD = False
global SENSIT_TYPE
SENSIT_TYPE = 'std'
global RUN_DATE
RUN_DATE = None
global STAGE_INPUT
STAGE_INPUT = ''
global RUN_THROUGH
RUN_THROUGH = True
global CARBON_POOL_EXTENT
CARBON_POOL_EXTENT = ''
global EMITTED_POOLS
EMITTED_POOLS = ''
global THRESH
THRESH = ''
global STD_NET_FLUX
STD_NET_FLUX = ''
global INCLUDE_MANGROVES
INCLUDE_MANGROVES = False
global INCLUDE_US
INCLUDE_US = False
global SAVE_INTERMEDIATES
SAVE_INTERMEDIATES = True
global SINGLE_PROCESSOR
SINGLE_PROCESSOR = True
global LOG_NOTE
LOG_NOTE = ''


# Number of years of tree cover loss. If input loss raster is changed, this must be changed, too.
loss_years = 21

# Number of years in tree cover gain. If input cover gain raster is changed, this must be changed, too.
gain_years = 12

# Biomass to carbon ratio for aboveground, belowground, and deadwood in non-mangrove forests (planted and non-planted)
biomass_to_c_non_mangrove = 0.47

# Biomass to carbon ratio for litter in non-mangrove forests (planted and non-planted).
# From IPCC guidelines chapter 2, pdf page 23.
biomass_to_c_non_mangrove_litter = 0.37

# Biomass to carbon ratio for mangroves (IPCC wetlands supplement table 4.2)
biomass_to_c_mangrove = 0.45

# Carbon to CO2 ratio
# Needs the decimal places in order to be cast as a float
c_to_co2 = 44.0/12.0

# Canopy cover threshold for inclusion in forest extent
canopy_threshold = 30

# Number of metric tonnes in a megatonne
tonnes_to_megatonnes = 1000000

# Belowground to aboveground biomass ratios. Mangrove values are from Table 4.5 of IPCC wetland supplement.
# Non-mangrove value is the average slope of the AGB:BGB relationship in Figure 3 of Mokany et al. 2006.
below_to_above_non_mang = 0.26
below_to_above_trop_wet_mang = 0.49
below_to_above_trop_dry_mang = 0.29
below_to_above_subtrop_mang = 0.96

# Litter to aboveground biomass ratios for mangroves. Calculated from IPCC Wetland Supplement Tables 4.2, 4.3, and 4.7
# but elaborated on here: https://3.basecamp.com/3656819/buckets/7989024/todos/1235627617
litter_to_above_trop_wet_mang = 0.008
litter_to_above_trop_dry_mang = 0.0169
litter_to_above_subtrop_mang = 0.0169

# Deadwood to aboveground biomass ratios for mangroves. Calculated from IPCC Wetland Supplement Tables 4.2, 4.3, and 4.7
# but elaborated on here: https://3.basecamp.com/3656819/buckets/7989024/todos/1235627617
deadwood_to_above_trop_wet_mang = 0.123
deadwood_to_above_trop_dry_mang = 0.258
deadwood_to_above_subtrop_mang = 0.258

# The size of a Hansen loss/Landsat pixel, in decimal degrees (approximately 30x30 m at the equator)
Hansen_res = 0.00025

# Number of rows and columns of pixels in a 10x10 degree tile
tile_width = 10 / Hansen_res
tile_height = 10 / Hansen_res

# Pixel window sizes for rewindowed input rasters
agg_pixel_window = int(tile_width * 0.004)

# m2 per hectare
m2_per_ha = 100 * 100

# Number of processors on the machine being used
count = multiprocessing.cpu_count()


##########                  ##########
##### File names and directories #####
##########                  ##########

# Directory for the climate model files on s3
s3_base_dir = 's3://gfw2-data/climate/carbon_model/'

# Directory for all tiles in the Docker container
docker_base_dir = '/usr/local/tiles/'

docker_tmp = '/usr/local/tmp'

docker_app = '/usr/local/app'

c_emis_compile_dst = '{0}/emissions/cpp_util'.format(docker_app)

# Model log
start = datetime.datetime.now()
date = datetime.datetime.now()
date_formatted = date.strftime("%Y_%m_%d__%H_%M_%S")
model_log_dir = os.path.join(s3_base_dir, 'model_logs/v{}/'.format(version))
model_log = "flux_model_log_{}.txt".format(date_formatted)


# Blank created tile list txt
# Stores the tile names for blank tiles. These tiles will be deleted at the end of the script so that they
# don't get counted as actual tiles of this type
blank_tile_txt = "blank_tiles.txt"


# Tile summary spreadsheets
tile_stats_pattern = 'tile_stats_model'
tile_stats_dir = os.path.join(s3_base_dir, 'tile_stats/')

######
### Model extent
######
pattern_model_extent = 'model_extent'
model_extent_dir = os.path.join(s3_base_dir, 'model_extent/standard/20229999/')

######
### Biomass tiles
######

## Biomass in 2000
# Woods Hole aboveground biomass 2000 version 4 tiles
pattern_WHRC_biomass_2000_unmasked = "t_aboveground_biomass_ha_2000"
WHRC_biomass_2000_unmasked_dir = 's3://gfw2-data/climate/WHRC_biomass/WHRC_V4/Processed/'

##### This is deprecated but there are still some references to it in the Brazil sensitivity analysis
# # Woods Hole aboveground biomass 2000 version 4 tiles without mangrove or planted forest pixels
# pattern_WHRC_biomass_2000_non_mang_non_planted = "t_aboveground_biomass_ha_2000_non_mangrove_non_planted"
# WHRC_biomass_2000_non_mang_non_planted_dir = os.path.join(s3_base_dir, 'biomass_non_mangrove_non_planted/standard/20190225/')

# Raw Lola Fatoyinbo aboveground mangrove biomass in the year 2000 rasters
mangrove_biomass_raw_dir = os.path.join(s3_base_dir, 'mangrove_biomass/raw_from_Nathan_Thomas_20190215/')
mangrove_biomass_raw_file = 'MaskedSRTMCountriesAGB_V2_Tiff.zip'

# Processed mangrove aboveground biomass in the year 2000
pattern_mangrove_biomass_2000 = 'mangrove_agb_t_ha_2000'
mangrove_biomass_2000_dir = os.path.join(s3_base_dir, 'mangrove_biomass/processed/standard/20190220/')
pattern_mangrove_biomass_2000_rewindow = 'mangrove_agb_t_ha_2000_rewindow'
mangrove_biomass_2000_rewindow_dir = os.path.join(s3_base_dir, 'rewindow/mangrove_biomass/20210621/')


######
### Miscellaneous inputs
######

# The area of each pixel in m^2
pattern_pixel_area = 'hanson_2013_area'
pixel_area_dir = 's3://gfw2-data/analyses/area_28m/'
pattern_pixel_area_rewindow = 'hanson_2013_area_rewindow'
pixel_area_rewindow_dir = os.path.join(s3_base_dir, 'rewindow/pixel_area/20210621/')


# Spreadsheet with annual removals rates
gain_spreadsheet = 'gain_rate_continent_ecozone_age_20220914.xlsx'
gain_spreadsheet_dir = os.path.join(s3_base_dir, 'removal_rate_tables/')

# Annual Hansen loss tiles (2001-2021)
pattern_loss = 'GFW2021'
loss_dir = 's3://gfw2-data/forest_change/hansen_2021/'

# Hansen removals tiles (2001-2012)
pattern_gain = 'Hansen_GFC2015_gain'
gain_dir = 's3://gfw2-data/forest_change/tree_cover_gain/gaindata_2012/'
pattern_gain_rewindow = 'Hansen_GFC2015_gain_rewindow'
gain_rewindow_dir = os.path.join(s3_base_dir, 'rewindow/tree_cover_gain_2001_2012/20210621/')

# Tree cover density 2000 tiles
pattern_tcd = 'Hansen_GFC2014_treecover2000'
tcd_dir = 's3://gfw2-data/forest_cover/2000_treecover/'
pattern_tcd_rewindow = 'Hansen_GFC2014_treecover2000_rewindow'
tcd_rewindow_dir = os.path.join(s3_base_dir, 'rewindow/2000_treecover_density/20210621/')

# Intact forest landscape 2000 tiles
pattern_ifl = 'res_ifl_2000'
ifl_dir = os.path.join(s3_base_dir, 'other_emissions_inputs/ifl_2000/')

# Primary forest 2001 raw rasters
primary_raw_dir = 's3://gfw2-data/forest_cover/primary_forest/jan_2019/'

# Primary forest/IFL merged tiles
pattern_ifl_primary = 'ifl_2000_primary_2001_merged'
ifl_primary_processed_dir = os.path.join(s3_base_dir, 'ifl_primary_merged/processed/20200724/')

# Processed FAO ecozone shapefile
cont_ecozone_shp = 'fao_ecozones_fra_2000_continents_assigned_dissolved_FINAL_20180906.zip'

# Directory and names for the continent-ecozone tiles, raw and processed
pattern_cont_eco_raw = 'fao_ecozones_continents_raw'
pattern_cont_eco_processed = 'fao_ecozones_continents_processed'
cont_eco_s3_zip = os.path.join(s3_base_dir, 'fao_ecozones/fao_ecozones_fra_2000_continents_assigned_dissolved_FINAL_20180906.zip')
cont_eco_zip = 'fao_ecozones_fra_2000_continents_assigned_dissolved_FINAL_20180906.zip'
cont_eco_raw_dir = os.path.join(s3_base_dir, 'fao_ecozones/ecozone_continent/20190116/raw/')
cont_eco_dir = os.path.join(s3_base_dir, 'fao_ecozones/ecozone_continent/20190116/processed/')

# Plantation type: palm oil (code=1), wood fiber (code=2), and other (code=3)
pattern_planted_forest_type_unmasked = 'plantation_type_oilpalm_woodfiber_other_unmasked'
planted_forest_type_unmasked_dir = os.path.join(s3_base_dir, 'other_emissions_inputs/plantation_type/standard/20200730/')

# Peat mask inputs
peat_unprocessed_dir = os.path.join(s3_base_dir, 'other_emissions_inputs/peatlands/raw/')
cifor_peat_file = 'cifor_peat_mask.tif'
jukka_peat_zip = 'Jukka_peatland.zip'
jukka_peat_shp = 'peatland_drainage_proj.shp'
soilgrids250_peat_url = 'https://files.isric.org/soilgrids/latest/data/wrb/MostProbable/'   #Value 14 is histosol according to https://files.isric.org/soilgrids/latest/data/wrb/MostProbable.qml
pattern_soilgrids_most_likely_class = 'geotiff'

# Peat mask
pattern_peat_mask = 'peat_mask_processed'
peat_mask_dir = os.path.join(s3_base_dir, 'other_emissions_inputs/peatlands/processed/20200807/')

# Climate zone
climate_zone_raw_dir = os.path.join(s3_base_dir, 'other_emissions_inputs/climate_zone/raw/')
climate_zone_raw = 'climate_zone.tif'
pattern_climate_zone = 'climate_zone_processed'
climate_zone_processed_dir = os.path.join(s3_base_dir, 'other_emissions_inputs/climate_zone/processed/20200724/')

# Pre-2000 plantations
plant_pre_2000_raw_dir = os.path.join(s3_base_dir, 'other_emissions_inputs/IDN_MYS_plantation_pre_2000/raw/')
pattern_plant_pre_2000_raw = 'plant_est_2000_or_earlier'
pattern_plant_pre_2000 = 'plantation_2000_or_earlier_processed'
plant_pre_2000_processed_dir = os.path.join(s3_base_dir, 'other_emissions_inputs/IDN_MYS_plantation_pre_2000/processed/20200724/')

# Drivers of tree cover loss
drivers_raw_dir = os.path.join(s3_base_dir, 'other_emissions_inputs/tree_cover_loss_drivers/raw/')
pattern_drivers_raw = 'Final_Classification_2021__reproj_nearest_0-005_0-005_deg__20220316.tif'
pattern_drivers = 'tree_cover_loss_driver_processed'
drivers_processed_dir = os.path.join(s3_base_dir, 'other_emissions_inputs/tree_cover_loss_drivers/processed/drivers_2021/20220316/')

# Burn year
burn_area_raw_ftp = 'sftp://fuoco.geog.umd.edu/data/MODIS/C6/MCD64A1/HDF/' # per https://modis-fire.umd.edu/files/MODIS_C6_BA_User_Guide_1.3.pdf
burn_year_hdf_raw_dir = os.path.join(s3_base_dir, 'other_emissions_inputs/burn_year/raw_hdf/')
burn_year_stacked_hv_tif_dir = os.path.join(s3_base_dir, 'other_emissions_inputs/burn_year/stacked_hv_tifs/')
burn_year_warped_to_Hansen_dir = os.path.join(s3_base_dir, 'other_emissions_inputs/burn_year/burn_year_10x10_clip/')
pattern_burn_year = "burnyear_with_Hansen_loss"
burn_year_dir = os.path.join(s3_base_dir, 'other_emissions_inputs/burn_year/burn_year_with_Hansen_loss/20220308/')

######
### Plantation processing
######

gadm_dir = 's3://gfw2-data/alerts-tsv/gis_source/'
gadm_zip = 'gadm_3_6_adm2_final.zip'
gadm_shp = 'gadm_3_6_adm2_final.shp'
gadm_iso = 'gadm_3_6_with_planted_forest_iso.shp'
gadm_path = os.path.join(gadm_dir, gadm_zip)
gadm_plant_1x1_index_dir = os.path.join(s3_base_dir, 'gadm_plantation_1x1_tile_index/')
pattern_gadm_1x1_index = 'gadm_index_1x1'
pattern_plant_1x1_index = 'plantation_index_1x1'

# Countries with planted forests in them according to the planted forest geodatabase
plantation_countries = [
                        'ARG', 'VNM', 'VEN', 'THA', 'RWA', 'PNG', 'PHL', 'PAN', 'NIC', 'IND', 'HND', 'CRI', 'COD', 'COL',
                        'GAB', 'GHA', 'GTM', 'IDN', 'KEN', 'KHM', 'PRK', 'KOR', 'LBR', 'LKA', 'MEX', 'MMR', 'MWI', 'MGA',
                        'NPL', 'NZL', 'PAK', 'PER', 'SLB', 'URY', 'USA', 'ZAF', 'AUS', 'BRA', 'CHL', 'CHN', 'CIV', 'CMR',
                        'JPN', 'MYS', 'ECU',
                        'AUT', 'BEL', 'BGR', 'HRV', 'CYP', 'CZE', 'DNK', 'EST', 'FIN', 'FRA', 'DEU', 'GRC', 'HUN', 'IRL',
                        'ITA', 'LVA', 'LTU', 'LUX', 'MLT', 'NLD', 'POL', 'PRT', 'ROU', 'SVK', 'SVN', 'ESP', 'SWE', 'GBR',
                        'ALA', 'ALB', 'ARM', 'AZE', 'BIH', 'BLR', 'CHE', 'GEO', 'IRQ', 'ISL', 'MDA', 'MKD', 'MNE',
                        'NGA', 'NOR', 'SRB', 'SYR', 'TUR', 'UKR', 'XKO'
                        ]

######
### Removals
######

### Forest age category

# US forest age category tiles
name_age_cat_natrl_forest_US_raw = 'forest_age_category_US__0_20__20_100__100plus__20200723.tif'
age_cat_natrl_forest_US_raw_dir = os.path.join(s3_base_dir, 'forest_age_category_natural_forest_US/raw/20200723/')

pattern_age_cat_natrl_forest_US = 'forest_age_category_natural_forest_US'
age_cat_natrl_forest_US_dir = os.path.join(s3_base_dir, 'forest_age_category_natural_forest_US/processed/standard/20200724/')

# Age categories over entire model extent, as a precursor to assigning IPCC default removal rates
pattern_age_cat_IPCC = 'forest_age_category_IPCC__1_young_2_mid_3_old'
age_cat_IPCC_dir = os.path.join(s3_base_dir, 'forest_age_category_IPCC/standard/20229999/')


### US-specific removal precursors

name_FIA_regions_raw = 'Forest_Management_Regions_Final_Integrated_from_Thailynn_Munroe_via_Slack_20200723.tif'
FIA_regions_raw_dir = os.path.join(s3_base_dir, 'US_FIA_region/raw/20200723/')

pattern_FIA_regions_processed = 'FIA_regions_processed'
FIA_regions_processed_dir = os.path.join(s3_base_dir, 'US_FIA_region/processed/20200724/')

name_FIA_forest_group_raw = 'forest_group_composite_set_no_data_20191223.tif'
FIA_forest_group_raw_dir = os.path.join(s3_base_dir, 'US_forest_group/intermediate/')

pattern_FIA_forest_group_processed = 'FIA_forest_group_processed'
FIA_forest_group_processed_dir = os.path.join(s3_base_dir, 'US_forest_group/processed/20200724/')

table_US_removal_rate = 'ICLEI R factors_livebiomass_all_nat_forest_types_withSD__20200831.xlsx'
US_removal_rate_table_dir = os.path.join(s3_base_dir, 'removal_rate_tables/')



### Annual carbon and biomass removals rates for specific forest types that are precursors for composite annual removal factor

# Annual aboveground and belowground carbon removals rate for planted forests, with removals rates everywhere inside the plantation boundaries (includes mangrove pixels)
pattern_annual_gain_AGC_BGC_planted_forest_unmasked = 'annual_gain_rate_AGC_BGC_t_ha_planted_forest_unmasked'
annual_gain_AGC_BGC_planted_forest_unmasked_dir = os.path.join(s3_base_dir, 'annual_gain_rate_AGC_BGC_planted_forest_unmasked/standard/20200730/')

# Annual aboveground carbon removals rate for <20 year secondary, non-mangrove, non-planted natural forests (raw)
name_annual_gain_AGC_natrl_forest_young_raw = 'sequestration_rate__mean__aboveground__full_extent__Mg_C_ha_yr.tif'
annual_gain_AGC_natrl_forest_young_raw_URL = 'http://gfw2-data.s3.amazonaws.com/climate/carbon_seqr_AI4E/Nature_publication_final_202007/full_extent/sequestration_rate__mean__aboveground__full_extent__Mg_C_ha_yr.tif'

# Annual aboveground carbon removals rate for young (<20 year secondary), non-mangrove, non-planted natural forests (processed)
pattern_annual_gain_AGC_natrl_forest_young = 'annual_gain_rate_AGC_t_ha_natural_forest_young_secondary'
annual_gain_AGC_natrl_forest_young_dir = os.path.join(s3_base_dir, 'annual_gain_rate_AGC_natural_forest_young_secondary/standard/20200728/')

# Annual aboveground+belowground carbon removals rate for natural European forests (raw)
name_annual_gain_AGC_BGC_natrl_forest_Europe_raw = 'annual_gain_rate_AGC_BGC_t_ha_natural_forest_raw_Europe.tif'
annual_gain_AGC_BGC_natrl_forest_Europe_raw_dir = os.path.join(s3_base_dir, 'annual_gain_rate_AGC_BGC_natural_forest_Europe/raw/standard/20200722/')

# Annual aboveground+belowground carbon removals rate for natural European forests (processed tiles)
# https://www.efi.int/knowledge/maps/treespecies
pattern_annual_gain_AGC_BGC_natrl_forest_Europe = 'annual_gain_rate_AGC_BGC_t_ha_natural_forest_Europe'
annual_gain_AGC_BGC_natrl_forest_Europe_dir = os.path.join(s3_base_dir, 'annual_gain_rate_AGC_BGC_natural_forest_Europe/processed/standard/20200724/')

# Annual aboveground+belowground carbon removals rate for natural US forests (processed tiles)
pattern_annual_gain_AGC_BGC_natrl_forest_US = 'annual_removal_factor_AGC_BGC_Mg_ha_natural_forest_US'
annual_gain_AGC_BGC_natrl_forest_US_dir = os.path.join(s3_base_dir, 'annual_gain_rate_AGC_BGC_natural_forest_US/processed/standard/20200831/')

# Annual aboveground biomass removals rate for mangroves
pattern_annual_gain_AGB_mangrove = 'annual_removal_factor_AGB_Mg_ha_mangrove'
annual_gain_AGB_mangrove_dir = os.path.join(s3_base_dir, 'annual_removal_factor_AGB_mangrove/standard/20200824/')

# Annual belowground biomass removals rate for mangroves
pattern_annual_gain_BGB_mangrove = 'annual_removal_factor_BGB_Mg_ha_mangrove'
annual_gain_BGB_mangrove_dir = os.path.join(s3_base_dir, 'annual_removal_factor_BGB_mangrove/standard/20200824/')

# Annual aboveground biomass removals rate using IPCC default removal rates
pattern_annual_gain_AGB_IPCC_defaults = 'annual_removal_factor_AGB_Mg_ha_IPCC_defaults_all_ages'
annual_gain_AGB_IPCC_defaults_dir = os.path.join(s3_base_dir, 'annual_removal_factor_AGB_IPCC_defaults_all_ages/standard/20229999/')

# Annual aboveground biomass removals rate using IPCC default removal rates
pattern_annual_gain_BGB_IPCC_defaults = 'annual_removal_factor_BGB_Mg_ha_IPCC_defaults_all_ages'
annual_gain_BGB_IPCC_defaults_dir = os.path.join(s3_base_dir, 'annual_removal_factor_BGB_IPCC_defaults_all_ages/standard/20229999/')

### Annual composite removal factor

# Annual aboveground removals rate for all forest types
pattern_annual_gain_AGC_all_types = 'annual_removal_factor_AGC_Mg_ha_all_forest_types'
annual_gain_AGC_all_types_dir = os.path.join(s3_base_dir, 'annual_removal_factor_AGC_all_forest_types/standard/20229999/')

# Annual belowground removals rate for all forest types
pattern_annual_gain_BGC_all_types = 'annual_removal_factor_BGC_Mg_ha_all_forest_types'
annual_gain_BGC_all_types_dir = os.path.join(s3_base_dir, 'annual_removal_factor_BGC_all_forest_types/standard/20229999/')

# Annual aboveground+belowground removals rate for all forest types
pattern_annual_gain_AGC_BGC_all_types = 'annual_removal_factor_AGC_BGC_Mg_ha_all_forest_types'
annual_gain_AGC_BGC_all_types_dir = os.path.join(s3_base_dir, 'annual_removal_factor_AGC_BGC_all_forest_types/standard/20229999/')

### Removal forest types (sources)

# Forest type used in removals model
pattern_removal_forest_type = 'removal_forest_type'
removal_forest_type_dir = os.path.join(s3_base_dir, 'removal_forest_type/standard/20229999/')

# Removal model forest type codes
mangrove_rank = 6
europe_rank = 5
planted_forest_rank = 4
US_rank = 3
young_natural_rank = 2
old_natural_rank = 1


### Number of years of carbon removal (removals year count)

# Number of removals years for all forest types
pattern_gain_year_count = 'gain_year_count_all_forest_types'
gain_year_count_dir = os.path.join(s3_base_dir, 'gain_year_count_all_forest_types/standard/20229999/')


### Cumulative gross carbon dioxide removals

# Gross aboveground removals for all forest types
pattern_cumul_gain_AGCO2_all_types = 'gross_removals_AGCO2_Mg_ha_all_forest_types_2001_{}'.format(loss_years)
cumul_gain_AGCO2_all_types_dir = os.path.join(s3_base_dir, 'gross_removals_AGCO2_all_forest_types/standard/per_hectare/20229999/')

# Gross belowground removals for all forest types
pattern_cumul_gain_BGCO2_all_types = 'gross_removals_BGCO2_Mg_ha_all_forest_types_2001_{}'.format(loss_years)
cumul_gain_BGCO2_all_types_dir = os.path.join(s3_base_dir, 'gross_removals_BGCO2_all_forest_types/standard/per_hectare/20229999/')

# Gross aboveground and belowground removals for all forest types in all pixels
pattern_cumul_gain_AGCO2_BGCO2_all_types = 'gross_removals_AGCO2_BGCO2_Mg_ha_all_forest_types_2001_{}'.format(loss_years)
cumul_gain_AGCO2_BGCO2_all_types_dir = os.path.join(s3_base_dir, 'gross_removals_AGCO2_BGCO2_all_forest_types/standard/full_extent/per_hectare/20229999/')

# Gross aboveground and belowground removals for all forest types in pixels within forest extent
pattern_cumul_gain_AGCO2_BGCO2_all_types_forest_extent = 'gross_removals_AGCO2_BGCO2_Mg_ha_all_forest_types_forest_extent_2001_{}'.format(loss_years)
cumul_gain_AGCO2_BGCO2_all_types_forest_extent_dir = os.path.join(s3_base_dir, 'gross_removals_AGCO2_BGCO2_all_forest_types/standard/forest_extent/per_hectare/20229999/')


######
### Carbon emitted_pools
######


### Non-biomass inputs to carbon emitted_pools

# FAO ecozones as boreal/temperate/tropical
pattern_fao_ecozone_raw = 'fao_ecozones_bor_tem_tro_20180619.zip'
fao_ecozone_raw_dir = os.path.join(s3_base_dir, 'inputs_for_carbon_pools/raw/{}'.format(pattern_fao_ecozone_raw))
pattern_bor_tem_trop_intermediate = 'fao_ecozones_bor_tem_tro_intermediate'
pattern_bor_tem_trop_processed = 'fao_ecozones_bor_tem_tro_processed'
bor_tem_trop_processed_dir = os.path.join(s3_base_dir, 'inputs_for_carbon_pools/processed/fao_ecozones_bor_tem_tro/20190418/')

# Precipitation
precip_raw_dir = os.path.join(s3_base_dir, 'inputs_for_carbon_pools/raw/add_30s_precip.tif')
pattern_precip = 'precip_mm_annual'
precip_processed_dir = os.path.join(s3_base_dir, 'inputs_for_carbon_pools/processed/precip/20190418/')

# Elevation
srtm_raw_dir = os.path.join(s3_base_dir, 'inputs_for_carbon_pools/raw/elevation/')
pattern_elevation = 'elevation'
elevation_processed_dir = os.path.join(s3_base_dir, 'inputs_for_carbon_pools/processed/elevation/20190418/')


### Carbon emitted_pools

# Base directory for all carbon emitted_pools
base_carbon_pool_dir = os.path.join(s3_base_dir, 'carbon_pools/')

## Carbon emitted_pools in loss year

# Date to include in the output directory for all emissions year carbon emitted_pools
emis_pool_run_date = '20229999'

# Aboveground carbon in the year of emission for all forest types in loss pixels
pattern_AGC_emis_year = "Mg_AGC_ha_emis_year"
AGC_emis_year_dir = os.path.join(base_carbon_pool_dir, 'aboveground_carbon/loss_pixels/standard/{}/'.format(emis_pool_run_date))

# Belowground carbon in loss pixels
pattern_BGC_emis_year = 'Mg_BGC_ha_emis_year'
BGC_emis_year_dir = os.path.join(base_carbon_pool_dir, 'belowground_carbon/loss_pixels/standard/{}/'.format(emis_pool_run_date))

# Deadwood in loss pixels
pattern_deadwood_emis_year_2000 = 'Mg_deadwood_C_ha_emis_year_2000'
deadwood_emis_year_2000_dir = os.path.join(base_carbon_pool_dir, 'deadwood_carbon/loss_pixels/standard/{}/'.format(emis_pool_run_date))

# Litter in loss pixels
pattern_litter_emis_year_2000 = 'Mg_litter_C_ha_emis_year_2000'
litter_emis_year_2000_dir = os.path.join(base_carbon_pool_dir, 'litter_carbon/loss_pixels/standard/{}/'.format(emis_pool_run_date))

# Soil C in loss pixels
pattern_soil_C_emis_year_2000 = 'Mg_soil_C_ha_emis_year_2000'
soil_C_emis_year_2000_dir = os.path.join(base_carbon_pool_dir, 'soil_carbon/loss_pixels/standard/{}/'.format(emis_pool_run_date))

# All carbon emitted_pools combined in loss pixels, with emitted values
pattern_total_C_emis_year = 'Mg_total_C_ha_emis_year'
total_C_emis_year_dir = os.path.join(base_carbon_pool_dir, 'total_carbon/loss_pixels/standard/{}/'.format(emis_pool_run_date))

## Carbon emitted_pools in 2000

pool_2000_run_date = '20200826'

# Aboveground carbon for the full biomass 2000 (mangrove and non-mangrove) extent based on 2000 stocks
pattern_AGC_2000 = "Mg_AGC_ha_2000"
AGC_2000_dir = os.path.join(base_carbon_pool_dir, 'aboveground_carbon/extent_2000/standard/{}/'.format(emis_pool_run_date))

# Belowground carbon for the full biomass 2000 (mangrove and non-mangrove) extent based on 2000 stocks
pattern_BGC_2000 = "Mg_BGC_ha_2000"
BGC_2000_dir = os.path.join(base_carbon_pool_dir, 'belowground_carbon/extent_2000/standard/{}/'.format(emis_pool_run_date))

# Deadwood carbon for the full biomass 2000 (mangrove and non-mangrove) extent based on 2000 stocks
pattern_deadwood_2000 = "Mg_deadwood_C_ha_2000"
deadwood_2000_dir = os.path.join(base_carbon_pool_dir, 'deadwood_carbon/extent_2000/standard/{}/'.format(emis_pool_run_date))

# Litter carbon for the full biomass 2000 (mangrove and non-mangrove) extent based on 2000 stocks
pattern_litter_2000 = "Mg_litter_C_ha_2000"
litter_2000_dir = os.path.join(base_carbon_pool_dir, 'litter_carbon/extent_2000/standard/{}/'.format(emis_pool_run_date))

# Raw mangrove soil C
mangrove_soil_C_dir = os.path.join(s3_base_dir, 'carbon_pools/soil_carbon/raw/')
name_mangrove_soil_C = 'Mangroves_SOCS_0_100cm_30m.zip'
pattern_mangrove_soil_C_raw = 'dSOCS_0_100cm'
# Raw mineral soil C file site, SoilGrids250, updated summer 2020
pattern_mineral_soil_C_raw = 'tileSG'
mineral_soil_C_url = 'https://files.isric.org/soilgrids/latest/data/ocs/ocs_0-30cm_mean/'

# Soil C full extent but just from SoilGrids250 (mangrove soil C layer not added in)
# Not used in model.
pattern_soil_C_full_extent_2000_non_mang = 'soil_C_ha_full_extent_2000_non_mangrove_Mg_ha'
soil_C_full_extent_2000_non_mang_dir = os.path.join(base_carbon_pool_dir, 'soil_carbon/intermediate_full_extent/no_mangrove/20210414/')

# Soil C full extent (all soil pixels, with mangrove soil C in Giri mangrove extent getting priority over mineral soil C)
# Non-mangrove C is 0-30 cm, mangrove C is 0-100 cm
pattern_soil_C_full_extent_2000 = 't_soil_C_ha_full_extent_2000'
soil_C_full_extent_2000_dir = os.path.join(base_carbon_pool_dir, 'soil_carbon/intermediate_full_extent/standard/20200724/')

# Total carbon (all carbon emitted_pools combined) for the full biomass 2000 (mangrove and non-mangrove) extent based on 2000 stocks
pattern_total_C_2000 = "Mg_total_C_ha_2000"
total_C_2000_dir = os.path.join(base_carbon_pool_dir, 'total_carbon/extent_2000/standard/{}/'.format(emis_pool_run_date))


######
### Gross emissions (directory and pattern names changed in script to soil_only-- no separate variables for those)
######

### Emissions from biomass and soil (all carbon emitted_pools)

# Date to include in the output directory
emis_run_date_biomass_soil = '20229999'

# pattern_gross_emis_commod_biomass_soil = 'gross_emis_commodity_Mg_CO2e_ha_biomass_soil_2001_{}'.format(loss_years)
pattern_gross_emis_commod_biomass_soil = 'gross_emis_commodity_Mg_CO2e_ha_biomass_soil_2001_{}'.format(loss_years)
gross_emis_commod_biomass_soil_dir = '{0}gross_emissions/commodities/biomass_soil/standard/{1}/'.format(s3_base_dir, emis_run_date_biomass_soil)

pattern_gross_emis_forestry_biomass_soil = 'gross_emis_forestry_Mg_CO2e_ha_biomass_soil_2001_{}'.format(loss_years)
gross_emis_forestry_biomass_soil_dir = '{0}gross_emissions/forestry/biomass_soil/standard/{1}/'.format(s3_base_dir, emis_run_date_biomass_soil)

pattern_gross_emis_shifting_ag_biomass_soil = 'gross_emis_shifting_ag_Mg_CO2e_ha_biomass_soil_2001_{}'.format(loss_years)
gross_emis_shifting_ag_biomass_soil_dir = '{0}gross_emissions/shifting_ag/biomass_soil/standard/{1}/'.format(s3_base_dir, emis_run_date_biomass_soil)

pattern_gross_emis_urban_biomass_soil = 'gross_emis_urbanization_Mg_CO2e_ha_biomass_soil_2001_{}'.format(loss_years)
gross_emis_urban_biomass_soil_dir = '{0}gross_emissions/urbanization/biomass_soil/standard/{1}/'.format(s3_base_dir, emis_run_date_biomass_soil)

pattern_gross_emis_wildfire_biomass_soil = 'gross_emis_wildfire_Mg_CO2e_ha_biomass_soil_2001_{}'.format(loss_years)
gross_emis_wildfire_biomass_soil_dir = '{0}gross_emissions/wildfire/biomass_soil/standard/{1}/'.format(s3_base_dir, emis_run_date_biomass_soil)

pattern_gross_emis_no_driver_biomass_soil = 'gross_emis_no_driver_Mg_CO2e_ha_biomass_soil_2001_{}'.format(loss_years)
gross_emis_no_driver_biomass_soil_dir = '{0}gross_emissions/no_driver/biomass_soil/standard/{1}/'.format(s3_base_dir, emis_run_date_biomass_soil)

pattern_gross_emis_co2_only_all_drivers_biomass_soil = 'gross_emis_CO2_only_all_drivers_Mg_CO2e_ha_biomass_soil_2001_{}'.format(loss_years)
gross_emis_co2_only_all_drivers_biomass_soil_dir = '{0}gross_emissions/all_drivers/CO2_only/biomass_soil/standard/{1}/'.format(s3_base_dir, emis_run_date_biomass_soil)

pattern_gross_emis_non_co2_all_drivers_biomass_soil = 'gross_emis_non_CO2_all_drivers_Mg_CO2e_ha_biomass_soil_2001_{}'.format(loss_years)
gross_emis_non_co2_all_drivers_biomass_soil_dir = '{0}gross_emissions/all_drivers/non_CO2/biomass_soil/standard/{1}/'.format(s3_base_dir, emis_run_date_biomass_soil)

pattern_gross_emis_all_gases_all_drivers_biomass_soil = 'gross_emis_all_gases_all_drivers_Mg_CO2e_ha_biomass_soil_2001_{}'.format(loss_years)
gross_emis_all_gases_all_drivers_biomass_soil_dir = '{0}gross_emissions/all_drivers/all_gases/biomass_soil/standard/full_extent/per_hectare/{1}/'.format(s3_base_dir, emis_run_date_biomass_soil)

pattern_gross_emis_all_gases_all_drivers_biomass_soil_forest_extent = 'gross_emis_all_gases_all_drivers_Mg_CO2e_ha_biomass_soil_forest_extent_2001_{}'.format(loss_years)
gross_emis_all_gases_all_drivers_biomass_soil_forest_extent_dir = '{0}gross_emissions/all_drivers/all_gases/biomass_soil/standard/forest_extent/per_hectare/{1}/'.format(s3_base_dir, emis_run_date_biomass_soil)

pattern_gross_emis_nodes_biomass_soil = 'gross_emis_decision_tree_nodes_biomass_soil_2001_{}'.format(loss_years)
gross_emis_nodes_biomass_soil_dir = '{0}gross_emissions/decision_tree_nodes/biomass_soil/standard/{1}/'.format(s3_base_dir, emis_run_date_biomass_soil)

### Emissions from soil only

# Date to include in the output directory
emis_run_date_soil_only = '20229999'

pattern_gross_emis_commod_soil_only = 'gross_emis_commodity_Mg_CO2e_ha_soil_only_2001_{}'.format(loss_years)
gross_emis_commod_soil_only_dir = '{0}gross_emissions/commodities/soil_only/standard/{1}/'.format(s3_base_dir, emis_run_date_soil_only)

pattern_gross_emis_forestry_soil_only = 'gross_emis_forestry_Mg_CO2e_ha_soil_only_2001_{}'.format(loss_years)
gross_emis_forestry_soil_only_dir = '{0}gross_emissions/forestry/soil_only/standard/{1}/'.format(s3_base_dir, emis_run_date_soil_only)

pattern_gross_emis_shifting_ag_soil_only = 'gross_emis_shifting_ag_Mg_CO2e_ha_soil_only_2001_{}'.format(loss_years)
gross_emis_shifting_ag_soil_only_dir = '{0}gross_emissions/shifting_ag/soil_only/standard/{1}/'.format(s3_base_dir, emis_run_date_soil_only)

pattern_gross_emis_urban_soil_only = 'gross_emis_urbanization_Mg_CO2e_ha_soil_only_2001_{}'.format(loss_years)
gross_emis_urban_soil_only_dir = '{0}gross_emissions/urbanization/soil_only/standard/{1}/'.format(s3_base_dir, emis_run_date_soil_only)

pattern_gross_emis_wildfire_soil_only = 'gross_emis_wildfire_Mg_CO2e_ha_soil_only_2001_{}'.format(loss_years)
gross_emis_wildfire_soil_only_dir = '{0}gross_emissions/wildfire/soil_only/standard/{1}/'.format(s3_base_dir, emis_run_date_soil_only)

pattern_gross_emis_no_driver_soil_only = 'gross_emis_no_driver_Mg_CO2e_ha_soil_only_2001_{}'.format(loss_years)
gross_emis_no_driver_soil_only_dir = '{0}gross_emissions/no_driver/soil_only/standard/{1}/'.format(s3_base_dir, emis_run_date_soil_only)

pattern_gross_emis_all_gases_all_drivers_soil_only = 'gross_emis_all_gases_all_drivers_Mg_CO2e_ha_soil_only_2001_{}'.format(loss_years)
gross_emis_all_gases_all_drivers_soil_only_dir = '{0}gross_emissions/all_drivers/all_gases/soil_only/standard/{1}/'.format(s3_base_dir, emis_run_date_soil_only)

pattern_gross_emis_co2_only_all_drivers_soil_only = 'gross_emis_CO2_only_all_drivers_Mg_CO2e_ha_soil_only_2001_{}'.format(loss_years)
gross_emis_co2_only_all_drivers_soil_only_dir = '{0}gross_emissions/all_drivers/CO2_only/soil_only/standard/{1}/'.format(s3_base_dir, emis_run_date_soil_only)

pattern_gross_emis_non_co2_all_drivers_soil_only = 'gross_emis_non_CO2_all_drivers_Mg_CO2e_ha_soil_only_2001_{}'.format(loss_years)
gross_emis_non_co2_all_drivers_soil_only_dir = '{0}gross_emissions/all_drivers/non_CO2/soil_only/standard/{1}/'.format(s3_base_dir, emis_run_date_soil_only)

pattern_gross_emis_nodes_soil_only = 'gross_emis_decision_tree_nodes_soil_only_2001_{}'.format(loss_years)
gross_emis_nodes_soil_only_dir = '{0}gross_emissions/decision_tree_nodes/soil_only/standard/{1}/'.format(s3_base_dir, emis_run_date_soil_only)


### Net flux
######

# Net emissions for all forest types and all carbon emitted_pools in all pixels
pattern_net_flux = 'net_flux_Mg_CO2e_ha_biomass_soil_2001_{}'.format(loss_years)
net_flux_dir = os.path.join(s3_base_dir, 'net_flux_all_forest_types_all_drivers/biomass_soil/standard/full_extent/per_hectare/20229999/')

# Net emissions for all forest types and all carbon emitted_pools in forest extent
pattern_net_flux_forest_extent = 'net_flux_Mg_CO2e_ha_biomass_soil_forest_extent_2001_{}'.format(loss_years)
net_flux_forest_extent_dir = os.path.join(s3_base_dir, 'net_flux_all_forest_types_all_drivers/biomass_soil/standard/forest_extent/per_hectare/20229999/')


### Per pixel model outputs
######

# Gross removals per pixel in all pixels
pattern_cumul_gain_AGCO2_BGCO2_all_types_per_pixel_full_extent = 'gross_removals_AGCO2_BGCO2_Mg_pixel_all_forest_types_full_extent_2001_{}'.format(loss_years)
cumul_gain_AGCO2_BGCO2_all_types_per_pixel_full_extent_dir = os.path.join(s3_base_dir, 'gross_removals_AGCO2_BGCO2_all_forest_types/standard/full_extent/per_pixel/20229999/')

# Gross removals per pixel in forest extent
pattern_cumul_gain_AGCO2_BGCO2_all_types_per_pixel_forest_extent = 'gross_removals_AGCO2_BGCO2_Mg_pixel_all_forest_types_forest_extent_2001_{}'.format(loss_years)
cumul_gain_AGCO2_BGCO2_all_types_per_pixel_forest_extent_dir = os.path.join(s3_base_dir, 'gross_removals_AGCO2_BGCO2_all_forest_types/standard/forest_extent/per_pixel/20229999/')

# Gross emissions per pixel in all pixels
pattern_gross_emis_all_gases_all_drivers_biomass_soil_per_pixel_full_extent = 'gross_emis_all_gases_all_drivers_Mg_CO2e_pixel_biomass_soil_full_extent_2001_{}'.format(loss_years)
gross_emis_all_gases_all_drivers_biomass_soil_per_pixel_full_extent_dir = os.path.join(s3_base_dir, 'gross_emissions/all_drivers/all_gases/biomass_soil/standard/full_extent/per_pixel/20229999/')

# Gross emissions per pixel in forest extent
pattern_gross_emis_all_gases_all_drivers_biomass_soil_per_pixel_forest_extent = 'gross_emis_all_gases_all_drivers_Mg_CO2e_pixel_biomass_soil_forest_extent_2001_{}'.format(loss_years)
gross_emis_all_gases_all_drivers_biomass_soil_per_pixel_forest_extent_dir = os.path.join(s3_base_dir, 'gross_emissions/all_drivers/all_gases/biomass_soil/standard/forest_extent/per_pixel/20229999/')

# Net flux per pixel in all pixels
pattern_net_flux_per_pixel_full_extent = 'net_flux_Mg_CO2e_pixel_biomass_soil_full_extent_2001_{}'.format(loss_years)
net_flux_per_pixel_full_extent_dir = os.path.join(s3_base_dir, 'net_flux_all_forest_types_all_drivers/biomass_soil/standard/full_extent/per_pixel/20229999/')

# Net flux per pixel in forest extent
pattern_net_flux_per_pixel_forest_extent = 'net_flux_Mg_CO2e_pixel_biomass_soil_forest_extent_2001_{}'.format(loss_years)
net_flux_per_pixel_forest_extent_dir = os.path.join(s3_base_dir, 'net_flux_all_forest_types_all_drivers/biomass_soil/standard/forest_extent/per_pixel/20229999/')


### 4x4 km aggregation tiles for mapping
######

pattern_aggreg = '0_04deg_modelv{}'.format(version_filename)
pattern_aggreg_sensit_perc_diff = 'net_flux_0_04deg_modelv{}_perc_diff_std'.format(version_filename)
pattern_aggreg_sensit_sign_change = 'net_flux_0_04deg_modelv{}_sign_change_std'.format(version_filename)

output_aggreg_dir = os.path.join(s3_base_dir, '0_04deg_output_aggregation/biomass_soil/standard/20229999/')



### Standard deviation maps
######

# Standard deviation for annual aboveground biomass removal factors for mangroves
pattern_stdev_annual_gain_AGB_mangrove = 'annual_removal_factor_stdev_AGB_Mg_ha_mangrove'
stdev_annual_gain_AGB_mangrove_dir = os.path.join(s3_base_dir, 'stdev_annual_removal_factor_AGB_mangrove/standard/20200824/')

# Standard deviation for annual aboveground+belowground carbon removal factors for natural European forests (raw)
name_stdev_annual_gain_AGC_BGC_natrl_forest_Europe_raw = 'annual_removal_factor_stdev_AGC_BGC_t_ha_natural_forest_Europe_raw.tif'
stdev_annual_gain_AGC_BGC_natrl_forest_Europe_raw_dir = os.path.join(s3_base_dir, 'stdev_annual_removal_factor_AGC_BGC_natural_forest_Europe/raw/standard/20200722/')

# Standard deviation for annual aboveground+belowground carbon removal factors for natural European forests (processed tiles)
# https://www.efi.int/knowledge/maps/treespecies
pattern_stdev_annual_gain_AGC_BGC_natrl_forest_Europe = 'annual_gain_rate_stdev_AGC_BGC_Mg_ha_natural_forest_Europe'
stdev_annual_gain_AGC_BGC_natrl_forest_Europe_dir = os.path.join(s3_base_dir, 'stdev_annual_removal_factor_AGC_BGC_natural_forest_Europe/processed/standard/20200724/')

# Standard deviation for annual aboveground+belowground carbon removal factors for planted forests
pattern_stdev_annual_gain_AGC_BGC_planted_forest_unmasked = 'annual_gain_rate_stdev_AGC_BGC_t_ha_planted_forest_unmasked'
stdev_annual_gain_AGC_BGC_planted_forest_unmasked_dir = 's3://gfw2-data/climate/carbon_model/stdev_annual_removal_factor_AGC_BGC_planted_forest_unmasked/standard/20200801/'

# Standard deviation for annual aboveground+belowground carbon removals rate for natural US forests
pattern_stdev_annual_gain_AGC_BGC_natrl_forest_US = 'annual_removal_factor_stdev_AGC_BGC_Mg_ha_natural_forest_US'
stdev_annual_gain_AGC_BGC_natrl_forest_US_dir = os.path.join(s3_base_dir, 'stdev_annual_removal_factor_AGC_BGC_natural_forest_US/processed/standard/20200831/')

# Standard deviation for annual aboveground carbon removal factors for <20 year secondary, non-mangrove, non-planted natural forests (raw input)
name_stdev_annual_gain_AGC_natrl_forest_young_raw = 'sequestration_rate__stdev__aboveground__full_extent__Mg_C_ha_yr.tif'
stdev_annual_gain_AGC_natrl_forest_young_raw_URL = 's3://gfw2-data/climate/carbon_seqr_AI4E/Nature_publication_final_202007/full_extent/sequestration_rate__stdev__aboveground__full_extent__Mg_C_ha_yr.tif'

# Standard deviation for annual aboveground carbon removal factors for <20 year secondary, non-mangrove, non-planted natural forests
pattern_stdev_annual_gain_AGC_natrl_forest_young = 'annual_gain_rate_stdev_AGC_t_ha_natural_forest_young_secondary'
stdev_annual_gain_AGC_natrl_forest_young_dir = os.path.join(s3_base_dir, 'stdev_annual_removal_factor_AGC_natural_forest_young_secondary/processed/standard/20200728/')

# Standard deviation for annual aboveground biomass removal factors using IPCC default removal rates
pattern_stdev_annual_gain_AGB_IPCC_defaults = 'annual_removal_factor_stdev_AGB_Mg_ha_IPCC_defaults_all_ages'
stdev_annual_gain_AGB_IPCC_defaults_dir = os.path.join(s3_base_dir, 'stdev_annual_removal_factor_AGB_IPCC_defaults_all_ages/standard/20229999/')

# Standard deviation for aboveground and belowground removal factors for all forest types
pattern_stdev_annual_gain_AGC_all_types = 'annual_removal_factor_stdev_AGC_Mg_ha_all_forest_types'
stdev_annual_gain_AGC_all_types_dir = os.path.join(s3_base_dir, 'stdev_annual_removal_factor_AGC_all_forest_types/standard/20229999/')


# Raw mineral soil C file site
pattern_uncert_mineral_soil_C_raw = 'tileSG'
CI5_mineral_soil_C_url = 'https://files.isric.org/soilgrids/latest/data/ocs/ocs_0-30cm_Q0.05/'
CI95_mineral_soil_C_url = 'https://files.isric.org/soilgrids/latest/data/ocs/ocs_0-30cm_Q0.95/'


# Standard deviation in soil C stocks (0-30 cm)
pattern_stdev_soil_C_full_extent = 'Mg_soil_C_ha_stdev_full_extent_2000'
stdev_soil_C_full_extent_2000_dir = os.path.join(s3_base_dir, 'stdev_soil_carbon_full_extent/standard/20200828/')


### Testing materials
######

test_data_dir = '/usr/local/carbon-budget/test/test_data/'
test_data_out_dir = f'{test_data_dir}tmp_out/'
pattern_test_suffix= 'top_005deg'
pattern_comparison_suffix = f'comparison_{pattern_test_suffix}'

### Sensitivity analysis
######

sensitivity_list = ['std', 'maxgain', 'no_shifting_ag', 'convert_to_grassland',
                    'biomass_swap', 'US_removals', 'no_primary_gain', 'legal_Amazon_loss', 'Mekong_loss']

model_type_arg_help = 'Argument for whether the model is being run in standard form or as a sensitivity analysis run. ' \
                      '{0} = Standard model. {1} = Maximize gain years. {2} = Shifting agriculture is treated as commodity-driven deforestation. ' \
                      '{3} = Commodity-driven deforestation results in grassland rather than cropland.' \
                      '{4} = Replace Baccini AGB map with Saatchi biomass map. ' \
                      '{5} = Use US-specific removals. {6} = Assume primary forests and IFLs have a removal rate of 0.' \
                      '{7} = Use Brazilian national loss data from PRODES for the legal Amazon.'\
                      '{8} = Use Hansen v2.0 loss data for the Mekong (first loss year only).'\
    .format(sensitivity_list[0], sensitivity_list[1], sensitivity_list[2], sensitivity_list[3], sensitivity_list[4],
            sensitivity_list[5], sensitivity_list[6], sensitivity_list[7], sensitivity_list[8])

# ## US-specific removals
#
# name_FIA_regions_raw = 'FIA_regions_dissolve_20191210.zip'
# FIA_regions_raw_dir = os.path.join(s3_base_dir, 'sensit_analysis_US_removals/FIA_region/raw/')
#
# pattern_FIA_regions_processed = 'FIA_regions_processed'
# FIA_regions_processed_dir = os.path.join(s3_base_dir, 'sensit_analysis_US_removals/FIA_region/processed/20191216/')
#
# name_US_forest_age_cat_raw = 'stand_age_category_all_US_reclass_focal_composite_set_no_data_20191218.tif'
# US_forest_age_cat_raw_dir = os.path.join(s3_base_dir, 'sensit_analysis_US_removals/forest_age_category/intermediate/')
#
# pattern_US_forest_age_cat_processed = 'US_forest_age_category_processed'
# US_forest_age_cat_processed_dir = os.path.join(s3_base_dir, 'sensit_analysis_US_removals/forest_age_category/processed/20191218/')
#
# name_FIA_forest_group_raw = 'forest_group_composite_set_no_data_20191223.tif'
# FIA_forest_group_raw_dir = os.path.join(s3_base_dir, 'sensit_analysis_US_removals/forest_group/intermediate/')
#
# pattern_FIA_forest_group_processed = 'FIA_forest_group_processed'
# FIA_forest_group_processed_dir = os.path.join(s3_base_dir, 'sensit_analysis_US_removals/forest_group/processed/20191223/')
#
# table_US_removal_rate = 'US_removal_rates_flux_model_20200623.xlsx'
# US_removal_rate_table_dir = os.path.join(s3_base_dir, 'removal_rate_tables/')
#
# # Annual aboveground biomass removals rate for non-mangrove, non-planted natural forests
# pattern_US_annual_gain_AGB_natrl_forest = 'annual_gain_rate_AGB_t_ha_natural_forest_non_mangrove_non_planted_US_removals'
# US_annual_gain_AGB_natrl_forest_dir = os.path.join(s3_base_dir, 'annual_gain_rate_AGB_natural_forest/US_removals/20200107/')
#
# # Annual belowground biomass removals rate for non-mangrove, non-planted natural forests using US-specific removal rates
# pattern_US_annual_gain_BGB_natrl_forest = 'annual_gain_rate_BGB_t_ha_natural_forest_non_mangrove_non_planted_US_removals'
# US_annual_gain_BGB_natrl_forest_dir = os.path.join(s3_base_dir, 'annual_gain_rate_BGB_natural_forest/US_removals/20200107/')

## Alternative aboveground biomass in 2000 (Sassan Saatchi/JPL 2011)

JPL_raw_name = "Saatchi_JPL_AGB_Mg_ha_1km_2000_non_integer_pixels_20200107.tif"
JPL_raw_dir = 's3://gfw2-data/climate/Saatchi_JPL_biomass/1km_2000/raw_combined/'

pattern_JPL_unmasked_processed = "Mg_aboveground_biomass_ha_2000_JPL"
JPL_processed_dir = 's3://gfw2-data/climate/Saatchi_JPL_biomass/1km_2000/processed/20200107/'

## Brazil-specific loss

Brazil_forest_extent_2000_raw_dir = os.path.join(s3_base_dir, 'sensit_analysis_legal_Amazon_loss/forest_extent_2000/raw/2020113/')

pattern_Brazil_forest_extent_2000_merged = 'legal_Amazon_forest_extent_2000_merged'
Brazil_forest_extent_2000_merged_dir = os.path.join(s3_base_dir, 'sensit_analysis_legal_Amazon_loss/forest_extent_2000/processed/combined/20200116/')

pattern_Brazil_forest_extent_2000_processed = 'legal_Amazon_forest_extent_2000'
Brazil_forest_extent_2000_processed_dir = os.path.join(s3_base_dir, 'sensit_analysis_legal_Amazon_loss/forest_extent_2000/processed/tiles/20200116/')

Brazil_annual_loss_raw_dir = os.path.join(s3_base_dir, 'sensit_analysis_legal_Amazon_loss/annual_loss/raw/20200920/')

pattern_Brazil_annual_loss_merged = 'legal_Amazon_annual_loss_2001_20{}_merged'.format(loss_years)
Brazil_annual_loss_merged_dir = os.path.join(s3_base_dir, 'sensit_analysis_legal_Amazon_loss/annual_loss/processed/combined/20200920/')

pattern_Brazil_annual_loss_processed = 'legal_Amazon_annual_loss_2001_20{}'.format(loss_years)
Brazil_annual_loss_processed_dir = os.path.join(s3_base_dir, 'sensit_analysis_legal_Amazon_loss/annual_loss/processed/tiles/20200920/')

## Mekong loss (Hansen v2.0)

Mekong_loss_raw_dir = os.path.join('s3://gfw2-data/forest_change/mekong_2_0/')
pattern_Mekong_loss_raw = 'Loss_20'

Mekong_loss_processed_dir = os.path.join(s3_base_dir, 'sensit_analysis_Mekong_loss/processed/20200210/')
pattern_Mekong_loss_processed = 'Mekong_loss_2001_15'
