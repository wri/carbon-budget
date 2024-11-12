import os
import multiprocessing
import universal_util as uu
import datetime

########     ########
##### Constants #####
########     ########

# Model version
version = '1.3.2'
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
global STD_NET_FLUX
STD_NET_FLUX = ''
global INCLUDE_MANGROVES
INCLUDE_MANGROVES = False
global INCLUDE_US
INCLUDE_US = False
global SAVE_INTERMEDIATES
SAVE_INTERMEDIATES = True
global SINGLE_PROCESSOR
SINGLE_PROCESSOR = False
global LOG_NOTE
LOG_NOTE = ''


### Constants

# Number of years of tree cover loss. If input loss raster is changed, this must be changed, too.
loss_years = 23

# Number of years in tree cover gain. If input cover gain raster is changed, this must be changed, too.
gain_years = 20

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
# Non-mangrove ratio below is the average slope of the AGB:BGB relationship in Figure 3 of Mokany et al. 2006.
# and is only used where Huang et al. 2021 can't reach (remote Pacific islands).
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

# Resolution of aggregated output rasters in decimal degrees
agg_pixel_res = 0.04

agg_pixel_res_filename = str(agg_pixel_res).replace('.', '_')

# Pixel window sizes for rewindowed input rasters
agg_pixel_window = int(tile_width * 0.004)

# m2 per hectare
m2_per_ha = 100 * 100

# Number of processors on the machine being used
count = multiprocessing.cpu_count()

planted_forest_postgis_db = 'all_plant'
planted_forest_output_date = '20230911'
planted_forest_version = 'SDPTv2'


##########                  ##########
##### File names and directories #####
##########                  ##########

# Directory for the climate model files on s3
s3_base_dir = 's3://gfw2-data/climate/carbon_model/'

# Directory for all tiles in the Docker container
docker_tile_dir = '/usr/local/tiles/'

docker_tmp = '/usr/local/tmp'

docker_app = '/usr/local/app'

c_emis_compile_dst = f'{docker_app}/emissions/cpp_util'

# Model log
start = datetime.datetime.now()
date = datetime.datetime.now()
date_formatted = date.strftime("%Y_%m_%d__%H_%M_%S")
model_log_dir = os.path.join(s3_base_dir, f'model_logs/v{version}/')
model_log = f'flux_model_log_{date_formatted}.txt'


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
model_extent_dir = os.path.join(s3_base_dir, 'model_extent/standard/20240308/')

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

# Belowground biomass:aboveground biomass ratio tiles
name_raw_AGB_Huang_global = 'pergridarea_agb.nc'
name_raw_BGB_Huang_global = 'pergridarea_bgb.nc'
AGB_BGB_Huang_raw_dir = os.path.join(s3_base_dir, 'BGB_AGB_ratio/raw_AGB_BGB_Huang_et_al_2021/')

name_rasterized_AGB_Huang_global = 'AGB_global_from_Huang_2021_Mg_ha__20230201.tif'
name_rasterized_BGB_Huang_global = 'BGB_global_from_Huang_2021_Mg_ha__20230201.tif'
name_rasterized_BGB_AGB_Huang_global = 'BGB_AGB_ratio_global_from_Huang_2021__20230201.tif'
name_rasterized_BGB_AGB_Huang_global_extended = 'BGB_AGB_ratio_global_from_Huang_2021__20230201_extended_1400.tif'
AGB_BGB_Huang_rasterized_dir = os.path.join(s3_base_dir, 'BGB_AGB_ratio/rasterized_AGB_BGB_and_ratio_Huang_et_al_2021/')

pattern_BGB_AGB_ratio = 'BGB_AGB_ratio'
BGB_AGB_ratio_dir = os.path.join(s3_base_dir, 'BGB_AGB_ratio/processed/20230216/')



######
### Miscellaneous inputs
######

# The area of each pixel in m^2
pattern_pixel_area = 'hanson_2013_area'
pixel_area_dir = 's3://gfw2-data/analyses/area_28m/'

# Spreadsheet with annual removals rates
gain_spreadsheet = 'gain_rate_continent_ecozone_age_20230821.xlsx'
gain_spreadsheet_dir = os.path.join(s3_base_dir, 'removal_rate_tables/')

# Annual Hansen loss tiles (2001-2023)
pattern_loss = 'GFW2023'
loss_dir = 's3://gfw2-data/forest_change/hansen_2023/'

# Hansen removals tiles based on canopy height (2000-2020)
# From https://www.frontiersin.org/articles/10.3389/frsen.2022.856903/full
pattern_data_lake = ''
pattern_gain_ec2 = 'tree_cover_gain_2000_2020'
gain_dir = 's3://gfw-data-lake/umd_tree_cover_gain_from_height/v202206/raster/epsg-4326/10/40000/gain/geotiff/'

# Tree cover density 2000 tiles
pattern_tcd = 'Hansen_GFC2014_treecover2000'
tcd_dir = 's3://gfw2-data/forest_cover/2000_treecover/'

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


### Planted forests

# Note that planted forest data was rasterized using the gfw-data-api and the original copies live in
# s3://gfw-data-lake/gfw_planted_forests/v20230911/raster/epsg-4326/10/40000/.
# I then copied them into gfw2-data and renamed them to use my preferred patterns.

# Planted forest aboveground only removal factors
datalake_pf_agc_rf_dir = 's3://gfw-data-lake/gfw_planted_forests/v20230911/raster/epsg-4326/10/40000/removalFactorAGCMgHaYr/geotiff/'
pattern_pf_rf_agc_ec2 = 'annual_gain_rate_AGC_Mg_ha_planted_forest'
planted_forest_aboveground_removalfactor_dir = os.path.join(s3_base_dir, f'annual_removal_factor_planted_forest/{planted_forest_version}_AGC/{planted_forest_output_date}/')

# Planted forest aboveground + belowground removal factors
datalake_pf_agcbgc_rf_dir = 's3://gfw-data-lake/gfw_planted_forests/v20230911/raster/epsg-4326/10/40000/removalFactorAGCBGCMgHaYr/geotiff/'
pattern_pf_rf_agcbgc_ec2 = 'annual_gain_rate_AGC_BGC_Mg_ha_planted_forest'
planted_forest_aboveground_belowground_removalfactor_dir = os.path.join(s3_base_dir, f'annual_removal_factor_planted_forest/{planted_forest_version}_AGC_BGC/{planted_forest_output_date}/')

# Planted forest aboveground only standard deviations
datalake_pf_agc_sd_dir = 's3://gfw-data-lake/gfw_planted_forests/v20230911/raster/epsg-4326/10/40000/removalFactorAGCstdevMgHaYr/geotiff/'
pattern_pf_sd_agc_ec2 = 'annual_gain_rate_stdev_AGC_Mg_ha_planted_forest_unmasked'
planted_forest_aboveground_standard_deviation_dir = os.path.join(s3_base_dir, f'stdev_annual_removal_factor_planted_forest/{planted_forest_version}_AGC/{planted_forest_output_date}/')

# Planted forest aboveground + belowground standard deviations
datalake_pf_agcbgc_sd_dir = 's3://gfw-data-lake/gfw_planted_forests/v20230911/raster/epsg-4326/10/40000/removalFactorAGCBGCstdevMgHaYr/geotiff/'
pattern_pf_sd_agcbgc_ec2 = 'annual_gain_rate_stdev_AGC_BGC_Mg_ha_planted_forest_unmasked'
planted_forest_aboveground_belowground_standard_deviation_dir = os.path.join(s3_base_dir, f'stdev_annual_removal_factor_planted_forest/{planted_forest_version}_AGC_BGC/{planted_forest_output_date}/')

# Planted forest type (simpleName): palm oil (code=1), wood fiber (code=2), and other (code=3)
datalake_pf_simplename_dir = 's3://gfw-data-lake/gfw_planted_forests/v20230911/raster/epsg-4326/10/40000/simpleName/geotiff/'
pattern_planted_forest_type = 'plantation_type_oilpalm_woodfiber_other'
planted_forest_type_dir = os.path.join(s3_base_dir, f'other_emissions_inputs/plantation_type/{planted_forest_version}/{planted_forest_output_date}/')

# Planted forest establishment year
#datalake_pf_estab_year_dir =
pattern_planted_forest_estab_year = 'planted_forest_establishment_year'
planted_forest_estab_year_dir = os.path.join(s3_base_dir, f'planted_forest_estab_year/{planted_forest_version}/{planted_forest_output_date}/')

### Peatland delineation

# Peat mask inputs
peat_unprocessed_dir = os.path.join(s3_base_dir, 'other_emissions_inputs/peatlands/raw/')

# Gumbricht et al. 2017 (CIFOR) used for 40N to 60S
# https://data.cifor.org/dataset.xhtml?persistentId=doi:10.17528/CIFOR/DATA.00058
# https://data.cifor.org/file.xhtml?fileId=1727&version=7.0
Gumbricht_peat_name = 'Gumbricht_2017_CIFOR__TROP_SUBTROP_PeatV21_2016.tif'

# Creeze et al. 2022 for the Congo basin
# https://congopeat.net/maps/
# Probability layers of the 5 landcover types (GIS files) as published: https://drive.google.com/file/d/1zsUyFeO9TqRs5oxys3Ld4Ikgk8OYgHgc/
# Peat is codes 4 and 5
Crezee_name = 'Crezee_et_al_2022__Congo_Basin__Unsmoothed_Classification_Most_likely_class__compressed_20230315.tif'
Crezee_peat_name = 'Crezee_et_al_2022__Congo_Basin__Unsmoothed_Classification_Most_likely_class__compressed_20230315__peat_only.tif'

# Hastie et al. 2022 for Peru peat
# https://www.nature.com/articles/s41561-022-00923-4
Hastie_name = 'Hastie_et_al_2022__Peru__Peatland_Extent_LPA_50m__compressed_20230315.tif'

# Miettinen et al. 2016 for Indonesia and Malaysia
# https://www.sciencedirect.com/science/article/pii/S2351989415300470
Miettinen_peat_zip = 'Miettinen_2016__IDN_MYS_peat__aka_peatland_drainage_proj.zip'
Miettinen_peat_shp = 'Miettinen_2016__IDN_MYS_peat__aka_peatland_drainage_proj.shp'
Miettinen_peat_tif = 'Miettinen_2016__IDN_MYS_peat__aka_peatland_drainage_proj.tif'

# Xu et al. 2018 for >40N (and <60S, though there's no land down there)
# Xu et al. 2018 for >40N (and <60S, though there's no land down there)
# https://www.sciencedirect.com/science/article/abs/pii/S0341816217303004#ec0005
Xu_peat_zip = 'Xu_et_al_north_of_40N_reproj__20230302.zip'
Xu_peat_shp = 'Xu_et_al_north_of_40N_reproj__20230302.shp'
Xu_peat_tif = 'Xu_et_al_north_of_40N_reproj__20230302.tif'

# Combined peat mask tiles
pattern_peat_mask = 'peat_mask_processed'
peat_mask_dir = os.path.join(s3_base_dir, 'other_emissions_inputs/peatlands/processed/20230315/')


### Other emissions inputs

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
pattern_drivers_raw = 'Goode_FinalClassification_2023_wgs84_v20240402.tif'
pattern_drivers = 'tree_cover_loss_driver_processed'
drivers_processed_dir = os.path.join(s3_base_dir, 'other_emissions_inputs/tree_cover_loss_drivers/processed/drivers_2023/20240402/')

# drivers_raw_dir = 's3://gfw2-data/drivers_of_loss/1_km/raw/20241004/'
# pattern_drivers_raw = 'drivers_of_TCL_1_km_20241004.tif'
# pattern_drivers = 'drivers_of_TCL_1_km_20241004'
# drivers_processed_dir = 's3://gfw2-data/drivers_of_loss/1_km/processed/20241004/'
#TODO: Change drivers from 10km to 1km after all code updates are made

# Tree cover loss from fires
TCLF_raw_dir = 's3://gfw2-data/forest_change/hansen_2023_fire/'
TCLF_processed_dir = os.path.join(s3_base_dir, 'other_emissions_inputs/tree_cover_loss_fires/20240304/processed/')
pattern_TCLF_processed = 'tree_cover_loss_fire_processed'


######
### Plantation processing
######

gadm_dir = 's3://gfw2-data/alerts-tsv/gis_source/'
gadm_zip = 'gadm_3_6_adm2_final.zip'
gadm_shp = 'gadm_3_6_adm2_final.shp'
gadm_iso = 'gadm_3_6_with_planted_forest_iso.shp'
gadm_path = os.path.join(gadm_dir, gadm_zip)
gadm_plant_1x1_index_dir = os.path.join(s3_base_dir, 'gadm_plantation_1x1_tile_index/')
# pattern_gadm_1x1_index = 'gadm_index_1x1'
pattern_plant_1x1_index = 'plantation_index_1x1'

plantations_dir = os.path.join(s3_base_dir, 'plantations/')
pattern_gadm_1x1_index = 'fishnet_1x1_deg_SDPTv2_extent__20230821'

# Countries with planted forests in them according to the Spatial Database of Planted Trees v2
SDPT_v2_feature_classes = [
'AGO', 'ARG', 'ARM', 'AUS', 'AZE', 'BDI', 'BEN', 'BFA', 'BGD', 'BLZ', 'BOL', 'BRA', 'BRN', 'BTN', 'CAF', 'CAN',
'CHL', 'CHN', 'CIV', 'CMR', 'COD', 'COG', 'COL', 'CPV', 'CRI', 'CUB', 'CYP', 'DOM', 'DZA', 'ECU', 'EGY', 'ERI',
'ETH', 'FJI', 'GAB', 'GHA', 'GIN', 'GLP', 'GMB', 'GNB', 'GNQ', 'GTM', 'GUF', 'HND', 'HTI', 'IDN', 'IND',
'IRN', 'IRQ', 'ISR', 'JAM', 'JOR', 'JPN', 'KAZ', 'KEN', 'KGZ', 'KHM', 'KOR', 'LAO', 'LBN', 'LBR', 'LBY', 'LKA',
'LSO', 'MAR', 'MDG', 'MEX', 'MLI', 'MMR', 'MNG', 'MOZ', 'MRT', 'MWI', 'MYS', 'NCL', 'NGA', 'NIC', 'NPL', 'NZL',
'OMN', 'PAK', 'PAN', 'PER', 'PHL', 'PNG', 'PRK', 'PRY', 'RUS', 'RWA', 'SEN', 'SLB', 'SLE', 'SLV', 'SOM', 'SSD',
'STP', 'SUR', 'SWZ', 'SYR', 'TGO', 'THA', 'TJK', 'TTO', 'TUN', 'TUR', 'TZA', 'UGA', 'URY', 'USA', 'UZB', 'VEN',
'VNM', 'VUT', 'ZAF', 'ZMB', 'ZWE', 'EU'
]

SDPT_v2_iso_codes = [
'AGO', 'ARG', 'ARM', 'AUS', 'AZE', 'BDI', 'BEN', 'BFA', 'BGD', 'BLZ', 'BOL', 'BRA', 'BRN', 'BTN', 'CAF', 'CAN',
'CHL', 'CHN', 'CIV', 'CMR', 'COD', 'COG', 'COL', 'CPV', 'CRI', 'CUB', 'CYP', 'DOM', 'DZA', 'ECU', 'EGY', 'ERI',
'ETH', 'FJI', 'GAB', 'GHA', 'GIN', 'GLP', 'GMB', 'GNB', 'GNQ', 'GTM', 'GUF', 'HND', 'HTI', 'IDN', 'IND',
'IRN', 'IRQ', 'ISR', 'JAM', 'JOR', 'JPN', 'KAZ', 'KEN', 'KGZ', 'KHM', 'KOR', 'LAO', 'LBN', 'LBR', 'LBY', 'LKA',
'LSO', 'MAR', 'MDG', 'MEX', 'MLI', 'MMR', 'MNG', 'MOZ', 'MRT', 'MWI', 'MYS', 'NCL', 'NGA', 'NIC', 'NPL', 'NZL',
'OMN', 'PAK', 'PAN', 'PER', 'PHL', 'PNG', 'PRK', 'PRY', 'RUS', 'RWA', 'SEN', 'SLB', 'SLE', 'SLV', 'SOM', 'SSD',
'STP', 'SUR', 'SWZ', 'SYR', 'TGO', 'THA', 'TJK', 'TTO', 'TUN', 'TUR', 'TZA', 'UGA', 'URY', 'USA', 'UZB', 'VEN',
'VNM', 'VUT', 'ZAF', 'ZMB', 'ZWE',
# EU countries
'AUT', 'BEL', 'BGR', 'HRV', 'CYP', 'CZE', 'DNK', 'EST', 'FIN', 'FRA', 'DEU', 'GRC', 'HUN', 'IRL', 'ITA', 'LVA',
'LTU', 'LUX', 'MLT', 'NLD', 'POL', 'PRT', 'ROU', 'SVK', 'SVN', 'ESP', 'SWE',
# Countries that had SDPT in v1 but aren't in v2 ISO list
'NOR', 'GBR', 'MNE', 'XKO', 'SRB', 'ALB', 'BIH', 'MKD', 'MDA', 'UKR', 'BLR', 'ISL', 'GEO', 'ALA', 'CHE'
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
age_cat_IPCC_dir = os.path.join(s3_base_dir, 'forest_age_category_IPCC/standard/20240308/')



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
# Note that planted forest data was rasterized using the gfw-data-api and the original copies live in
# s3://gfw-data-lake/gfw_planted_forests/v20230911/raster/epsg-4326/10/40000/.
# I then copied them into gfw2-data and renamed them to use my preferred patterns.
pattern_annual_gain_AGC_BGC_planted_forest = 'annual_gain_rate_AGC_BGC_Mg_ha_planted_forest'
annual_gain_AGC_BGC_planted_forest_dir = os.path.join(s3_base_dir, f'annual_removal_factor_planted_forest/{planted_forest_version}_AGC_BGC/{planted_forest_output_date}/')

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
annual_gain_AGB_IPCC_defaults_dir = os.path.join(s3_base_dir, 'annual_removal_factor_AGB_IPCC_defaults_all_ages/standard/20240308/')

# Annual aboveground biomass removals rate using IPCC default removal rates
pattern_annual_gain_BGB_IPCC_defaults = 'annual_removal_factor_BGB_Mg_ha_IPCC_defaults_all_ages'
annual_gain_BGB_IPCC_defaults_dir = os.path.join(s3_base_dir, 'annual_removal_factor_BGB_IPCC_defaults_all_ages/standard/20240308/')


### Annual composite removal factor

# Annual aboveground removals rate for all forest types
pattern_annual_gain_AGC_all_types = 'annual_removal_factor_AGC_Mg_ha_all_forest_types'
annual_gain_AGC_all_types_dir = os.path.join(s3_base_dir, 'annual_removal_factor_AGC_all_forest_types/standard/20240308/')

# Annual belowground removals rate for all forest types
pattern_annual_gain_BGC_all_types = 'annual_removal_factor_BGC_Mg_ha_all_forest_types'
annual_gain_BGC_all_types_dir = os.path.join(s3_base_dir, 'annual_removal_factor_BGC_all_forest_types/standard/20240308/')

# Annual aboveground+belowground removals rate for all forest types
pattern_annual_gain_AGC_BGC_all_types = 'annual_removal_factor_AGC_BGC_Mg_ha_all_forest_types'
annual_gain_AGC_BGC_all_types_dir = os.path.join(s3_base_dir, 'annual_removal_factor_AGC_BGC_all_forest_types/standard/20240308/')


### Removal forest types (sources)

# Forest type used in removals model
pattern_removal_forest_type = 'removal_forest_type'
removal_forest_type_dir = os.path.join(s3_base_dir, 'removal_forest_type/standard/20240308/')


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
gain_year_count_dir = os.path.join(s3_base_dir, 'gain_year_count_all_forest_types/standard/20240308/')



### Cumulative gross carbon dioxide removals

# Gross aboveground removals for all forest types
pattern_cumul_gain_AGCO2_all_types = f'gross_removals_AGCO2_Mg_ha_all_forest_types_2001_{loss_years}'
cumul_gain_AGCO2_all_types_dir = os.path.join(s3_base_dir, 'gross_removals_AGCO2_all_forest_types/standard/per_hectare/20240308/')

# Gross belowground removals for all forest types
pattern_cumul_gain_BGCO2_all_types = f'gross_removals_BGCO2_Mg_ha_all_forest_types_2001_{loss_years}'
cumul_gain_BGCO2_all_types_dir = os.path.join(s3_base_dir, 'gross_removals_BGCO2_all_forest_types/standard/per_hectare/20240308/')

# Gross aboveground and belowground removals for all forest types in all pixels
pattern_cumul_gain_AGCO2_BGCO2_all_types = f'gross_removals_AGCO2_BGCO2_Mg_ha_all_forest_types_2001_{loss_years}'
cumul_gain_AGCO2_BGCO2_all_types_dir = os.path.join(s3_base_dir, 'gross_removals_AGCO2_BGCO2_all_forest_types/standard/full_extent/per_hectare/20240308/')

# Gross aboveground and belowground removals for all forest types in pixels within forest extent
pattern_cumul_gain_AGCO2_BGCO2_all_types_forest_extent = f'gross_removals_AGCO2_BGCO2_Mg_ha_all_forest_types_forest_extent_2001_{loss_years}'
cumul_gain_AGCO2_BGCO2_all_types_forest_extent_dir = os.path.join(s3_base_dir, 'gross_removals_AGCO2_BGCO2_all_forest_types/standard/forest_extent/per_hectare/20240308/')


######
### Carbon emitted_pools
######


### Non-biomass inputs to carbon emitted_pools

# FAO ecozones as boreal/temperate/tropical
pattern_fao_ecozone_raw = 'fao_ecozones_bor_tem_tro_20180619.zip'
fao_ecozone_raw_dir = os.path.join(s3_base_dir, f'inputs_for_carbon_pools/raw/{pattern_fao_ecozone_raw}')
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
emis_pool_run_date = '20240308'


# Aboveground carbon in the year of emission for all forest types in loss pixels
pattern_AGC_emis_year = "Mg_AGC_ha_emis_year"
AGC_emis_year_dir = os.path.join(base_carbon_pool_dir, f'aboveground_carbon/loss_pixels/standard/{emis_pool_run_date}/')

# Belowground carbon in loss pixels
pattern_BGC_emis_year = 'Mg_BGC_ha_emis_year'
BGC_emis_year_dir = os.path.join(base_carbon_pool_dir, f'belowground_carbon/loss_pixels/standard/{emis_pool_run_date}/')

# Deadwood in loss pixels
pattern_deadwood_emis_year_2000 = 'Mg_deadwood_C_ha_emis_year_2000'
deadwood_emis_year_2000_dir = os.path.join(base_carbon_pool_dir, f'deadwood_carbon/loss_pixels/standard/{emis_pool_run_date}/')

# Litter in loss pixels
pattern_litter_emis_year_2000 = 'Mg_litter_C_ha_emis_year_2000'
litter_emis_year_2000_dir = os.path.join(base_carbon_pool_dir, f'litter_carbon/loss_pixels/standard/{emis_pool_run_date}/')

# Soil C in loss pixels
pattern_soil_C_emis_year_2000 = 'Mg_soil_C_ha_emis_year_2000'
soil_C_emis_year_2000_dir = os.path.join(base_carbon_pool_dir, f'soil_carbon/loss_pixels/standard/{emis_pool_run_date}/')

# All carbon emitted_pools combined in loss pixels, with emitted values
pattern_total_C_emis_year = 'Mg_total_C_ha_emis_year'
total_C_emis_year_dir = os.path.join(base_carbon_pool_dir, f'total_carbon/loss_pixels/standard/{emis_pool_run_date}/')

## Carbon emitted_pools in 2000

pool_2000_run_date = '20230222'

# Aboveground carbon for the full biomass 2000 (mangrove and non-mangrove) extent based on 2000 stocks
pattern_AGC_2000 = "Mg_AGC_ha_2000"
AGC_2000_dir = os.path.join(base_carbon_pool_dir, f'aboveground_carbon/extent_2000/standard/{pool_2000_run_date}/')

# Belowground carbon for the full biomass 2000 (mangrove and non-mangrove) extent based on 2000 stocks
pattern_BGC_2000 = "Mg_BGC_ha_2000"
BGC_2000_dir = os.path.join(base_carbon_pool_dir, f'belowground_carbon/extent_2000/standard/{pool_2000_run_date}/')

# Deadwood carbon for the full biomass 2000 (mangrove and non-mangrove) extent based on 2000 stocks
pattern_deadwood_2000 = "Mg_deadwood_C_ha_2000"
deadwood_2000_dir = os.path.join(base_carbon_pool_dir, f'deadwood_carbon/extent_2000/standard/{pool_2000_run_date}/')

# Litter carbon for the full biomass 2000 (mangrove and non-mangrove) extent based on 2000 stocks
pattern_litter_2000 = "Mg_litter_C_ha_2000"
litter_2000_dir = os.path.join(base_carbon_pool_dir, f'litter_carbon/extent_2000/standard/{pool_2000_run_date}/')

# Raw mangrove soil C
mangrove_soil_C_dir = os.path.join(s3_base_dir, 'carbon_pools/soil_carbon/raw/')
name_mangrove_soil_C = 'Mangroves_SOCS_0_100cm_30m.zip'
pattern_mangrove_soil_C_raw = 'dSOCS_0_100cm'
# Raw mineral soil C file site, SoilGrids250, updated November 2023.
# SoilGrids250 last modified date for these files is 2023-02-01.
pattern_mineral_soil_C_raw = 'tileSG'
mineral_soil_C_url = 'https://files.isric.org/soilgrids/latest/data/ocs/ocs_0-30cm_mean/'

# Soil C for mangroves only, maskes to mangrove AGB extent
pattern_soil_C_mangrove = 'mangrove_soil_C_masked_to_mangrove_AGB_Mg_C_ha'
soil_C_mangrove_processed_dir = os.path.join(base_carbon_pool_dir, 'soil_carbon/intermediate_full_extent/mangrove_only/20231108/')

# Soil C full extent but just from SoilGrids250 (mangrove soil C layer not added in)
# Not used in model.
pattern_soil_C_full_extent_2000_non_mang = 'soil_C_full_extent_2000_non_mangrove_Mg_C_ha'
soil_C_full_extent_2000_non_mang_dir = os.path.join(base_carbon_pool_dir, 'soil_carbon/intermediate_full_extent/no_mangrove/20231108/')

# Soil C full extent (all soil pixels, with mangrove soil C in Giri mangrove extent getting priority over mineral soil C)
# Non-mangrove C is 0-30 cm, mangrove C is 0-100 cm
pattern_soil_C_full_extent_2000 = 'soil_C_full_extent_2000_Mg_C_ha'
soil_C_full_extent_2000_dir = os.path.join(base_carbon_pool_dir, 'soil_carbon/intermediate_full_extent/standard/20231108/')

# Total carbon (all carbon emitted_pools combined) for the full biomass 2000 (mangrove and non-mangrove) extent based on 2000 stocks
pattern_total_C_2000 = "Mg_total_C_ha_2000"
# total_C_2000_dir = os.path.join(base_carbon_pool_dir, f'total_carbon/extent_2000/standard/{pool_2000_run_date}/')
total_C_2000_dir = os.path.join(base_carbon_pool_dir, f'total_carbon/extent_2000/standard/20231108/')


######
### Gross emissions (directory and pattern names changed in script to soil_only-- no separate variables for those)
######

### Emissions from biomass and soil (all carbon emitted_pools)

# Date to include in the output directory
emis_run_date_biomass_soil = '20249999'
#TODO: Change date when running 1km drivers

# # pattern_gross_emis_commod_biomass_soil = f'gross_emis_commodity_Mg_CO2e_ha_biomass_soil_2001_{loss_years}'
# pattern_gross_emis_commod_biomass_soil = f'gross_emis_commodity_Mg_CO2e_ha_biomass_soil_2001_{loss_years}'
# gross_emis_commod_biomass_soil_dir = f'{s3_base_dir}gross_emissions/commodities/biomass_soil/standard/{emis_run_date_biomass_soil}/'
#
# pattern_gross_emis_forestry_biomass_soil = f'gross_emis_forestry_Mg_CO2e_ha_biomass_soil_2001_{loss_years}'
# gross_emis_forestry_biomass_soil_dir = f'{s3_base_dir}gross_emissions/forestry/biomass_soil/standard/{emis_run_date_biomass_soil}/'
#
# pattern_gross_emis_shifting_ag_biomass_soil = f'gross_emis_shifting_ag_Mg_CO2e_ha_biomass_soil_2001_{loss_years}'
# gross_emis_shifting_ag_biomass_soil_dir = f'{s3_base_dir}gross_emissions/shifting_ag/biomass_soil/standard/{emis_run_date_biomass_soil}/'
#
# pattern_gross_emis_urban_biomass_soil = f'gross_emis_urbanization_Mg_CO2e_ha_biomass_soil_2001_{loss_years}'
# gross_emis_urban_biomass_soil_dir = f'{s3_base_dir}gross_emissions/urbanization/biomass_soil/standard/{emis_run_date_biomass_soil}/'
#
# pattern_gross_emis_wildfire_biomass_soil = f'gross_emis_wildfire_Mg_CO2e_ha_biomass_soil_2001_{loss_years}'
# gross_emis_wildfire_biomass_soil_dir = f'{s3_base_dir}gross_emissions/wildfire/biomass_soil/standard/{emis_run_date_biomass_soil}/'
#
# pattern_gross_emis_no_driver_biomass_soil = f'gross_emis_no_driver_Mg_CO2e_ha_biomass_soil_2001_{loss_years}'
# gross_emis_no_driver_biomass_soil_dir = f'{s3_base_dir}gross_emissions/no_driver/biomass_soil/standard/{emis_run_date_biomass_soil}/'
#TODO: Delete after testing, commenting out for now

pattern_gross_emis_co2_only_all_drivers_biomass_soil = f'gross_emis_CO2_only_all_drivers_Mg_CO2e_ha_biomass_soil_2001_{loss_years}'
gross_emis_co2_only_all_drivers_biomass_soil_dir = f'{s3_base_dir}gross_emissions/all_drivers/CO2_only/biomass_soil/standard/{emis_run_date_biomass_soil}/'

pattern_gross_emis_non_co2_all_drivers_biomass_soil = f'gross_emis_non_CO2_all_drivers_Mg_CO2e_ha_biomass_soil_2001_{loss_years}'
gross_emis_non_co2_all_drivers_biomass_soil_dir = f'{s3_base_dir}gross_emissions/all_drivers/non_CO2/biomass_soil/standard/{emis_run_date_biomass_soil}/'

pattern_gross_emis_ch4_only_all_drivers_biomass_soil = f'gross_emis_CH4_only_all_drivers_Mg_CO2e_ha_biomass_soil_2001_{loss_years}'
gross_emis_ch4_only_all_drivers_biomass_soil_dir = f'{s3_base_dir}gross_emissions/all_drivers/CH4_only/biomass_soil/standard/{emis_run_date_biomass_soil}/'

pattern_gross_emis_n2o_only_all_drivers_biomass_soil = f'gross_emis_N2O_only_all_drivers_Mg_CO2e_ha_biomass_soil_2001_{loss_years}'
gross_emis_n2o_only_all_drivers_biomass_soil_dir = f'{s3_base_dir}gross_emissions/all_drivers/N2O_only/biomass_soil/standard/{emis_run_date_biomass_soil}/'

pattern_gross_emis_all_gases_all_drivers_biomass_soil = f'gross_emis_all_gases_all_drivers_Mg_CO2e_ha_biomass_soil_2001_{loss_years}'
gross_emis_all_gases_all_drivers_biomass_soil_dir = f'{s3_base_dir}gross_emissions/all_drivers/all_gases/biomass_soil/standard/full_extent/per_hectare/{emis_run_date_biomass_soil}/'

pattern_gross_emis_all_gases_all_drivers_biomass_soil_forest_extent = f'gross_emis_all_gases_all_drivers_Mg_CO2e_ha_biomass_soil_forest_extent_2001_{loss_years}'
gross_emis_all_gases_all_drivers_biomass_soil_forest_extent_dir = f'{s3_base_dir}gross_emissions/all_drivers/all_gases/biomass_soil/standard/forest_extent/per_hectare/{emis_run_date_biomass_soil}/'

pattern_gross_emis_nodes_biomass_soil = f'gross_emis_decision_tree_nodes_biomass_soil_2001_{loss_years}'
gross_emis_nodes_biomass_soil_dir = f'{s3_base_dir}gross_emissions/decision_tree_nodes/biomass_soil/standard/{emis_run_date_biomass_soil}/'

### Emissions from soil only

# Date to include in the output directory
emis_run_date_soil_only = '20240402'


pattern_gross_emis_commod_soil_only = f'gross_emis_commodity_Mg_CO2e_ha_soil_only_2001_{loss_years}'
gross_emis_commod_soil_only_dir = f'{s3_base_dir}gross_emissions/commodities/soil_only/standard/{emis_run_date_soil_only}/'

pattern_gross_emis_forestry_soil_only = f'gross_emis_forestry_Mg_CO2e_ha_soil_only_2001_{loss_years}'
gross_emis_forestry_soil_only_dir = f'{s3_base_dir}gross_emissions/forestry/soil_only/standard/{emis_run_date_soil_only}/'

pattern_gross_emis_shifting_ag_soil_only = f'gross_emis_shifting_ag_Mg_CO2e_ha_soil_only_2001_{loss_years}'
gross_emis_shifting_ag_soil_only_dir = f'{s3_base_dir}gross_emissions/shifting_ag/soil_only/standard/{emis_run_date_soil_only}/'

pattern_gross_emis_urban_soil_only = f'gross_emis_urbanization_Mg_CO2e_ha_soil_only_2001_{loss_years}'
gross_emis_urban_soil_only_dir = f'{s3_base_dir}gross_emissions/urbanization/soil_only/standard/{emis_run_date_soil_only}/'

pattern_gross_emis_wildfire_soil_only = f'gross_emis_wildfire_Mg_CO2e_ha_soil_only_2001_{loss_years}'
gross_emis_wildfire_soil_only_dir = f'{s3_base_dir}gross_emissions/wildfire/soil_only/standard/{emis_run_date_soil_only}/'

pattern_gross_emis_no_driver_soil_only = f'gross_emis_no_driver_Mg_CO2e_ha_soil_only_2001_{loss_years}'
gross_emis_no_driver_soil_only_dir = f'{s3_base_dir}gross_emissions/no_driver/soil_only/standard/{emis_run_date_soil_only}/'

pattern_gross_emis_all_gases_all_drivers_soil_only = f'gross_emis_all_gases_all_drivers_Mg_CO2e_ha_soil_only_2001_{loss_years}'
gross_emis_all_gases_all_drivers_soil_only_dir = f'{s3_base_dir}gross_emissions/all_drivers/all_gases/soil_only/standard/{emis_run_date_soil_only}/'

pattern_gross_emis_co2_only_all_drivers_soil_only = f'gross_emis_CO2_only_all_drivers_Mg_CO2e_ha_soil_only_2001_{loss_years}'
gross_emis_co2_only_all_drivers_soil_only_dir = f'{s3_base_dir}gross_emissions/all_drivers/CO2_only/soil_only/standard/{emis_run_date_soil_only}/'

pattern_gross_emis_non_co2_all_drivers_soil_only = f'gross_emis_non_CO2_all_drivers_Mg_CO2e_ha_soil_only_2001_{loss_years}'
gross_emis_non_co2_all_drivers_soil_only_dir = f'{s3_base_dir}gross_emissions/all_drivers/non_CO2/soil_only/standard/{emis_run_date_soil_only}/'

pattern_gross_emis_nodes_soil_only = f'gross_emis_decision_tree_nodes_soil_only_2001_{loss_years}'
gross_emis_nodes_soil_only_dir = f'{s3_base_dir}gross_emissions/decision_tree_nodes/soil_only/standard/{emis_run_date_soil_only}/'


### Net flux
######

# Net emissions for all forest types and all carbon emitted_pools in all pixels
pattern_net_flux = f'net_flux_Mg_CO2e_ha_biomass_soil_2001_{loss_years}'
net_flux_dir = os.path.join(s3_base_dir, 'net_flux_all_forest_types_all_drivers/biomass_soil/standard/full_extent/per_hectare/20240402/')

# Net emissions for all forest types and all carbon emitted_pools in forest extent
pattern_net_flux_forest_extent = f'net_flux_Mg_CO2e_ha_biomass_soil_forest_extent_2001_{loss_years}'
net_flux_forest_extent_dir = os.path.join(s3_base_dir, 'net_flux_all_forest_types_all_drivers/biomass_soil/standard/forest_extent/per_hectare/20240402/')


### Per pixel model outputs
######

# Gross removals per pixel in all pixels
pattern_cumul_gain_AGCO2_BGCO2_all_types_per_pixel_full_extent = f'gross_removals_AGCO2_BGCO2_Mg_pixel_all_forest_types_full_extent_2001_{loss_years}'
cumul_gain_AGCO2_BGCO2_all_types_per_pixel_full_extent_dir = os.path.join(s3_base_dir, 'gross_removals_AGCO2_BGCO2_all_forest_types/standard/full_extent/per_pixel/20240308/')

# Gross removals per pixel in forest extent
pattern_cumul_gain_AGCO2_BGCO2_all_types_per_pixel_forest_extent = f'gross_removals_AGCO2_BGCO2_Mg_pixel_all_forest_types_forest_extent_2001_{loss_years}'
cumul_gain_AGCO2_BGCO2_all_types_per_pixel_forest_extent_dir = os.path.join(s3_base_dir, 'gross_removals_AGCO2_BGCO2_all_forest_types/standard/forest_extent/per_pixel/20240308/')

# Gross emissions per pixel in all pixels
pattern_gross_emis_all_gases_all_drivers_biomass_soil_per_pixel_full_extent = f'gross_emis_all_gases_all_drivers_Mg_CO2e_pixel_biomass_soil_full_extent_2001_{loss_years}'
gross_emis_all_gases_all_drivers_biomass_soil_per_pixel_full_extent_dir = os.path.join(s3_base_dir, 'gross_emissions/all_drivers/all_gases/biomass_soil/standard/full_extent/per_pixel/20240402/')

# Gross emissions per pixel in forest extent
pattern_gross_emis_all_gases_all_drivers_biomass_soil_per_pixel_forest_extent = f'gross_emis_all_gases_all_drivers_Mg_CO2e_pixel_biomass_soil_forest_extent_2001_{loss_years}'
gross_emis_all_gases_all_drivers_biomass_soil_per_pixel_forest_extent_dir = os.path.join(s3_base_dir, 'gross_emissions/all_drivers/all_gases/biomass_soil/standard/forest_extent/per_pixel/20240402/')

# Net flux per pixel in all pixels
pattern_net_flux_per_pixel_full_extent = f'net_flux_Mg_CO2e_pixel_biomass_soil_full_extent_2001_{loss_years}'
net_flux_per_pixel_full_extent_dir = os.path.join(s3_base_dir, 'net_flux_all_forest_types_all_drivers/biomass_soil/standard/full_extent/per_pixel/20240402/')

# Net flux per pixel in forest extent
pattern_net_flux_per_pixel_forest_extent = f'net_flux_Mg_CO2e_pixel_biomass_soil_forest_extent_2001_{loss_years}'
net_flux_per_pixel_forest_extent_dir = os.path.join(s3_base_dir, 'net_flux_all_forest_types_all_drivers/biomass_soil/standard/forest_extent/per_pixel/20240402/')


### 4x4 km aggregation tiles for mapping
######

pattern_aggreg = f'0_04deg_modelv{version_filename}'
pattern_aggreg_sensit_perc_diff = f'net_flux_0_04deg_modelv{version_filename}_perc_diff_std'
pattern_aggreg_sensit_sign_change = f'net_flux_0_04deg_modelv{version_filename}_sign_change_std'

output_aggreg_dir = os.path.join(s3_base_dir, '0_04deg_output_aggregation/biomass_soil/standard/20240402/')



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
# Note that planted forest data was rasterized using the gfw-data-api and the original copies live in
# s3://gfw-data-lake/gfw_planted_forests/v20230911/raster/epsg-4326/10/40000/.
# I then copied them into gfw2-data and renamed them to use my preferred patterns.
pattern_stdev_annual_gain_AGC_BGC_planted_forest = 'annual_gain_rate_stdev_AGC_BGC_Mg_ha_planted_forest_unmasked'
stdev_annual_gain_AGC_BGC_planted_forest_dir = os.path.join(s3_base_dir, f'stdev_annual_removal_factor_planted_forest/{planted_forest_version}_AGC_BGC/{planted_forest_output_date}/')

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
stdev_annual_gain_AGB_IPCC_defaults_dir = os.path.join(s3_base_dir, 'stdev_annual_removal_factor_AGB_IPCC_defaults_all_ages/standard/20240308/')

# Standard deviation for aboveground and belowground removal factors for all forest types
pattern_stdev_annual_gain_AGC_all_types = 'annual_removal_factor_stdev_AGC_Mg_ha_all_forest_types'
stdev_annual_gain_AGC_all_types_dir = os.path.join(s3_base_dir, 'stdev_annual_removal_factor_AGC_all_forest_types/standard/20240308/')


# Raw mineral soil C file site
pattern_uncert_mineral_soil_C_raw = 'tileSG'
CI5_mineral_soil_C_url = 'https://files.isric.org/soilgrids/latest/data/ocs/ocs_0-30cm_Q0.05/'
CI95_mineral_soil_C_url = 'https://files.isric.org/soilgrids/latest/data/ocs/ocs_0-30cm_Q0.95/'


# Standard deviation in soil C stocks (0-30 cm)
pattern_stdev_soil_C_full_extent = 'Mg_soil_C_ha_stdev_full_extent_2000'
stdev_soil_C_full_extent_2000_dir = os.path.join(s3_base_dir, 'stdev_soil_carbon_full_extent/standard/20231108/')


### Testing materials
######

test_data_dir = '/usr/local/app/test/test_data/'
test_data_out_dir = f'{test_data_dir}tmp_out/'
pattern_test_suffix= 'top_005deg'
pattern_comparison_suffix = f'comparison_{pattern_test_suffix}'

### Sensitivity analysis
######

sensitivity_list = ['std', 'maxgain', 'no_shifting_ag', 'convert_to_grassland',
                    'biomass_swap', 'US_removals', 'no_primary_gain', 'legal_Amazon_loss', 'Mekong_loss']

model_type_arg_help = 'Argument for whether the model is being run in standard form or as a sensitivity analysis run. ' \
                      '{0} = Standard model. ' \
                      '{1} = Maximize gain years. ' \
                      '{2} = Shifting agriculture is treated as commodity-driven deforestation. ' \
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

pattern_Brazil_annual_loss_merged = f'legal_Amazon_annual_loss_2001_20{loss_years}_merged'
Brazil_annual_loss_merged_dir = os.path.join(s3_base_dir, 'sensit_analysis_legal_Amazon_loss/annual_loss/processed/combined/20200920/')

pattern_Brazil_annual_loss_processed = f'legal_Amazon_annual_loss_2001_20{loss_years}'
Brazil_annual_loss_processed_dir = os.path.join(s3_base_dir, 'sensit_analysis_legal_Amazon_loss/annual_loss/processed/tiles/20200920/')

## Mekong loss (Hansen v2.0)

Mekong_loss_raw_dir = os.path.join('s3://gfw2-data/forest_change/mekong_2_0/')
pattern_Mekong_loss_raw = 'Loss_20'

Mekong_loss_processed_dir = os.path.join(s3_base_dir, 'sensit_analysis_Mekong_loss/processed/20200210/')
pattern_Mekong_loss_processed = 'Mekong_loss_2001_15'
