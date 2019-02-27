import universal_util as uu
import os

########     ########
##### Constants #####
########     ########

# Number of years of tree cover loss. If input loss raster is changed, this must be changed, too.
loss_years = 15

# Number of years in tree cover gain. If input gain raster is changed, this must be changed, too.
gain_years = 12

# Number of biomass tiles
biomass_tile_count = 280

# Biomass to carbon ratio for non-mangrove, non-planted forests and for planted forests
biomass_to_c_natrl_forest = 0.5

# Biomass to carbon ratio for mangroves (IPCC wetlands supplement table 4.2)
biomass_to_c_mangrove = 0.45

# Carbon to CO2 ratio
c_to_co2 = 44/12

# m2 per hectare
m2_per_ha = 100 * 100

# Aboveground to belowground biomass ratios. Mangrove values are from Table 4.5 of IPCC wetland supplement
below_to_above_natrl_forest = 0.26
below_to_above_trop_wet_mang = 0.49
below_to_above_trop_dry_mang = 0.29
below_to_above_subtrop_mang = 0.96

# The size of a Hansen loss pixel, in decimal degrees
Hansen_res = 0.00025


##########                  ##########
##### File names and directories #####
##########                  ##########


### Biomass tiles
######

## Biomass in 2000
# Woods Hole aboveground biomass 2000 version 4 tiles
pattern_WHRC_biomass_2000_unmasked = "t_aboveground_biomass_ha_2000"
WHRC_biomass_2000_unmasked_dir = 's3://gfw2-data/climate/WHRC_biomass/WHRC_V4/Processed/'

# Woods Hole aboveground biomass 2000 version 4 tiles without mangrove or planted forest pixels
pattern_WHRC_biomass_2000_non_mang_non_planted = "t_aboveground_biomass_ha_2000_non_mangrove_non_planted"
WHRC_biomass_2000_non_mang_non_planted_dir = 's3://gfw2-data/climate/carbon_model/biomass_non_mangrove_non_planted/20190225/'

# Raw Lola Fatoyinbo aboveground mangrove biomass in the year 2000 rasters
mangrove_biomass_raw_dir = 's3://gfw2-data/climate/carbon_model/mangrove_biomass/raw_from_Nathan_Thomas_20190215/'
mangrove_biomass_raw_file = 'MaskedSRTMCountriesAGB_V2_Tiff.zip'

# Processed mangrove aboveground biomass in the year 2000
pattern_mangrove_biomass_2000 = 'mangrove_agb_t_ha_2000'
mangrove_biomass_2000_dir = 's3://gfw2-data/climate/carbon_model/mangrove_biomass/processed/20190220/'

## Biomass in the year of emission
base_biomass_emitted_dir = 's3://gfw2-data/climate/carbon_model/emitted_biomass/20181226/'

# Woods Hole aboveground biomass in the year of emission
pattern_natrl_forest_biomass_emitted = "t_aboveground_biomass_ha_emitted"
natrl_forest_biomass_emitted_dir = '{0}/natural_forest/'.format(base_biomass_emitted_dir)

# Mangrove aboveground biomass in the year of emission
pattern_mangrove_biomass_emitted = 'mangrove_agb_t_ha_emitted'
mangrove_biomass_emitted_dir = '{0}/mangrove/'.format(base_biomass_emitted_dir)

# Biomass tile list (WHRC/natural forests and mangroves)
pattern_biomass_tile_list = 'biomass_tile_list.txt'
biomass_tile_list_dir = 's3://gfw2-data/climate/carbon_model/biomass_tile_list/'


### Miscellaneous inputs
######

# Spreadsheet with annual gain rates
gain_spreadsheet_dir = 's3://gfw2-data/climate/carbon_model/'
gain_spreadsheet = 'gain_rate_continent_ecozone_age_20190116.xlsx'

# Annual Hansen loss tiles (2001-2015)
loss_dir = 's3://gfw2-data/forest_change/hansen_2015/Loss_tiles/'

# Hansen gain tiles (2001-2012)
pattern_gain = 'Hansen_GFC2015_gain'
gain_dir = 's3://gfw2-data/forest_change/tree_cover_gain/gaindata_2012/'

# Tree cover density 2000 tiles
pattern_tcd = 'Hansen_GFC2014_treecover2000'
tcd_dir = 's3://gfw2-data/forest_cover/2000_treecover/'

# Intact forest landscape 2000 tiles
pattern_ifl = 'res_ifl_2000'
ifl_dir = 's3://gfw2-data/climate/carbon_model/other_emissions_inputs/ifl_2000/'

# Processed FAO ecozone shapefile
cont_ecozone_shp = 'fao_ecozones_fra_2000_continents_assigned_dissolved_FINAL_20180906.zip'

# Directory and names for the continent-ecozone tiles, raw and processed
pattern_cont_eco_raw = 'fao_ecozones_continents_raw'
pattern_cont_eco_processed = 'fao_ecozones_continents_processed'
cont_eco_s3_zip = 's3://gfw2-data/climate/carbon_model/fao_ecozones/fao_ecozones_fra_2000_continents_assigned_dissolved_FINAL_20180906.zip'
cont_eco_zip = 'fao_ecozones_fra_2000_continents_assigned_dissolved_FINAL_20180906.zip'
cont_eco_raw_dir = 's3://gfw2-data/climate/carbon_model/fao_ecozones/ecozone_continent/20190116/raw/'
cont_eco_dir = 's3://gfw2-data/climate/carbon_model/fao_ecozones/ecozone_continent/20190116/processed/'


### Number of gain years (gain year count)
######

# Number of gain years for mangroves
pattern_gain_year_count_mangrove = 'gain_year_count_mangrove'
gain_year_count_mangrove_dir = 's3://gfw2-data/climate/carbon_model/gain_year_count_mangrove/20190225/'

# Number of gain years for non-mangrove planted forests
pattern_gain_year_count_planted_forest_non_mangrove = 'gain_year_count_planted_forest_non_mangrove'
gain_year_count_planted_forest_non_mangrove_dir = 's3://gfw2-data/climate/carbon_model/gain_year_count_planted_forest_non_mangrove/20190227/'

# Number of gain years for non-mangrove, non-planted natural forests
pattern_gain_year_count_natrl_forest = 'gain_year_count_natural_forest_non_mangrove_non_planted'
gain_year_count_natrl_forest_dir = 's3://gfw2-data/climate/carbon_model/gain_year_count_natural_forest/20190227/'


### Forest age category
######

# Non-mangrove, non-planted natural forest age category tiles
pattern_age_cat_natrl_forest = 'forest_age_category_natural_forest'
age_cat_natrl_forest_dir = 's3://gfw2-data/climate/carbon_model/forest_age_category_natural_forest/20190225/'


### Annual biomass gain rates
######

# Annual aboveground biomass gain rate for mangroves
pattern_annual_gain_AGB_mangrove = 'annual_gain_rate_AGB_t_ha_mangrove'
annual_gain_AGB_mangrove_dir = 's3://gfw2-data/climate/carbon_model/annual_gain_rate_AGB_mangrove/20190225/'

# Annual belowground biomass gain rate for mangroves
pattern_annual_gain_BGB_mangrove = 'annual_gain_rate_BGB_t_ha_mangrove'
annual_gain_BGB_mangrove_dir = 's3://gfw2-data/climate/carbon_model/annual_gain_rate_BGB_mangrove/20190225/'

# Annual aboveground carbon gain rate for planted forests, with gain rates everywhere inside the plantation boundaries (includes mangrove pixels)
pattern_annual_gain_AGC_BGC_planted_forest_unmasked = 'annual_gain_rate_AGC_BGC_t_ha_planted_forest_unmasked'
annual_gain_AGC_BGC_planted_forest_unmasked_dir = 's3://gfw2-data/climate/carbon_model/annual_gain_rate_AGC_BGC_planted_forest_unmasked/20190207/'

# Annual aboveground biomass gain rate for planted forests where there are no mangroves (non-mangrove planted forests)
pattern_annual_gain_AGB_planted_forest_non_mangrove = 'annual_gain_rate_AGB_t_ha_planted_forest_non_mangrove'
annual_gain_AGB_planted_forest_non_mangrove_dir = 's3://gfw2-data/climate/carbon_model/annual_gain_rate_AGB_planted_forest_non_mangrove/20190227/'

# Annual belowground biomass gain rate for planted forests where there are no mangroves (non-mangrove planted forests)
pattern_annual_gain_BGB_planted_forest_non_mangrove = 'annual_gain_rate_BGB_t_ha_planted_forest_non_mangrove'
annual_gain_BGB_planted_forest_non_mangrove_dir = 's3://gfw2-data/climate/carbon_model/annual_gain_rate_BGB_planted_forest_non_mangrove/20190227/'

# Annual aboveground biomass gain rate for non-mangrove, non-planted natural forests
pattern_annual_gain_AGB_natrl_forest = 'annual_gain_rate_AGB_t_ha_natural_forest_non_mangrove_non_planted'
annual_gain_AGB_natrl_forest_dir = 's3://gfw2-data/climate/carbon_model/annual_gain_rate_AGB_natural_forest/20190227/'

# Annual belowground biomass gain rate for non-mangrove, non-planted natural forests
pattern_annual_gain_BGB_natrl_forest = 'annual_gain_rate_BGB_t_ha_natural_forest_non_mangrove_non_planted'
annual_gain_BGB_natrl_forest_dir = 's3://gfw2-data/climate/carbon_model/annual_gain_rate_BGB_natural_forest/20190227/'

# Annual aboveground gain rate for all forest types
pattern_annual_gain_combo = 'annual_gain_rate_AGB_BGB_t_ha_all_forest_types'
annual_gain_combo_dir = 's3://gfw2-data/climate/carbon_model/annual_gain_rate_all_forest_types/20181105/'

# Cumulative gain for all forest types
pattern_cumul_gain_combo = 'cumul_gain_AGC_BGC_t_ha_all_forest_types_2001_15'
cumul_gain_combo_dir = 's3://gfw2-data/climate/carbon_model/cumulative_gain_all_forest_types/20181105/'


### Cumulative carbon gain rates
######

# Cumulative aboveground gain for natural forests
pattern_cumul_gain_AGC_natrl_forest = 'cumul_gain_AGC_t_ha_natural_forest_2001_15'
cumul_gain_AGC_natrl_forest_dir = 's3://gfw2-data/climate/carbon_model/cumulative_gain_AGC_natural_forest/20181104/'

# Cumulative aboveground gain for mangroves
pattern_cumul_gain_AGC_mangrove = 'cumul_gain_AGC_t_ha_mangrove_2001_15'
cumul_gain_AGC_mangrove_dir = 's3://gfw2-data/climate/carbon_model/cumulative_gain_AGC_mangrove/20190227/'

# Cumulative belowground gain for natural forests
pattern_cumul_gain_BGC_natrl_forest = 'cumul_gain_BGC_t_ha_natural_forest_2001_15'
cumul_gain_BGC_natrl_forest_dir = 's3://gfw2-data/climate/carbon_model/cumulative_gain_BGC_natural_forest/20181104/'

# Cumulative belowground gain for mangroves
pattern_cumul_gain_BGC_mangrove = 'cumul_gain_BGC_t_ha_mangrove_2001_15'
cumul_gain_BGC_mangrove_dir = 's3://gfw2-data/climate/carbon_model/cumulative_gain_BGC_mangrove/20190227/'


### Non-biomass inputs to carbon pools
######

# FAO ecozones as boreal/temperate/tropical
pattern_fao_ecozone_raw = 'fao_ecozones_bor_tem_tro_20180619.zip'
fao_ecozone_raw_dir = 's3://gfw2-data/climate/carbon_model/inputs_for_carbon_pools/raw/{}'.format(pattern_fao_ecozone_raw)
pattern_fao_ecozone_processed = 'res_fao_ecozones_bor_tem_tro'
fao_ecozone_processed_dir = 's3://gfw2-data/climate/carbon_model/inputs_for_carbon_pools/processed/fao_ecozones_bor_tem_tro/'

# Precipitation
precip_raw_dir = 's3://gfw2-data/climate/carbon_model/inputs_for_carbon_pools/raw/add_30s_precip.tif'
pattern_precip = 'res_precip'
precip_processed_dir = 's3://gfw2-data/gfw2-data/climate/carbon_model/inputs_for_carbon_pools/processed/precip/'

# Soil C
soil_C_raw_dir = 's3://gfw2-data/climate/carbon_model/inputs_for_carbon_pools/raw/hwsd_oc_final.tif'
pattern_soil_C = 'soil_t_C_ha'
soil_C_processed_dir = 's3://gfw2-data/climate/carbon_model/inputs_for_carbon_pools/processed/soil/'

# Elevation
srtm_raw_dir = 's3://gfw2-data/analyses/srtm/'
pattern_srtm = 'res_srtm'
srtm_processed_dir = 's3://gfw2-data/climate/carbon_model/inputs_for_carbon_pools/processed/srtm/'


### Carbon pools
### NOTE: the patterns for the carbon pools must be set separately in carbon_pools/calc_carbon_pools.cpp
######

# Base directory for all carbon pools
base_carbon_pool_dir = 's3://gfw2-data/climate/carbon_model/carbon_pools/20180815/'

# NOTE: These must match the word before .tif in the outnames of calc_carbon_pools.cpp, e.g., "string outname_litter_total = tile_id + "_t_litter_C_ha_total.tif";"
pool_types = ['natrl', 'mangrove', 'total']

# Aboveground carbon
pattern_agc = 't_AGC_ha'
agc_dir = '{}/aboveground_C/'.format(base_carbon_pool_dir)

# Belowground carbon
pattern_bgc = 't_BGC_ha'
bgc_dir = '{}/belowground_C/'.format(base_carbon_pool_dir)

# Deadwood
pattern_deadwood = 't_deadwood_C_ha'
deadwood_dir = '{}/deadwood_C/'.format(base_carbon_pool_dir)

# Litter
pattern_litter = 't_litter_C_ha'
litter_dir = '{}/litter_C/'.format(base_carbon_pool_dir)

# Soil
pattern_soil_pool = 't_soil_C_ha'
soil_C_pool_dir = '{}/soil_C/'.format(base_carbon_pool_dir)

# All carbon pools combined
pattern_total_C = 't_total_C_ha'
total_C_dir = '{}/total_carbon/'.format(base_carbon_pool_dir)


### Gross emissions
######

pattern_gross_emissions = 'disturbance_model_noData_reclass'
gross_emissions_dir = 's3://gfw2-data/climate/carbon_model/output_emissions/20180828/disturbance_model_noData_removed/'


# Net emissions for all forest types and all carbon pools
pattern_net_flux = 'net_flux_t_CO2_ha_all_forest_types_all_drivers_2001_15'
net_flux_dir = 's3://gfw2-data/climate/carbon_model/net_emissions_all_forest_types_all_drivers/20181107/'

# Tile summary spreadsheets
tile_stats = 'tile_stats_{}.csv'.format(uu.date)
tile_stats_dir = 's3://gfw2-data/climate/carbon_model/tile_stats/'

# The area of each pixel in m^2
pattern_pixel_area = 'hanson_2013_area'
pixel_area_dir = 's3://gfw2-data/analyses/area_28m/'

# Locations of tsvs from model output
tsv_output_dir = 's3://gfw2-data/climate/carbon_model/model_output_tsv/20181119/'

# Location of raw Hadoop output
hadoop_raw_dir = 'gfw2-data/climate/carbon_model/model_output_Hadoop/raw/'

# Location of processed (cumsummed) Hadoop output
hadoop_processed_s3_dir = 'gfw2-data/climate/carbon_model/model_output_Hadoop/processed/'
hadoop_processed_local_dir = 'C:\GIS\Carbon_model\model_output_Hadoop'

gadm_dir = 's3://gfw2-data/alerts-tsv/gis_source/'
gadm_zip = 'gadm_3_6_adm2_final.zip'
gadm_shp = 'gadm_3_6_adm2_final.shp'
gadm_iso = 'gadm_3_6_with_planted_forest_iso.shp'
gadm_path = os.path.join(gadm_dir, gadm_zip)
gadm_plant_1x1_index_dir = 's3://gfw2-data/climate/carbon_model/gadm_plantation_1x1_tile_index/'
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

