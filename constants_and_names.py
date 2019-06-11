import os

########     ########
##### Constants #####
########     ########

# Number of years of tree cover loss. If input loss raster is changed, this must be changed, too.
loss_years = 15

# Number of years in tree cover gain. If input gain raster is changed, this must be changed, too.
gain_years = 12

# Biomass to carbon ratio for aboveground, belowground, and deadwood in non-mangrove forests (planted and non-planted)
biomass_to_c_non_mangrove = 0.5

# Biomass to carbon ratio for litter in non-mangrove forests (planted and non-planted).
# From IPCC guidelines chapter 2, pdf page 23.
biomass_to_c_non_mangrove_litter = 0.37

# Biomass to carbon ratio for mangroves (IPCC wetlands supplement table 4.2)
biomass_to_c_mangrove = 0.45

# Carbon to CO2 ratio
# Needs the decimal places in order to be cast as a float
c_to_co2 = 44.0/12.0

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

# The size of a Hansen loss pixel, in decimal degrees
Hansen_res = 0.00025

# 10km res
aggreg_res = 0.096342599

# m2 per hectare
m2_per_ha = 100 * 100


##########                  ##########
##### File names and directories #####
##########                  ##########

# Directory for the climate model files
base_dir = 's3://gfw2-data/climate/carbon_model/'

### Biomass tiles
######

## Biomass in 2000
# Woods Hole aboveground biomass 2000 version 4 tiles
pattern_WHRC_biomass_2000_unmasked = "t_aboveground_biomass_ha_2000"
WHRC_biomass_2000_unmasked_dir = 's3://gfw2-data/climate/WHRC_biomass/WHRC_V4/Processed/'

# Woods Hole aboveground biomass 2000 version 4 tiles without mangrove or planted forest pixels
pattern_WHRC_biomass_2000_non_mang_non_planted = "t_aboveground_biomass_ha_2000_non_mangrove_non_planted"
WHRC_biomass_2000_non_mang_non_planted_dir = os.path.join(base_dir, 'biomass_non_mangrove_non_planted/20190225/')

# Raw Lola Fatoyinbo aboveground mangrove biomass in the year 2000 rasters
mangrove_biomass_raw_dir = os.path.join(base_dir, 'mangrove_biomass/raw_from_Nathan_Thomas_20190215/')
mangrove_biomass_raw_file = 'MaskedSRTMCountriesAGB_V2_Tiff.zip'

# Processed mangrove aboveground biomass in the year 2000
pattern_mangrove_biomass_2000 = 'mangrove_agb_t_ha_2000'
mangrove_biomass_2000_dir = os.path.join(base_dir, 'mangrove_biomass/processed/20190220/')


### Miscellaneous inputs
######

# Spreadsheet with annual gain rates
gain_spreadsheet_dir = os.path.join(base_dir)
gain_spreadsheet = 'gain_rate_continent_ecozone_age_20190116.xlsx'

# Annual Hansen loss tiles (2001-2015)
pattern_loss_pre_2000_plant_masked = 'loss_pre_2000_plant_masked'
loss_dir = 's3://gfw2-data/forest_change/hansen_2015/Loss_tiles/'

# Hansen gain tiles (2001-2012)
pattern_gain = 'Hansen_GFC2015_gain'
gain_dir = 's3://gfw2-data/forest_change/tree_cover_gain/gaindata_2012/'

# Tree cover density 2000 tiles
pattern_tcd = 'Hansen_GFC2014_treecover2000'
tcd_dir = 's3://gfw2-data/forest_cover/2000_treecover/'

# Intact forest landscape 2000 tiles
pattern_ifl = 'res_ifl_2000'
ifl_dir = os.path.join(base_dir, 'other_emissions_inputs/ifl_2000/')

# Processed FAO ecozone shapefile
cont_ecozone_shp = 'fao_ecozones_fra_2000_continents_assigned_dissolved_FINAL_20180906.zip'

# Directory and names for the continent-ecozone tiles, raw and processed
pattern_cont_eco_raw = 'fao_ecozones_continents_raw'
pattern_cont_eco_processed = 'fao_ecozones_continents_processed'
cont_eco_s3_zip = os.path.join(base_dir, 'fao_ecozones/fao_ecozones_fra_2000_continents_assigned_dissolved_FINAL_20180906.zip')
cont_eco_zip = 'fao_ecozones_fra_2000_continents_assigned_dissolved_FINAL_20180906.zip'
cont_eco_raw_dir = os.path.join(base_dir, 'fao_ecozones/ecozone_continent/20190116/raw/')
cont_eco_dir = os.path.join(base_dir, 'fao_ecozones/ecozone_continent/20190116/processed/')

# Plantation type: palm oil (code=1), wood fiber (code=2), and other (code=3)
pattern_planted_forest_type_unmasked = 'plantation_type_oilpalm_woodfiber_other_unmasked'
planted_forest_type_unmasked_dir = os.path.join(base_dir, 'other_emissions_inputs/plantation_type/20190424/')

peat_unprocessed_dir = os.path.join(base_dir, 'other_emissions_inputs/peatlands/raw/')
cifor_peat_file = 'cifor_peat_mask.tif'
jukka_peat_zip = 'Jukka_peatland.zip'
jukka_peat_shp = 'peatland_drainage_proj.shp'
soilgrids250_peat_file = 'TAXNWRB_250m_ll.tif'   #Keys 61 to 65 from https://files.isric.org/soilgrids/data/recent/TAXNWRB_250m_ll.tif, metadata: https://files.isric.org/soilgrids/data/recent/TAXNWRB_250m_ll.xml
pattern_peat_mask = 'peat_mask_processed'
peat_mask_dir = os.path.join(base_dir, 'other_emissions_inputs/peatlands/processed/20190429/')

climate_zone_raw_dir = os.path.join(base_dir, 'other_emissions_inputs/climate_zone/raw/')
climate_zone_raw = 'climate_zone.tif'
pattern_climate_zone = 'climate_zone_processed'
climate_zone_processed_dir = os.path.join(base_dir, 'other_emissions_inputs/climate_zone/processed/20190430/')

plant_pre_2000_raw_dir = os.path.join(base_dir, 'other_emissions_inputs/IDN_MYS_plantation_pre_2000/raw/')
pattern_plant_pre_2000_raw = 'plant_est_2000_or_earlier'
pattern_plant_pre_2000 = 'plantation_2000_or_earlier_processed'
plant_pre_2000_processed_dir = os.path.join(base_dir, 'other_emissions_inputs/IDN_MYS_plantation_pre_2000/processed/20190430/')

drivers_raw_dir = os.path.join(base_dir, 'other_emissions_inputs/tree_cover_loss_drivers/raw/')
pattern_drivers_raw = 'Goode_FinalClassification_19_05pcnt_prj'
pattern_drivers = 'tree_cover_loss_driver_processed'
drivers_processed_dir = os.path.join(base_dir, 'other_emissions_inputs/tree_cover_loss_drivers/processed/20190430/')

pattern_burn_year = "burnyear"
burn_year_dir = os.path.join(base_dir, 'other_emissions_inputs/burn_year/burn_year_with_Hansen_loss/')


### Plantation processing
######

gadm_dir = 's3://gfw2-data/alerts-tsv/gis_source/'
gadm_zip = 'gadm_3_6_adm2_final.zip'
gadm_shp = 'gadm_3_6_adm2_final.shp'
gadm_iso = 'gadm_3_6_with_planted_forest_iso.shp'
gadm_path = os.path.join(gadm_dir, gadm_zip)
gadm_plant_1x1_index_dir = os.path.join(base_dir, 'gadm_plantation_1x1_tile_index/')
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


### Number of gain years (gain year count)
######

# Number of gain years for mangroves
pattern_gain_year_count_mangrove = 'gain_year_count_mangrove'
gain_year_count_mangrove_dir = os.path.join(base_dir, 'gain_year_count_mangrove/20190225/')

# Number of gain years for non-mangrove planted forests
pattern_gain_year_count_planted_forest_non_mangrove = 'gain_year_count_planted_forest_non_mangrove'
gain_year_count_planted_forest_non_mangrove_dir = os.path.join(base_dir, 'gain_year_count_planted_forest_non_mangrove/20190227/')

# Number of gain years for non-mangrove, non-planted natural forests
pattern_gain_year_count_natrl_forest = 'gain_year_count_natural_forest_non_mangrove_non_planted'
gain_year_count_natrl_forest_dir = os.path.join(base_dir, 'gain_year_count_natural_forest/20190227/')


### Forest age category
######

# Non-mangrove, non-planted natural forest age category tiles
pattern_age_cat_natrl_forest = 'forest_age_category_natural_forest'
age_cat_natrl_forest_dir = os.path.join(base_dir, 'forest_age_category_natural_forest/20190225/')


### Annual biomass gain rates
######

# Annual aboveground biomass gain rate for mangroves
pattern_annual_gain_AGB_mangrove = 'annual_gain_rate_AGB_t_ha_mangrove'
annual_gain_AGB_mangrove_dir = os.path.join(base_dir, 'annual_gain_rate_AGB_mangrove/20190225/')

# Annual belowground biomass gain rate for mangroves
pattern_annual_gain_BGB_mangrove = 'annual_gain_rate_BGB_t_ha_mangrove'
annual_gain_BGB_mangrove_dir = os.path.join(base_dir, 'annual_gain_rate_BGB_mangrove/20190225/')

# Annual aboveground carbon gain rate for planted forests, with gain rates everywhere inside the plantation boundaries (includes mangrove pixels)
pattern_annual_gain_AGC_BGC_planted_forest_unmasked = 'annual_gain_rate_AGC_BGC_t_ha_planted_forest_unmasked'
annual_gain_AGC_BGC_planted_forest_unmasked_dir = os.path.join(base_dir, 'annual_gain_rate_AGC_BGC_planted_forest_unmasked/20190207/')

# Annual aboveground biomass gain rate for planted forests where there are no mangroves (non-mangrove planted forests)
pattern_annual_gain_AGB_planted_forest_non_mangrove = 'annual_gain_rate_AGB_t_ha_planted_forest_non_mangrove'
annual_gain_AGB_planted_forest_non_mangrove_dir = os.path.join(base_dir, 'annual_gain_rate_AGB_planted_forest_non_mangrove/20190227/')

# Annual belowground biomass gain rate for planted forests where there are no mangroves (non-mangrove planted forests)
pattern_annual_gain_BGB_planted_forest_non_mangrove = 'annual_gain_rate_BGB_t_ha_planted_forest_non_mangrove'
annual_gain_BGB_planted_forest_non_mangrove_dir = os.path.join(base_dir, 'annual_gain_rate_BGB_planted_forest_non_mangrove/20190227/')

# Annual aboveground biomass gain rate for non-mangrove, non-planted natural forests
pattern_annual_gain_AGB_natrl_forest = 'annual_gain_rate_AGB_t_ha_natural_forest_non_mangrove_non_planted'
annual_gain_AGB_natrl_forest_dir = os.path.join(base_dir, 'annual_gain_rate_AGB_natural_forest/20190227/')

# Annual belowground biomass gain rate for non-mangrove, non-planted natural forests
pattern_annual_gain_BGB_natrl_forest = 'annual_gain_rate_BGB_t_ha_natural_forest_non_mangrove_non_planted'
annual_gain_BGB_natrl_forest_dir = os.path.join(base_dir, 'annual_gain_rate_BGB_natural_forest/20190227/')

# Annual aboveground gain rate for all forest types
pattern_annual_gain_combo = 'annual_gain_rate_AGB_BGB_t_ha_all_forest_types'
annual_gain_combo_dir = os.path.join(base_dir, 'annual_gain_rate_all_forest_types/20190228/')


### Cumulative carbon gain rates
######

# Cumulative aboveground gain for mangroves
pattern_cumul_gain_AGC_mangrove = 'cumul_gain_AGC_t_ha_mangrove_2001_15'
cumul_gain_AGC_mangrove_dir = os.path.join(base_dir, 'cumulative_gain_AGC_mangrove/20190227/')

# Cumulative belowground gain for mangroves
pattern_cumul_gain_BGC_mangrove = 'cumul_gain_BGC_t_ha_mangrove_2001_15'
cumul_gain_BGC_mangrove_dir = os.path.join(base_dir, 'cumulative_gain_BGC_mangrove/20190227/')

# Cumulative aboveground gain for non-mangrove planted natural forests
pattern_cumul_gain_AGC_planted_forest_non_mangrove = 'cumul_gain_AGC_t_ha_planted_forest_non_mangrove_2001_15'
cumul_gain_AGC_planted_forest_non_mangrove_dir = os.path.join(base_dir, 'cumulative_gain_AGC_planted_forest_non_mangrove/20190227/')

# Cumulative belowground gain for non-mangrove planted natural forests
pattern_cumul_gain_BGC_planted_forest_non_mangrove = 'cumul_gain_BGC_t_ha_planted_forest_non_mangrove_2001_15'
cumul_gain_BGC_planted_forest_non_mangrove_dir = os.path.join(base_dir, 'cumulative_gain_BGC_planted_forest_non_mangrove/20190227/')

# Cumulative aboveground gain for non-mangrove, non-planted natural forests
pattern_cumul_gain_AGC_natrl_forest = 'cumul_gain_AGC_t_ha_natural_forest_non_mangrove_non_planted_2001_15'
cumul_gain_AGC_natrl_forest_dir = os.path.join(base_dir, 'cumulative_gain_AGC_natural_forest/20190228/')

# Cumulative belowground gain for non-mangrove, non-planted natural forests
pattern_cumul_gain_BGC_natrl_forest = 'cumul_gain_BGC_t_ha_natural_forest_non_mangrove_non_planted_2001_15'
cumul_gain_BGC_natrl_forest_dir = os.path.join(base_dir, 'cumulative_gain_BGC_natural_forest/20190228/')

# Cumulative gain for all forest types
pattern_cumul_gain_combo = 'cumul_gain_AGC_BGC_t_ha_all_forest_types_2001_15'
cumul_gain_combo_dir = os.path.join(base_dir, 'cumulative_gain_all_forest_types/20190228/')


### Non-biomass inputs to carbon pools
######

# FAO ecozones as boreal/temperate/tropical
pattern_fao_ecozone_raw = 'fao_ecozones_bor_tem_tro_20180619.zip'
fao_ecozone_raw_dir = os.path.join(base_dir, 'inputs_for_carbon_pools/raw/{}'.format(pattern_fao_ecozone_raw))
pattern_bor_tem_trop_intermediate = 'fao_ecozones_bor_tem_tro_intermediate'
pattern_bor_tem_trop_processed = 'fao_ecozones_bor_tem_tro_processed'
bor_tem_trop_processed_dir = os.path.join(base_dir, 'inputs_for_carbon_pools/processed/fao_ecozones_bor_tem_tro/20190418/')

# Precipitation
precip_raw_dir = os.path.join(base_dir, 'inputs_for_carbon_pools/raw/add_30s_precip.tif')
pattern_precip = 'precip_mm_annual'
precip_processed_dir = os.path.join(base_dir, 'inputs_for_carbon_pools/processed/precip/20190418/')

# Elevation
srtm_raw_dir = os.path.join(base_dir, 'inputs_for_carbon_pools/raw/elevation/')
pattern_elevation = 'elevation'
elevation_processed_dir = os.path.join(base_dir, 'inputs_for_carbon_pools/processed/elevation/20190418/')


### Carbon pools
######

# Base directory for all carbon pools
base_carbon_pool_dir = os.path.join(base_dir, 'carbon_pools/')

# Aboveground carbon in the year of emission for all forest types in loss pixels
pattern_AGC_emis_year = "t_AGC_ha_emis_year"
AGC_emis_year_dir = '{0}aboveground_carbon/loss_pixels/20190418/'.format(base_carbon_pool_dir)

# Aboveground carbon for the full biomass 2000 (mangrove and non-mangrove) extent based on 2000 stocks
pattern_AGC_2000 = "t_AGC_ha_2000"
AGC_2000_dir = '{0}aboveground_carbon/extent_2000/20190531/'.format(base_carbon_pool_dir)

# Belowground carbon in loss pixels
pattern_BGC_emis_year = 't_BGC_ha_emis_year'
BGC_emis_year_dir = '{}belowground_carbon/loss_pixels/20190430/'.format(base_carbon_pool_dir)

# Belowground carbon for the full biomass 2000 (mangrove and non-mangrove) extent based on 2000 stocks
pattern_BGC_2000 = "t_BGC_ha_2000"
BGC_2000_dir = '{0}belowground_carbon/extent_2000/20190531/'.format(base_carbon_pool_dir)

# Deadwood in loss pixels
pattern_deadwood_emis_year_2000 = 't_deadwood_C_ha_emis_year_2000'
deadwood_emis_year_2000_dir = '{}deadwood_carbon/loss_pixels/20190430/'.format(base_carbon_pool_dir)

# Deadwood carbon for the full biomass 2000 (mangrove and non-mangrove) extent based on 2000 stocks
pattern_deadwood_2000 = "t_deadwood_C_ha_2000"
deadwood_2000_dir = '{0}deadwood_carbon/extent_2000/20190531/'.format(base_carbon_pool_dir)

# Litter in loss pixels
pattern_litter_emis_year_2000 = 't_litter_C_ha_emis_year_2000'
litter_emis_year_2000_dir = '{}litter_carbon/loss_pixels/20190430/'.format(base_carbon_pool_dir)

# Litter carbon for the full biomass 2000 (mangrove and non-mangrove) extent based on 2000 stocks
pattern_litter_2000 = "t_litter_C_ha_2000"
litter_2000_dir = '{0}litter_carbon/extent_2000/20190531/'.format(base_carbon_pool_dir)

# Raw mangrove soil C
mangrove_soil_C_dir = os.path.join(base_dir, 'carbon_pools/soil_carbon/raw/')
pattern_mangrove_soil_C = 'Mangroves_SOCS_0_100cm_30m.zip'
# Raw mineral soil C
mineral_soil_C_dir = os.path.join(base_dir, 'carbon_pools/soil_carbon/raw/')
pattern_mineral_soil_C = 'OCSTHA_M_30cm_250m_ll.tif'

# Soil C full extent (all soil pixels, with mangrove soil C in Giri mangrove extent getting priority over mineral soil C)
pattern_soil_C_full_extent_2000 = 't_soil_C_ha_full_extent_2000'
soil_C_full_extent_2000_dir = '{}soil_carbon/intermediate_full_extent/20190419/'.format(base_carbon_pool_dir)

# Soil C in loss pixels
pattern_soil_C_emis_year_2000 = 't_soil_C_ha_emis_year_2000'
soil_C_emis_year_2000_dir = '{}soil_carbon/loss_pixels/20190430/'.format(base_carbon_pool_dir)

# All carbon pools combined in loss pixels, with emitted values
pattern_total_C_emis_year = 't_total_C_ha_emis_year'
total_C_emis_year_dir = '{}total_carbon/loss_pixels/20190430/'.format(base_carbon_pool_dir)

# Total carbon (all carbon pools combined) for the full biomass 2000 (mangrove and non-mangrove) extent based on 2000 stocks
pattern_total_C_2000 = "t_total_C_ha_2000"
total_C_2000_dir = '{0}total_carbon/extent_2000/20190602/'.format(base_carbon_pool_dir)


### Gross emissions
######

pattern_gross_emis_commod = 'commodity_t_CO2_ha_gross_emis_year'
gross_emis_commod_dir = '{}gross_emissions/commodities/20190523/'.format(base_dir)

pattern_gross_emis_forestry = 'forestry_t_CO2_ha_gross_emis_year'
gross_emis_forestry_dir = '{}gross_emissions/forestry/20190523/'.format(base_dir)

pattern_gross_emis_shifting_ag = 'shifting_ag_t_CO2_ha_gross_emis_year'
gross_emis_shifting_ag_dir = '{}gross_emissions/shifting_ag/20190523/'.format(base_dir)

pattern_gross_emis_urban = 'urbanization_t_CO2_ha_gross_emis_year'
gross_emis_urban_dir = '{}gross_emissions/urbanization/20190523/'.format(base_dir)

pattern_gross_emis_wildfire = 'wildfire_t_CO2_ha_gross_emis_year'
gross_emis_wildfire_dir = '{}gross_emissions/wildfire/20190523/'.format(base_dir)

pattern_gross_emis_no_driver = 'no_driver_t_CO2_ha_gross_emis_year'
gross_emis_no_driver_dir = '{}gross_emissions/no_driver/20190523/'.format(base_dir)

pattern_gross_emis_all_drivers = 'all_drivers_t_CO2_ha_gross_emis_year'
gross_emis_all_drivers_dir = '{}gross_emissions/all_drivers/20190523/'.format(base_dir)

pattern_gross_emis_nodes = 'decision_tree_nodes_gross_emis'
gross_emis_nodes_dir = '{}gross_emissions/decision_tree_nodes/20190523/'.format(base_dir)


### Net emissions
######

# Net emissions for all forest types and all carbon pools
pattern_net_flux = 'net_flux_t_CO2_ha_2001_15'
net_flux_dir = os.path.join(base_dir, 'net_flux_all_forest_types_all_drivers/20190524/')



### 10x10 km aggregation tiles for mapping
#####

pattern_annual_gain_combo_aggreg = '{}_10km'.format(pattern_annual_gain_combo)
pattern_cumul_gain_combo_aggreg = '{}_10km'.format(pattern_cumul_gain_combo)
pattern_gross_emis_all_drivers_aggreg = '{}_10km'.format(pattern_gross_emis_all_drivers)
pattern_net_flux_aggreg = '{}_10km'.format(pattern_net_flux)
output_aggreg_dir = '{}10km_output_aggregation/20190610/'.format(base_dir)


# Tile summary spreadsheets
tile_stats_pattern = 'tile_stats.csv'
tile_stats_dir = os.path.join(base_dir, 'tile_stats/')

# The area of each pixel in m^2
pattern_pixel_area = 'hanson_2013_area'
pixel_area_dir = 's3://gfw2-data/analyses/area_28m/'

# Locations of tsvs from model output
tsv_output_dir = os.path.join(base_dir, 'model_output_tsv/20181119/')

# Location of raw Hadoop output
hadoop_raw_dir = 'gfw2-data/climate/carbon_model/model_output_Hadoop/raw/'

# Location of processed (cumsummed) Hadoop output
hadoop_processed_s3_dir = 'gfw2-data/climate/carbon_model/model_output_Hadoop/processed/'
hadoop_processed_local_dir = 'C:\GIS\Carbon_model\model_output_Hadoop'

