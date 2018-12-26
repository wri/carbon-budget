### This script calculates various statistics on all tiles in input folders and saves them to a txt.
### Users can input as many folders as they want for calculating statistics on each tile

import multiprocessing
import tile_statistics
import subprocess
import sys
sys.path.append('../')
import constants_and_names
import universal_util

# # Creates list of tiles to iterate through, for testing
# download_tile_list = ["00N_000E", "00N_050W"] # test tiles
# print download_tile_list
#
# # For copying individual tiles to spot machine for testing
# for tile in download_tile_list:
#     universal_util.s3_file_download('{0}{1}_{2}.tif'.format(constants_and_names.mangrove_biomass_dir, constants_and_names.pattern_mangrove_biomass, tile), '.')
#     universal_util.s3_file_download('{0}{1}_{2}.tif'.format(constants_and_names.biomass_dir, tile, constants_and_names.pattern_biomass), '.')
#     universal_util.s3_file_download('{0}{1}_{2}.tif'.format(constants_and_names.pixel_area_dir, constants_and_names.pattern_pixel_area, tile), '.')

# For downloading all tiles in selected folders
download_list = [
                 constants_and_names.pixel_area_dir
                 ,constants_and_names.mangrove_biomass_dir
                 ,constants_and_names.biomass_dir
                 ,'s3://gfw2-data/climate/carbon_model/carbon_pools/20180815/carbon/'
                 ,'s3://gfw2-data/climate/carbon_model/carbon_pools/20180815/bgc/'
                 ,'s3://gfw2-data/climate/carbon_model/carbon_pools/20180815/deadwood/'
                 ,'s3://gfw2-data/climate/carbon_model/carbon_pools/20180815/litter/'
                 ,'s3://gfw2-data/climate/carbon_model/carbon_pools/20180815/soil/'
                 ,'s3://gfw2-data/climate/carbon_model/carbon_pools/20180815/total_carbon/'
                 ,'s3://gfw2-data/climate/carbon_model/output_emissions/20180828/deforestation_model/'
                 ,'s3://gfw2-data/climate/carbon_model/output_emissions/20180828/disturbance_model_noData_removed/'
                 ,'s3://gfw2-data/climate/carbon_model/output_emissions/20180828/forestry_model/'
                 ,'s3://gfw2-data/climate/carbon_model/output_emissions/20180828/shiftingag_model/'
                 ,'s3://gfw2-data/climate/carbon_model/output_emissions/20180828/urbanization_model/'
                 ,'s3://gfw2-data/climate/carbon_model/output_emissions/20180828/wildfire_model/'
                 ,constants_and_names.annual_gain_combo_dir
                 ,constants_and_names.cumul_gain_AGC_natrl_forest_dir
                 ,constants_and_names.cumul_gain_AGC_mangrove_dir
                 ,constants_and_names.cumul_gain_BGC_natrl_forest_dir
                 ,constants_and_names.cumul_gain_BGC_mangrove_dir
                 ,constants_and_names.cumul_gain_combo_dir
                 ,constants_and_names.net_flux_dir
]

for input in download_list:
    universal_util.s3_folder_download('{}'.format(input), '.')

# The column names for the tile summary statistics.
# If the statistics calculations are changed in tile_statistics.py, the list here needs to be changed, too.
headers = ['tile_id', 'tile_name', 'pixel_count', 'mean', 'median', 'percentile10', 'percentile25',
           'percentile75', 'percentile90', 'min', 'max', 'sum']
header_no_brackets = ', '.join(headers)

# List of all the tiles on the spot machine to be summarized (excludes pixel area tiles and tiles created by gdal_calc
# (in case this script was already run on this spot machine and created output from gdal_calc)
tile_list = universal_util.tile_list_spot_machine(".")
# from https://stackoverflow.com/questions/12666897/removing-an-item-from-list-matching-a-substring
tile_list = [i for i in tile_list if not ('hanson_2013' in i or 'value_per_pixel' in i)]
# tile_list = ['00N_000E_biomass.tif']
# tile_list = download_tile_list
print tile_list

# Creates the output text file with the column names
with open(constants_and_names.tile_stats, 'w+') as f:
    f.write(header_no_brackets  +'\r\n')
f.close()

# For multiprocessor use
count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(processes=5)
pool.map(tile_statistics.create_tile_statistics, tile_list)

# # For single processor use
# for tile in tile_list:
#     tile_statistics.create_tile_statistics(tile)

# Copies the text file to the location on s3 that the tiles are from
cmd = ['aws', 's3', 'cp', constants_and_names.tile_stats, constants_and_names.tile_stats_dir]
subprocess.check_call(cmd)