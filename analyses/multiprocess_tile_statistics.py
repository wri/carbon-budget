### This script calculates various statistics on all tiles in input folders and saves them to a txt.
### Users can input as many folders as they want for calculating statistics on each tile

import multiprocessing
import tile_statistics
import subprocess
import os
import glob
import sys
sys.path.append('../')
import constants_and_names
import universal_util

# The column names for the tile summary statistics.
# If the statistics calculations are changed in tile_statistics.py, the list here needs to be changed, too.
headers = ['tile_id', 'tile_name', 'pixel_count', 'mean', 'median', 'percentile10', 'percentile25',
           'percentile75', 'percentile90', 'min', 'max', 'sum']
header_no_brackets = ', '.join(headers)

# Creates the output text file with the column names
with open(constants_and_names.tile_stats, 'w+') as f:
    f.write(header_no_brackets  +'\r\n')
f.close()

# # Creates list of tiles to iterate through, for testing
# download_tile_list = ["00N_000E", "00N_050W"] # test tiles
# print download_tile_list
#
# # For copying individual tiles to spot machine for testing
# for tile in download_tile_list:
#     universal_util.s3_file_download('{0}{1}_{2}.tif'.format(constants_and_names.mangrove_biomass_2000_dir, constants_and_names.pattern_mangrove_biomass_2000, tile), '.')
#     universal_util.s3_file_download('{0}{1}_{2}.tif'.format(constants_and_names.natrl_forest_biomass_2000_dir, tile, constants_and_names.pattern_natrl_forest_biomass_2000), '.')
#     universal_util.s3_file_download('{0}{1}_{2}.tif'.format(constants_and_names.pixel_area_dir, constants_and_names.pattern_pixel_area, tile), '.')


# Pixel area tiles-- necessary for calculating sum of pixels for any set of tiles
universal_util.s3_folder_download(constants_and_names.pixel_area_dir, '.')

# For downloading all tiles in selected folders
download_list = [
                 # 's3://gfw2-data/climate/carbon_model/mangrove_biomass/processed/20181019/'
                 # , constants_and_names.natrl_forest_biomass_2000_dir
                 # , 's3://gfw2-data/climate/carbon_model/carbon_pools/20180815/carbon/'
                 # , 's3://gfw2-data/climate/carbon_model/carbon_pools/20180815/bgc/'
                 # , constants_and_names.annual_gain_combo_dir
                 # , constants_and_names.cumul_gain_AGC_natrl_forest_dir
                 # , constants_and_names.cumul_gain_AGC_mangrove_dir
                 # , constants_and_names.cumul_gain_BGC_natrl_forest_dir      # Not doing this one
                 # , constants_and_names.cumul_gain_BGC_mangrove_dir          # Not doing this one
                 # , constants_and_names.cumul_gain_combo_dir
                 # , constants_and_names.net_flux_dir
                 # , 's3://gfw2-data/climate/carbon_model/output_emissions/20180828/deforestation_model/'
                 # , 's3://gfw2-data/climate/carbon_model/output_emissions/20180828/disturbance_model_noData_removed/'
                 # , 's3://gfw2-data/climate/carbon_model/output_emissions/20180828/forestry_model/'
                 # , 's3://gfw2-data/climate/carbon_model/output_emissions/20180828/shiftingag_model/'
                 # , 's3://gfw2-data/climate/carbon_model/output_emissions/20180828/urbanization_model/'
                 # , 's3://gfw2-data/climate/carbon_model/output_emissions/20180828/wildfire_model/'
                 's3://gfw2-data/climate/carbon_model/carbon_pools/20180815/deadwood/'
                 , 's3://gfw2-data/climate/carbon_model/carbon_pools/20180815/litter/'
                 , 's3://gfw2-data/climate/carbon_model/carbon_pools/20180815/soil/'
                 , 's3://gfw2-data/climate/carbon_model/carbon_pools/20180815/total_carbon/'


]

# Iterates through each set of tiles and gets statistics of it
for input in download_list:
    universal_util.s3_folder_download(input, '.')

    # List of all the tiles on the spot machine to be summarized (excludes pixel area tiles and tiles created by gdal_calc
    # (in case this script was already run on this spot machine and created output from gdal_calc)
    tile_list = universal_util.tile_list_spot_machine(".", "*.tif")
    # from https://stackoverflow.com/questions/12666897/removing-an-item-from-list-matching-a-substring
    tile_list = [i for i in tile_list if not ('hanson_2013' in i or 'value_per_pixel' in i)]
    # tile_list = ['00N_000E_biomass.tif']
    # tile_list = download_tile_list
    print tile_list

    # For multiprocessor use.
    # This runs out of memory with 8 processors.
    count = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=5)
    pool.map(tile_statistics.create_tile_statistics, tile_list)
    # Added these in response to error12: Cannot allocate memory error.
    # This fix was mentioned here: of https://stackoverflow.com/questions/26717120/python-cannot-allocate-memory-using-multiprocessing-pool
    # Could also try this: https://stackoverflow.com/questions/42584525/python-multiprocessing-debugging-oserror-errno-12-cannot-allocate-memory
    pool.close()
    pool.join()

    # # For single processor use
    # for tile in tile_list:
    #     tile_statistics.create_tile_statistics(tile)

    # Even an m4.16xlarge spot machine can't handle all these sets of tiles, so this deletes each set of tiles after it is analyzed
    print "Deleting tiles..."
    for tile in tile_list:
        os.remove(tile)
        tile_short = tile[:-4]
        outname = '{0}_value_per_pixel.tif'.format(tile_short)
        os.remove(outname)
        print "  Tiles deleted"

    # Copies the text file to the location on s3 that the tiles are from
    cmd = ['aws', 's3', 'cp', constants_and_names.tile_stats, constants_and_names.tile_stats_dir]
    subprocess.check_call(cmd)