### This script calculates the cumulative carbon gain in natural non-mangrove forest pixels from 2001-2015.
### It multiplies the annual biomass gain rate by the number of years of gain by the biomass-to-carbon conversion

import multiprocessing
import utilities
import cumulative_gain_natrl_forest
import sys
sys.path.append('../')
import constants_and_names as cn

biomass_tile_list = utilities.tile_list(cn.natrl_forest_biomass_2000_dir)
# biomass_tile_list = ['20S_110E', '30S_110E'] # test tiles
# biomass_tile_list = ['20S_110E'] # test tiles
print biomass_tile_list

# For downloading all tiles in the input folders
download_list = [cn.annual_gain_AGB_natrl_forest_dir, cn.annual_gain_BGB_natrl_forest_dir, cn.gain_year_count_natrl_forest_dir]

for input in download_list:
    utilities.s3_folder_download(input, '.')

# # For copying individual tiles to spot machine for testing
# for tile in biomass_tile_list:
#
#     utilities.s3_file_download('{0}{1}_{2}.tif'.format(cn.annual_gain_AGB_natrl_forest_dir, tile, cn.pattern_annual_gain_AGB_natrl_forest), '.')  # annual AGB gain rate tiles
#     utilities.s3_file_download('{0}{1}_{2}.tif'.format(cn.annual_gain_BGB_natrl_forest_dir, tile, cn.pattern_annual_gain_BGB_natrl_forest), '.')  # annual AGB gain rate tiles
#     utilities.s3_file_download('{0}{1}_{2}.tif'.format(cn.gain_year_count_natrl_forest_dir, tile, cn.pattern_gain_year_count_natrl_forest), '.')  # number of years with gain tiles

count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(count / 4)
pool.map(cumulative_gain_natrl_forest.cumulative_gain, biomass_tile_list)

# # For single processor use
# for tile in biomass_tile_list:
#
#     cumulative_gain_natrl_forest.cumulative_gain(tile)

