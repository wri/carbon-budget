### This script calculates the cumulative carbon gain in natural non-mangrove forest pixels from 2001-2015.
### It multiplies the annual biomass gain rate by the number of years of gain by the biomass-to-carbon conversion

import multiprocessing
import utilities
import cumulative_gain_all_forest_types

biomass_tile_list = utilities.tile_list(utilities.biomass_dir)
# biomass_tile_list = ['20S_110E', '30S_110E'] # test tiles
# biomass_tile_list = ['20S_110E'] # test tiles
print biomass_tile_list

# For downloading all tiles in the input folders
download_list = [utilities.cumul_gain_natrl_forest_dir, utilities.cumul_gain_mangrove_dir]

for input in download_list:
    utilities.s3_folder_download('{}'.format(input), '.')

# # For copying individual tiles to spot machine for testing
# for tile in biomass_tile_list:
#
#     utilities.s3_file_download('{0}{1}_{2}.tif'.format(utilities.cumul_gain_natrl_forest_dir, utilities.pattern_cumul_gain_natrl_forest, tile), '.')           # annual gain rate tiles for natural forests
#     utilities.s3_file_download('{0}{1}_{2}.tif'.format(utilities.cumul_gain_mangrove_dir, utilities.pattern_cumul_gain_mangrove, tile), '.')  # annual gain rate tiles for mangroves

count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(count / 4)
pool.map(cumulative_gain_all_forest_types.cumulative_gain, biomass_tile_list)

# # For single processor use
# for tile in biomass_tile_list:
#
#     cumulative_gain_all_forest_types.cumulative_gain(tile)

