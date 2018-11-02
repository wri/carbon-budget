### This script calculates the cumulative above and belowground carbon gain in mangrove forest pixels from 2001-2015.
### It multiplies the annual biomass gain rate by the number of years of gain by the biomass-to-carbon conversion.

import multiprocessing
import utilities
import cumulative_gain_mangrove

### Need to update and install some packages on spot machine before running
### sudo pip install rasterio --upgrade

# mangrove_biomass_tile_list = utilities.tile_list(utilities.mangrove_biomass_dir)
# biomass_tile_list = ['20S_110E', '30S_110E'] # test tiles
mangrove_biomass_tile_list = ['10S_080W'] # test tiles
print mangrove_biomass_tile_list

# # For downloading all tiles in the input folders
# download_list = [utilities.annual_gain_AGB_mangrove_dir, utilities.annual_gain_BGB_mangrove_dir, utilities.gain_year_count_mangrove_dir]
#
# for input in download_list:
#     utilities.s3_folder_download('{}'.format(input), '.')

# For copying individual tiles to spot machine for testing
for tile in mangrove_biomass_tile_list:

    utilities.s3_file_download('{0}{1}_{2}.tif'.format(utilities.annual_gain_AGB_mangrove_dir, utilities.pattern_annual_gain_AGB_mangrove, tile), '.')           # annual AGB gain rate tiles
    utilities.s3_file_download('{0}{1}_{2}.tif'.format(utilities.annual_gain_BGB_mangrove_dir, utilities.pattern_annual_gain_BGB_mangrove, tile), '.')  # annual AGB gain rate tiles
    utilities.s3_file_download('{0}{1}_{2}.tif'.format(utilities.gain_year_count_mangrove_dir, utilities.pattern_gain_year_count_mangrove, tile), '.')        # number of years with gain tiles

# count = multiprocessing.cpu_count()
# pool = multiprocessing.Pool(count / 4)
# pool.map(cumulative_gain_mangrove.cumulative_gain, mangrove_biomass_tile_list)

# For single processor use
for tile in mangrove_biomass_tile_list:

    cumulative_gain_mangrove.cumulative_gain(tile)

