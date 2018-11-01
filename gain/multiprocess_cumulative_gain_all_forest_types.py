### This script calculates the cumulative carbon gain in natural non-mangrove forest pixels from 2001-2015.
### It multiplies the annual biomass gain rate by the number of years of gain by the biomass-to-carbon conversion

import multiprocessing
import utilities
import cumulative_gain_natrl_forest

### Need to update and install some packages on spot machine before running
### sudo pip install rasterio --upgrade
### sudo pip install xlrd

biomass_tile_list = utilities.tile_list(utilities.biomass_dir)
# biomass_tile_list = ['20S_110E', '30S_110E'] # test tiles
# biomass_tile_list = ['20S_110E'] # test tiles
print biomass_tile_list

# For downloading all tiles in the input folders
download_list = [utilities.annual_gain_natrl_forest_dir, utilities.gain_year_count_natrl_forest_dir]

for input in download_list:
    utilities.s3_folder_download('{}'.format(input), '.')

# # For copying individual tiles to spot machine for testing
# for tile in biomass_tile_list:
#
#     utilities.s3_file_download('{0}{1}_{2}.tif'.format(utilities.annual_gain_natrl_forest_dir, utilities.pattern_annual_gain_natrl_forest, tile), '.')           # annual gain rate tiles
#     utilities.s3_file_download('{0}{1}_{2}.tif'.format(utilities.gain_year_count_natrl_forest_dir, utilities.pattern_gain_year_count_natrl_forest, tile), '.')        # number of years with gain tiles

count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(count / 4)
pool.map(cumulative_gain_natrl_forest.cumulative_gain, biomass_tile_list)

# # For single processor use
# for tile in biomass_tile_list:
#
#     cumulative_gain_natrl_forest.cumulative_gain(tile)

