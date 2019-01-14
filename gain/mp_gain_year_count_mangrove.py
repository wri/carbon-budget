### Creates tiles in which each mangrove pixel is the number of years that trees are believed to have been growing there between 2001 and 2015.
### It is based on the annual Hansen loss data and the 2000-2012 Hansen gain data (as well as the 2000 tree cover density data).
### First it calculates rasters of gain years for mangrove pixels that had loss only, gain only, neither loss nor gain, and both loss and gain.
### The gain years for each of these conditions are calculated according to rules that are found in the function called by the multiprocessor command.
### At this point, those rules are the same as for non-mangrove natural forests, except that no change pixels don't use tcd as a factor.
### Then it combines those four rasters into a single gain year raster for each tile.
### This is one of the mangrove inputs for the carbon gain model.
### If different input rasters for loss (e.g., 2001-2017) and gain (e.g., 2000-2018) are used, the constants in create_gain_year_count_mangrove.py must be changed.

import multiprocessing
import utilities
import gain_year_count_mangrove
import sys
sys.path.append('../')
import constants_and_names as cn


# Lists the mangrove biomass tiles instead of the general tree biomass tiles because
# there are many fewer mangrove biomass tiles (88 vs 315)
mangrove_biomass_tile_list = utilities.tile_list(cn.mangrove_biomass_2000_dir)
# mangrove_biomass_tile_list = ['20S_110E', '30S_110E'] # test tiles
# mangrove_biomass_tile_list = ['10N_080W'] # test tiles
print mangrove_biomass_tile_list

# For downloading all tiles in the input folders
download_list = [cn.loss_dir, cn.gain_dir, cn.mangrove_biomass_2000_dir]

for input in download_list:
    utilities.s3_folder_download('{}'.format(input), '.')

# # For copying individual tiles to s3 for testing
# for tile in mangrove_biomass_tile_list:
#
#     utilities.s3_file_download('{0}{1}.tif'.format(cn.loss_dir, tile), '.')
#     utilities.s3_file_download('{0}{1}_{2}.tif'.format(cn.gain_dir, tile, cn.pattern_gain), '.')
#     utilities.s3_file_download('{0}{1}_{2}.tif'.format(cn.mangrove_biomass_2000_dir, tile, cn.pattern_mangrove_biomass_2000), '.')

count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(4)
pool.map(gain_year_count_mangrove.create_gain_year_count, mangrove_biomass_tile_list)

# # For single processor use
# for tile in mangrove_biomass_tile_list:
#
#     gain_year_count_mangrove.create_gain_year_count(tile)