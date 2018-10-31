### Creates tiles in which each natural non-mangrove forest pixel is the number of years that trees are believed to have been growing there between 2001 and 2015.
### It is based on the annual Hansen loss data and the 2000-2012 Hansen gain data (as well as the 2000 tree cover density data).
### First it calculates rasters of gain years for natural forest pixels that had loss only, gain only, neither loss nor gain, and both loss and gain.
### The gain years for each of these conditions are calculated according to rules that are found in the function called by the multiprocessor command.
### Then it combines those four rasters into a single gain year raster for each tile.
### This is one of the natural forest inputs for the carbon gain model.
### If different input rasters for loss (e.g., 2001-2017) and gain (e.g., 2000-2018) are used, the constants in create_gain_year_count_natrl_forest.py must be changed.

import multiprocessing
import utilities
import gain_year_count_natrl_forest

# The list of tiles to iterate through
biomass_tile_list = utilities.tile_list(utilities.biomass_dir)
# biomass_tile_list = ["00N_000E", "00N_050W", "00N_060W", "00N_010E", "00N_020E", "00N_030E", "00N_040E", "10N_000E", "10N_010E", "10N_010W", "10N_020E", "10N_020W"] # test tiles
# biomass_tile_list = ['10N_080W'] # test tile
print biomass_tile_list

# # For downloading all tiles in the folders
# utilities.s3_folder_download('{}'.format(utilities.loss_dir), '.')
# utilities.s3_folder_download('{}'.format(utilities.gain_dir), '.')
# utilities.s3_folder_download('{}'.format(utilities.tcd_dir), '.')
# utilities.s3_folder_download('{}'.format(utilities.mangrove_biomass_dir), '.')

# # For copying individual tiles to s3 for testing
# for tile in biomass_tile_list:
#
#     utilities.s3_file_download('{0}{1}.tif'.format(utilities.loss_dir, tile), '.')
#     utilities.s3_file_download('{0}{1}_{2}.tif'.format(utilities.gain_dir, utilities.pattern_gain, tile), '.')
#     utilities.s3_file_download('{0}{1}_{2}.tif'.format(utilities.tcd_dir, utilities.pattern_tcd, tile), '.')
#     utilities.s3_file_download('{0}{1}_{2}.tif'.format(utilities.mangrove_biomass_dir, utilities.pattern_mangrove_biomass, tile), '.')

count = multiprocessing.cpu_count()
pool = multiprocessing.Pool()
pool.map(gain_year_count_natrl_forest.create_gain_year_count, biomass_tile_list)

# # For single processor use
# for tile in biomass_tile_list:
#
#     gain_year_count_natrl_forest.create_gain_year_count(tile)