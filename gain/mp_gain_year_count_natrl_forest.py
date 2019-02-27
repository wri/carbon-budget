### Creates tiles in which each natural non-mangrove non-planted forest biomass pixel is the number of years that trees are believed to have been growing there between 2001 and 2015.
### It is based on the annual Hansen loss data and the 2000-2012 Hansen gain data (as well as the 2000 tree cover density data).
### First it calculates rasters of gain years for non-mangrove non-planted forest biomass pixels that had loss only, gain only, neither loss nor gain, and both loss and gain.
### The gain years for each of these conditions are calculated according to rules that are found in the function called by the multiprocessor command.
### Then it combines those four rasters into a single gain year raster for each tile.
### Only the merged raster is used later in the model; the 4 intermediates are saved just for checking.
### If different input rasters for loss (e.g., 2001-2017) and gain (e.g., 2000-2018) are used, the constants in create_gain_year_count_natrl_forest.py must be changed.

import multiprocessing
import utilities
import gain_year_count_natrl_forest
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# The list of tiles to iterate through
biomass_tile_list = uu.tile_list(cn.WHRC_biomass_2000_non_mang_non_planted_dir)
# biomass_tile_list = ["00N_000E", "00N_050W", "00N_060W", "00N_010E", "00N_020E", "00N_030E", "00N_040E", "10N_000E", "10N_010E", "10N_010W", "10N_020E", "10N_020W"] # test tiles
# biomass_tile_list = ['10N_080W'] # test tile
print biomass_tile_list
print "There are {} tiles to process".format(str(len(biomass_tile_list)))

# For downloading all tiles in the folders
download_list = [cn.loss_dir, cn.gain_dir, cn.tcd_dir, cn.WHRC_biomass_2000_non_mang_non_planted_dir]

for input in download_list:
    uu.s3_folder_download(input, '.')

# # For copying individual tiles to s3 for testing
# for tile in biomass_tile_list:
#
#     uu.s3_file_download('{0}{1}.tif'.format(cn.loss_dir, tile), '.')
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.gain_dir, tile, cn.pattern_gain), '.')
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.tcd_dir, tile, cn.pattern_tcd), '.')
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.WHRC_biomass_2000_non_mang_non_planted_dir, tile, cn.pattern_WHRC_biomass_2000_non_mang_non_planted), '.')

# Creates gain year count tiles using only pixels that had only loss
count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(count/3)
pool.map(gain_year_count_natrl_forest.create_gain_year_count_loss_only, biomass_tile_list)

# Creates gain year count tiles using only pixels that had only gain
pool.map(gain_year_count_natrl_forest.create_gain_year_count_gain_only, biomass_tile_list)

# Creates gain year count tiles using only pixels that had neither loss nor gain pixels
pool.map(gain_year_count_natrl_forest.create_gain_year_count_no_change, biomass_tile_list)

# Creates gain year count tiles using only pixels that had both loss and gain pixels
pool.map(gain_year_count_natrl_forest.create_gain_year_count_loss_and_gain, biomass_tile_list)

# Merges the four above gain year count tiles for each Hansen tile into a single output tile
count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(count/6)
pool.map(gain_year_count_natrl_forest.create_gain_year_count_merge, biomass_tile_list)
pool.close()
pool.join()

# # For single processor use
# for tile in tile_list:
#     gain_year_count_natrl_forest.create_gain_year_count_loss_only(tile)
#
# for tile in tile_list:
#     gain_year_count_natrl_forest.create_gain_year_count_gain_only(tile)
#
# for tile in tile_list:
#     gain_year_count_natrl_forest.create_gain_year_count_no_change(tile)
#
# for tile in tile_list:
#     gain_year_count_natrl_forest.create_gain_year_count_loss_and_gain(tile)
#
# for tile in tile_list:
#     gain_year_count_natrl_forest.create_gain_year_count_merge(tile)

# Intermediate output tiles for checking outputs
uu.upload_final_set(cn.gain_year_count_natrl_forest_dir, "growth_years_loss_only")
uu.upload_final_set(cn.gain_year_count_natrl_forest_dir, "growth_years_gain_only")
uu.upload_final_set(cn.gain_year_count_natrl_forest_dir, "growth_years_no_change")
uu.upload_final_set(cn.gain_year_count_natrl_forest_dir, "growth_years_loss_and_gain")

# This is the final output used later in the model
uu.upload_final_set(cn.gain_year_count_natrl_forest_dir, cn.pattern_gain_year_count_natrl_forest)