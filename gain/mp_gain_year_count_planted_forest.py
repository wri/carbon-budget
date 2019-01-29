###

import multiprocessing
import gain_year_count_planted_forest
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# The list of tiles to iterate through
tile_list = uu.tile_list(cn.annual_gain_AGB_planted_forest_non_mangrove_dir)
# biomass_tile_list = ['10N_080W'] # test tile
print tile_list
print "There are {} tiles to process".format(str(len(tile_list)))

# For downloading all tiles in the folders
download_list = [cn.loss_dir, cn.gain_dir, cn.tcd_dir, cn.ifl_dir, cn.mangrove_biomass_2000_dir]

for input in download_list:
    uu.s3_folder_download(input, '.')

# # For copying individual tiles to s3 for testing
# for tile in biomass_tile_list:
#
#     uu.s3_file_download('{0}{1}.tif'.format(utilities.loss_dir, tile), '.')
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.gain_dir, tile, cn.pattern_gain), '.')
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.tcd_dir, tile, cn.pattern_tcd), '.')
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.mangrove_biomass_2000_dir, tile, cn.pattern_mangrove_biomass_2000), '.')

# Creates gain year count tiles using only pixels that had only loss
count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(count/3)
pool.map(gain_year_count_planted_forest.create_gain_year_count_loss_only, tile_list)

# Creates gain year count tiles using only pixels that had only gain
pool.map(gain_year_count_planted_forest.create_gain_year_count_gain_only, tile_list)

# Creates gain year count tiles using only pixels that had neither loss nor gain pixels
pool.map(gain_year_count_planted_forest.create_gain_year_count_no_change, tile_list)

# Creates gain year count tiles using only pixels that had both loss and gain pixels
pool.map(gain_year_count_planted_forest.create_gain_year_count_loss_and_gain, tile_list)

# Merges the four above gain year count tiles for each Hansen tile into a single output tile
pool = multiprocessing.Pool(count/6)
pool.map(gain_year_count_planted_forest.create_gain_year_count_merge, tile_list)
pool.close()
pool.join()

# # For single processor use
# for tile in tile_list:
#     gain_year_count_planted_forest.create_gain_year_count_loss_only(tile)
#
# for tile in tile_list:
#     gain_year_count_planted_forest.create_gain_year_count_gain_only(tile)

# for tile in tile_list:
#     gain_year_count_planted_forest.create_gain_year_count_no_change(tile)
#
# for tile in tile_list:
#     gain_year_count_planted_forest.create_gain_year_count_loss_and_gain(tile)
#
# for tile in tile_list:
#     gain_year_count_planted_forest.create_gain_year_count_merge(tile)

# Intermediate output tiles for checking outputs
uu.upload_final_set(cn.gain_year_count_planted_forest_dir, "growth_years_loss_only")
uu.upload_final_set(cn.gain_year_count_planted_forest_dir, "growth_years_gain_only")
uu.upload_final_set(cn.gain_year_count_planted_forest_dir, "growth_years_no_change")
uu.upload_final_set(cn.gain_year_count_planted_forest_dir, "growth_years_loss_and_gain")

# This is the final output used later in the model
uu.upload_final_set(cn.gain_year_count_planted_forest_dir, cn.pattern_gain_year_count_planted_forest)