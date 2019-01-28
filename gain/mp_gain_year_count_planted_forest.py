###

import multiprocessing
import gain_year_count_natrl_forest
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

count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(count/4)
pool.map(gain_year_count_natrl_forest.create_gain_year_count, tile_list)

# # For single processor use
# for tile in biomass_tile_list:
#
#     gain_year_count_natrl_forest.create_gain_year_count(tile)