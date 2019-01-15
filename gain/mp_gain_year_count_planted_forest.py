###

import multiprocessing
import utilities
import gain_year_count_natrl_forest
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# The list of tiles to iterate through
tile_list = uu.tile_list(cn.annual_gain_AGB_planted_forest_dir)
# biomass_tile_list = ["00N_000E", "00N_050W", "00N_060W", "00N_010E", "00N_020E", "00N_030E", "00N_040E", "10N_000E", "10N_010E", "10N_010W", "10N_020E", "10N_020W"] # test tiles
# biomass_tile_list = ['10N_080W'] # test tile
print tile_list

# For downloading all tiles in the folders
download_list = [cn.loss_dir, cn.gain_dir, cn.tcd_dir, cn.ifl_dir, cn.mangrove_biomass_2000_dir]

for input in download_list:
    utilities.s3_folder_download('{}'.format(input), '.')

# # For copying individual tiles to s3 for testing
# for tile in biomass_tile_list:
#
#     utilities.s3_file_download('{0}{1}.tif'.format(utilities.loss_dir, tile), '.')
#     utilities.s3_file_download('{0}{1}_{2}.tif'.format(cn.gain_dir, tile, cn.pattern_gain), '.')
#     utilities.s3_file_download('{0}{1}_{2}.tif'.format(cn.tcd_dir, tile, cn.pattern_tcd), '.')
#     utilities.s3_file_download('{0}{1}_{2}.tif'.format(cn.mangrove_biomass_2000_dir, tile, cn.pattern_mangrove_biomass_2000), '.')

count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(count/10)
pool.map(gain_year_count_natrl_forest.create_gain_year_count, biomass_tile_list)

# # For single processor use
# for tile in biomass_tile_list:
#
#     gain_year_count_natrl_forest.create_gain_year_count(tile)