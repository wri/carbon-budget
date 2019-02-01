###

import multiprocessing
import non_mangrove_non_planted_WHRC_biomass_2000
import pandas as pd
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# biomass_tile_list = uu.tile_list(cn.WHRC_biomass_2000_dir)
biomass_tile_list = ['80N_020E', '00N_000E', '00N_020E', '00N_110E'] # test tiles: no mangrove or planted forest, mangrove only, planted forest only, mangrove and planted forest
# biomass_tile_list = ['00N_000E']
print biomass_tile_list
print "There are {} tiles to process".format(str(len(biomass_tile_list)))

# For downloading all tiles in the input folders.
# Mangrove biomass and full-extent planted forests are used to mask out mangrove and planted forests from the natural forests.
download_list = [cn.mangrove_biomass_2000_dir, cn.annual_gain_AGC_planted_forest_unmasked_dir, cn.WHRC_biomass_2000_unmasked_dir]

# for input in download_list:
#     uu.s3_folder_download(input, '.')

# # For copying individual tiles to spot machine for testing
# for tile in biomass_tile_list:
#
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.mangrove_biomass_2000_dir, tile, cn.pattern_mangrove_biomass_2000), '.')  # mangrove aboveground biomass
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.annual_gain_AGC_planted_forest_unmasked_dir, tile, cn.pattern_annual_gain_AGC_planted_forest_unmasked), '.')  # planted forest extent (not masked by mangroves)
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.WHRC_biomass_2000_unmasked_dir, tile, cn.pattern_WHRC_biomass_2000_unmasked), '.')   # WHRC biomass 2000 not masked by anything

# # For multiprocessing
# count = multiprocessing.cpu_count()
# pool = multiprocessing.Pool(count/3)
# pool.map(non_mangrove_non_planted_WHRC_biomass_2000.mask_biomass, biomass_tile_list)
# pool.close()
# pool.join()

# For single processor use
for tile in biomass_tile_list:
    non_mangrove_non_planted_WHRC_biomass_2000.mask_biomass(tile)

print "Tiles processed. Uploading to s3 now..."
uu.upload_final_set(cn.non_mang_non_planted_biomass_2000_dir, cn.pattern_non_mang_non_planted_biomass_2000)
