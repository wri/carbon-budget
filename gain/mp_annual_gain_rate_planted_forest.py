### This script assigns annual above and belowground biomass gain rates (in the units of IPCC Table 4.9 (currently tonnes
### biomass/ha/yr)) to non-mangrove planted forest pixels. It masks mangrove pixels from the planted forest carbon gain
### rate tiles so that different forest types are non-overlapping. These are then used in the next step of the carbon model.


import multiprocessing
import utilities
import annual_gain_rate_planted_forest
import pandas as pd
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

pd.options.mode.chained_assignment = None

# tile_list = uu.tile_list(cn.annual_gain_AGC_planted_forest_dir)
tile_list = ['80N_020E', '00N_000E', '00N_020E', '00N_110E'] # test tiles: no mangrove or planted forest, mangrove only, planted forest only, mangrove and planted forest
print tile_list
print "There are {} tiles to process".format(str(len(tile_list)))

# # For downloading all tiles in the input folders
# download_list = [cn.annual_gain_AGC_BGC_planted_forest_unmasked_dir, cn.mangrove_biomass_2000_dir]
#
# for input in download_list:
#     utilities.s3_folder_download(input, '.')

# For copying individual tiles to spot machine for testing
for tile in tile_list:

    utilities.s3_file_download('{0}{1}_{2}.tif'.format(cn.annual_gain_AGC_BGC_planted_forest_unmasked_dir, tile, cn.pattern_annual_gain_AGC_BGC_planted_forest_unmasked), '.')
    utilities.s3_file_download('{0}{1}_{2}.tif'.format(cn.mangrove_biomass_2000_dir, tile, cn.pattern_mangrove_biomass_2000), '.')

# This configuration of the multiprocessing call is necessary for passing multiple arguments to the main function
# It is based on the example here: http://spencerimp.blogspot.com/2015/12/python-multiprocess-with-multiple.html
count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(count/3)
pool.map(annual_gain_rate_planted_forest.mask_mangroves, tile_list)

pool.map(annual_gain_rate_planted_forest.create_AGB_rate, tile_list)

pool.map(annual_gain_rate_planted_forest.create_BGB_rate, tile_list)
pool.close()
pool.join()

# # For single processor use
# for tile in biomass_tile_list:
#
#     annual_gain_rate_natrl_forest.annual_gain_rate(tile, gain_table_dict)

print "Tiles processed. Uploading to s3 now..."
uu.upload_final_set(cn.annual_gain_AGB_planted_forest_non_mangrove_dir, cn.pattern_annual_gain_AGB_planted_forest_non_mangrove)
uu.upload_final_set(cn.annual_gain_BGB_planted_forest_non_mangrove_dir, cn.pattern_annual_gain_BGB_planted_forest_non_mangrove)

