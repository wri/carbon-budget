### This script assigns annual above and belowground biomass gain rates (in the units of IPCC Table 4.9 (currently tonnes
### biomass/ha/yr)) to non-mangrove planted forest pixels.


from multiprocessing.pool import Pool
import utilities
import annual_gain_rate_planted_forest
import pandas as pd
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

pd.options.mode.chained_assignment = None

# tile_list = uu.tile_list(cn.annual_gain_AGC_planted_forest_dir)
tile_list = ['00N_010E', '00N_030E'] # test tiles. These have both mangroved and planted forest in them.
print tile_list

# # For downloading all tiles in the input folders
# download_list = [cn.annual_gain_AGC_planted_forest_dir, cn.mangrove_biomass_2000_dir]
#
# for input in download_list:
#     utilities.s3_folder_download(input, '.')

# For copying individual tiles to spot machine for testing
for tile in tile_list:

    utilities.s3_file_download('{0}{1}_{2}.tif'.format(cn.annual_gain_AGC_planted_forest_dir, tile, cn.pattern_annual_gain_AGC_planted_forest_full_extent), '.')
    utilities.s3_file_download('{0}{1}_{2}.tif'.format(cn.mangrove_biomass_2000_dir, tile, cn.pattern_mangrove_biomass_2000), '.')

# This configuration of the multiprocessing call is necessary for passing multiple arguments to the main function
# It is based on the example here: http://spencerimp.blogspot.com/2015/12/python-multiprocess-with-multiple.html
num_of_processes = 8
pool = Pool(num_of_processes)
pool.map(annual_gain_rate_planted_forest.annual_gain_rate, tile_list)
pool.close()
pool.join()

# # For single processor use
# for tile in biomass_tile_list:
#
#     annual_gain_rate_natrl_forest.annual_gain_rate(tile, gain_table_dict)

print "Tiles processed. Uploading to s3 now..."
uu.upload_final_set(cn.annual_gain_AGB_planted_forest_non_mangrove_dir, cn.pattern_annual_gain_AGB_planted_forest_non_mangrove)
uu.upload_final_set(cn.annual_gain_BGB_planted_forest_non_mangrove_dir, cn.pattern_annual_gain_BGB_planted_forest_non_mangrove)

