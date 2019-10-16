### This script creates tiles of non-planted, non-mangrove forest age category according to a decision tree.
### The age categories are: <= 20 year old secondary forest, >20 year old secondary forest, and primary forest.
### The decision tree uses several input tiles, including IFL status, gain, and loss.
### Downloading all of these tiles can take awhile.
### The decision tree is implemented as a series of numpy array statements rather than as nested if statements or gdal_calc operations.
### The output tiles have 10 possible values, each value representing an end of the decision tree.
### These 10 values map to the three natural forest age categories.
### The forest age category tiles are inputs for assigning gain rates to pixels.
### The extent of this layer is what determines the extent of the non-mangrove non-planted forest gain rate layer
### (in conjunction with ecozone and continent layers).
### That is, the forest age category layer should cover the entire non-mangrove non-planted annual gain rate layer.
### Unlike other multiprocessing scripts, this one passes two arguments to the main script: the tile list
### and the dictionary of gain rates for different continent-ecozone combinations (needed for one node in the decision tree).
### Uses an r4.16xlarge spot machine.

import multiprocessing
from multiprocessing.pool import Pool
from functools import partial
import forest_age_category_natrl_forest
import pandas as pd
import subprocess
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

biomass_tile_list = uu.tile_list(cn.WHRC_biomass_2000_non_mang_non_planted_dir)
# biomass_tile_list = ["00N_000E", "00N_050W", "00N_060W", "00N_010E", "00N_020E", "00N_030E", "00N_040E", "10N_000E", "10N_010E", "10N_010W", "10N_020E", "10N_020W"] # test tiles
biomass_tile_list = ['00N_000E', '80N_030E', '00N_110E'] # test tiles
print biomass_tile_list
print "There are {} tiles to process".format(str(len(biomass_tile_list)))

# For downloading all tiles in the folders
download_list = [cn.loss_dir, cn.gain_dir, cn.tcd_dir, cn.ifl_primary_processed_dir,
                 cn.WHRC_biomass_2000_non_mang_non_planted_dir, cn.cont_eco_dir,
                 cn.planted_forest_type_unmasked_dir, cn.mangrove_biomass_2000_dir]

# for input in download_list:
#     uu.s3_folder_download(input, '.')

# For copying individual tiles to spot machine for testing
for tile in biomass_tile_list:

    uu.s3_file_download('{0}{1}.tif'.format(cn.loss_dir, tile), '.')                                # loss tiles
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.gain_dir, cn.pattern_gain, tile), '.')            # gain tiles
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.tcd_dir, cn.pattern_tcd, tile), '.')    # tcd 2000
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.ifl_primary_processed_dir, tile, cn.pattern_ifl_primary), '.')                    # ifl 2000
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.WHRC_biomass_2000_non_mang_non_planted_dir, tile, cn.pattern_WHRC_biomass_2000_non_mang_non_planted), '.')                     # biomass 2000
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.cont_eco_dir, tile, cn.pattern_cont_eco_processed), '.')               # continents and FAO ecozones 2000
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.planted_forest_type_unmasked_dir, tile, cn.pattern_planted_forest_type_unmasked), '.')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.mangrove_biomass_2000_dir, tile, cn.pattern_mangrove_biomass_2000), '.')

# Table with IPCC Table 4.9 default gain rates
cmd = ['aws', 's3', 'cp', os.path.join(cn.gain_spreadsheet_dir, cn.gain_spreadsheet), '.']
subprocess.check_call(cmd)

# Imports the table with the ecozone-continent codes and the carbon gain rates
gain_table = pd.read_excel("{}".format(cn.gain_spreadsheet),
                           sheet_name = "natrl fores gain, for model")

# Removes rows with duplicate codes (N. and S. America for the same ecozone)
gain_table_simplified = gain_table.drop_duplicates(subset='gainEcoCon', keep='first')

# Converts the continent-ecozone codes and young forest gain rates to a dictionary
gain_table_dict = pd.Series(gain_table_simplified.growth_secondary_less_20.values,index=gain_table_simplified.gainEcoCon).to_dict()

# Adds a dictionary entry for where the ecozone-continent code is 0 (not in a continent)
gain_table_dict[0] = 0

# # This configuration of the multiprocessing call is necessary for passing multiple arguments to the main function
# # It is based on the example here: http://spencerimp.blogspot.com/2015/12/python-multiprocess-with-multiple.html
# # With processes=20, peak usage was about 280 GB, so plenty of room on an r4.16xlarge
# # With processes=30, peak usage was about 410 GB
# num_of_processes = 32
# pool = Pool(num_of_processes)
# pool.map(partial(forest_age_category_natrl_forest.forest_age_category, gain_table_dict=gain_table_dict), biomass_tile_list)
# pool.close()
# pool.join()

# For single processor use
for tile in biomass_tile_list:

    forest_age_category_natrl_forest.forest_age_category(tile, gain_table_dict)

print "Tiles processed. Uploading to s3 now..."
uu.upload_final_set(cn.age_cat_natrl_forest_dir, cn.pattern_age_cat_natrl_forest)
