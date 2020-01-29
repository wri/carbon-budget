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
### Uses an r4.16xlarge spot machine.

import multiprocessing
from multiprocessing.pool import Pool
from functools import partial
import forest_age_category_natrl_forest
import pandas as pd
import argparse
import subprocess
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def mp_forest_age_category_natrl_forest(sensit_type, tile_id_list):

    # Files to download for this script.
    download_dict = {cn.loss_dir: [''],
                     cn.gain_dir: [cn.pattern_gain],
                     cn.tcd_dir: [cn.pattern_tcd],
                     cn.ifl_primary_processed_dir: [cn.pattern_ifl_primary],
                     cn.WHRC_biomass_2000_non_mang_non_planted_dir: [cn.pattern_WHRC_biomass_2000_non_mang_non_planted],
                     cn.cont_eco_dir: [cn.pattern_cont_eco_processed],
                     cn.planted_forest_type_unmasked_dir: [cn.pattern_planted_forest_type_unmasked],
                     cn.mangrove_biomass_2000_dir: [cn.pattern_mangrove_biomass_2000]
    }


    # List of output directories and output file name patterns
    output_dir_list = [cn.age_cat_natrl_forest_dir]
    output_pattern_list = [cn.pattern_age_cat_natrl_forest]


    # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
    for key, values in download_dict.iteritems():
        dir = key
        pattern = values[0]
        uu.s3_flexible_download(dir, pattern, '.', sensit_type, tile_id_list)


    # If the model run isn't the standard one, the output directory and file names are changed
    if sensit_type != 'std':
        print "Changing output directory and file name pattern based on sensitivity analysis"
        output_dir_list = uu.alter_dirs(sensit_type, output_dir_list)
        output_pattern_list = uu.alter_patterns(sensit_type, output_pattern_list)


     # Table with IPCC Table 4.9 default gain rates
    cmd = ['aws', 's3', 'cp', os.path.join(cn.gain_spreadsheet_dir, cn.gain_spreadsheet), '.']
    subprocess.check_call(cmd)

    # Imports the table with the ecozone-continent codes and the carbon gain rates
    gain_table = pd.read_excel("{}".format(cn.gain_spreadsheet),
                               sheet_name = "natrl fores gain, for std model")

    # Removes rows with duplicate codes (N. and S. America for the same ecozone)
    gain_table_simplified = gain_table.drop_duplicates(subset='gainEcoCon', keep='first')

    # Converts the continent-ecozone codes and young forest gain rates to a dictionary
    gain_table_dict = pd.Series(gain_table_simplified.growth_secondary_less_20.values,index=gain_table_simplified.gainEcoCon).to_dict()

    # Adds a dictionary entry for where the ecozone-continent code is 0 (not in a continent)
    gain_table_dict[0] = 0


    # Creates a single filename pattern to pass to the multiprocessor call
    pattern = output_pattern_list[0]

    # This configuration of the multiprocessing call is necessary for passing multiple arguments to the main function
    # It is based on the example here: http://spencerimp.blogspot.com/2015/12/python-multiprocess-with-multiple.html
    # With processes=30, peak usage was about 350 GB using WHRC AGB.
    # processes=26 maxes out above 480 GB for biomass_swap, so better to use fewer than that.
    count = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=20)
    pool.map(partial(forest_age_category_natrl_forest.forest_age_category, gain_table_dict=gain_table_dict,
                     pattern=pattern, sensit_type=sensit_type), tile_id_list)
    pool.close()
    pool.join()

    # # For single processor use
    # for tile_id in tile_id_list:
    #
    #     forest_age_category_natrl_forest.forest_age_category(tile_id, gain_table_dict, pattern, sensit_type)

    # Uploads output tiles to s3
    uu.upload_final_set(output_dir_list[0], output_pattern_list[0])


if __name__ == '__main__':

    # The argument for what kind of model run is being done: standard conditions or a sensitivity analysis run
    parser = argparse.ArgumentParser(description='Create tiles of the number of years of carbon gain for mangrove forests')
    parser.add_argument('--model-type', '-t', required=True,
                        help='{}'.format(cn.model_type_arg_help))
    args = parser.parse_args()
    sensit_type = args.model_type
    # Checks whether the sensitivity analysis argument is valid
    uu.check_sensit_type(sensit_type)

    # List of tiles to run in the model
    # tile_id_list = uu.tile_list_s3(cn.WHRC_biomass_2000_non_mang_non_planted_dir, sensit_type)
    # tile_id_list = ["00N_000E", "00N_050W", "00N_060W", "00N_010E", "00N_020E", "00N_030E", "00N_040E", "10N_000E", "10N_010E", "10N_010W", "10N_020E", "10N_020W"] # test tiles
    tile_id_list = ['00N_110E'] # test tiles
    print tile_id_list
    print "There are {} tiles to process".format(str(len(tile_id_list))) + "\n"

    mp_forest_age_category_natrl_forest(sensit_type=sensit_type)

