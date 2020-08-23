'''
This script creates tiles of forest age category across the entire model extent (all pixels) according to a decision tree.
The age categories are: <= 20 year old secondary forest (1), >20 year old secondary forest (2), and primary forest (3).
The decision tree is implemented as a series of numpy array statements rather than as nested if statements or gdal_calc operations.
The output tiles have 3 possible values, each value representing an end of the decision tree.
The forest age category tiles are inputs for assigning gain rates to
non-mangrove, non-planted, non-European, non-US, older secondary and primary forests.pixels.
The extent of this layer is greater than the extent of the rates which are based on this, though.
This assigns forest age category to all pixels within the model but they are ultimately only used for
non-mangrove, non-planted, non-European, non-US, older secondary and primary forest pixels.
You can think of the output from this script as being the age category if IPCC Table 4.9 rates were to be applied there.
'''


import multiprocessing
from functools import partial
import pandas as pd
import datetime
import argparse
from subprocess import Popen, PIPE, STDOUT, check_call
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu
sys.path.append(os.path.join(cn.docker_app,'gain'))
import forest_age_category_IPCC

def mp_forest_age_category_IPCC(sensit_type, tile_id_list, run_date = None):

    os.chdir(cn.docker_base_dir)

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # List of tiles to run in the model
        tile_id_list = uu.tile_list_s3(cn.model_extent_dir, sensit_type)

    uu.print_log(tile_id_list)
    uu.print_log("There are {} tiles to process".format(str(len(tile_id_list))) + "\n")


    # Files to download for this script.
    download_dict = {
                     cn.model_extent_dir: [cn.pattern_model_extent],
                     cn.gain_dir: [cn.pattern_gain],
                     cn.ifl_primary_processed_dir: [cn.pattern_ifl_primary],
                     cn.cont_eco_dir: [cn.pattern_cont_eco_processed]
    }

    # Adds the correct loss tile to the download dictionary depending on the model run
    if sensit_type == 'legal_Amazon_loss':
        download_dict[cn.Brazil_annual_loss_processed_dir] = [cn.pattern_Brazil_annual_loss_processed]
    elif sensit_type == 'Mekong_loss':
        download_dict[cn.Mekong_loss_processed_dir] = [cn.pattern_Mekong_loss_processed]
    else:
        download_dict[cn.loss_dir] = [cn.pattern_loss]

    # Adds the correct biomass tile to the download dictionary depending on the model run
    if sensit_type == 'biomass_swap':
        download_dict[cn.JPL_processed_dir] = [cn.pattern_JPL_unmasked_processed]
    else:
        download_dict[cn.WHRC_biomass_2000_unmasked_dir] = [cn.pattern_WHRC_biomass_2000_unmasked]


    # List of output directories and output file name patterns
    output_dir_list = [cn.age_cat_IPCC_dir]
    output_pattern_list = [cn.pattern_age_cat_IPCC]

    # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
    for key, values in download_dict.items():
        dir = key
        pattern = values[0]
        uu.s3_flexible_download(dir, pattern, cn.docker_base_dir, sensit_type, tile_id_list)


    # If the model run isn't the standard one, the output directory and file names are changed
    if sensit_type != 'std':
        uu.print_log("Changing output directory and file name pattern based on sensitivity analysis")
        output_dir_list = uu.alter_dirs(sensit_type, output_dir_list)
        output_pattern_list = uu.alter_patterns(sensit_type, output_pattern_list)

    # A date can optionally be provided by the full model script or a run of this script.
    # This replaces the date in constants_and_names.
    if run_date is not None:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, run_date)


     # Table with IPCC Table 4.9 default gain rates
    cmd = ['aws', 's3', 'cp', os.path.join(cn.gain_spreadsheet_dir, cn.gain_spreadsheet), cn.docker_base_dir]
    uu.log_subprocess_output_full(cmd)


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
    if cn.count == 96:
        processes = 42  # 30 processors=460 GB peak; 36 = 550 GB peak; 42 = XXX GB peak
    else:
        processes = 26
    uu.print_log('Natural forest age category max processors=', processes)
    pool = multiprocessing.Pool(processes)
    pool.map(partial(forest_age_category_IPCC.forest_age_category, gain_table_dict=gain_table_dict,
                     pattern=pattern, sensit_type=sensit_type), tile_id_list)
    pool.close()
    pool.join()

    # # For single processor use
    # for tile_id in tile_id_list:
    #
    #     forest_age_category_IPCC.forest_age_category(tile_id, gain_table_dict, pattern, sensit_type)

    # Uploads output tiles to s3
    uu.upload_final_set(output_dir_list[0], output_pattern_list[0])


if __name__ == '__main__':

    # The arguments for what kind of model run is being run (standard conditions or a sensitivity analysis) and
    # the tiles to include
    parser = argparse.ArgumentParser(
        description='Create tiles of the annual AGB and BGB gain rates for mangrove forests')
    parser.add_argument('--model-type', '-t', required=True,
                        help='{}'.format(cn.model_type_arg_help))
    parser.add_argument('--tile_id_list', '-l', required=True,
                        help='List of tile ids to use in the model. Should be of form 00N_110E or 00N_110E,00N_120E or all.')
    parser.add_argument('--run-date', '-d', required=False,
                        help='Date of run. Must be format YYYYMMDD.')
    args = parser.parse_args()
    sensit_type = args.model_type
    tile_id_list = args.tile_id_list
    run_date = args.run_date

    # Create the output log
    uu.initiate_log(tile_id_list=tile_id_list, sensit_type=sensit_type, run_date=run_date)

    # Checks whether the sensitivity analysis and tile_id_list arguments are valid
    uu.check_sensit_type(sensit_type)
    tile_id_list = uu.tile_id_list_check(tile_id_list)

    mp_forest_age_category_IPCC(sensit_type=sensit_type, tile_id_list=tile_id_list, run_date=run_date)

