"""
This script creates tiles of forest age category across the entire model extent (all pixels) according to a decision tree.
The age categories are: <= 20 year old secondary forest (1), >20 year old secondary forest (2), and primary forest (3).
The decision tree is implemented as a series of numpy array statements rather than as nested if statements or gdal_calc operations.
The output tiles have 3 possible values, each value representing an end of the decision tree.
The forest age category tiles are inputs for assigning removals rates to
non-mangrove, non-planted, non-European, non-US, older secondary and primary forests.pixels.
The extent of this layer is greater than the extent of the rates which are based on this, though.
This assigns forest age category to all pixels within the model but they are ultimately only used for
non-mangrove, non-planted, non-European, non-US, older secondary and primary forest pixels.
You can think of the output from this script as being the age category if IPCC Table 4.9 rates were to be applied there.

python -m removals.mp_forest_age_category_IPCC -t std -l 00N_000E -nu
python -m removals.mp_forest_age_category_IPCC -t std -l all
"""


import argparse
from functools import partial
import pandas as pd
import multiprocessing
import os
import sys

import constants_and_names as cn
import universal_util as uu
from . import forest_age_category_IPCC

def mp_forest_age_category_IPCC(tile_id_list):
    """
    :param tile_id_list: list of tile ids to process
    :return: set of tiles denoting three broad forest age categories: 1- young (<20), 2- middle, 3- old/primary
    """

    os.chdir(cn.docker_tile_dir)

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # List of tiles to run in the model
        tile_id_list = uu.tile_list_s3(cn.model_extent_dir, cn.SENSIT_TYPE)

    uu.print_log(tile_id_list)
    uu.print_log(f'There are {str(len(tile_id_list))} tiles to process', "\n")


    # Files to download for this script.
    download_dict = {
                     cn.model_extent_dir: [cn.pattern_model_extent],
                     cn.gain_dir: [cn.pattern_data_lake],
                     cn.ifl_primary_processed_dir: [cn.pattern_ifl_primary],
                     cn.cont_eco_dir: [cn.pattern_cont_eco_processed]
    }

    # Adds the correct loss tile to the download dictionary depending on the model run
    if cn.SENSIT_TYPE == 'legal_Amazon_loss':
        download_dict[cn.Brazil_annual_loss_processed_dir] = [cn.pattern_Brazil_annual_loss_processed]
    elif cn.SENSIT_TYPE == 'Mekong_loss':
        download_dict[cn.Mekong_loss_processed_dir] = [cn.pattern_Mekong_loss_processed]
    else:
        download_dict[cn.loss_dir] = [cn.pattern_loss]

    # Adds the correct biomass tile to the download dictionary depending on the model run
    if cn.SENSIT_TYPE == 'biomass_swap':
        download_dict[cn.JPL_processed_dir] = [cn.pattern_JPL_unmasked_processed]
    else:
        download_dict[cn.WHRC_biomass_2000_unmasked_dir] = [cn.pattern_WHRC_biomass_2000_unmasked]


    # List of output directories and output file name patterns
    output_dir_list = [cn.age_cat_IPCC_dir]
    output_pattern_list = [cn.pattern_age_cat_IPCC]


    # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
    for key, values in download_dict.items():
        directory = key
        pattern = values[0]
        uu.s3_flexible_download(directory, pattern, cn.docker_tile_dir, cn.SENSIT_TYPE, tile_id_list)


    # If the model run isn't the standard one, the output directory and file names are changed
    if cn.SENSIT_TYPE != 'std':
        uu.print_log('Changing output directory and file name pattern based on sensitivity analysis')
        output_dir_list = uu.alter_dirs(cn.SENSIT_TYPE, output_dir_list)
        output_pattern_list = uu.alter_patterns(cn.SENSIT_TYPE, output_pattern_list)

    # A date can optionally be provided by the full model script or a run of this script.
    # This replaces the date in constants_and_names.
    # Only done if output upload is enabled.
    if cn.RUN_DATE is not None and cn.NO_UPLOAD is not None:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, cn.RUN_DATE)


    # Table with IPCC Table 4.9 default removals rates
    # cmd = ['aws', 's3', 'cp', os.path.join(cn.gain_spreadsheet_dir, cn.gain_spreadsheet), cn.docker_base_dir, '--no-sign-request']
    cmd = ['aws', 's3', 'cp', os.path.join(cn.gain_spreadsheet_dir, cn.gain_spreadsheet), cn.docker_tile_dir]
    uu.log_subprocess_output_full(cmd)


    # Imports the table with the ecozone-continent codes and the carbon removals rates
    gain_table = pd.read_excel(f'{cn.gain_spreadsheet}', sheet_name = "natrl fores gain, for std model")

    # Removes rows with duplicate codes (N. and S. America for the same ecozone)
    gain_table_simplified = gain_table.drop_duplicates(subset='gainEcoCon', keep='first')

    # Converts the continent-ecozone codes and young forest removals rates to a dictionary
    gain_table_dict = pd.Series(gain_table_simplified.growth_secondary_less_20.values,index=gain_table_simplified.gainEcoCon).to_dict()

    # Adds a dictionary entry for where the ecozone-continent code is 0 (not in a continent)
    gain_table_dict[0] = 0


    # Creates a single filename pattern to pass to the multiprocessor call
    pattern = output_pattern_list[0]

    if cn.SINGLE_PROCESSOR:
        for tile_id in tile_id_list:
            forest_age_category_IPCC.forest_age_category(tile_id, gain_table_dict, pattern)

    else:
        # This configuration of the multiprocessing call is necessary for passing multiple arguments to the main function
        # It is based on the example here: http://spencerimp.blogspot.com/2015/12/python-multiprocess-with-multiple.html
        # With processes=30, peak usage was about 350 GB using WHRC AGB.
        # processes=26 maxes out above 480 GB for biomass_swap, so better to use fewer than that.
        if cn.count == 96:
            if cn.SENSIT_TYPE == 'biomass_swap':
                processes = 32 # 32 processors = 610 GB peak
            else:
                processes = 44 # 30 processors=460 GB peak; 36 = 550 GB peak; 42 = 700 GB peak (slow increase later on); 44=725 GB peak
        else:
            processes = 2
        uu.print_log(f'Natural forest age category max processors={processes}')
        with multiprocessing.Pool(processes) as pool:
            pool.map(partial(forest_age_category_IPCC.forest_age_category, gain_table_dict=gain_table_dict, pattern=pattern),
                     tile_id_list)
            pool.close()
            pool.join()

    # If no_upload flag is not activated (by choice or by lack of AWS credentials), output is uploaded
    if not cn.NO_UPLOAD:
        uu.upload_final_set(output_dir_list[0], output_pattern_list[0])


if __name__ == '__main__':

    # The arguments for what kind of model run is being run (standard conditions or a sensitivity analysis) and
    # the tiles to include
    parser = argparse.ArgumentParser(
        description='Create tiles of the forest age category (<20 years, >20 years secondary, primary)')
    parser.add_argument('--model-type', '-t', required=True,
                        help=f'{cn.model_type_arg_help}')
    parser.add_argument('--tile_id_list', '-l', required=True,
                        help='List of tile ids to use in the model. Should be of form 00N_110E or 00N_110E,00N_120E or all.')
    parser.add_argument('--run-date', '-d', required=False,
                        help='Date of run. Must be format YYYYMMDD.')
    parser.add_argument('--no-upload', '-nu', action='store_true',
                       help='Disables uploading of outputs to s3')
    parser.add_argument('--single-processor', '-sp', action='store_true',
                       help='Uses single processing rather than multiprocessing')
    args = parser.parse_args()

    # Sets global variables to the command line arguments
    cn.SENSIT_TYPE = args.model_type
    cn.RUN_DATE = args.run_date
    cn.NO_UPLOAD = args.no_upload
    cn.SINGLE_PROCESSOR = args.single_processor

    tile_id_list = args.tile_id_list

    # Disables upload to s3 if no AWS credentials are found in environment
    if not uu.check_aws_creds():
        cn.NO_UPLOAD = True

    # Create the output log
    uu.initiate_log(tile_id_list)

    # Checks whether the sensitivity analysis and tile_id_list arguments are valid
    uu.check_sensit_type(cn.SENSIT_TYPE)
    tile_id_list = uu.tile_id_list_check(tile_id_list)

    mp_forest_age_category_IPCC(tile_id_list)
