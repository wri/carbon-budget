'''
Creates tiles of annual aboveground and belowground biomass removal rates for mangroves using IPCC Wetlands Supplement Table 4.4 rates.
Its inputs are the continent-ecozone tiles, mangrove biomass tiles (for locations of mangroves), and the IPCC mangrove
removals rate table.
Also creates tiles of standard deviation in mangrove aboveground biomass removal rates based on the 95% CI in IPCC Wetlands Supplement Table 4.4.

python -m removals.mp_annual_gain_rate_mangrove -t std -l 00N_000E -nu
python -m removals.mp_annual_gain_rate_mangrove -t std -l all
'''

import multiprocessing
from functools import partial
import argparse
import datetime
import pandas as pd
from subprocess import Popen, PIPE, STDOUT, check_call
import os
import sys
import constants_and_names as cn
import universal_util as uu
from . import annual_gain_rate_mangrove

def mp_annual_gain_rate_mangrove(tile_id_list):

    os.chdir(cn.docker_tile_dir)
    pd.options.mode.chained_assignment = None


    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # Lists the tiles that have both mangrove biomass and FAO ecozone information because both of these are necessary for
        # calculating mangrove removals
        mangrove_biomass_tile_list = uu.tile_list_s3(cn.mangrove_biomass_2000_dir)
        ecozone_tile_list = uu.tile_list_s3(cn.cont_eco_dir)
        tile_id_list = list(set(mangrove_biomass_tile_list).intersection(ecozone_tile_list))

    uu.print_log(tile_id_list)
    uu.print_log(f'There are {str(len(tile_id_list))} tiles to process', "\n")


    download_dict = {
        cn.cont_eco_dir: [cn.pattern_cont_eco_processed],
        cn.mangrove_biomass_2000_dir: [cn.pattern_mangrove_biomass_2000]
    }


    # List of output directories and output file name patterns
    output_dir_list = [cn.annual_gain_AGB_mangrove_dir, cn.annual_gain_BGB_mangrove_dir, cn.stdev_annual_gain_AGB_mangrove_dir]
    output_pattern_list = [cn.pattern_annual_gain_AGB_mangrove, cn.pattern_annual_gain_BGB_mangrove, cn.pattern_stdev_annual_gain_AGB_mangrove]

    # A date can optionally be provided by the full model script or a run of this script.
    # This replaces the date in constants_and_names.
    if cn.RUN_DATE is not None:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, cn.RUN_DATE)


    # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list, if AWS credentials are found
    for key, values in download_dict.items():
        dir = key
        pattern = values[0]
        uu.s3_flexible_download(dir, pattern, cn.docker_tile_dir, cn.SENSIT_TYPE, tile_id_list)


    # Table with IPCC Wetland Supplement Table 4.4 default mangrove removals rates
    # cmd = ['aws', 's3', 'cp', os.path.join(cn.gain_spreadsheet_dir, cn.gain_spreadsheet), cn.docker_base_dir, '--no-sign-request']
    cmd = ['aws', 's3', 'cp', os.path.join(cn.gain_spreadsheet_dir, cn.gain_spreadsheet), cn.docker_tile_dir]
    uu.log_subprocess_output_full(cmd)


    ### To make the removal factor dictionaries

    # Imports the table with the ecozone-continent codes and the carbon removals rates
    gain_table = pd.read_excel("{}".format(cn.gain_spreadsheet),
                               sheet_name = "mangrove gain, for model")

    # Removes rows with duplicate codes (N. and S. America for the same ecozone)
    gain_table_simplified = gain_table.drop_duplicates(subset='gainEcoCon', keep='first')

    # Creates belowground:aboveground biomass ratio dictionary for the three mangrove types, where the keys correspond to
    # the "mangType" field in the removals rate spreadsheet.
    # If the assignment of mangTypes to ecozones changes, that column in the spreadsheet may need to change and the
    # keys in this dictionary would need to change accordingly.
    type_ratio_dict = {'1': cn.below_to_above_trop_dry_mang, '2'  :cn.below_to_above_trop_wet_mang, '3': cn.below_to_above_subtrop_mang}
    type_ratio_dict_final = {int(k):float(v) for k,v in list(type_ratio_dict.items())}

    # Applies the belowground:aboveground biomass ratios for the three mangrove types to the annual aboveground removals rates to get
    # a column of belowground annual removals rates by mangrove type
    gain_table_simplified['BGB_AGB_ratio'] = gain_table_simplified['mangType'].map(type_ratio_dict_final)
    gain_table_simplified['BGB_annual_rate'] = gain_table_simplified.AGB_gain_tons_ha_yr * gain_table_simplified.BGB_AGB_ratio

    # Converts the continent-ecozone codes and corresponding removals rates to dictionaries for aboveground and belowground removals rates
    gain_above_dict = pd.Series(gain_table_simplified.AGB_gain_tons_ha_yr.values,index=gain_table_simplified.gainEcoCon).to_dict()
    gain_below_dict = pd.Series(gain_table_simplified.BGB_annual_rate.values,index=gain_table_simplified.gainEcoCon).to_dict()

    # Adds a dictionary entry for where the ecozone-continent code is 0 (not in a continent)
    gain_above_dict[0] = 0
    gain_below_dict[0] = 0

    # Converts all the keys (continent-ecozone codes) to float type
    gain_above_dict = {float(key): value for key, value in gain_above_dict.items()}
    gain_below_dict = {float(key): value for key, value in gain_below_dict.items()}


    ### To make the removal factor standard deviation dictionary

    # Imports the table with the ecozone-continent codes and the carbon removals rates
    stdev_table = pd.read_excel("{}".format(cn.gain_spreadsheet),
                               sheet_name = "mangrove stdev, for model")

    # Removes rows with duplicate codes (N. and S. America for the same ecozone)
    stdev_table_simplified = stdev_table.drop_duplicates(subset='gainEcoCon', keep='first')

    # Converts the continent-ecozone codes and corresponding removals rate standard deviations to dictionaries for aboveground and belowground removals rate stdevs
    stdev_dict = pd.Series(stdev_table_simplified.AGB_gain_stdev_tons_ha_yr.values,index=stdev_table_simplified.gainEcoCon).to_dict()

    # Adds a dictionary entry for where the ecozone-continent code is 0 (not in a continent)
    stdev_dict[0] = 0

    # Converts all the keys (continent-ecozone codes) to float type
    stdev_dict = {float(key): value for key, value in stdev_dict.items()}


    if cn.SINGLE_PROCESSOR:
        for tile in tile_id_list:
            annual_gain_rate_mangrove.annual_gain_rate(tile, output_pattern_list, gain_above_dict, gain_below_dict, stdev_dict)

    else:
        # This configuration of the multiprocessing call is necessary for passing multiple arguments to the main function
        # It is based on the example here: http://spencerimp.blogspot.com/2015/12/python-multiprocess-with-multiple.html
        # Ran with 18 processors on r4.16xlarge (430 GB memory peak)
        if cn.count == 96:
            processes = 20    #26 processors = >740 GB peak; 18 = 550 GB peak; 20 = 610 GB peak; 23 = 700 GB peak; 24 > 750 GB peak
        else:
            processes = 4
        uu.print_log('Mangrove annual removals rate max processors=', processes)
        pool = multiprocessing.Pool(processes)
        pool.map(partial(annual_gain_rate_mangrove.annual_gain_rate, output_pattern_list=output_pattern_list,
                         gain_above_dict=gain_above_dict, gain_below_dict=gain_below_dict, stdev_dict=stdev_dict),
                 tile_id_list)
        pool.close()
        pool.join()


    # If no_upload flag is not activated (by choice or by lack of AWS credentials), output is uploaded
    if not no_upload:
        for i in range(0, len(output_dir_list)):
            uu.upload_final_set(output_dir_list[i], output_pattern_list[i])


if __name__ == '__main__':

    # The arguments for what kind of model run is being run (standard conditions or a sensitivity analysis) and
    # the tiles to include
    parser = argparse.ArgumentParser(
        description='Create tiles of removal factors for mangrove forests')
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

    mp_annual_gain_rate_mangrove(tile_id_list)