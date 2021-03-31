'''
Creates tiles of annual aboveground and belowground removal rates for the entire model extent (all forest types).
Also, creates tiles that show what the source of the removal factor is each for each pixel. This can correspond to
particular forest types (mangrove, planted, natural) or data sources (US, Europe, young natural forests from Cook-Patton et al.,
older natural forests from IPCC defaults).
The current hierarchy where pixels overlap is: mangrove > Europe > planted forests > US forests > Cook-Patton et al.
rates for young secondary forests > IPCC defaults for old secondary and primary forests.
This hierarchy is reflected in the removal rates and the forest type rasters.
The different removal rate inputs are in different units but all are standardized to AGC/ha/yr and BGC/ha/yr.
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
import annual_gain_rate_AGC_BGC_all_forest_types

def mp_annual_gain_rate_AGC_BGC_all_forest_types(sensit_type, tile_id_list, run_date = None, no_upload = None):

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
        cn.annual_gain_AGB_mangrove_dir: [cn.pattern_annual_gain_AGB_mangrove],
        cn.annual_gain_BGB_mangrove_dir: [cn.pattern_annual_gain_BGB_mangrove],
        cn.annual_gain_AGC_BGC_natrl_forest_Europe_dir: [cn.pattern_annual_gain_AGC_BGC_natrl_forest_Europe],
        cn.annual_gain_AGC_BGC_planted_forest_unmasked_dir: [cn.pattern_annual_gain_AGC_BGC_planted_forest_unmasked],
        cn.annual_gain_AGC_BGC_natrl_forest_US_dir: [cn.pattern_annual_gain_AGC_BGC_natrl_forest_US],
        cn.annual_gain_AGC_natrl_forest_young_dir: [cn.pattern_annual_gain_AGC_natrl_forest_young],
        cn.age_cat_IPCC_dir: [cn.pattern_age_cat_IPCC],
        cn.annual_gain_AGB_IPCC_defaults_dir: [cn.pattern_annual_gain_AGB_IPCC_defaults],

        cn.stdev_annual_gain_AGB_mangrove_dir: [cn.pattern_stdev_annual_gain_AGB_mangrove],
        cn.stdev_annual_gain_AGC_BGC_natrl_forest_Europe_dir: [cn.pattern_stdev_annual_gain_AGC_BGC_natrl_forest_Europe],
        cn.stdev_annual_gain_AGC_BGC_planted_forest_unmasked_dir: [cn.pattern_stdev_annual_gain_AGC_BGC_planted_forest_unmasked],
        cn.stdev_annual_gain_AGC_BGC_natrl_forest_US_dir: [cn.pattern_stdev_annual_gain_AGC_BGC_natrl_forest_US],
        cn.stdev_annual_gain_AGC_natrl_forest_young_dir: [cn.pattern_stdev_annual_gain_AGC_natrl_forest_young],
        cn.stdev_annual_gain_AGB_IPCC_defaults_dir: [cn.pattern_stdev_annual_gain_AGB_IPCC_defaults]
    }


    # List of output directories and output file name patterns
    output_dir_list = [cn.removal_forest_type_dir,
                       cn.annual_gain_AGC_all_types_dir, cn.annual_gain_BGC_all_types_dir,
                       cn.annual_gain_AGC_BGC_all_types_dir, cn.stdev_annual_gain_AGC_all_types_dir]
    output_pattern_list = [cn.pattern_removal_forest_type,
                           cn.pattern_annual_gain_AGC_all_types, cn.pattern_annual_gain_BGC_all_types,
                           cn.pattern_annual_gain_AGC_BGC_all_types, cn.pattern_stdev_annual_gain_AGC_all_types]

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


    # This configuration of the multiprocessing call is necessary for passing multiple arguments to the main function
    # It is based on the example here: http://spencerimp.blogspot.com/2015/12/python-multiprocess-with-multiple.html
    if cn.count == 96:
        if sensit_type == 'biomass_swap':
            processes = 13
        else:
            processes = 17  # 30 processors > 740 GB peak; 18 = >740 GB peak; 16 = 660 GB peak; 17 = >680 GB peak
    else:
        processes = 2
    uu.print_log('Removal factor processors=', processes)
    pool = multiprocessing.Pool(processes)
    pool.map(partial(annual_gain_rate_AGC_BGC_all_forest_types.annual_gain_rate_AGC_BGC_all_forest_types,
                     output_pattern_list=output_pattern_list, sensit_type=sensit_type, no_upload=no_upload), tile_id_list)
    pool.close()
    pool.join()

    # # For single processor use
    # for tile_id in tile_id_list:
    #     annual_gain_rate_AGC_BGC_all_forest_types.annual_gain_rate_AGC_BGC_all_forest_types(tile_id, sensit_type, no_upload)

    # Checks the gross removals outputs for tiles with no data
    for output_pattern in output_pattern_list:
        if cn.count <= 2:  # For local tests
            processes = 1
            uu.print_log("Checking for empty tiles of {0} pattern with {1} processors using light function...".format(
                output_pattern, processes))
            pool = multiprocessing.Pool(processes)
            pool.map(partial(uu.check_and_delete_if_empty_light, output_pattern=output_pattern), tile_id_list)
            pool.close()
            pool.join()
        else:
            processes = 55  # 50 processors = XXX GB peak
            uu.print_log(
                "Checking for empty tiles of {0} pattern with {1} processors...".format(output_pattern, processes))
            pool = multiprocessing.Pool(processes)
            pool.map(partial(uu.check_and_delete_if_empty, output_pattern=output_pattern), tile_id_list)
            pool.close()
            pool.join()


    # If no_upload flag is not activated, output is uploaded
    if not no_upload:

        for i in range(0, len(output_dir_list)):
            uu.upload_final_set(output_dir_list[i], output_pattern_list[i])


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
    parser.add_argument('--no-upload', '-nu', action='store_true',
                       help='Disables uploading of outputs to s3')
    args = parser.parse_args()
    sensit_type = args.model_type
    tile_id_list = args.tile_id_list
    run_date = args.run_date
    no_upload = args.no_upload

    # Create the output log
    uu.initiate_log(tile_id_list=tile_id_list, sensit_type=sensit_type, run_date=run_date, no_upload=no_upload)

    # Checks whether the sensitivity analysis and tile_id_list arguments are valid
    uu.check_sensit_type(sensit_type)
    tile_id_list = uu.tile_id_list_check(tile_id_list)

    mp_annual_gain_rate_AGC_BGC_all_forest_types(sensit_type=sensit_type, tile_id_list=tile_id_list,
                                                 run_date=run_date, no_upload=no_upload)

