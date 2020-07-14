### This script calculates the cumulative above and belowground CO2 gain in non-mangrove, non-planted natural forest pixels from 2001-2015.
### It multiplies the annual biomass gain rate by the number of years of gain by the biomass-to-carbon conversion and C to CO2 conversion.

import multiprocessing
import argparse
import os
import datetime
from functools import partial
import sys
sys.path.append('/usr/local/app/gain/')
import cumulative_gain_natrl_forest
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def mp_cumulative_gain_natrl_forest(sensit_type, tile_id_list, run_date = None):

    os.chdir(cn.docker_base_dir)

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # List of tiles to run in the model
        tile_id_list = uu.tile_list_s3(cn.WHRC_biomass_2000_non_mang_non_planted_dir, sensit_type)

    uu.print_log(tile_id_list)
    uu.print_log("There are {} tiles to process".format(str(len(tile_id_list))) + "\n")


    # Files to download for this script.
    download_dict = {
        cn.annual_gain_AGB_natrl_forest_dir: [cn.pattern_annual_gain_AGB_natrl_forest],
        cn.annual_gain_BGB_natrl_forest_dir: [cn.pattern_annual_gain_BGB_natrl_forest],
        cn.gain_year_count_natrl_forest_dir: [cn.pattern_gain_year_count_natrl_forest]
    }

    
    # List of output directories and output file name patterns
    output_dir_list = [cn.cumul_gain_AGCO2_natrl_forest_dir, cn.cumul_gain_BGCO2_natrl_forest_dir]
    output_pattern_list = [cn.pattern_cumul_gain_AGCO2_natrl_forest, cn.pattern_cumul_gain_BGCO2_natrl_forest]


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


    # Calculates cumulative aboveground carbon gain in non-mangrove planted forests
    # Processors=26 peaks at 400 - 450 GB of memory, which works on an r4.16xlarge (different runs had different maxes)
    if cn.count == 96:
        processes = 44   # 26 processors = 370 GB peak; 32 = 470 GB peak; 38 = 540 GB peak; 44 = XXX GB peak
    else:
        processes = 24
    uu.print_log('Cumulative gain AGC rate natural forest max processors=', processes)
    pool = multiprocessing.Pool(processes)
    pool.map(partial(cumulative_gain_natrl_forest.cumulative_gain_AGCO2, output_pattern_list=output_pattern_list,
                     sensit_type=sensit_type), tile_id_list)

    # Calculates cumulative belowground carbon gain in non-mangrove planted forests
    # Processors=26 peaks at 400 - 450 GB of memory, which works on an r4.16xlarge (different runs had different maxes)
    if cn.count == 96:
        processes = 44   # 26 processors = 400 GB peak; 32 = 470 GB peak; 38 = 540 GB peak; 44 = XXX GB peak
    else:
        processes = 24
    uu.print_log('Cumulative gain BGC rate natural forest max processors=', processes)
    pool = multiprocessing.Pool(processes)
    pool.map(partial(cumulative_gain_natrl_forest.cumulative_gain_BGCO2, output_pattern_list=output_pattern_list,
                     sensit_type=sensit_type), tile_id_list)
    pool.close()
    pool.join()

    # # For single processor use
    # for tile_id in tile_id_list:
    #     cumulative_gain_natrl_forest.cumulative_gain_AGCO2(tile_id, output_pattern_list, sensit_type)
    #
    # for tile_id in tile_id_list:
    #     cumulative_gain_natrl_forest.cumulative_gain_BGCO2(tile_id, output_pattern_list, sensit_type)

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
    args = parser.parse_args()
    sensit_type = args.model_type
    tile_id_list = args.tile_id_list
    run_date = args.run_date

    # Create the output log
    uu.initiate_log(tile_id_list=tile_id_list, sensit_type=sensit_type, run_date=run_date)

    # Checks whether the sensitivity analysis and tile_id_list arguments are valid
    uu.check_sensit_type(sensit_type)
    tile_id_list = uu.tile_id_list_check(tile_id_list)

    mp_cumulative_gain_natrl_forest(sensit_type=sensit_type, tile_id_list=tile_id_list, run_date=run_date)