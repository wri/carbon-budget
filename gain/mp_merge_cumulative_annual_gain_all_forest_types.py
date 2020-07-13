### This script combines the annual gain rate tiles from different forest types (non-mangrove natural forests, mangroves,
### plantations) into combined tiles. It does the same for cumulative CO2 gain over the study period (above and belowground).

import multiprocessing
import argparse
import os
import datetime
from functools import partial
import sys
sys.path.append('/usr/local/app/gain/')
import merge_cumulative_annual_gain_all_forest_types
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def mp_merge_cumulative_annual_gain_all_forest_types(sensit_type, tile_id_list, run_date = None):

    os.chdir(cn.docker_base_dir)

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # List of tiles to run in the model
        tile_id_list = uu.create_combined_tile_list(cn.WHRC_biomass_2000_non_mang_non_planted_dir,
                                                    cn.annual_gain_AGB_mangrove_dir,
                                                    set3=cn.annual_gain_AGB_planted_forest_non_mangrove_dir,
                                                    sensit_type=sensit_type
                                                    )
    uu.print_log(tile_id_list)
    uu.print_log("There are {} tiles to process".format(str(len(tile_id_list))) + "\n")

    # Files to download for this script
    download_dict = {
        cn.annual_gain_AGB_mangrove_dir: [cn.pattern_annual_gain_AGB_mangrove],
        cn.annual_gain_AGB_planted_forest_non_mangrove_dir: [cn.pattern_annual_gain_AGB_planted_forest_non_mangrove],
        cn.annual_gain_AGB_natrl_forest_dir: [cn.pattern_annual_gain_AGB_natrl_forest],

        cn.annual_gain_BGB_mangrove_dir: [cn.pattern_annual_gain_BGB_mangrove],
        cn.annual_gain_BGB_planted_forest_non_mangrove_dir: [cn.pattern_annual_gain_BGB_planted_forest_non_mangrove],
        cn.annual_gain_BGB_natrl_forest_dir: [cn.pattern_annual_gain_BGB_natrl_forest],

        cn.cumul_gain_AGCO2_mangrove_dir: [cn.pattern_cumul_gain_AGCO2_mangrove],
        cn.cumul_gain_AGCO2_planted_forest_non_mangrove_dir: [cn.pattern_cumul_gain_AGCO2_planted_forest_non_mangrove],
        cn.cumul_gain_AGCO2_natrl_forest_dir: [cn.pattern_cumul_gain_AGCO2_natrl_forest],

        cn.cumul_gain_BGCO2_mangrove_dir: [cn.pattern_cumul_gain_BGCO2_mangrove],
        cn.cumul_gain_BGCO2_planted_forest_non_mangrove_dir: [cn.pattern_cumul_gain_BGCO2_planted_forest_non_mangrove],
        cn.cumul_gain_BGCO2_natrl_forest_dir: [cn.pattern_cumul_gain_BGCO2_natrl_forest]
    }


    # List of output directories and output file name patterns
    output_dir_list = [cn.annual_gain_AGB_BGB_all_types_dir, cn.cumul_gain_AGCO2_BGCO2_all_types_dir]
    output_pattern_list = [cn.pattern_annual_gain_AGB_BGB_all_types, cn.pattern_cumul_gain_AGCO2_BGCO2_all_types]


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


    # For multiprocessing
    # Count/4 seems to pretty consistently use about 390 GB memory on an r4.16xlarge (not so much of an initial peak).
    if cn.count == 96:
        processes = 19   # 18 processors = 690 GB peak; 18 = 690 GB max; 19 = XXX GB max
    else:
        processes = int(cn.count/5)
    uu.print_log('Cumulative gain all forest types max processors=', processes)
    pool = multiprocessing.Pool(processes)
    pool.map(partial(merge_cumulative_annual_gain_all_forest_types.gain_merge, output_pattern_list=output_pattern_list,
                     sensit_type=sensit_type), tile_id_list)
    pool.close()
    pool.join()

    # # For single processor use
    # for tile_id in tile_id_list:
    #     merge_cumulative_annual_gain_all_forest_types.gain_merge(tile_id, output_pattern_list, sensit_type)


    # Uploads output tiles to s3
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

    mp_merge_cumulative_annual_gain_all_forest_types(sensit_type=sensit_type, tile_id_list=tile_id_list,
                                                     run_date=run_date)