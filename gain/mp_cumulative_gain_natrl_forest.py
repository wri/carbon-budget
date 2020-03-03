### This script calculates the cumulative above and belowground CO2 gain in non-mangrove, non-planted natural forest pixels from 2001-2015.
### It multiplies the annual biomass gain rate by the number of years of gain by the biomass-to-carbon conversion and C to CO2 conversion.

import multiprocessing
import cumulative_gain_natrl_forest
import argparse
from functools import partial
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def mp_cumulative_gain_natrl_forest(sensit_type, tile_id_list, run_date = None):

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # List of tiles to run in the model
        tile_id_list = uu.tile_list_s3(cn.WHRC_biomass_2000_non_mang_non_planted_dir, sensit_type)

    print tile_id_list
    print "There are {} tiles to process".format(str(len(tile_id_list))) + "\n"


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
    for key, values in download_dict.iteritems():
        dir = key
        pattern = values[0]
        uu.s3_flexible_download(dir, pattern, '.', sensit_type, tile_id_list)


    # If the model run isn't the standard one, the output directory and file names are changed
    if sensit_type != 'std':
        print "Changing output directory and file name pattern based on sensitivity analysis"
        output_dir_list = uu.alter_dirs(sensit_type, output_dir_list)
        output_pattern_list = uu.alter_patterns(sensit_type, output_pattern_list)

    # If the script is called from the full model run script, a date is provided.
    # This replaces the date in constants_and_names.
    if run_date is not None:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, run_date)


    # Calculates cumulative aboveground carbon gain in non-mangrove planted forests
    # Processors=26 peaks at 400 - 450 GB of memory, which works on an r4.16xlarge (different runs had different maxes)
    count = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(26)
    pool.map(partial(cumulative_gain_natrl_forest.cumulative_gain_AGCO2, output_pattern_list=output_pattern_list,
                     sensit_type=sensit_type), tile_id_list)

    # Calculates cumulative belowground carbon gain in non-mangrove planted forests
    # Processors=26 peaks at 400 - 450 GB of memory, which works on an r4.16xlarge (different runs had different maxes)
    pool = multiprocessing.Pool(26)
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

    # The argument for what kind of model run is being done: standard conditions or a sensitivity analysis run
    parser = argparse.ArgumentParser(
        description='Create tiles of the number of years of carbon gain for mangrove forests')
    parser.add_argument('--model-type', '-t', required=True,
                        help='{}'.format(cn.model_type_arg_help))
    parser.add_argument('--tile_id_list', '-l', required=True,
                        help='List of tile ids to use in the model. Should be of form 00N_110E or all.')
    args = parser.parse_args()
    sensit_type = args.model_type
    tile_id_list = args.tile_id_list

    # Checks whether the sensitivity analysis and tile_id_list arguments are valid
    uu.check_sensit_type(sensit_type)
    tile_id_list = uu.tile_id_list_check(tile_id_list)

    mp_cumulative_gain_natrl_forest(sensit_type=sensit_type, tile_id_list=tile_id_list)