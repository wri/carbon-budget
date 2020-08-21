'''
Creates tiles of the number of years in which carbon removals occur during the model duration (2001 to 2019 currently).
It is based on the annual Hansen loss data and the 2000-2012 Hansen gain data.
First it separately calculates rasters of gain years for model pixels that had loss only,
gain only, neither loss nor gain, and both loss and gain.
The gain years for each of these conditions are calculated according to rules that are found in the function called by the multiprocessor commands.
The same gain year count rules are applied to all types of forest (mangrove, planted, etc.).
Then it combines those four rasters into a single gain year raster for each tile using rasterio because
summing the arrays using rasterio is faster and uses less memory than combining them with gdalmerge.
If different input rasters for loss (e.g., 2001-2017) and gain (e.g., 2000-2018) are used, the year count constants in constants_and_names.py must be changed.
'''

import multiprocessing
import argparse
import os
import datetime
from functools import partial
import sys
import gain_year_count_all_forest_types
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def mp_gain_year_count_all_forest_types(sensit_type, tile_id_list, run_date = None):

    os.chdir(cn.docker_base_dir)

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # List of tiles to run in the model
        tile_id_list = uu.tile_list_s3(cn.model_extent_dir, sensit_type)

    uu.print_log(tile_id_list)
    uu.print_log("There are {} tiles to process".format(str(len(tile_id_list))) + "\n")

    # Files to download for this script. 'true'/'false' says whether the input directory and pattern should be
    # changed for a sensitivity analysis. This does not need to change based on what run is being done;
    # this assignment should be true for all sensitivity analyses and the standard model.
    download_dict = {
        cn.loss_dir: [cn.pattern_loss],
        cn.gain_dir: [cn.pattern_gain],
        cn.removal_forest_type_dir: [cn.pattern_model_extent]
    }
    
    
    output_dir_list = [cn.gain_year_count_dir]
    output_pattern_list = [cn.pattern_gain_year_count]


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

    # Creates a single filename pattern to pass to the multiprocessor call
    pattern = output_pattern_list[0]

    # Creates gain year count tiles using only pixels that had only loss
    # count/3 maxes out at about 300 GB
    if cn.count == 96:
        processes = 66   # 48 processors = 580 GB peak; 62 = 660 GB peak; 66 = XXX GB peak
    else:
        processes = int(cn.count/2)
    uu.print_log('Planted forest gain year count loss only pixels max processors=', processes)
    pool = multiprocessing.Pool(processes)
    pool.map(gain_year_count_all_forest_types.create_gain_year_count_loss_only, tile_id_list)

    if cn.count == 96:
        processes = 66   # 48 processors = 580 GB peak; 62 = 660 GB peak; 66 = XXX GB peak
    else:
        processes = int(cn.count/2)
    uu.print_log('Planted forest gain year count gain only pixels max processors=', processes)
    pool = multiprocessing.Pool(processes)
    if sensit_type == 'maxgain':
        # Creates gain year count tiles using only pixels that had only gain
        pool.map(gain_year_count_all_forest_types.create_gain_year_count_gain_only_maxgain, tile_id_list)
    else:
        # Creates gain year count tiles using only pixels that had only gain
        pool.map(gain_year_count_all_forest_types.create_gain_year_count_gain_only_standard, tile_id_list)

    # Creates gain year count tiles using only pixels that had neither loss nor gain pixels
    if cn.count == 96:
        processes = 66   # 48 processors = 580 GB peak; 62 = 660 GB peak; 66 = XXX GB peak
    else:
        processes = int(cn.count/2)
    uu.print_log('Planted forest gain year count no change pixels max processors=', processes)
    pool = multiprocessing.Pool(processes)
    pool.map(gain_year_count_all_forest_types.create_gain_year_count_no_change, tile_id_list)

    if cn.count == 96:
        processes = 66   # 48 processors = 580 GB peak; 62 = 660 GB peak; 66 = XXX GB peak
    else:
        processes = int(cn.count/2)
    uu.print_log('Planted forest gain year count loss & gain pixels max processors=', processes)
    pool = multiprocessing.Pool(processes)
    if sensit_type == 'maxgain':
        # Creates gain year count tiles using only pixels that had only gain
        pool.map(gain_year_count_all_forest_types.create_gain_year_count_loss_and_gain_maxgain, tile_id_list)
    else:
        # Creates gain year count tiles using only pixels that had only gain
        pool.map(gain_year_count_all_forest_types.create_gain_year_count_loss_and_gain_standard, tile_id_list)

    # Combines the four above gain year count tiles for each Hansen tile into a single output tile
    if cn.count == 96:
        processes = 28   # 28 processors = XXX GB peak
    if cn.count < 4:
        processes = 1
    else:
        processes = int(cn.count/4)
    uu.print_log('Planted forest gain year count gain merge all combos max processors=', processes)
    pool = multiprocessing.Pool(processes)
    pool.map(partial(gain_year_count_all_forest_types.create_gain_year_count_merge, pattern=pattern, sensit_type=sensit_type), tile_id_list)
    pool.close()
    pool.join()


    # # For single processor use
    # for tile_id in tile_id_list:
    #     gain_year_count_all_forest_types.create_gain_year_count_loss_only(tile_id)
    #
    # for tile_id in tile_id_list:
    #     if sensit_type == 'maxgain':
    #         gain_year_count_all_forest_types.create_gain_year_count_gain_only_maxgain(tile_id)
    #     else:
    #         gain_year_count_all_forest_types.create_gain_year_count_gain_only_standard(tile_id)
    #
    # for tile_id in tile_id_list:
    #     gain_year_count_all_forest_types.create_gain_year_count_no_change(tile_id)
    #
    # for tile_id in tile_id_list:
    #     if sensit_type == 'maxgain':
    #         gain_year_count_all_forest_types.create_gain_year_count_loss_and_gain_maxgain(tile_id)
    #     else:
    #         gain_year_count_all_forest_types.create_gain_year_count_loss_and_gain_standard(tile_id)
    #
    # for tile_id in tile_id_list:
    #     gain_year_count_all_forest_types.create_gain_year_count_merge(tile_id, pattern, sensit_type)


    # Intermediate output tiles for checking outputs
    uu.upload_final_set(output_dir_list[0], "growth_years_loss_only")
    uu.upload_final_set(output_dir_list[0], "growth_years_gain_only")
    uu.upload_final_set(output_dir_list[0], "growth_years_no_change")
    uu.upload_final_set(output_dir_list[0], "growth_years_loss_and_gain")

    # This is the final output used later in the model
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

    mp_gain_year_count_all_forest_types(sensit_type=sensit_type, tile_id_list=tile_id_list, run_date=run_date)