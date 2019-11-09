### Creates tiles in which each pixel is the number of years that natural forest trees are believed to have been growing there between 2001 and 2015.
### While it is for use in calculating cumulative non-mangrove non-planted forest gains, it is not limited to the extent of
### non-mangrove non-planted forest gains.
### Rather, it can include plantation and mangrove pixels, but these are filtered out during the calculation of the
### non-mangrove non-planted forest cumulative gains.
### Thus, this is an overly expansive layer.
### It is based on the annual Hansen loss data and the 2000-2012 Hansen gain data (as well as the 2000 tree cover density data).
### First it calculates rasters of gain years for non-mangrove non-planted forest biomass pixels that had loss only, gain only, neither loss nor gain, and both loss and gain.
### The gain years for each of these conditions are calculated according to rules that are found in the function called by the multiprocessor command.
### Then it combines those four rasters into a single gain year raster for each tile.
### Only the merged raster is used later in the model; the 4 intermediates are saved just for checking.
### If different input rasters for loss (e.g., 2001-2017) and gain (e.g., 2000-2018) are used, the constants in create_gain_year_count_natrl_forest.py must be changed.

import multiprocessing
import gain_year_count_natrl_forest
import argparse
from functools import partial
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def main ():

    # Files to download for this script. 'true'/'false' says whether the input directory and pattern should be
    # changed for a sensitivity analysis. This does not need to change based on what run is being done;
    # this assignment should be true for all sensitivity analyses and the standard model.
    download_dict = {
        cn.loss_dir: ['', 'false'],
        cn.gain_dir: [cn.pattern_gain, 'false'],
        cn.tcd_dir: [cn.pattern_tcd, 'false'],
        cn.WHRC_biomass_2000_non_mang_non_planted_dir: [cn.pattern_WHRC_biomass_2000_non_mang_non_planted, 'false'],
        cn.planted_forest_type_unmasked_dir: [cn.pattern_planted_forest_type_unmasked, 'false'],
        cn.mangrove_biomass_2000_dir: [cn.pattern_mangrove_biomass_2000, 'false']
    }


    # List of output directories and output file name patterns
    output_dir_list = [cn.gain_year_count_natrl_forest_dir]
    output_pattern_list = [cn.pattern_gain_year_count_natrl_forest]


    # The list of tiles to iterate through
    tile_id_list = uu.tile_list(cn.WHRC_biomass_2000_non_mang_non_planted_dir)
    # tile_id_list = ["00N_000E", "00N_050W", "00N_060W", "00N_010E", "00N_020E", "00N_030E", "00N_040E", "10N_000E", "10N_010E", "10N_010W", "10N_020E", "10N_020W"] # test tiles
    # tile_id_list = ['00N_110E'] # test tile
    print tile_id_list
    print "There are {} tiles to process".format(str(len(tile_id_list))) + "\n"


    # The argument for what kind of model run is being done: standard conditions or a sensitivity analysis run
    parser = argparse.ArgumentParser(description='Create tiles of the number of years of carbon gain for mangrove forests')
    parser.add_argument('--model-type', '-t', required=True,
                        help='{}'.format(cn.model_type_arg_help))
    args = parser.parse_args()
    sensit_type = args.model_type
    # Checks whether the sensitivity analysis argument is valid
    uu.check_sensit_type(sensit_type)


    # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
    for key, values in download_dict.iteritems():
        dir = key
        pattern = values[0]
        sensit_use = values[1]
        uu.s3_flexible_download(dir, pattern, '.', sensit_type, sensit_use, tile_id_list)


    # If the model run isn't the standard one, the output directory and file names are changed
    if sensit_type != 'std':
        print "Changing output directory and file name pattern based on sensitivity analysis"
        output_dir_list = uu.alter_dirs(sensit_type, output_dir_list)
        output_pattern_list = uu.alter_patterns(sensit_type, output_pattern_list)


    # Creates gain year count tiles using only pixels that had only loss
    # count/3 uses about 220 GB on an r4.16xlarge machine
    # count/2 uses about 330 GB on an r4.16xlarge machine
    count = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=36)
    pool.map(gain_year_count_natrl_forest.create_gain_year_count_loss_only, tile_id_list)

    # processes=36 maxes out at about 200 GB
    pool = multiprocessing.Pool(processes=36)
    if sensit_type == 'maxgain':
        # Creates gain year count tiles using only pixels that had only gain
        # count/2 uses about 200 GB on an r4.16xlarge machine
        pool.map(gain_year_count_natrl_forest.create_gain_year_count_gain_only_maxgain, tile_id_list)
    else:
        # Creates gain year count tiles using only pixels that had only gain
        pool.map(gain_year_count_natrl_forest.create_gain_year_count_gain_only_standard, tile_id_list)

    # Creates gain year count tiles using only pixels that had neither loss nor gain pixels
    # processes/36 maxes out at about 320 GB
    pool = multiprocessing.Pool(processes=36)
    pool.map(gain_year_count_natrl_forest.create_gain_year_count_no_change, tile_id_list)

    # processes/36 maxes out at about 220 GB
    pool = multiprocessing.Pool(processes=36)
    if sensit_type == 'maxgain':
        # Creates gain year count tiles using only pixels that had both loss and gain pixels
        pool.map(gain_year_count_natrl_forest.create_gain_year_count_loss_and_gain_maxgain, tile_id_list)
    else:
        # Creates gain year count tiles using only pixels that had both loss and gain pixels
        pool.map(gain_year_count_natrl_forest.create_gain_year_count_loss_and_gain_standard, tile_id_list)

    # Creates a single filename pattern to pass to the multiprocessor call
    pattern = output_pattern_list[0]

    # Merges the four above gain year count tiles for each Hansen tile into a single output tile
    # count/6 maxes out at about 230 GB
    count = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(count/6)
    pool.map(partial(gain_year_count_natrl_forest.create_gain_year_count_merge, pattern=pattern), tile_id_list)
    pool.close()
    pool.join()

    # # For single processor use
    # for tile_id in tile_id_list:
    #     gain_year_count_natrl_forest.create_gain_year_count_loss_only(tile_id)
    #
    # for tile_id in tile_id_list:
    #     if sensit_type == 'maxgain':
    #         gain_year_count_natrl_forest.create_gain_year_count_gain_only_maxgain(tile_id)
    #     else:
    #         gain_year_count_natrl_forest.create_gain_year_count_gain_only_standard(tile_id)
    #
    # for tile_id in tile_id_list:
    #     gain_year_count_natrl_forest.create_gain_year_count_no_change(tile_id)
    #
    # for tile_id in tile_id_list:
    #     if sensit_type == 'maxgain':
    #         gain_year_count_natrl_forest.create_gain_year_count_loss_and_gain_maxgain(tile_id)
    #     else:
    #         gain_year_count_natrl_forest.create_gain_year_count_loss_and_gain_standard(tile_id)
    #
    # for tile_id in tile_id_list:
    #     gain_year_count_natrl_forest.create_gain_year_count_merge(tile_id, output_pattern_list[0])

    # Intermediate output tiles for checking outputs
    uu.upload_final_set(output_dir_list[0], "growth_years_loss_only")
    uu.upload_final_set(output_dir_list[0], "growth_years_gain_only")
    uu.upload_final_set(output_dir_list[0], "growth_years_no_change")
    uu.upload_final_set(output_dir_list[0], "growth_years_loss_and_gain")

    # This is the final output used later in the model
    uu.upload_final_set(output_dir_list[0], output_pattern_list[0])


if __name__ == '__main__':
    main()