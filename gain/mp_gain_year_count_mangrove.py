### Creates tiles in which each mangrove pixel is the number of years that trees are believed to have been growing there between 2001 and 2015.
### It is based on the annual Hansen loss data and the 2000-2012 Hansen gain data (as well as the 2000 tree cover density data).
### First it calculates rasters of gain years for mangrove pixels that had loss only, gain only, neither loss nor gain, and both loss and gain.
### The gain years for each of these conditions are calculated according to rules that are found in the function called by the multiprocessor commands.
### More gdalcalc commands can be run at the same time than gdalmerge so that's why the number of processors being used is higher
### for the first four processing steps (which use gdalcalc).
### At this point, those rules are the same as for non-mangrove natural forests, except that no change pixels don't use tcd as a factor.
### Then it combines those four rasters into a single gain year raster for each tile.
### This is one of the mangrove inputs for the carbon gain model.
### If different input rasters for loss (e.g., 2001-2017) and gain (e.g., 2000-2018) are used, the year count constants in constants_and_names.py must be changed.

import multiprocessing
import gain_year_count_mangrove
import argparse
from functools import partial
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def mp_gain_year_count_mangrove(sensit_type, tile_id_list, run_date = None):

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # Lists the tiles that have both mangrove biomass and FAO ecozone information because both of these are necessary for
        # calculating mangrove gain
        mangrove_biomass_tile_list = uu.tile_list_s3(cn.mangrove_biomass_2000_dir)
        ecozone_tile_list = uu.tile_list_s3(cn.cont_eco_dir)
        tile_id_list = list(set(mangrove_biomass_tile_list).intersection(ecozone_tile_list))

    print tile_id_list
    print "There are {} tiles to process".format(str(len(tile_id_list))) + "\n"


    # Files to download for this script. 'true'/'false' says whether the input directory and pattern should be
    # changed for a sensitivity analysis. This does not need to change based on what run is being done;
    # this assignment should be true for all sensitivity analyses and the standard model.
    download_dict = {
        cn.mangrove_biomass_2000_dir: [cn.pattern_mangrove_biomass_2000],
        cn.loss_dir: [''],
        cn.gain_dir: [cn.pattern_gain]
    }


    # List of output directories and output file name patterns
    output_dir_list = [cn.gain_year_count_mangrove_dir]
    output_pattern_list = [cn.pattern_gain_year_count_mangrove]


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


    # Creates gain year count tiles using only pixels that had only loss. Worked on a r4.16xlarge machine.
    count = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(count/2)
    pool.map(gain_year_count_mangrove.create_gain_year_count_loss_only, tile_id_list)

    pool = multiprocessing.Pool(count/2)
    if sensit_type == 'maxgain':
        # Creates gain year count tiles using only pixels that had only gain
        pool.map(gain_year_count_mangrove.create_gain_year_count_gain_only_maxgain, tile_id_list)
    else:
        # Creates gain year count tiles using only pixels that had only gain
        pool.map(gain_year_count_mangrove.create_gain_year_count_gain_only_standard, tile_id_list)

    # Creates gain year count tiles using only pixels that had neither loss nor gain pixels
    # count/3 maxes out at 250 GB
    pool = multiprocessing.Pool(count/2)
    pool.map(gain_year_count_mangrove.create_gain_year_count_no_change, tile_id_list)

    # count/3 maxes out at 255 GB
    pool = multiprocessing.Pool(count/2)
    if sensit_type == 'maxgain':
        # Creates gain year count tiles using only pixels that had both loss and gain pixels
        pool.map(gain_year_count_mangrove.create_gain_year_count_loss_and_gain_maxgain, tile_id_list)
    else:
        # Creates gain year count tiles using only pixels that had both loss and gain pixels
        pool.map(gain_year_count_mangrove.create_gain_year_count_loss_and_gain_standard, tile_id_list)

    # Creates a single filename pattern to pass to the multiprocessor call
    pattern = output_pattern_list[0]

    # Merges the four above gain year count tiles for each Hansen tile into a single output tile.
    # Using a r4.16xlarge machine, calling one sixth of the processors uses just about all the memory without going over
    # (e.g., about 450 GB out of 480 GB).
    count = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(count/6)
    pool.map(partial(gain_year_count_mangrove.create_gain_year_count_merge, pattern=pattern), tile_id_list)
    pool.close()
    pool.join()

    # # For single processor use
    # for tile_id in tile_id_list:
    #     gain_year_count_mangrove.create_gain_year_count_loss_only(tile_id)
    #
    # for tile_id in tile_id_list:
    #     if sensit_type == 'maxgain':
    #         gain_year_count_mangrove.create_gain_year_count_gain_only_maxgain(tile_id)
    #     else:
    #         gain_year_count_mangrove.create_gain_year_count_gain_only_standard(tile_id)
    #
    # for tile_id in tile_id_list:
    #     gain_year_count_mangrove.create_gain_year_count_no_change(tile_id)
    #
    # for tile_id in tile_id_list:
    #     if sensit_type == 'maxgain':
    #         gain_year_count_mangrove.create_gain_year_count_loss_and_gain_maxgain(tile_id)
    #     else:
    #         gain_year_count_mangrove.create_gain_year_count_loss_and_gain_standard(tile_id)
    #
    # for tile_id in tile_id_list:
    #     gain_year_count_mangrove.create_gain_year_count_merge(tile_id, output_pattern_list[0])

    # Intermediate output tiles for checking outputs
    uu.upload_final_set(output_dir_list[0], "growth_years_loss_only")
    uu.upload_final_set(output_dir_list[0], "growth_years_gain_only")
    uu.upload_final_set(output_dir_list[0], "growth_years_no_change")
    uu.upload_final_set(output_dir_list[0], "growth_years_loss_and_gain")

    # Uploads final output tiles to s3
    for i in range(0, len(output_dir_list)):
        uu.upload_final_set(output_dir_list[i], output_pattern_list[i])


if __name__ == '__main__':

    # The arguments for what kind of model run is being run (standard conditions or a sensitivity analysis) and
    # the tiles to include
    parser = argparse.ArgumentParser(
        description='Create tiles of the number of years of carbon gain for mangrove forests')
    parser.add_argument('--model-type', '-t', required=True,
                        help='{}'.format(cn.model_type_arg_help))
    parser.add_argument('--tile_id_list', '-l', required=True,
                        help='List of tile ids to use in the model. Should be of form 00N_110E or 00N_110E,00N_120E or all.')
    args = parser.parse_args()
    sensit_type = args.model_type
    tile_id_list = args.tile_id_list

    # Checks whether the sensitivity analysis and tile_id_list arguments are valid
    uu.check_sensit_type(sensit_type)
    tile_id_list = uu.tile_id_list_check(tile_id_list)

    mp_gain_year_count_mangrove(sensit_type=sensit_type, tile_id_list=tile_id_list)