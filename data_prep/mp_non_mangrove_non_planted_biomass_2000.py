### Masks out mangrove and planted forest pixels from WHRC biomass 2000 raster so that
### only non-mangrove, non-planted forest pixels are left of the WHRC biomass 2000 raster

import multiprocessing
from functools import partial
import non_mangrove_non_planted_biomass_2000
import argparse
import datetime
import os
import pandas as pd
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu


def mp_non_mangrove_non_planted_biomass_2000(tile_id_list, run_date = None):

    os.chdir(cn.docker_base_dir)
    sensit_type = 'std'

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # List of tiles to run in the model
        tile_id_list = uu.tile_list_s3(cn.WHRC_biomass_2000_unmasked_dir, sensit_type)

    uu.print_log(tile_id_list)
    uu.print_log("There are {} tiles to process".format(str(len(tile_id_list))) + "\n")


    # Files to download for this script.
    # Mangrove biomass and full-extent planted forests are used to mask out mangrove and planted forests from the natural forests
    download_dict = {
        cn.mangrove_biomass_2000_dir: [cn.pattern_mangrove_biomass_2000],
        cn.annual_gain_AGC_BGC_planted_forest_unmasked_dir: [cn.pattern_annual_gain_AGC_BGC_planted_forest_unmasked]
    }

    # Which biomass tiles to download depends on which model run is being performed
    if sensit_type == 'biomass_swap':   # Uses the JPL AGB tiles for the biomass_swap sensitivity analysis
        download_dict[cn.JPL_processed_dir] = [cn.pattern_JPL_unmasked_processed]
    else:   # Uses the WHRC AGB tiles for all other model runs
        download_dict[cn.WHRC_biomass_2000_unmasked_dir] = [cn.pattern_WHRC_biomass_2000_unmasked]


    # List of output directories and output file name patterns
    output_dir_list = [cn.WHRC_biomass_2000_non_mang_non_planted_dir]
    output_pattern_list = [cn.pattern_WHRC_biomass_2000_non_mang_non_planted]


    # A date can optionally be provided by the full model script or a run of this script.
    # This replaces the date in constants_and_names.
    if run_date is not None:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, run_date)


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


    # Creates a single filename pattern to pass to the multiprocessor call
    pattern = output_pattern_list[0]

    # For multiprocessing. count/2 uses more than 470GB of memory for JPL AGB.
    # processes=26 maxes out at about 420 GB of memory for JPL AGB.
    pool = multiprocessing.Pool(processes=16)
    pool.map(partial(non_mangrove_non_planted_biomass_2000.mask_biomass, pattern=pattern, sensit_type=sensit_type), tile_id_list)
    pool.close()
    pool.join()

    # # For single processor use
    # for tile_id in tile_id_list:
    #     non_mangrove_non_planted_biomass_2000.mask_biomass(tile_id, pattern, sensit_type)

    # Uploads output tiles to s3
    uu.upload_final_set(output_dir_list[0], output_pattern_list[0])


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Create tiles of the annual AGB and BGB gain rates for mangrove forests')
    parser.add_argument('--tile_id_list', '-l', required=True,
                        help='List of tile ids to use in the model. Should be of form 00N_110E or 00N_110E,00N_120E or all.')
    parser.add_argument('--run-date', '-d', required=False,
                        help='Date of run. Must be format YYYYMMDD.')
    args = parser.parse_args()
    tile_id_list = args.tile_id_list
    run_date = args.run_date

    # Create the output log
    uu.initiate_log(tile_id_list=tile_id_list, run_date=run_date)

    mp_non_mangrove_non_planted_biomass_2000(tile_id_list=tile_id_list, run_date=run_date)