
import multiprocessing
from functools import partial
import pandas as pd
import datetime
import argparse
from subprocess import Popen, PIPE, STDOUT, check_call
import os
import sys
sys.path.append('/usr/local/app/data_prep/')
import model_extent
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu


def mp_model_extent(sensit_type, tile_id_list, run_date = None):

    os.chdir(cn.docker_base_dir)

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # List of tiles to run in the model
        tile_id_list = uu.create_combined_tile_list(cn.WHRC_biomass_2000_unmasked_dir,
                                             cn.mangrove_biomass_2000_dir,
                                             cn.gain_dir, cn.tcd_dir
                                             )

    uu.print_log(tile_id_list)
    uu.print_log("There are {} tiles to process".format(str(len(tile_id_list))) + "\n")


    # Files to download for this script.
    download_dict = {
                    cn.mangrove_biomass_2000_dir: [cn.pattern_mangrove_biomass_2000],
                    cn.gain_dir: [cn.pattern_gain],
                    cn.tcd_dir: [cn.pattern_tcd],
                    cn.WHRC_biomass_2000_unmasked_dir: [cn.pattern_WHRC_biomass_2000_unmasked],
                    cn.plant_pre_2000_processed_dir: [cn.pattern_plant_pre_2000]
    }

    # # Adds the correct loss tile to the download dictionary depending on the model run
    # if sensit_type == 'legal_Amazon_loss':
    #     download_dict[cn.Brazil_annual_loss_processed_dir] = [cn.pattern_Brazil_annual_loss_processed]
    # elif sensit_type == 'Mekong_loss':
    #     download_dict[cn.Mekong_loss_processed_dir] = [cn.pattern_Mekong_loss_processed]
    # else:
    #     download_dict[cn.loss_dir] = ['']


    # List of output directories and output file name patterns
    output_dir_list = [cn.model_extent_dir]
    output_pattern_list = [cn.pattern_model_extent]

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

    # This configuration of the multiprocessing call is necessary for passing multiple arguments to the main function
    # It is based on the example here: http://spencerimp.blogspot.com/2015/12/python-multiprocess-with-multiple.html
    # With processes=30, peak usage was about 350 GB using WHRC AGB.
    # processes=26 maxes out above 480 GB for biomass_swap, so better to use fewer than that.
    if cn.count == 96:
        processes = 30
        # 20 processors=500 GB peak; 28=660 GB peak (stops @ 600 for a while, then increases slowly);
        # 32=shut off at 730 peak (stops @ 686 for a while, then increases slowly);
        # 30=710 GB peak (stops @ 653 for a while, then increases slowly)
    else:
        processes = 26
    uu.print_log('Removal model forest extent processors=', processes)
    pool = multiprocessing.Pool(processes)
    pool.map(partial(model_extent_forest_type.model_extent_forest_type, pattern=pattern, sensit_type=sensit_type), tile_id_list)
    pool.close()
    pool.join()

    # # For single processor use
    # for tile_id in tile_id_list:
    #     model_extent.model_extent(tile_id, pattern, sensit_type)

    # Uploads output tiles to s3
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

    mp_model_extent(sensit_type=sensit_type, tile_id_list=tile_id_list, run_date=run_date)

