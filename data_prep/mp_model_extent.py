'''
This script creates a binary raster of the model extent at the pixel level.
The model extent is ((TCD2000>0 AND WHRC AGB2000>0) OR Hansen gain=1 OR mangrove AGB2000>0) NOT IN pre-2000 plantations
The rest of the model uses this to mask its extent.
For biomass_swap sensitivity analysis, NASA JPL AGB 2000 replaces WHRC 2000.
For legal_Amazon_loss sensitivity analysis, PRODES 2000 forest extent replaces Hansen tree cover 2000 and Hansen gain
pixels and mangrove pixels outside of (PRODES extent AND WHRC AGB) are not included.
'''


import multiprocessing
import tracemalloc
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
sys.path.append(os.path.join(cn.docker_app,'data_prep'))
import model_extent


def mp_model_extent(sensit_type, tile_id_list, run_date = None, no_upload = None):

    os.chdir(cn.docker_base_dir)

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # List of tiles to run in the model. Which biomass tiles to use depends on sensitivity analysis
        if sensit_type == 'biomass_swap':
            tile_id_list = uu.tile_list_s3(cn.JPL_processed_dir, sensit_type)
        elif sensit_type == 'legal_Amazon_loss':
            tile_id_list = uu.tile_list_s3(cn.Brazil_forest_extent_2000_processed_dir, sensit_type)
        else:
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
                    cn.plant_pre_2000_processed_dir: [cn.pattern_plant_pre_2000]
    }

    if sensit_type == 'legal_Amazon_loss':
        download_dict[cn.Brazil_forest_extent_2000_processed_dir] = [cn.pattern_Brazil_forest_extent_2000_processed]
    else:
        download_dict[cn.tcd_dir] = [cn.pattern_tcd]

    if sensit_type == 'biomass_swap':
        download_dict[cn.JPL_processed_dir] = [cn.pattern_JPL_unmasked_processed]
    else:
        download_dict[cn.WHRC_biomass_2000_unmasked_dir] = [cn.pattern_WHRC_biomass_2000_unmasked]

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
    # Only done if output upload is enabled.
    if run_date is not None and no_upload is not None:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, run_date)

    # Creates a single filename pattern to pass to the multiprocessor call
    pattern = output_pattern_list[0]

    # This configuration of the multiprocessing call is necessary for passing multiple arguments to the main function
    # It is based on the example here: http://spencerimp.blogspot.com/2015/12/python-multiprocess-with-multiple.html
    if cn.count == 96:
        if sensit_type == 'biomass_swap':
            processes = 38
        else:
            processes = 45 # 30 processors = 480 GB peak (sporadic decreases followed by sustained increases);
            # 36 = 550 GB peak; 40 = 590 GB peak; 42 = 631 GB peak; 45 = XXX GB peak
    else:
        processes = 3
    uu.print_log('Model extent processors=', processes)
    pool = multiprocessing.Pool(processes)
    pool.map(partial(model_extent.model_extent, pattern=pattern, sensit_type=sensit_type, no_upload=no_upload), tile_id_list)
    pool.close()
    pool.join()

    # # For single processor use
    # for tile_id in tile_id_list:
    #     model_extent.model_extent(tile_id, pattern, sensit_type, no_upload)


    output_pattern = output_pattern_list[0]
    if cn.count <= 2:  # For local tests
        processes = 1
        uu.print_log(
            "Checking for empty tiles of {0} pattern with {1} processors using light function...".format(output_pattern, processes))
        pool = multiprocessing.Pool(processes)
        pool.map(partial(uu.check_and_delete_if_empty_light, output_pattern=output_pattern), tile_id_list)
        pool.close()
        pool.join()
    else:
        processes = 58  # 50 processors = 620 GB peak; 55 = 640 GB; 58 = 650 GB (continues to increase very slowly several hundred tiles in)
        uu.print_log("Checking for empty tiles of {0} pattern with {1} processors...".format(output_pattern, processes))
        pool = multiprocessing.Pool(processes)
        pool.map(partial(uu.check_and_delete_if_empty, output_pattern=output_pattern), tile_id_list)
        pool.close()
        pool.join()


    # If no_upload flag is not activated (by choice or by lack of AWS credentials), output is uploaded
    if not no_upload:

        uu.upload_final_set(output_dir_list[0], output_pattern_list[0])


if __name__ == '__main__':

    # The arguments for what kind of model run is being run (standard conditions or a sensitivity analysis) and
    # the tiles to include
    parser = argparse.ArgumentParser(
        description='Create tiles of the pixels included in the model (model extent)')
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

    # Disables upload to s3 if no AWS credentials are found in environment
    if not uu.check_aws_creds():
        no_upload = True

    # Create the output log
    uu.initiate_log(tile_id_list=tile_id_list, sensit_type=sensit_type, run_date=run_date, no_upload=no_upload)

    # tracemalloc.start()

    # Checks whether the sensitivity analysis and tile_id_list arguments are valid
    uu.check_sensit_type(sensit_type)
    tile_id_list = uu.tile_id_list_check(tile_id_list)

    mp_model_extent(sensit_type=sensit_type, tile_id_list=tile_id_list, run_date=run_date, no_upload=no_upload)

    # current, peak = tracemalloc.get_traced_memory()
    # print(f"Current memory usage is {current / 10 ** 6}MB; Peak was {peak / 10 ** 6}MB")
    # tracemalloc.stop()

