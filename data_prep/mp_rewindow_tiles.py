'''
Rewindows tiles from 40000x1 pixels to 160x160 pixels for use in aggregate map creation.
Specifically, does tiles that are not model outputs but are used in aggregate map creation:
tree cover density, pixel area, Hansen gain, and mangrove biomass.
This must be done before the model is run so that the aggregate maps can be created successfully
(aggregate map pixels are the sum of the rewindowed 160x160 pixel windows).
'''


import multiprocessing
from subprocess import Popen, PIPE, STDOUT, check_call
from functools import partial
import datetime
import argparse
import os
import glob
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu


def mp_rewindow_tiles(tile_id_list, run_date = None, no_upload = None):

    os.chdir(cn.docker_base_dir)

    # Sensitivity analysis model type is not used in this script
    sensit_type = 'std'

    # Files to download for this script
    download_dict = {
             cn.pixel_area_dir: [cn.pattern_pixel_area],
             cn.tcd_dir: [cn.pattern_tcd],
             cn.gain_dir: [cn.pattern_gain],
             cn.mangrove_biomass_2000_dir: [cn.pattern_mangrove_biomass_2000]
             }

    uu.print_log("Layers to process are:", download_dict)

    # List of output directories. Mut match order of output patterns.
    output_dir_list = [cn.pixel_area_rewindow_dir, cn.tcd_rewindow_dir,
                       cn.gain_rewindow_dir, cn.mangrove_biomass_2000_rewindow_dir]

    # List of output patterns. Must match order of output directories.
    output_pattern_list = [cn.pattern_pixel_area_rewindow, cn.pattern_tcd_rewindow,
                       cn.pattern_gain_rewindow, cn.pattern_mangrove_biomass_2000_rewindow]

    # A date can optionally be provided.
    # This replaces the date in constants_and_names.
    # Only done if output upload is enabled.
    if run_date is not None and no_upload is not None:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, run_date)


    # Iterates through the types of tiles to be processed
    for dir, download_pattern in list(download_dict.items()):

        download_pattern_name = download_pattern[0]

        # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
        # If a full model run is specified, the correct set of tiles for the particular script is listed
        if tile_id_list == 'all':
            # List of tiles to run in the model
            tile_id_list = uu.tile_list_s3(dir, sensit_type)

        uu.s3_flexible_download(dir, download_pattern_name, cn.docker_base_dir, sensit_type, tile_id_list)

        uu.print_log("There are {0} tiles to process for pattern {1}".format(str(len(tile_id_list)), download_pattern_name) + "\n")
        uu.print_log("Processing:", dir, "; ", download_pattern_name)


        # Converts the 10x10 degree Hansen tiles that are in windows of 40000x1 pixels to windows of 160x160 pixels
        if cn.count == 96:
            processes = 54  # 40 processors = 480 GB peak; 62 = >750 GB peak
        else:
            processes = 8
        uu.print_log('Rewindow max processors=', processes)
        pool = multiprocessing.Pool(processes)
        pool.map(partial(uu.rewindow, download_pattern_name=download_pattern_name,
                         no_upload=no_upload), tile_id_list)
        pool.close()
        pool.join()

        # # For single processor use
        # for tile_id in tile_id_list:
        #
        #     uu.rewindow(tile_id, download_pattern_name, no_upload)


    # If no_upload flag is not activated (by choice or by lack of AWS credentials), output is uploaded
    if not no_upload:

        uu.print_log("Tiles processed. Uploading to s3 now...")
        for i in range(0, len(output_dir_list)):
            uu.upload_final_set(output_dir_list[i], output_pattern_list[i])



if __name__ == '__main__':

    # The argument for what kind of model run is being done: standard conditions or a sensitivity analysis run
    parser = argparse.ArgumentParser(
        description='Creates 160x160 pixel rewindowed basic input tiles (TCD, gain, mangroves, pixel area)')
    parser.add_argument('--tile_id_list', '-l', required=True,
                        help='List of tile ids to use in the model. Should be of form 00N_110E or 00N_110E,00N_120E or all.')
    parser.add_argument('--run-date', '-d', required=False,
                        help='Date of run. Must be format YYYYMMDD.')
    parser.add_argument('--no-upload', '-nu', action='store_true',
                       help='Disables uploading of outputs to s3')
    args = parser.parse_args()
    tile_id_list = args.tile_id_list
    run_date = args.run_date
    no_upload = args.no_upload

    # Disables upload to s3 if no AWS credentials are found in environment
    if not uu.check_aws_creds():
        no_upload = True

    # Create the output log
    uu.initiate_log(tile_id_list=tile_id_list, run_date=run_date, no_upload=no_upload)

    # Checks whether the tile_id_list argument is valid
    tile_id_list = uu.tile_id_list_check(tile_id_list)

    mp_rewindow_tiles(tile_id_list=tile_id_list, run_date=run_date, no_upload=no_upload)