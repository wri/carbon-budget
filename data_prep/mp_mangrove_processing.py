### Creates Hansen-style tiles for aboveground mangrove biomass (Mg/ha) from Lola Fatoyinbo's country
### mangrove data.
### Output tiles conform to the dimensions, resolution, and other properties of Hansen loss tiles.

import multiprocessing
import sys
import argparse
import datetime
from functools import partial
import os
from subprocess import Popen, PIPE, STDOUT, check_call
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def mp_mangrove_processing(tile_id_list, run_date = None, no_upload = None):

    os.chdir(cn.docker_base_dir)

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # List of tiles to run in the model
        tile_id_list = uu.tile_list_s3(cn.pixel_area_dir)

    uu.print_log(tile_id_list)
    uu.print_log("There are {} tiles to process".format(str(len(tile_id_list))) + "\n")


    # Downloads zipped raw mangrove files
    uu.s3_file_download(os.path.join(cn.mangrove_biomass_raw_dir, cn.mangrove_biomass_raw_file), cn.docker_base_dir, 'std')

    # Unzips mangrove images into a flat structure (all tifs into main folder using -j argument)
    # NOTE: Unzipping some tifs (e.g., Australia, Indonesia) takes a very long time, so don't worry if the script appears to stop on that.
    cmd = ['unzip', '-o', '-j', cn.mangrove_biomass_raw_file]
    uu.log_subprocess_output_full(cmd)


    # Creates vrt for the Saatchi biomass rasters
    mangrove_vrt = 'mangrove_biomass.vrt'
    os.system('gdalbuildvrt {} *.tif'.format(mangrove_vrt))

    # Converts the mangrove AGB vrt into Hansen tiles
    source_raster = mangrove_vrt
    out_pattern = cn.pattern_mangrove_biomass_2000
    dt = 'float32'
    processes=int(cn.count/4)
    uu.print_log('Mangrove preprocessing max processors=', processes)
    pool = multiprocessing.Pool(processes)
    pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt,
                     no_upload=no_upload), tile_id_list)

    # # For single processor use, for testing purposes
    # for tile_id in tile_id_list:
    #
    #     mangrove_processing.create_mangrove_tiles(tile_id, source_raster, out_pattern, no_upload)

    # Checks if each tile has data in it. Only tiles with data are uploaded.
    upload_dir = cn.mangrove_biomass_2000_dir
    pattern = cn.pattern_mangrove_biomass_2000
    processes=int(cn.count-5)
    uu.print_log('Mangrove check for data max processors=', processes)
    pool = multiprocessing.Pool(processes)
    pool.map(partial(uu.check_and_upload, upload_dir=upload_dir, pattern=pattern), tile_id_list)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Create tiles of the aboveground carbon density for mangroves')
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

    mp_mangrove_processing(tile_id_list=tile_id_list, run_date=run_date, no_upload=no_upload)