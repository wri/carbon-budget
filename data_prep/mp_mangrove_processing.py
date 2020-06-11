### Creates Hansen-style tiles for aboveground mangrove biomass (Mg/ha) from Lola Fatoyinbo's country
### mangrove data.
### Output tiles conform to the dimensions, resolution, and other properties of Hansen loss tiles.

import multiprocessing
import sys
import argparse
from functools import partial
import os
import subprocess
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def mp_mangrove_processing(tile_id_list, run_date = None):

    os.chdir(cn.docker_base_dir)
    sensit_type = 'std'

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
    subprocess.check_call(cmd)

    # Creates vrt for the Saatchi biomass rasters
    mangrove_vrt = 'mangrove_biomass.vrt'
    os.system('gdalbuildvrt {} *.tif'.format(mangrove_vrt))

    # Converts the mangrove AGB vrt into Hansen tiles
    source_raster = mangrove_vrt
    out_pattern = cn.pattern_mangrove_biomass_2000
    dt = 'float32'
    pool = multiprocessing.Pool(int(cn.count/4))
    pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt), tile_id_list)

    # # For single processor use, for testing purposes
    # for tile_id in tile_id_list:
    #
    #     mangrove_processing.create_mangrove_tiles(tile_id, source_raster, out_pattern)

    # Checks if each tile has data in it. Only tiles with data are uploaded.
    upload_dir = cn.mangrove_biomass_2000_dir
    pattern = cn.pattern_mangrove_biomass_2000
    pool = multiprocessing.Pool(cn.count - 5)
    pool.map(partial(uu.check_and_upload, upload_dir=upload_dir, pattern=pattern), tile_id_list)


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

    mp_mangrove_processing(tile_id_list=tile_id_list, run_date=run_date)