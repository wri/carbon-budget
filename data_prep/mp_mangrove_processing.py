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

def main ():

    # The argument for what kind of model run is being done: standard conditions or a sensitivity analysis run
    parser = argparse.ArgumentParser(description='Create tiles of the number of years of carbon gain for mangrove forests')
    parser.add_argument('--model-type', '-t', required=True,
                        help='{}'.format(cn.model_type_arg_help))
    args = parser.parse_args()
    sensit_type = args.model_type
    # Checks whether the sensitivity analysis argument is valid
    uu.check_sensit_type(sensit_type)

    # Iterates through all possible tiles (not just WHRC biomass tiles) to create mangrove biomass tiles that don't have analogous WHRC tiles
    tile_id_list = uu.tile_list_s3(cn.pixel_area_dir)
    # tile_id_list = ['00N_000E', '00N_100E', '00N_110E'] # test tile
    print tile_id_list

    # Downloads zipped raw mangrove files
    uu.s3_file_download(os.path.join(cn.mangrove_biomass_raw_dir, cn.mangrove_biomass_raw_file), '.', 'std')

    # Unzips mangrove images into a flat structure (all tifs into main folder using -j argument)
    # NOTE: Unzipping some tifs (e.g., Australia, Indonesia) takes a very long time, so don't worry if the script appears to stop on that.
    cmd = ['unzip', '-o', '-j', cn.mangrove_biomass_raw_file]
    subprocess.check_call(cmd)

    # Creates vrt for the Saatchi biomass rasters
    mangrove_vrt = 'mangrove_biomass.vrt'
    os.system('gdalbuildvrt {} *.tif'.format(mangrove_vrt))

    count = multiprocessing.cpu_count()

    # Converts the mangrove AGB vrt into Hansen tiles
    source_raster = mangrove_vrt
    out_pattern = cn.pattern_mangrove_biomass_2000
    dt = 'float32'
    pool = multiprocessing.Pool(count/4)
    pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt), tile_id_list)

    # # For single processor use, for testing purposes
    # for tile_id in tile_id_list:
    #
    #     mangrove_processing.create_mangrove_tiles(tile_id, source_raster, out_pattern)

    # Checks if each tile has data in it. Only tiles with data are uploaded.
    upload_dir = cn.mangrove_biomass_2000_dir
    pattern = cn.pattern_mangrove_biomass_2000
    pool = multiprocessing.Pool(count - 5)
    pool.map(partial(uu.check_and_upload, upload_dir=upload_dir, pattern=pattern), tile_id_list)


if __name__ == '__main__':
    main()