'''
Converts 250m AGB2000 rasters (tropics only) from Sassan Saatchi (NASA JPL) into Hansen tiles.
There is no Saatchi_biomass_prep.py because this script just uses the utility warp_to_Hansen.
'''

import multiprocessing
import datetime
from multiprocessing.pool import Pool
from functools import partial
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def main ():

    no_upload = False

    # Create the output log
    uu.initiate_log()

    os.chdir(cn.docker_base_dir)

    # The list of tiles to iterate through
    tile_id_list = uu.tile_list_s3(cn.WHRC_biomass_2000_unmasked_dir)
    # tile_id_list = ["00N_000E", "00N_050W", "00N_060W", "00N_010E", "00N_020E", "00N_030E", "00N_040E", "10N_000E", "10N_010E", "10N_010W", "10N_020E", "10N_020W"] # test tiles
    # tile_id_list = ['00N_110E'] # test tile
    uu.print_log(tile_id_list)
    uu.print_log(f'There are {str(len(tile_id_list))} tiles to process', '\n')

    # By definition, this script is for the biomass swap analysis (replacing WHRC AGB with Saatchi/JPL AGB)
    sensit_type = 'biomass_swap'

    # Downloads a pan-tropical raster that has the erroneous integer values in the oceans removed
    uu.s3_file_download(cn.JPL_raw_dir, cn.JPL_raw_name, sensit_type)

    # Converts the Saatchi AGB vrt to Hansen tiles
    source_raster = cn.JPL_raw_name
    out_pattern = cn.pattern_JPL_unmasked_processed
    dt = 'Float32'
    pool = multiprocessing.Pool(cn.count-5)  # count-5 peaks at 320GB of memory
    pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt),
             tile_id_list)

    # Checks if each tile has data in it. Only tiles with data are uploaded.
    upload_dir = cn.JPL_processed_dir
    pattern = cn.pattern_JPL_unmasked_processed
    pool = multiprocessing.Pool(cn.count - 5)  # count-5 peaks at 410GB of memory
    pool.map(partial(uu.check_and_upload, upload_dir=upload_dir, pattern=pattern), tile_id_list)


if __name__ == '__main__':
    main()
