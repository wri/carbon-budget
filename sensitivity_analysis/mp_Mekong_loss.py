'''
'''

import multiprocessing
from functools import partial
import Mekong_loss
import datetime
from subprocess import Popen, PIPE, STDOUT, check_call
import os
import glob
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def main ():

    no_upload = False

    # Create the output log
    uu.initiate_log()

    os.chdir(cn.docker_tile_dir)

    # List of tiles that could be run. This list is only used to create the FIA region tiles if they don't already exist.
    tile_id_list = uu.tile_list_s3(cn.WHRC_biomass_2000_unmasked_dir)
    # tile_id_list = ['50N_130W'] # test tiles
    uu.print_log(tile_id_list)
    uu.print_log(f'There are {str(len(tile_id_list))} tiles to process', "\n")


    # Downloads the Mekong loss folder. Each year of loss has its own raster
    uu.s3_folder_download(cn.Mekong_loss_raw_dir, cn.docker_tile_dir, sensit_type)

    # The list of all annual loss rasters
    annual_loss_list = glob.glob('Loss_20*tif')
    uu.print_log(annual_loss_list)

    uu.print_log("Creating first year of loss Hansen tiles for Mekong region...")
    # Recodes raw loss rasters with their loss year (for model years only)
    pool = multiprocessing.Pool(int(cn.count/2))
    pool.map(Mekong_loss.recode_tiles, annual_loss_list)

    # Makes a single raster of all first loss year pixels in the Mekong (i.e. where loss occurred in multiple years,
    # the earlier loss gets)
    uu.print_log("Merging all loss years within model range...")
    loss_composite = "Mekong_loss_2001_2015.tif"
    cmd = ['gdal_merge.py', '-o', loss_composite, '-co', 'COMPRESS=DEFLATE', '-a_nodata', '0', '-ot', 'Byte',
           "Mekong_loss_recoded_2015.tif", "Mekong_loss_recoded_2014.tif", "Mekong_loss_recoded_2013.tif",
           "Mekong_loss_recoded_2012.tif", "Mekong_loss_recoded_2011.tif", "Mekong_loss_recoded_2010.tif",
           "Mekong_loss_recoded_2009.tif", "Mekong_loss_recoded_2008.tif", "Mekong_loss_recoded_2007.tif",
           "Mekong_loss_recoded_2006.tif", "Mekong_loss_recoded_2005.tif", "Mekong_loss_recoded_2004.tif",
           "Mekong_loss_recoded_2003.tif", "Mekong_loss_recoded_2002.tif", "Mekong_loss_recoded_2001.tif"]
    # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        uu.log_subprocess_output(process.stdout)

    # Creates Hansen tiles out of the composite Mekong loss
    source_raster = loss_composite
    out_pattern = cn.pattern_Mekong_loss_processed
    dt = 'Byte'
    pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt),
             tile_id_list)

    # This is necessary for changing NoData values to 0s (so they are recognized as 0s)
    pool.map(Mekong_loss.recode_tiles, tile_id_list)

    # Only uploads tiles that actually have Mekong loss in them
    upload_dir = cn.Mekong_loss_processed_dir
    pattern = cn.pattern_Mekong_loss_processed
    pool.map(partial(uu.check_and_upload, upload_dir=upload_dir, pattern=pattern), tile_id_list)


if __name__ == '__main__':
    main()