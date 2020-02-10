'''
'''

import multiprocessing
from functools import partial
import Mekong_loss
import subprocess
import os
import glob
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def main ():

    # List of tiles that could be run. This list is only used to create the FIA region tiles if they don't already exist.
    tile_id_list = uu.tile_list_s3(cn.WHRC_biomass_2000_unmasked_dir)
    # tile_id_list = ['50N_130W'] # test tiles
    print tile_id_list
    print "There are {} tiles to process".format(str(len(tile_id_list))) + "\n"


    # By definition, this script is for US-specific removals
    sensit_type = 'Mekong_loss'


    # # Downloads the Mekong loss folder. Each year of loss has its own raster
    # uu.s3_folder_download(cn.Mekong_loss_raw_dir, '.', sensit_type)
    #
    count = multiprocessing.cpu_count()
    #
    # # The list of all annual loss rasters
    # annual_loss_list = glob.glob('Loss_20*tif')
    # print annual_loss_list
    #
    # print "Creating first year of loss Hansen tiles for Mekong region..."
    # # Recodes raw loss rasters with their loss year (for model years only)
    pool = multiprocessing.Pool(count/2)
    # pool.map(Mekong_loss.recode_tiles, annual_loss_list)
    #
    # # Makes a single raster of all first loss year pixels in the Mekong (i.e. where loss occurred in multiple years,
    # # the earlier loss gets)
    # print "Merging all loss years within model range..."
    loss_composite = "Mekong_loss_2001_2015.tif"
    # cmd = ['gdal_merge.py', '-o', loss_composite, '-co', 'COMPRESS=LZW', '-a_nodata', '0', '-ot', 'Byte',
    #        "Mekong_loss_recoded_2015.tif", "Mekong_loss_recoded_2014.tif", "Mekong_loss_recoded_2013.tif",
    #        "Mekong_loss_recoded_2012.tif", "Mekong_loss_recoded_2011.tif", "Mekong_loss_recoded_2010.tif",
    #        "Mekong_loss_recoded_2009.tif", "Mekong_loss_recoded_2008.tif", "Mekong_loss_recoded_2007.tif",
    #        "Mekong_loss_recoded_2006.tif", "Mekong_loss_recoded_2005.tif", "Mekong_loss_recoded_2004.tif",
    #        "Mekong_loss_recoded_2003.tif", "Mekong_loss_recoded_2002.tif", "Mekong_loss_recoded_2001.tif"]
    # subprocess.check_call(cmd)

    # Creates Hansen tiles out of the composite Mekong loss
    source_raster = loss_composite
    out_pattern = cn.pattern_Mekong_loss_processed
    dt = 'Byte'
    pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt), tile_id_list)

    # Only uploads tiles that actually have Mekong loss in them
    uu.check_and_upload(tile_id_list, cn.Mekong_loss_processed_dir, cn.pattern_Mekong_loss_processed)


if __name__ == '__main__':
    main()