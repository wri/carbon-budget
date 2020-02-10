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

    # Files to download for this script.
    download_dict = {cn.Mekong_loss_raw_dir: [cn.pattern_Mekong_loss_raw]}

    # List of tiles that could be run. This list is only used to create the FIA region tiles if they don't already exist.
    tile_id_list = uu.tile_list_s3(cn.WHRC_biomass_2000_unmasked_dir)
    # tile_id_list = ["00N_000E", "00N_050W", "00N_060W", "00N_010E", "00N_020E", "00N_030E", "00N_040E", "10N_000E", "10N_010E", "10N_010W", "10N_020E", "10N_020W"] # test tiles
    # tile_id_list = ['50N_130W'] # test tiles
    print tile_id_list
    print "There are {} tiles to process".format(str(len(tile_id_list))) + "\n"


    # By definition, this script is for US-specific removals
    sensit_type = 'Mekong_loss'


    uu.s3_folder_download(cn.Mekong_loss_raw_dir, '.', sensit_type)

    count = multiprocessing.cpu_count()

    annual_loss_list = glob.glob('Loss_20*tif')

    print "Creating first year of loss Hansen tiles for Mekong region..."
    # Converts the region shapefile to Hansen tiles
    pool = multiprocessing.Pool(count/2)
    pool.map(Mekong_loss.recode_tiles, annual_loss_list)

    loss_composite = "Mekong_loss_2001_2015.tif"
    cmd = ['gdal_merge.py', '-o', loss_composite, '-co', 'COMPRESS=LZW', '-a_nodata', '0', '-ot', 'Byte',
           "Mekong_loss_recoded_15.tif", "Mekong_loss_recoded_14.tif", "Mekong_loss_recoded_13.tif",
           "Mekong_loss_recoded_12.tif", "Mekong_loss_recoded_11.tif", "Mekong_loss_recoded_10.tif",
           "Mekong_loss_recoded_09.tif", "Mekong_loss_recoded_08.tif", "Mekong_loss_recoded_07.tif",
           "Mekong_loss_recoded_06.tif", "Mekong_loss_recoded_05.tif", "Mekong_loss_recoded_04.tif",
           "Mekong_loss_recoded_03.tif", "Mekong_loss_recoded_02.tif", "Mekong_loss_recoded_01.tif"]
    subprocess.check_call(cmd)

    uu.mp_warp_to_Hansen(tile_id_list, loss_composite, cn.pattern_Mekong_loss_processed, 'Byte')

    uu.check_and_upload(tile_id_list, cn.Mekong_loss_processed_dir, cn.pattern_Mekong_loss_processed)


if __name__ == '__main__':
    main()