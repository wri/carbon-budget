import subprocess
import datetime
import os
import glob


def calc_bgc(tile_id):
    start = datetime.datetime.now()

    print "copy down biomass tile"
    file_to_include = '*{}.tif'.format(tile_id)
    copy_bio = ['aws', 's3', 'cp', 's3://WHRC-carbon/global_27m_tiles/final_global_27m_tiles/', '.', '--exclude', '*', '--include', file_to_include, '--recursive']

    subprocess.check_call(copy_bio)
    biomass_tile = glob.glob(file_to_include)[0]

    # send biomass to "create_bgb_tile.cpp"
    print 'writing below ground carbon tile for {}'.format(tile_id)
    bgc_tile = '{}_bgc.tif'.format(tile_id)
    bgc_tiles_cmd = ['./bgc_stock.exe', biomass_tile, bgc_tile]
    subprocess.check_call(bgc_tiles_cmd)

    print 'uploading belowground carbon tile to s3'
    copy_bgctile = ['aws', 's3', 'cp', bgc_tile, 's3://gfw-files/sam/carbon_budget/carbon_061417/bgc/']
    subprocess.check_call(copy_bgctile)

    print "deleting intermediate data"
    tiles_to_remove = [biomass_tile]

    for tile in tiles_to_remove:
        try:
            os.remove(tile)
        except:
            pass

    print "elapsed time: {}".format(datetime.datetime.now() - start)
