import subprocess
import datetime
import os


def calc_bgb(tile_id):
    start = datetime.datetime.now()

    print "copy down biomass tile"
    biomass_tile = '{}_biomass.tif'.format(tile_id)
    copy_bio = ['wget', 'http://s3.amazonaws.com/WHRC-carbon/global_27m_tiles/redo_tiles/{}.tif'.format(tile_id),
                '-O', biomass_tile]

    subprocess.check_call(copy_bio)

    print 'writing carbon tile for {}'.format(tile_id)
    c_tile = '{}_carbon.tif'.format(tile_id)
    c_tiles_cmd = ['./c_stock.exe', biomass_tile, bgb_tile]
    subprocess.check_call(c_tiles_cmd)

    # print 'uploading belowground biomass tile to s3'
    copy_carbontile = ['aws', 's3', 'cp', c_tile, 's3://gfw-files/sam/carbon_budget/carbon/']
    subprocess.check_call(copy_carbontile)

    print "deleting intermediate data"
    tiles_to_remove = [biomass_tile, c_tile]

    for tile in tiles_to_remove:
        try:
            os.remove(tile)
        except:
            pass

    print "elapsed time: {}".format(datetime.datetime.now() - start)