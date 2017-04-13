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

    # send biomass to "create_bgb_tile.cpp"
    print 'writing below ground biomass tile for {}'.format(tile_id)
    if not os.path.exists('bgb'):
        os.mkdir('bgb')
    bgb_tile = 'bgb/{}_bgb.tif'.format(tile_id)
    bgb_tiles_cmd = ['./bgb_stock.exe', biomass_tile, bgb_tile]
    subprocess.check_call(bgb_tiles_cmd)

    # print 'uploading belowground biomass tile to s3'
    # copy_bgbtile = ['aws', 's3', 'cp', bgb_tile, 's3://gfw-files/sam/carbon_budget/belowgroundbiomass/']
    # subprocess.check_call(copy_bgbtile)

    print "deleting intermediate data"
    tiles_to_remove = [biomass_tile]

    for tile in tiles_to_remove:
        try:
            os.remove(tile)
        except:
            pass

    print "elapsed time: {}".format(datetime.datetime.now() - start)