import subprocess
import datetime
import os

import get_extent


def calc_soil(tile_id):
    start = datetime.datetime.now()
    print "/n-------TILE ID: {}".format(tile_id)
    print "copy down biomass tile"
    biomass_tile = '{}_biomass.tif'.format(tile_id)
    copy_bio = ['aws', 's3', 'cp', 's3://WHRC-carbon/global_27m_tiles/redo_tiles/{}.tif'.format(tile_id), biomass_tile]
    subprocess.check_call(copy_bio)

    print "get extent of biomass tile"
    xmin, ymin, xmax, ymax = get_extent.get_extent(biomass_tile)

    print "clip soil"
    soil_raster = 'hwsd_oc_final.tif'
    clip_soil_tile = '{}_clip_soil.tif'.format(tile_id)
    clip_soil = ['gdal_translate', '-projwin', str(xmin), str(ymax), str(xmax), str(ymin), '-tr', '.00025', '.00025', '-co', 'COMPRESS=LZW', soil_raster, clip_soil_tile]
    subprocess.check_call(clip_soil)
    
    print 'uploading deadwood tile to s3'
    copy_deadwoodtile = ['aws', 's3', 'cp', clip_soil_tile, 's3://gfw-files/sam/carbon_budget/soil/']
    subprocess.check_call(copy_deadwoodtile)

    print "deleting intermediate data"
    tiles_to_remove = [clip_soil_tile, biomass_tile]

    for tile in tiles_to_remove:
        try:
            os.remove(tile)
        except:
            pass

    print "elapsed time: {}".format(datetime.datetime.now() - start)
