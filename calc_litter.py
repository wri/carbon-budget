
import subprocess
import datetime
import os

import get_extent

# 1 time, download all landcover tiles
# 1 time, download climate zone, 1 file.
# make vrt

# copy biomass tile

# get extent

# clip climate zone

# resample climate zone to .00025

# clip landcover

# resample landcover

####### climate zones
# boreal dry = 8
# boreal moist = 7
# cold temperate, dry = 4
# cold temperate moist = 3
# warm temp dry = 2
# warm temp moist = 1

# tropical = 9, 10, 11, 12

####### landcover
# needleleaf evergreen = 1
# broadleaf diciduous = 4


def calc_litter(tile_id):
    start = datetime.datetime.now()
    print "-------TILE ID: {}".format(tile_id)
    print "copy down biomass tile"
    biomass_tile = '{}_biomass.tif'.format(tile_id)
    copy_bio = ['aws', 's3', 'cp', 's3://WHRC-carbon/global_27m_tiles/redo_tiles/{}.tif'.format(tile_id), biomass_tile]
    subprocess.check_call(copy_bio)

    print "get extent of biomass tile"
    xmin, ymin, xmax, ymax = get_extent.get_extent(biomass_tile)

    print "clip climate zone"
    climate_zone = 'climate_zone.tif'
    climate_zone_tile = "{}_climatezone.tif".format(tile_id)
    print climate_zone_tile
    
    clip_climatezone = ['gdal_translate', '-co', 'COMPRESS=LZW', '-projwin', str(xmin), str(ymax), str(xmax), str(ymin), climate_zone, climate_zone_tile]
    subprocess.check_call(clip_climatezone)

    print "resampling climate zone"
    resampled_climatezone = "{}_res_climatezone.tif".format(tile_id)
    resample_climatezone = ['gdal_translate', '-co', 'COMPRESS=LZW', '-tr', '.00025', '.00025', climate_zone_tile, resampled_climatezone]
    subprocess.check_call(resample_climatezone)

    print "clip landcover"
    landcover_vrt = 'landcover.vrt'
    landcover_tile = "{}_landcover.tif".format(tile_id)
    clip_landcover = ['gdal_translate', '-co', 'COMPRESS=LZW', '-projwin', str(xmin), str(ymax), str(xmax), str(ymin), landcover_vrt, landcover_tile]
    subprocess.check_call(clip_landcover)

    print "resampling landcover"
    resampled_landcover = "{}_res_landcover.tif".format(tile_id)
    resample_landcover = ['gdal_translate', '-co', 'COMPRESS=LZW', '-tr', '.00025', '.00025', landcover_tile, resampled_landcover]
    subprocess.check_call(resample_landcover)

    # send 1) resampled climate zone 2) resampled landcover to "create_litter_tile.cpp"

    print 'writing litter tile'
    litter_tile = '{}_litter.tif'.format(tile_id)
    litter_tiles_cmd = ['./litter_stock.exe', biomass_tile, resampled_climatezone, resampled_landcover, litter_tile]
    subprocess.check_call(litter_tiles_cmd)

    print 'uploading litter tile to s3'
    copy_littertile = ['aws', 's3', 'cp', litter_tile, 's3://gfw-files/sam/carbon_budget/litter/']
    subprocess.check_call(copy_littertile)

    print "deleting intermediate data"
    tiles_to_remove = [biomass_tile, landcover_tile, resampled_landcover, climate_zone_tile, resampled_climatezone, litter_tile]

    for tile in tiles_to_remove:
        try:
            os.remove(tile)
        except:
            pass

    print "elapsed time: {}".format(datetime.datetime.now() - start)
