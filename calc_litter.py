import subprocess
import datetime
import os

import get_extent


def calc_litter(tile_id):
    start = datetime.datetime.now()
    print "/n-------TILE ID: {}".format(tile_id)
    print "copy down biomass tile"
    biomass_tile = '{}_biomass.tif'.format(tile_id)
    copy_bio = ['aws', 's3', 'cp', 's3://WHRC-carbon/global_27m_tiles/redo_tiles/{}.tif'.format(tile_id), biomass_tile]
    subprocess.check_call(copy_bio)

    print "get extent of biomass tile"
    xmin, ymin, xmax, ymax = get_extent.get_extent(biomass_tile)

    print "rasterizing eco zone"
    fao_eco_zones = 'fao_ecozones_bor_tem_tro.shp'
    rasterized_eco_zone_tile = "{}_ecozone.tif".format(tile_id)
    rasterize = ['gdal_rasterize', '-co', 'COMPRESS=LZW', '-te', str(xmin), str(ymin), str(xmax), str(ymax),
    '-tr', '0.00025', '0.00025', '-ot', 'Byte', '-a', 'recode', '-a_nodata',
    '0', fao_eco_zones, rasterized_eco_zone_tile]
    subprocess.check_call(rasterize)

    print "clipping srtm"
    tile_srtm = '{}_srtm.tif'.format(tile_id)
    srtm = 'srtm.vrt'
    clip_srtm = ['gdal_translate', '-projwin', str(xmin), str(ymax), str(xmax), str(ymin), '-co', 'COMPRESS=LZW', srtm, tile_srtm]
    subprocess.check_call(clip_srtm)

    print "resampling srtm"
    tile_res_srtm = '{}_res_srtm.tif'.format(tile_id)
    resample = ['gdal_translate', '-co', 'COMPRESS=LZW', '-tr', '.00025', '.00025', tile_srtm, tile_res_srtm]
    subprocess.check_call(resample)

    # grab precip tiles
    print "clip precip"
    precip_raster = 'add_30s_precip.tif'
    clipped_precip_tile = '{}_clip_precip.tif'.format(tile_id)
    clip_precip_tile = ['gdal_translate', '-projwin', str(xmin), str(ymax), str(xmax), str(ymin), '-co', 'COMPRESS=LZW', precip_raster, clipped_precip_tile]
    subprocess.check_call(clip_precip_tile)

    print "resample precip"
    resample_precip_tile = '{}_res_precip.tif'.format(tile_id)
    resample_precip = ['gdal_translate', '-co', 'COMPRESS=LZW', '-tr', '.00025', '.00025', clipped_precip_tile, resample_precip_tile]
    subprocess.check_call(resample_precip)
    # send 1) biomass 2) rasterized climate zone 3) elevation 4) precip to "create_deadwood_tile.cpp"
    # output is a tile matching res/extent of biomass, each pixel is mg deadwood biomass /ha

    print 'writing litter tile'
    litter_tile = '{}_litter.tif'.format(tile_id)
    litter_tiles_cmd = ['./litter_stock.exe', biomass_tile, resampled_ecozone, tile_res_srtm, resample_precip_tile,
                          litter_tile]
    subprocess.check_call(litter_tiles_cmd)

    print 'uploading litter tile to s3'
    copy_littertile = ['aws', 's3', 'cp', litter_tile, 's3://gfw-files/sam/carbon_budget/litter/']
    #subprocess.check_call(copy_littertile)

    print "deleting intermediate data"
    tiles_to_remove = [litter_tile, resample_precip_tile, clipped_precip_tile, biomass_tile, resampled_ecozone, tile_res_srtm, tile_srtm, rasterized_eco_zone_tile]

    for tile in tiles_to_remove:
        try:
            print "test"
            #os.remove(tile)
        except:
            pass

    print "elapsed time: {}".format(datetime.datetime.now() - start)
