import subprocess
import datetime
import os

import get_extent


def calc_deadwood(tile_id):
    start = datetime.datetime.now()
    print "/n-------TILE ID: {}".format(tile_id)
    print "copy down biomass tile"
    file_to_include = '*{}.tif'.format(tile_id)
    copy_bio = ['aws', 's3', 'cp', 's3://WHRC-carbon/global_27m_tiles/final_global_27m_tiles/', '.', '--exclude', '*', '--include', file_to_include, '--recursive']

    subprocess.check_call(copy_bio)

    print "get extent of biomass tile"
    xmin, ymin, xmax, ymax = get_extent.get_extent(biomass_tile)

    print "rasterizing eco zone"
    fao_eco_zones = 'fao_ecozones.shp'
    rasterized_eco_zone_tile = "{}_ecozone.tif".format(tile_id)
    rasterize = ['gdal_rasterize', '-co', 'COMPRESS=LZW', '-te', str(xmin), str(ymin), str(xmax), str(ymax),
    '-tr', '0.008', '0.008', '-ot', 'Byte', '-a', 'recode', '-a_nodata',
    '0', fao_eco_zones, rasterized_eco_zone_tile]
    subprocess.check_call(rasterize)

    print "resampling eco zone"
    resampled_ecozone =  "{}_res_ecozone.tif".format(tile_id)
    resample_ecozone = ['gdal_translate', '-co', 'COMPRESS=LZW', '-tr', '.00025', '.00025', rasterized_eco_zone_tile, resampled_ecozone]
    subprocess.check_call(resample_ecozone)

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

    print 'writing deadwood tile'
    deadwood_tile = '{}_deadwood.tif'.format(tile_id)
    deadwood_tiles_cmd = ['./dead_wood_c_stock.exe', biomass_tile, resampled_ecozone, tile_res_srtm, resample_precip_tile,
                          deadwood_tile]
    subprocess.check_call(deadwood_tiles_cmd)

    print 'uploading deadwood tile to s3'
    copy_deadwoodtile = ['aws', 's3', 'cp', deadwood_tile, 's3://gfw-files/sam/carbon_budget/carbon_061417/deadwood/']
    subprocess.check_call(copy_deadwoodtile)

    print "deleting intermediate data"
    tiles_to_remove = [deadwood_tile, resample_precip_tile, clipped_precip_tile, biomass_tile, resampled_ecozone, tile_res_srtm, tile_srtm, rasterized_eco_zone_tile]

    for tile in tiles_to_remove:
        try:
            print "test"
            #os.remove(tile)
        except:
            pass

    print "elapsed time: {}".format(datetime.datetime.now() - start)
