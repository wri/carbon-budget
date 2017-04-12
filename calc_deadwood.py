import subprocess
import datetime

import get_extent


def calc_deadwood(tile_id):
    start = datetime.datetime.now()

    print "copy down biomass tile"
    biomass_tile = '{}_biomass.tif'.format(tile_id)
    copy_bio = ['aws', 's3', 'cp', 's3://WHRC-carbon/global_27m_tiles/redo_tiles/{}.tif'.format(tile_id), biomass_tile]
    subprocess.check_call(copy_bio)

    # get extent
    print "get extent of biomass tile"
    xmin, ymin, xmax, ymax = get_extent.get_extent(biomass_tile)

    print "rasterizing eco zone"
    fao_eco_zones = 'fao_ecozones_reclass.shp'
    rasterized_eco_zone_tile = "{}_ecozone.tif".format(tile_id)
    rasterize = ['gdal_rasterize', '-co', 'COMPRESS=LZW', '-te', str(xmin), str(ymin), str(xmax), str(ymax),
    '-tr', '0.008', '0.008', '-ot', 'Byte', '-a', 'ATTRIBUTE', '-a_nodata',
    '0', fao_eco_zones, rasterized_eco_zone_tile]
    subprocess.check_call(rasterize)

    print "resampling eco zone"
    resampled_ecozone =  "{}_res_ecozone.tif".format(tile_id)
    resample_ecozone = ['gdal_translate', '-co', 'COMPRESS=LZW', '-tr', '.00025', '.00025', rasterized_eco_zone_tile, resampled_ecozone]
    subprocess.check_call(resample_ecozone)

    # tile srtm
    tile_srtm = '{}_srtm.tif'.format(tile_id)
    srtm = 'srtm.vrt'
    clip_srtm = ['gdal_translate', '-projwin', str(xmin), str(ymax), str(xmax), str(ymin), '-co', 'COMPRESS=LZW', srtm, tile_srtm]
    subprocess.check_call(clip_srtm)

    # resample srtm
    tile_res_srtm = '{}_res_srtm.tif'.format(tile_id)
    resample = ['gdal_translate', '-co', 'COMPRESS=LZW', '-tr', '.00025', '.00025', tile_srtm, tile_res_srtm]
    subprocess.check_call(resample)

    # grab precip tiles...not sure which format yet

    # send 1) biomass 2) rasterized climate zone 3) elevation 4) precip to "create_deadwood_tile.cpp"
    print 'writing deadwood tile for {}'.format(tile_id)
    deadwood_tile = '{}_deadwood.tif'.format(tile_id)
    deadwood_tiles_cmd = ['./dead_wood_c_stock.exe', biomass_tile, resampled_ecozone, tile_res_srtm, tile_res_srtm,
                          deadwood_tile]
    subprocess.check_call(deadwood_tiles_cmd)

    # delete intermediate tiles# output is a tile matching res/extent of biomass, each pixel is mg deadwood biomass /ha


    print "elapsed time: {}".format(datetime.datetime.now() - start)