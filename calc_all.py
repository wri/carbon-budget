import subprocess
import datetime
import os

import get_extent


def calc_all(tile_id):
    start = datetime.datetime.now()
    
    print "copy down biomass tile"
    file_to_include = '*{}.tif'.format(tile_id)
    copy_bio = ['aws', 's3', 'cp', 's3://WHRC-carbon/global_27m_tiles/final_global_27m_tiles/', '.', '--exclude', '*', '--include', file_to_include, '--recursive']

    subprocess.check_call(copy_bio)
    
    biomass_tile = glob.glob(file_to_include)[0]

    print "get extent of biomass tile"
    xmin, ymin, xmax, ymax = get_extent.get_extent(biomass_tile)

    print "copy down soil tile, used for total c"
    copy_soil = ['aws', 's3', 'cp', 's3://gfw-files/sam/carbon_budget/soil/{}_soil.tif'.format(tile_id), "."]
    subprocess.check_call(copy_soil)
    
    print "rasterizing eco zone"
    fao_eco_zones = 'fao_ecozones_bor_tem_tro.shp'
    resampled_ecozone = "{}_res_ecozone_bor_tem_tro.tif".format(tile_id)
    rasterize = ['gdal_rasterize', '-co', 'COMPRESS=LZW', '-te', str(xmin), str(ymin), str(xmax), str(ymax),
    '-tr', '0.00025', '0.00025', '-ot', 'Byte', '-a', 'recode', '-a_nodata',
    '0', fao_eco_zones, resampled_ecozone]
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

    print "clip precip"
    precip_raster = 'add_30s_precip.tif'
    clipped_precip_tile = '{}_clip_precip.tif'.format(tile_id)
    clip_precip_tile = ['gdal_translate', '-projwin', str(xmin), str(ymax), str(xmax), str(ymin), '-co', 'COMPRESS=LZW', precip_raster, clipped_precip_tile]
    subprocess.check_call(clip_precip_tile)

    print "resample precip"
    resample_precip_tile = '{}_res_precip.tif'.format(tile_id)
    resample_precip = ['gdal_translate', '-co', 'COMPRESS=LZW', '-tr', '.00025', '.00025', clipped_precip_tile, resample_precip_tile]
    subprocess.check_call(resample_precip)

    print 'writing carbon, bgc, deadwood, litter, total'
    calc_all_cmd = ['./calc_all.exe', tile_id]
    subprocess.check_call(calc_all_cmd)

    print 'uploading tiles to s3'
    tile_types  = ['carbon', 'bgc', 'deadwood', 'litter', 'total_carbon']
    for tile in tile_types:
        if tile == 'total_carbon':
            tile_name = "{}_totalc.tif".format(tile_id)
        else:
            tile_name = "{0}_{1}.tif".format(tile_id, tile)
            
        tile_dest = 's3://gfw-files/sam/carbon_budget/carbon_061417/{}/'.format(tile)

        upload_tile = ['aws', 's3', 'cp', tile_name, tile_dest]
        subprocess.check_call(copy_deadwoodtile)

    print "deleting intermediate data"
    tiles_to_remove = [deadwood_tile, resample_precip_tile, clipped_precip_tile, biomass_tile, resampled_ecozone, tile_res_srtm, tile_srtm, rasterized_eco_zone_tile]

    for tile in tiles_to_remove:
        try:
            os.remove(tile)
        except:
            pass

    print "elapsed time: {}".format(datetime.datetime.now() - start)
