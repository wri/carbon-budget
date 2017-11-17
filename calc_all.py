import subprocess
import datetime
import os

import get_extent
import glob
import shutil

def calc_all(tile_id):
    start = datetime.datetime.now()
    
    print "copy down biomass tile"
    biomass_tile = '{}_biomass.tif'.format(tile_id)
    copy_bio = ['aws', 's3', 'cp', 's3://WHRC-carbon/global_27m_tiles/final_global_27m_tiles/biomass_10x10deg/{}'.format(biomass_tile), '.']
    subprocess.check_call(copy_bio)
    
    print "get extent of biomass tile"
    xmin, ymin, xmax, ymax = get_extent.get_extent(biomass_tile)

    print "clip soil"
    soil_raster = 'hwsd_oc_final.tif'
    clip_soil_tile = '{}_soil.tif'.format(tile_id)
    clip_soil = ['gdal_translate', '-projwin', str(xmin), str(ymax), str(xmax), str(ymin), '-tr', '.00025', '.00025', '-co', 'COMPRESS=LZW', '-a_nodata', '0', soil_raster, clip_soil_tile]
    subprocess.check_call(clip_soil)

    print 'uploading soil tile to s3'
    copy_soil_tile = ['aws', 's3', 'cp', clip_soil_tile, 's3://gfw-files/sam/carbon_budget/data_inputs/soil/']
    #subprocess.check_call(copy_soil_tile)
    
    print "rasterizing eco zone"
    fao_eco_zones = 'fao_ecozones_bor_tem_tro.shp'
    rasterized_eco_zone_tile = "{}_fao_ecozones_bor_tem_tro.tif".format(tile_id)
    rasterize = ['gdal_rasterize', '-co', 'COMPRESS=LZW', '-te', str(xmin), str(ymin), str(xmax), str(ymax),
    '-tr', '0.008', '0.008', '-ot', 'Byte', '-a', 'recode', '-a_nodata',
    '0', fao_eco_zones, rasterized_eco_zone_tile]
    subprocess.check_call(rasterize)

    print "resampling eco zone"
    resampled_ecozone =  "{}_res_fao_ecozones_bor_tem_tro.tif".format(tile_id)
    resample_ecozone = ['gdal_translate', '-co', 'COMPRESS=LZW', '-tr', '.00025', '.00025', rasterized_eco_zone_tile, resampled_ecozone]
    subprocess.check_call(resample_ecozone)

    print "upload ecozone to input data"
    cmd = ['aws', 's3', 'cp', resampled_ecozone, 's3://gfw-files/sam/carbon_budget/data_inputs2/fao_ecozones_bor_tem_tro/']
    #subprocess.check_call(cmd)
    
    print "clipping srtm"
    tile_srtm = '{}_srtm.tif'.format(tile_id)
    srtm = 'srtm.vrt'
    clip_srtm = ['gdal_translate', '-projwin', str(xmin), str(ymax), str(xmax), str(ymin), '-co', 'COMPRESS=LZW', srtm, tile_srtm]
    subprocess.check_call(clip_srtm)

    print "resampling srtm"
    tile_res_srtm = '{}_res_srtm.tif'.format(tile_id)
    resample = ['gdal_translate', '-co', 'COMPRESS=LZW', '-tr', '.00025', '.00025', tile_srtm, tile_res_srtm]
    subprocess.check_call(resample)

    print "upload srtm to input data"
    cmd = ['aws', 's3', 'cp', tile_res_srtm, 's3://gfw-files/sam/carbon_budget/data_inputs2/srtm/']
    #subprocess.check_call(cmd)
    
    print "clip precip"
    precip_raster = 'add_30s_precip.tif'
    clipped_precip_tile = '{}_clip_precip.tif'.format(tile_id)
    clip_precip_tile = ['gdal_translate', '-projwin', str(xmin), str(ymax), str(xmax), str(ymin), '-co', 'COMPRESS=LZW', precip_raster, clipped_precip_tile]
    subprocess.check_call(clip_precip_tile)

    print "resample precip"
    resample_precip_tile = '{}_res_precip.tif'.format(tile_id)
    resample_precip = ['gdal_translate', '-co', 'COMPRESS=LZW', '-tr', '.00025', '.00025', clipped_precip_tile, resample_precip_tile]
    subprocess.check_call(resample_precip)

    print "upload precip to input data"
    cmd = ['aws', 's3', 'cp', resample_precip_tile, 's3://gfw-files/sam/carbon_budget/data_inputs2/precip/']
    #subprocess.check_call(cmd)
    
    print 'writing carbon, bgc, deadwood, litter, total'
    calc_all_cmd = ['./calc_all.exe', tile_id]
    subprocess.check_call(calc_all_cmd)

    # print 'uploading tiles to s3'
    # tile_types  = ['carbon', 'bgc', 'deadwood', 'litter', 'soil', 'total_carbon']
    # for tile in tile_types:
        # if tile == 'total_carbon':
            # tile_name = "{}_totalc.tif".format(tile_id)
        # else:
            # tile_name = "{0}_{1}.tif".format(tile_id, tile)
            
        # tile_dest = 's3://gfw-files/sam/carbon_budget/carbon_061417/{}/'.format(tile)

        # upload_tile = ['aws', 's3', 'cp', tile_name, tile_dest]
        # subprocess.check_call(upload_tile)

    # print "deleting intermediate data"
    # tiles_to_remove = ['{0}_res_fao_ecozones_bor_tem_tro.tif'.format(tile_id), '{}_srtm.tif'.format(tile_id), '{}_totalc.tif'.format(tile_id), biomass_tile, '{}_soil.tif'.format(tile_id), '{}_deadwood.tif'.format(tile_id), '{}_litter.tif'.format(tile_id), '{}_bgc.tif'.format(tile_id), '{}_carbon.tif'.format(tile_id), '{}_total.tif'.format(tile_id), clip_srtm, tile_res_srtm, clipped_precip_tile, resample_precip_tile]

    # for tile in tiles_to_remove:
        # try:
            # os.remove(tile)
        # except:
            # pass

    print "elapsed time: {}".format(datetime.datetime.now() - start)


