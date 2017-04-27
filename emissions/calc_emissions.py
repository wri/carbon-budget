import subprocess
import datetime
import os

import get_extent


def calc_emissions(tile_id):
    start = datetime.datetime.now()
    
    print "/n-------TILE ID: {}".format(tile_id)
    
    carbon_pool_files = ['bgc', 'carbon', 'deadwood', 'soil', 'litter']
    
    for carbon_file in carbon_pool_files:
    
        print "downloading {}".format(carbon_file)
        tile = '{0}_{1}.tif'.format(tile_id, carbon_file)
        download_tile = ['aws', 's3', 'cp', 's3://gfw-files/sam/carbon_budget/{0}/{1}_{0}.tif'.format(carbon_file, tile_id), biomass_tile]
        subprocess.check_call(download_tile)

    print "get extent of biomass tile"
    xmin, ymin, xmax, ymax = get_extent.get_extent(biomass_tile)

    print "rasterize eco zone and ifl"
    shapefiles_to_raterize = ['fao_ecozones_bor_temp_tro', 'ifl_2000']
    for shapefile in shapefiles_to_raterize:
        rasterized_tile = "{0}_{1}.tif".format(tile_id, shapefile)
        rasterize = ['gdal_rasterize', '-co', 'COMPRESS=LZW', '-te', str(xmin), str(ymin), str(xmax), str(ymax),
        '-tr', '0.008', '0.008', '-ot', 'Byte', '-a', 'recode', '-a_nodata',
        '0', shapefile + ".shp", rasterized_tile]
        subprocess.check_call(rasterize)

        print "resampling eco zone and ifl"
        resampled_tile =  "{0}_res_{1}.tif".format(tile_id, shapefile)
        resample = ['gdal_translate', '-co', 'COMPRESS=LZW', '-tr', '.00025', '.00025', rasterized_tile, resampled_tile]
        subprocess.check_call(resample)

    rasters_to_resample = ['peatdrainage', 'hwsd_histosoles', 'forest_model', 'climate_zone']
    
    for raster in rasters_to_resample:
    
        print "clipping {}".format(raster)
        
        clipped_raster = '{0}_{1}.tif'.format(tile_id, raster)

        clip = ['gdal_translate', '-projwin', str(xmin), str(ymax), str(xmax), str(ymin), '-co', 'COMPRESS=LZW', raster, clipped_raster]
        subprocess.check_call(clip)

        print "resampling {}".format(raster)
        
        resampled_raster = '{0}_res_{1}.tif'.format(tile_id, raster)
        
        resample = ['gdal_translate', '-co', 'COMPRESS=LZW', '-tr', '.00025', '.00025', clipped_raster, resampled_raster]
        
        subprocess.check_call(resample)

    print 'writing deadwood tile'
    deadwood_tile = '{}_deadwood.tif'.format(tile_id)
    deadwood_tiles_cmd = ['./dead_wood_c_stock.exe', biomass_tile, resampled_ecozone, tile_res_srtm, resample_precip_tile,
                          deadwood_tile]
    #subprocess.check_call(deadwood_tiles_cmd)

    print 'uploading deadwood tile to s3'
    copy_deadwoodtile = ['aws', 's3', 'cp', deadwood_tile, 's3://gfw-files/sam/carbon_budget/deadwood/']
    #subprocess.check_call(copy_deadwoodtile)

    print "deleting intermediate data"
    tiles_to_remove = [deadwood_tile, resample_precip_tile, clipped_precip_tile, biomass_tile, resampled_ecozone, tile_res_srtm, tile_srtm, rasterized_eco_zone_tile]

    for tile in tiles_to_remove:
        try:
            print "test"
            #os.remove(tile)
        except:
            pass

    print "elapsed time: {}".format(datetime.datetime.now() - start)
