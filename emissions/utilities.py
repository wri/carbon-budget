import subprocess

def download(carbon_pool_files):
    for carbon_file in carbon_pool_files:
        print "downloading {}".format(carbon_file)
        tile = '{0}_{1}.tif'.format(tile_id, carbon_file)
        download_tile = ['aws', 's3', 'cp', 's3://gfw-files/sam/carbon_budget/{0}/{1}_{0}.tif'.format(carbon_file, tile_id), biomass_tile]
        subprocess.check_call(download_tile)
        

def rasterize_shapefile(shapefiles_to_raterize):
    
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
        
        
def resample_raster(rasters_to_resample):

    for raster in rasters_to_resample:
        
        clipped_raster = '{0}_{1}.tif'.format(tile_id, raster)
        clip = ['gdal_translate', '-projwin', str(xmin), str(ymax), str(xmax), str(ymin), '-co', 'COMPRESS=LZW', raster, clipped_raster]
        
        subprocess.check_call(clip)

        print "resampling {}".format(raster)
        resampled_raster = '{0}_res_{1}.tif'.format(tile_id, raster)
        resample = clip + ['-tr', '.00025', '.00025']
        
        subprocess.check_call(resample)