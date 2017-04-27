import subprocess
import gdal
gdal.UseExceptions() 

def download(carbon_pool_files, tile_id):
    for carbon_file in carbon_pool_files:
        print "downloading {}".format(carbon_file)
        tile = '{0}_{1}.tif'.format(tile_id, carbon_file)
        download_tile = ['aws', 's3', 'cp', 's3://gfw-files/sam/carbon_budget/{0}/{1}'.format(carbon_file, tile), tile]
        subprocess.check_call(download_tile)
        

def rasterize_shapefile(shapefiles_to_raterize, tile_id, coords):
    rasterized_files = []

    for shapefile_dict in shapefiles_to_raterize:
        
        for shapefile in shapefile_dict:
     
            rvalue = shapefile_dict[shapefile]
            rasterized_tile = "{0}_{1}.tif".format(tile_id, shapefile)
            rasterize = ['gdal_rasterize', '-co', 'COMPRESS=LZW', '-tr', '0.008', '0.008', '-ot', 'Byte', '-a', rvalue, '-a_nodata',
            '0', shapefile + ".shp", rasterized_tile]
            rasterize += coords
            
            subprocess.check_call(rasterize)

            print "resampling eco zone and ifl"
            resampled_tile =  "{0}_res_{1}.tif".format(tile_id, shapefile)
            resample = ['gdal_translate', '-co', 'COMPRESS=LZW', '-tr', '.00025', '.00025', rasterized_tile, resampled_tile]
            subprocess.check_call(resample)
            rasterized_files.append(resampled_tile)

    return rasterized_files

def resample_raster(rasters_to_resample, tile_id, coords):

    resampled_tiles = []

    for raster in rasters_to_resample:
        
        clipped_raster = '{0}_{1}.tif'.format(tile_id, raster)
	
        clip = ['gdal_translate', '-co', 'COMPRESS=LZW', '-epo', raster + ".tif", clipped_raster]
        clip += coords

	try:
            subprocess.check_call(clip)

            print "resampling {}".format(raster)
            resampled_raster = '{0}_res_{1}.tif'.format(tile_id, raster)
            resample = clip + ['-tr', '.00025', '.00025']
        
            subprocess.check_call(resample)

	    resampled_tiles.append(resampled_raster)
        except:
            print "{} doesn't cover this extent".format(raster)

    return resampled_tiles
