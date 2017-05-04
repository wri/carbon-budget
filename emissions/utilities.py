import subprocess
import gdal

def download(carbon_pool_files, tile_id):
    for carbon_file in carbon_pool_files:
        print "downloading {}".format(carbon_file)

        tile = '{0}_{1}.tif'.format(tile_id, carbon_file)
        download_tile = ['aws', 's3', 'cp', 's3://gfw-files/sam/carbon_budget/{0}/{1}'.format(carbon_file, tile), tile]
        subprocess.check_call(download_tile)

        
def wgetloss(tile_id):
    print "download hansen loss tile"
    cmd = ['wget', r'http://glad.geog.umd.edu/Potapov/GFW_2015/tiles/{}.tif'.format(tile_id), '-O' '{}_loss.tif'.format(tile_id)]
    subprocess.check_call(cmd)

def rasterize_shapefile(shapefiles_to_raterize, tile_id, coords):
    rasterized_files = []

    for shapefile_dict in shapefiles_to_raterize:
        
        for shapefile in shapefile_dict:
            print "rasterizing {}".format(shapefile)
            rvalue = shapefile_dict[shapefile]
            rasterized_tile = "{0}_{1}.tif".format(tile_id, shapefile)
            rasterize = ['gdal_rasterize', '-co', 'COMPRESS=LZW', '-tr', '0.008', '0.008', '-ot', 'Byte', '-a', rvalue, '-a_nodata',
            '0', shapefile + ".shp", rasterized_tile]
            rasterize += coords
            
            subprocess.check_call(rasterize)

            print "resampling {}".format(rasterized_tile)

            resampled_tile =  "{0}_res_{1}.tif".format(tile_id, shapefile)
            resample = ['gdal_translate', '-co', 'COMPRESS=LZW', '-a_nodata', '0', '-tr', '.00025', '.00025', rasterized_tile, resampled_tile]
            subprocess.check_call(resample)
            rasterized_files.append(resampled_tile)

    return rasterized_files

def resample_raster(rasters_to_resample, tile_id, coords):

    resampled_tiles = []

    for raster in rasters_to_resample:
        try:
        
            print "clipping {}".format(raster)
            clipped_raster = '{0}_{1}.tif'.format(tile_id, raster)
            base_cmd = ['gdal_translate', '-ot', 'Byte', '-co', 'COMPRESS=LZW', '-a_nodata', '0', raster + ".tif", clipped_raster]
            clip_cmd = base_cmd + coords
            print clip_cmd
            subprocess.check_call(clip_cmd)

            print "resampling {}".format(raster)  
            resampled_raster = '{0}_res_{1}.tif'.format(tile_id, raster)
            resample_cmd = ['gdal_translate', '-co', 'COMPRESS=LZW', '-tr', '.00025', '.00025', '-a_nodata', '0', clipped_raster, resampled_raster]
            print resample_cmd
            subprocess.check_call(resample_cmd)

            resampled_tiles.append(resampled_raster)
        
        except:
            print "failed"

    return resampled_tiles

def download_burned_area(year_window_dict):
    winpath = year_window_dict['winpath']
    year = year_window_dict['year']
    ftp_path = 'ftp://ba1.geog.umd.edu/Collection6/TIFF/{0}/{1}/'.format(winpath, year)
    download_cmd = ['wget', '-r', '-l1', '--ftp-user=user', '--ftp-password=burnt_data', '--no-parent', '-A', '*burndate.tif', ftp_path]

    #subprocess.check_call(download_cmd)
