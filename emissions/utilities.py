import subprocess
#import gdal
import multiprocessing
import pandas as pd


def download(carbon_pool_files, tile_id):
    for carbon_file in carbon_pool_files:
        print "downloading {}".format(carbon_file)

        tile = '{0}_{1}.tif'.format(tile_id, carbon_file)
        download_tile = ['aws', 's3', 'cp', 's3://gfw-files/sam/carbon_budget/{0}/{1}'.format(carbon_file, tile), tile]
        subprocess.check_call(download_tile)

        
def wgetloss(tile_id):
    print "download hansen loss tile"
    cmd = ['wget', r'http://glad.geog.umd.edu/Potapov/GFW_2015/tiles/{}.tif'.format(tile_id),
           '-O' '{}_loss.tif'.format(tile_id)]

    subprocess.check_call(cmd)


def rasterize_shapefile(shapefiles_to_raterize, tile_id, coords):
    rasterized_files = []

    for shapefile_dict in shapefiles_to_raterize:
        
        for shapefile in shapefile_dict:
            print "rasterizing {}".format(shapefile)
            rvalue = shapefile_dict[shapefile]
            rasterized_tile = "{0}_{1}.tif".format(tile_id, shapefile)
            rasterize = ['gdal_rasterize', '-co', 'COMPRESS=LZW', '-tr', '0.008', '0.008', '-ot',
                         'Byte', '-a', rvalue, '-a_nodata', '0', shapefile + ".shp", rasterized_tile]
            rasterize += coords
            subprocess.check_call(rasterize)

            print "resampling {}".format(rasterized_tile)

            resampled_tile = "{0}_res_{1}.tif".format(tile_id, shapefile)
            resample = ['gdal_translate', '-co', 'COMPRESS=LZW', '-a_nodata', '0',
                        '-tr', '.00025', '.00025', rasterized_tile, resampled_tile]

            subprocess.check_call(resample)

            rasterized_files.append(resampled_tile)

    return rasterized_files

def clip_raster(raster, tile_id, coords):
    print "clipping {}".format(raster)
    clipped_raster = '{0}_{1}.tif'.format(tile_id, raster)
    input_raster = raster + ".tif"
    base_cmd = ['gdal_translate', '-ot', 'Byte', '-co', 'COMPRESS=LZW', '-a_nodata', '0',
                input_raster, clipped_raster]

    clip_cmd = base_cmd + coords
    print clip_cmd
    subprocess.check_call(clip_cmd)
    return clipped_raster


def resample_raster(raster, tile_id):

    print "resampling {}".format(raster)
    input_raster = raster + ".tif"
    resampled_raster = '{0}_res_{1}.tif'.format(tile_id, raster)
    resample_cmd = ['gdal_translate', '-co', 'COMPRESS=LZW', '-tr', '.00025', '.00025', '-a_nodata',
                    '0', input_raster, resampled_raster]
    subprocess.check_call(resample_cmd)
    return resampled_raster


def resample_clip_raster(rasters_to_resample, tile_id, coords):

    for raster in rasters_to_resample:
        print "resampling/clipping {}".format(raster)
        input_raster = raster + ".tif"
        clipped_raster = '{0}_res_{1}.tif'.format(tile_id, raster)
        base_cmd = ['gdal_translate', '-ot', 'Byte', '-co', 'COMPRESS=LZW', '-a_nodata', '0',
        input_raster, clipped_raster, '-tr', '.00025', '.00025']

        clip_cmd = base_cmd + coords
        print clip_cmd
        subprocess.check_call(clip_cmd)
        
    return clipped_raster




def download_burned_areas(window):
    window = "Win{}".format(window)
    ftp_path = 'ftp://ba1.geog.umd.edu/Collection6/TIFF/{0}/'.format(window)
    download_cmd = ['wget', '-r', '--ftp-user=user', '--ftp-password=burnt_data', '--no-directories', '--no-parent', '-A', '*burndate.tif', ftp_path]
    print download_cmd
    # subprocess.check_call(download_cmd)


def download_allburned_areas():
    
    ftp_path = 'ftp://ba1.geog.umd.edu/Collection6/TIFF/'
    download_cmd = ['wget', '-r', '--ftp-user=user', '--ftp-password=burnt_data', '--no-parent', '-A', '*burndate.tif', ftp_path]
    print download_cmd
    #subprocess.check_call(download_cmd)  


def multiprocess_download(windows):
    window_list = []
    for w in windows:
        if w < 10:
            w = "0{}".format(w)
        w = str(w)
        window_list.append(w)
    print window_list
    if __name__ == '__main__':
     count = multiprocessing.cpu_count()
     pool = multiprocessing.Pool(processes=2)
     pool.map(download_burned_areas, window_list)


def get_windows_in_tile(tile_id):

    csv = 'burned_area_tile_index.csv'

    burned_index_df = pd.read_csv(csv)
    
    # find the windows for the given tile id
    window = burned_index_df.loc[burned_index_df['tile'] == tile_id, 'window']
    
    # convert results to list
    list_of_windows = window.values.tolist()
    
    # remove any duplicates
    list_of_windows = list(set(list_of_windows))
    
    return list_of_windows


def recode_burned_area(raster):

    outfile_name = raster.strip(".tif") + "_recode.tif"
    outfile_cmd = '--outfile={}'.format(outfile_name)
    recode_cmd = ['gdal_calc.py', '-A', raster, '--calc=A>0', 'NoDataValue=0', '--co', 'COMPRESS=LZW', outfile_cmd]
    subprocess.check_call(recode_cmd)
