
import os
from subprocess import Popen, PIPE, STDOUT, check_call
import numpy as np
from osgeo import gdal
from gdalconst import GA_ReadOnly
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu


def hdf_to_array(hdf):
    hdf_open = gdal.Open(hdf).GetSubDatasets()
    ds = gdal.Open(hdf_open[0][0])
    array = ds.ReadAsArray()

    return array


def makedir(folder):
    if not os.path.exists(folder):
        os.mkdir(folder)


def wgetloss(tile_id):
    uu.print_log("download hansen loss tile")

    hansen_tile = '{}_loss.tif'.format(tile_id)
    cmd = ['wget', r'https://glad.umd.edu/Potapov/GFW_2018/forest_loss_2018/{}.tif'.format(tile_id)]
    # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        uu.log_subprocess_output(process.stdout)

    return hansen_tile


def raster_to_array(raster):
    ds = gdal.Open(raster)
    array = np.array(ds.GetRasterBand(1).ReadAsArray())
    
    return array


def array_to_raster_simple(array, outname, template):
    
    ds = gdal.Open(template)
    x_pixels = ds.RasterXSize
    y_pixels = ds.RasterYSize
    
    geoTransform = ds.GetGeoTransform()
    height = geoTransform[1]
    
    pixel_size = height
    
    minx = geoTransform[0]
    maxy = geoTransform[3]
    
    wkt_projection = ds.GetProjection()
    
    driver = gdal.GetDriverByName('GTiff')

    dataset = driver.Create(
        outname,
        x_pixels,
        y_pixels,
        1,
        gdal.GDT_Int16,
        options=["COMPRESS=LZW"])

    dataset.SetGeoTransform((
        minx,    # 0
        pixel_size,  # 1
        0,                      # 2
        maxy,    # 3
        0,                      # 4
        -pixel_size))  

    dataset.SetProjection(wkt_projection)
    dataset.GetRasterBand(1).WriteArray(array)
    dataset.FlushCache()  # Write to disk.
    
    return outname


def array_to_raster(global_grid_hv, year, array, template_hdf, outfolder):

    filename = '{0}_{1}.tif'.format(year, global_grid_hv)
    dst_filename = os.path.join(outfolder, filename)
    # x_pixels, y_pixels = get_extent.get_size(raster)
    hdf_open = gdal.Open(template_hdf).GetSubDatasets()
    ds = gdal.Open(hdf_open[0][0])
    x_pixels = ds.RasterXSize
    y_pixels = ds.RasterYSize

    geoTransform = ds.GetGeoTransform()

    pixel_size = geoTransform[1]

    minx = geoTransform[0]
    maxy = geoTransform[3]

    wkt_projection = ds.GetProjection()

    driver = gdal.GetDriverByName('GTiff')

    dataset = driver.Create(
        dst_filename,
        x_pixels,
        y_pixels,
        1,
        gdal.GDT_Int16, )

    dataset.SetGeoTransform((
        minx,    # 0
        pixel_size,  # 1
        0,                      # 2
        maxy,    # 3
        0,                      # 4
        -pixel_size))  

    dataset.SetProjection(wkt_projection)
    dataset.GetRasterBand(1).WriteArray(array)
    dataset.FlushCache()  # Write to disk.
    
    return dst_filename


def stack_arrays(list_of_year_arrays):

    stack = np.stack(list_of_year_arrays)
    
    return stack


def makedir(dir):
    if not os.path.exists(dir):
        os.makedirs(dir)

    return dir


def download_df(year, hv_tile, output_dir):
        include = '*A{0}*{1}*'.format(year, hv_tile)
        cmd = ['aws', 's3', 'cp', cn.burn_year_hdf_raw_dir, output_dir, '--recursive', '--exclude',
               "*", '--include', include]

        # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
        process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
        with process.stdout:
            uu.log_subprocess_output(process.stdout)


def remove_list_files(file_list):
    for file in file_list:
        try:
            uu.print_log("Removing ", file)
            os.remove(file)
        except:
            pass

def get_extent(tif):

    uu.print_log("Getting extent of", tif)

    data = gdal.Open(tif, GA_ReadOnly)
    geoTransform = data.GetGeoTransform()
    minx = geoTransform[0]
    maxy = geoTransform[3]
    maxx = minx + geoTransform[1] * data.RasterXSize
    miny = maxy + geoTransform[5] * data.RasterYSize
    uu.print_log([minx, miny, maxx, maxy])
    data = None

    return minx, miny, maxx, maxy

# Lists the tiles in a folder in s3
def list_tiles(source):

    ## For an s3 folder in a bucket using AWSCLI
    # Captures the list of the files in the folder
    out = subprocess.Popen(['aws', 's3', 'ls', source], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout, stderr = out.communicate()

    # Writes the output string to a text file for easier interpretation
    Hansen_tiles = open("Hansen_tiles.txt", "wb")
    Hansen_tiles.write(stdout)
    Hansen_tiles.close()

    file_list = []

    # Iterates through the text file to get the names of the tiles and appends them to list
    with open("Hansen_tiles.txt", 'r') as tile:
        for line in tile:

            num = len(line.strip('\n').split(" "))
            tile_name = line.strip('\n').split(" ")[num - 1]
            # tile_name_short = tile_name[0:8]
            file_list.append(tile_name)

    return file_list
