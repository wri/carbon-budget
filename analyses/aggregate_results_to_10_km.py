import numpy as np
import subprocess
import rasterio
from rasterio.transform import from_origin
import datetime
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Converts the 10x10 degree Hansen tiles that are in windows of 40000x1 pixels to windows of 400x400 pixels,
# which is the resolution of the output tiles. This will allow the 30x30 m pixels in each window to be summed.
def rewindow(tile):

    # start time
    start = datetime.datetime.now()

    print "Rewindowing {} to 400x400 pixel windows (0.1 degree x 0.1 degree)...". format(tile)

    # Extracts the tile id, tile type, and bounding box for the tile
    tile_id = uu.get_tile_id(tile)
    tile_type = uu.get_tile_type(tile)
    xmin, ymin, xmax, ymax = uu.coords(tile_id)

    # Raster name for 400x400 pixel tiles (intermediate output)
    input_rewindow = '{0}_{1}_rewindow.tif'.format(tile_id, tile_type)
    area_tile = '{0}_{1}.tif'.format(cn.pattern_pixel_area, tile_id)
    pixel_area_rewindow = '{0}_{1}_rewindow.tif'.format(cn.pattern_pixel_area, tile_id)

    # Converts the tile of interest to the 400x400 pixel windows
    cmd = ['gdalwarp', '-co', 'COMPRESS=LZW', '-overwrite',
           '-te', str(xmin), str(ymin), str(xmax), str(ymax), '-tap',
           '-tr', str(cn.Hansen_res), str(cn.Hansen_res),
           '-co', 'TILED=YES', '-co', 'BLOCKXSIZE=400', '-co', 'BLOCKYSIZE=400',
           tile, input_rewindow]
    subprocess.check_call(cmd)

    # Converts the pixel area tile to the 400x400 pixel windows
    cmd = ['gdalwarp', '-co', 'COMPRESS=LZW', '-overwrite', '-dstnodata', '0',
           '-te', str(xmin), str(ymin), str(xmax), str(ymax), '-tap',
           '-tr', str(cn.Hansen_res), str(cn.Hansen_res),
           '-co', 'TILED=YES', '-co', 'BLOCKXSIZE=400', '-co', 'BLOCKYSIZE=400',
           area_tile, pixel_area_rewindow]
    subprocess.check_call(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, tile_type)


# Converts the existing (per ha) values to per pixel values (e.g., emissions/ha to emissions/pixel)
# and sums those values in each 400x400 pixel window.
# The sum for each 400x400 pixel window is stored in a 2D array, which is then converted back into a raster at
# 0.1x0.1 degree resolution (approximately 10m in the tropics).
# Each pixel in that raster is the sum of the 30m pixels converted to value/pixel (instead of value/ha).
# The 0.1x0.1 degree tile is output.
def convert_to_per_pixel(tile):

    # start time
    start = datetime.datetime.now()

    # Extracts the tile id, tile type, and bounding box for the tile
    tile_id = uu.get_tile_id(tile)
    tile_type = uu.get_tile_type(tile)
    xmin, ymin, xmax, ymax = uu.coords(tile_id)

    print "  Converting {} to per-pixel values...".format(tile)

    # Name of inputs
    focal_tile_rewindow = '{0}_{1}_rewindow.tif'.format(tile_id, tile_type)
    pixel_area_rewindow = '{0}_{1}_rewindow.tif'.format(cn.pattern_pixel_area, tile_id)

    # Opens input tiles for rasterio
    in_src = rasterio.open(focal_tile_rewindow)
    pixel_area_src = rasterio.open(pixel_area_rewindow)

    # Grabs the windows of the tile (stripes) so we can iterate over the entire tif without running out of memory
    windows = in_src.block_windows(1)

    #2D array in which the 10x10 km aggregated sums will be stored
    sum_array = np.zeros([100,100], 'float32')

    # Iterates across the windows (400x400 30m pixels) of the input tile
    for idx, window in windows:

        # Creates windows for each input tile
        in_window = in_src.read(1, window=window)
        pixel_area_window = pixel_area_src.read(1, window=window)

        # Calculates the per-pixel value from the input tile value (/ha to /pixel)
        per_pixel_value = in_window * pixel_area_window / cn.m2_per_ha

        # Sums the pixels to create a total value for the 10x10 km pixel
        non_zero_pixel_sum = np.sum(per_pixel_value)

        # Stores the resulting value in the array
        sum_array[idx[0], idx[1]] = non_zero_pixel_sum

    print "Creating aggregated tile for {}...".format(tile)

    # Creates a tile at 0.1x0.1 degree resolution (approximately 10x10 km in the tropics) where the values are
    # from the 2D array created by rasterio above
    # https://gis.stackexchange.com/questions/279953/numpy-array-to-gtiff-using-rasterio-without-source-raster
    # aggregated = rasterio.open("{0}_{1}.tif".format(tile_id, cn.pattern_gross_emis_all_drivers_aggreg), 'w',
    #                             driver='GTiff', compress='lzw', nodata='0', dtype='float32', count=1,
    #                             height=100, width=100,
    #                             # pixelSizeY='0.1', pixelSizeX='0.1', height=100, width=100,
    #                             crs='EPSG:4326', transform=from_origin(xmin,ymax,0.1,0.1))
    aggregated = rasterio.open("{0}_{1}_10km.tif".format(tile_id, tile_type), 'w',
                                driver='GTiff', compress='lzw', nodata='0', dtype='float32', count=1,
                                height=100, width=100,
                                # pixelSizeY='0.1', pixelSizeX='0.1', height=100, width=100,
                                crs='EPSG:4326', transform=from_origin(xmin,ymax,0.1,0.1))
    aggregated.write(sum_array,1)
    aggregated.close()

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, '{}_10km'.format(tile_type))