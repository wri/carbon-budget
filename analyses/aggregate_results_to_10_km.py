from osgeo import gdal
import numpy as np
import subprocess
import rasterio
import datetime
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Converts the 10x10 degree Hansen tiles that are in windows of 40000x1 pixels to windows of 400x400 pixels
def rewindow(tile):

    # start time
    start = datetime.datetime.now()

    # Extracts the tile id, tile type, and bounding box for the tile
    tile_id = uu.get_tile_id(tile)
    tile_type = uu.get_tile_type(tile)
    xmin, ymin, xmax, ymax = uu.coords(tile_id)

    print "Rewindowing {} to 400x400 pixel windows (0.1 degree x 0.1 degree)...". format(tile)

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


# Converts the existing (per ha) values to per pixel values (e.g., emissions/ha to emissions/pixel),
# gets the average value of the 0.00025x0.00025 pixels in each 0.1x0.1 pixel to make a new 0.1x0.1 raster,
# and then multiplies the 0.1x0.1 raster cell by the number of 0.00025x0.00025 pixels in it.
# Based on https://gis.stackexchange.com/questions/152661/downsampling-geotiff-using-summation-gdal-numpy
def convert_to_per_pixel(tile, pixel_count_dict):

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

    # Per-pixel value tile (intermediate output)
    per_pixel = '{0}_{1}_per_pixel.tif'.format(tile_id, tile_type)

    # Opens input tiles for rasterio
    in_src = rasterio.open(focal_tile_rewindow)
    pixel_area_src = rasterio.open(pixel_area_rewindow)

    # Grabs the windows of the tile (stripes) so we can iterate over the entire tif without running out of memory
    windows = in_src.block_windows(1)

    # # Grabs metadata about the tif, like its location/projection/cellsize
    # kwargs = in_src.meta
    #
    # kwargs.update(
    #     driver='GTiff',
    #     count=1,
    #     compress='lzw',
    #     nodata=0,
    #     dtype='float32'
    # )
    #
    # # Opens the output tile, giving it the arguments of the input tiles
    # per_pixel_dst = rasterio.open(per_pixel, 'w', **kwargs)

    # # The number of pixels in the tile with values
    # non_zero_pixel_count = 0

    sum_array = np.zeros([100,100], 'float32')

    # Iterates across the windows (1 pixel strips) of the input tile
    for idx, window in windows:

        # Creates windows for each input tile
        in_window = in_src.read(1, window=window)
        pixel_area_window = pixel_area_src.read(1, window=window)
        # print idx

        # Calculates the per-pixel value from the input tile value (/ha to /pixel)
        per_pixel_value = in_window * pixel_area_window / cn.m2_per_ha
        # print per_pixel_value.shape

        # per_pixel_dst.write_band(1, per_pixel_value, window=window)

        # Adds the number of pixels with values in that window to the total for that tile
        # print np.size(per_pixel_value)
        non_zero_pixel_sum = np.sum(per_pixel_value)
        # print non_zero_pixel_sum

        sum_array[idx[0], idx[1]] = non_zero_pixel_sum
        # print sum_array

        # sys.quit()

    print sum_array

    # # Grabs metadata about the tif, like its location/projection/cellsize
    # kwargs = in_src.meta
    #
    # kwargs.update(
    #     driver='GTiff',
    #     count=1,
    #     compress='lzw',
    #     nodata=0,
    #     dtype='float32',
    #     height='100',
    #     width='100',
    #     pixelSizeY='0.1',
    #     pixelSizeX='0.1'
    #     # blockxsize='100',
    #     # blockysize='100',
    # )

    print "Creating sum tile..."

    # https://gis.stackexchange.com/questions/279953/numpy-array-to-gtiff-using-rasterio-without-source-raster
    new_dataset = rasterio.open("{0}_{1}.tif".format(tile_id, cn.pattern_gross_emis_all_drivers_aggreg), 'w',
                                driver='GTiff', compress='lzw', nodata='0', dtype='float32',
                                pixelSizeY='0.1', pixelSizeX='0.1', height=100, width=100)
    new_dataset.write(sum_array,1)
    new_dataset.close()

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_gross_emis_all_drivers_aggreg)