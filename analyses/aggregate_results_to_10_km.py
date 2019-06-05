from osgeo import gdal
import numpy as np
import subprocess
import rasterio
import datetime
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Calculates a range of tile statistics
def aggregate_results(tile, pixel_count_dict):

    print "Processing {}".format(tile)

    # start time
    start = datetime.datetime.now()

    # Extracts the tile id and the tile type from the full tile name
    tile_id = uu.get_tile_id(tile)
    tile_type = uu.get_tile_type(tile)
    xmin, ymin, xmax, ymax = uu.coords(tile_id)

    print "  Converting {} to per-pixel values".format(tile)

    # Names of pixel area tile
    area_tile = '{0}_{1}.tif'.format(cn.pattern_pixel_area, tile_id)

    # Per-pixel value tile (intermediate output)
    per_pixel = '{0}_{1}_per_pixel.tif'.format(tile_id, tile_type)

    # Opens input tiles for rasterio
    in_src = rasterio.open(tile)
    pixel_area_src = rasterio.open(area_tile)

    # Grabs metadata about the tif, like its location/projection/cellsize
    kwargs = in_src.meta

    # Grabs the windows of the tile (stripes) so we can iterate over the entire tif without running out of memory
    windows = in_src.block_windows(1)

    kwargs.update(
        driver='GTiff',
        count=1,
        compress='lzw',
        nodata=0,
        dtype='float32'
    )

    # Opens the output tile, giving it the arguments of the input tiles
    per_pixel_dst = rasterio.open(per_pixel, 'w', **kwargs)

    # The number of pixels in the tile with values
    non_zero_pixels = 0

    # Iterates across the windows (1 pixel strips) of the input tile
    for idx, window in windows:

        # Creates windows for each input tile
        in_window = in_src.read(1, window=window)
        pixel_area_window = pixel_area_src.read(1, window=window)

        # Calculates the per-pixel value from the input tile value (/ha to /pixel)
        per_pixel = in_window * pixel_area_window / cn.m2_per_ha

        per_pixel_dst.write_band(1, per_pixel, window=window)

        # Adds the number of pixels with values in that window to the total for that tile
        non_zero_pixels = non_zero_pixels + np.count_nonzero(in_window)

    print "Pixels with values in {}: {}".format(tile, non_zero_pixels)

    print "  Calculating average per-pixel value in", tile

    avg_10km = '{0}_{1}_average.tif'.format(tile_id, tile_type)

    cmd = ['gdalwarp', '-tap', '-tr', '{}'.format(str(0.096342599)), '{}'.format(str(0.096342599)),  '-co', 'COMPRESS=LZW',
           # '-te', str(xmin), str(ymin), str(xmax), str(ymax),
           per_pixel, avg_10km]
    subprocess.check_call(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, tile_type)