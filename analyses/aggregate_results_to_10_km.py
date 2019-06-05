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
def aggregate_results(tile):

    # Extracts the tile id from the full tile name
    tile_id = uu.get_tile_id(tile)
    tile_type = uu.get_tile_type(tile)

    print "Aggregating {}...".format(tile, tile_id)

    # start time
    start = datetime.datetime.now()

    # Names of the gain and emissions tiles
    area_tile = '{0}_{1}.tif'.format(cn.pattern_pixel_area, tile_id)

    # Output net emissions file
    per_pixel = '{0}_{1}_per_pixel.tif'.format(tile_id, tile_type)

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

    # Iterates across the windows (1 pixel strips) of the input tile
    for idx, window in windows:

        # Creates windows for each input tile
        in_window = in_src.read(1, window=window)
        pixel_area_window = pixel_area_src.read(1, window=window)

        per_pixel = in_window * pixel_area_window / cn.m2_per_ha

        per_pixel_dst.write_band(1, per_pixel, window=window)



    # # Source: http://gis.stackexchange.com/questions/90726
    # # Opens raster we're getting statistics on
    # focus_tile = gdal.Open(tile)
    #
    # nodata = uu.get_raster_nodata_value(tile)
    # print "NoData value =", nodata
    #
    # # Turns the raster into a numpy array
    # tile_array = np.array(focus_tile.GetRasterBand(1).ReadAsArray())
    #
    # # Flattens the numpy array to a single dimension
    # tile_array_flat = tile_array.flatten()
    #
    # # Removes NoData values from the array. NoData are generally either 0 or -9999.
    # tile_array_flat_mask = tile_array_flat[tile_array_flat != nodata]
    #
    # ### For converting value/hectare to value/pixel
    # # Tile with the area of each pixel in m2
    # area_tile = '{0}_{1}.tif'.format(cn.pattern_pixel_area, tile_id)
    #
    # # Output file name
    # tile_short = tile[:-4]
    # outname = '{0}_value_per_pixel.tif'.format(tile_short)
    #
    # # Equation argument for converting emissions from per hectare to per pixel.
    # # First, multiplies the per hectare emissions by the area of the pixel in m2, then divides by the number of m2 in a hectare.
    # calc = '--calc=A*B/{}'.format(cn.m2_per_ha)
    #
    # # Argument for outputting file
    # out = '--outfile={}'.format(outname)
    #
    # print "Converting {} from /ha to /pixel...".format(tile)
    # cmd = ['gdal_calc.py', '-A', tile, '-B', area_tile, calc, out, '--NoDataValue=0', '--co', 'COMPRESS=LZW',
    #        '--overwrite']
    # subprocess.check_call(cmd)
    # print "{} converted to /pixel".format(tile)
    #
    # print "Converting value/pixel tile {} to numpy array...".format(tile)
    # # Opens raster with value per pixel
    # value_per_pixel = gdal.Open(outname)
    #
    # # Turns the pixel area raster into a numpy array
    # value_per_pixel_array = np.array(value_per_pixel.GetRasterBand(1).ReadAsArray())
    #
    # # Flattens the pixel area numpy array to a single dimension
    # value_per_pixel_array_flat = value_per_pixel_array.flatten()



    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, stats[0])