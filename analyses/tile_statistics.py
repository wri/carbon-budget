from osgeo import gdal
import numpy as np
import subprocess
import datetime
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Calculates a range of tile statistics
def create_tile_statistics(tile):

    tile_stats = '{}_{}'.format(uu.date_today, cn.tile_stats_pattern)

    # Extracts the tile id from the full tile name
    tile_id = uu.get_tile_id(tile)

    print "Calculating tile statistics for {0}, tile id {1}...".format(tile, tile_id)

    # start time
    start = datetime.datetime.now()

    # Source: http://gis.stackexchange.com/questions/90726
    # Opens raster we're getting statistics on
    focus_tile = gdal.Open(tile)

    nodata = uu.get_raster_nodata_value(tile)
    print "NoData value =", nodata

    # Turns the raster into a numpy array
    tile_array = np.array(focus_tile.GetRasterBand(1).ReadAsArray())

    # Flattens the numpy array to a single dimension
    tile_array_flat = tile_array.flatten()

    # Removes NoData values from the array. NoData are generally either 0 or -9999.
    tile_array_flat_mask = tile_array_flat[tile_array_flat != nodata]

    ### For converting value/hectare to value/pixel
    # Tile with the area of each pixel in m2
    area_tile = '{0}_{1}.tif'.format(cn.pattern_pixel_area, tile_id)

    # Output file name
    tile_short = tile[:-4]
    outname = '{0}_value_per_pixel.tif'.format(tile_short)

    # Equation argument for converting emissions from per hectare to per pixel.
    # First, multiplies the per hectare emissions by the area of the pixel in m2, then divides by the number of m2 in a hectare.
    calc = '--calc=A*B/{}'.format(cn.m2_per_ha)

    # Argument for outputting file
    out = '--outfile={}'.format(outname)

    print "Converting {} from /ha to /pixel...".format(tile)
    cmd = ['gdal_calc.py', '-A', tile, '-B', area_tile, calc, out, '--NoDataValue=0', '--co', 'COMPRESS=LZW',
           '--overwrite']
    subprocess.check_call(cmd)
    print "{} converted to /pixel".format(tile)
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
    #
    # print "Converted {} to numpy array".format(tile)
    #
    # # Empty statistics list
    # stats = [None] * 13
    #
    # # Calculates basic tile info
    # stats[0] = tile_id
    # stats[1] = tile[9:-4]
    # stats[2] = tile
    # stats[3] = tile_array_flat_mask.size
    #
    # # If there are no pixels with values in the tile (as determined by the length of the array when NoData values are removed),
    # # the statistics are all N/A.
    # if stats[3] == 0:
    #
    #     stats[4] = "N/A"
    #     stats[5] = "N/A"
    #     stats[6] = "N/A"
    #     stats[7] = "N/A"
    #     stats[8] = "N/A"
    #     stats[9] = "N/A"
    #     stats[10] = "N/A"
    #     stats[11] = "N/A"
    #     stats[12] = "N/A"
    #
    # # If there are pixels with values in the tile, the following statistics are calculated
    # else:
    #
    #     stats[4] = np.mean(tile_array_flat_mask, dtype=np.float64)
    #     stats[5] = np.median(tile_array_flat_mask)
    #     stats[6] = np.percentile(tile_array_flat_mask, 10)
    #     stats[7] = np.percentile(tile_array_flat_mask, 25)
    #     stats[8] = np.percentile(tile_array_flat_mask, 75)
    #     stats[9] = np.percentile(tile_array_flat_mask, 90)
    #     stats[10] = np.amin(tile_array_flat_mask)
    #     stats[11] = np.amax(tile_array_flat_mask)
    #     stats[12] = np.sum(value_per_pixel_array_flat)
    #
    # stats_no_brackets = ', '.join(map(str, stats))
    #
    # print stats_no_brackets
    #
    # # Adds the tile's statistics to the txt file
    # with open(tile_stats, 'a+') as f:
    #     f.write(stats_no_brackets + '\r\n')
    # f.close()

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, 'value_per_pixel.tif')