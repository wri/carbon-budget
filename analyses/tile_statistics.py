import utilities
import subprocess
import rasterio
from osgeo import gdal
import numpy as np


def create_tile_statistics(tile_id):

    tile = '{}.tif'.format(tile_id)

    print "Checking if {} contains any data...".format(tile_id)
    # Source: http://gis.stackexchange.com/questions/90726
    # Opens raster and chooses band to find statistics
    gtif = gdal.Open(tile)
    srcband = gtif.GetRasterBand(1)
    stats = srcband.GetStatistics(True, True)
    print "  Tile stats =  Minimum=%.3f, Maximum=%.3f, Mean=%.3f, StdDev=%.3f" % (stats[0], stats[1], stats[2], stats[3])

    # Turns the raster into a numpy array
    tile_array = np.array(gtif.GetRasterBand(1).ReadAsArray())

    # Flattens the numpy array to a single dimension
    tile_array_flat = tile_array.flatten()

    # Removes 0s from the array
    tile_array_flat_mask = tile_array_flat[tile_array_flat != 0]


    stats['size'] = tile_array_flat_mask.size
    stats['median'] = np.median(tile_array_flat_mask)
    stats['10p'] = np.percentile(tile_array_flat_mask, 10)
    stats['25p'] = np.percentile(tile_array_flat_mask, 25)
    stats['75p'] = np.percentile(tile_array_flat_mask, 75)
    stats['90p'] = np.percentile(tile_array_flat_mask, 90)
    stats['mean'] = np.mean(tile_array_flat_mask, dtype=np.float64)
    stats['min'] = np.amin(tile_array_flat_mask)
    stats['max'] = np.amax(tile_array_flat_mask)

    print tile_array_flat_mask.size
    print np.median(tile_array_flat_mask)
    print np.percentile(tile_array_flat_mask, 50)
    print np.percentile(tile_array_flat_mask, 10)
    print np.percentile(tile_array_flat_mask, 25)
    print np.percentile(tile_array_flat_mask, 75)
    print np.percentile(tile_array_flat_mask, 90)
    print np.mean(tile_array_flat_mask, dtype=np.float64)
    print np.amin(tile_array_flat_mask)
    print np.amax(tile_array_flat_mask)