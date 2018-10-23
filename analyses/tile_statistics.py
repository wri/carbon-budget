import utilities
import subprocess
import rasterio
from osgeo import gdal
import numpy as np


def create_tile_statistics(tile_id):

    tile = '{}.tif'.format(tile_id)

    print "Calculating tile statistics for {}...".format(tile_id)
    # Source: http://gis.stackexchange.com/questions/90726
    # Opens raster and chooses band to find statistics
    gtif = gdal.Open(tile)

    # Turns the raster into a numpy array
    tile_array = np.array(gtif.GetRasterBand(1).ReadAsArray())

    # Flattens the numpy array to a single dimension
    tile_array_flat = tile_array.flatten()

    # Removes 0s from the array
    tile_array_flat_mask = tile_array_flat[tile_array_flat != 0]

    stat = [None] * 9


    stat[0] = tile_array_flat_mask.size
    stat[1] = np.mean(tile_array_flat_mask, dtype=np.float64)
    stat[2] = np.median(tile_array_flat_mask)
    stat[3] = np.percentile(tile_array_flat_mask, 10)
    stat[4] = np.percentile(tile_array_flat_mask, 25)
    stat[5] = np.percentile(tile_array_flat_mask, 75)
    stat[6] = np.percentile(tile_array_flat_mask, 90)
    stat[7] = np.amin(tile_array_flat_mask)
    stat[8] = np.amax(tile_array_flat_mask)

    print stat

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