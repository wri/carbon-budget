import utilities
import subprocess
import rasterio
from osgeo import gdal
import numpy as np


def create_tile_statistics(tile_id):

    tile = '{}.tif'.format(tile_id)

    print "Checking if {} contains any data...".format(tile_id)
    # Source: http://gis.stackexchange.com/questions/90726
    # Opens raster and chooses band to find min, max
    gtif = gdal.Open(tile)
    srcband = gtif.GetRasterBand(1)
    stats = srcband.GetStatistics(True, True)
    print "  Tile stats =  Minimum=%.3f, Maximum=%.3f, Mean=%.3f, StdDev=%.3f" % (stats[0], stats[1], stats[2], stats[3])

    myarray = np.array(gtif.GetRasterBand(1).ReadAsArray())

    print myarray
    print myarray.shape
    print myarray.size
    print np.median(myarray)

    myarray_flat = myarray.flatten()

    print myarray_flat
    print myarray_flat.shape
    print myarray_flat.size
    print np.median(myarray_flat)

    myarray_flat_mask = myarray_flat[myarray_flat != 0]

    print myarray_flat_mask
    print myarray_flat_mask.shape
    print myarray_flat_mask.size
    print np.median(myarray_flat_mask)

    myarray_flat_zero = myarray_flat[myarray_flat == 0]

    print myarray_flat_zero
    print myarray_flat_zero.shape
    print myarray_flat_zero.size
    print np.median(myarray_flat_zero)

    # # Opens continent-ecozone tile
    # with rasterio.open(tile) as tile_src:
    #
    #     # Grabs metadata about the tif, like its location/projection/cellsize
    #     kwargs = tile_src.meta
    #
    #     windows = tile_src.block_windows(1)
    #
    #     print tile_src