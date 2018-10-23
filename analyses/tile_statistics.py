import utilities
import subprocess
import rasterio
from osgeo import gdal


def create_tile_statistics(tile_id):

    tile = '{}.tif'.format(tile_id)

    print "Checking if {} contains any data...".format(tile_id)
    # Source: http://gis.stackexchange.com/questions/90726
    # Opens raster and chooses band to find min, max
    gtif = gdal.Open(tile)
    srcband = gtif.GetRasterBand(1)
    stats = srcband.GetStatistics(True, True)
    print "  Tile stats =  Minimum=%.3f, Maximum=%.3f, Mean=%.3f, StdDev=%.3f" % (stats[0], stats[1], stats[2], stats[3])

    # Opens continent-ecozone tile
    with rasterio.open(tile) as tile_src:

        # Grabs metadata about the tif, like its location/projection/cellsize
        kwargs = tile.meta

        windows = tile_src.block_windows(1)

        print tile_src