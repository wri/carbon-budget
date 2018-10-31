import utilities
import subprocess
import rasterio
from osgeo import gdal
import numpy as np

# Calculates a range of tile statistics
def create_tile_statistics(tile_id):

    tile = '{0}_{1}.tif'.format(utilities.pattern_mangrove_biomass, tile_id)

    print "Calculating tile statistics for {}...".format(tile)

    # Source: http://gis.stackexchange.com/questions/90726
    # Opens raster and chooses band to find statistics
    gtif = gdal.Open(tile)

    # Turns the raster into a numpy array
    tile_array = np.array(gtif.GetRasterBand(1).ReadAsArray())

    # Flattens the numpy array to a single dimension
    tile_array_flat = tile_array.flatten()

    # Removes 0s from the array
    tile_array_flat_mask = tile_array_flat[tile_array_flat != 0]

    # Empty statistics list
    stats = [None] * 11

    # Calculates the statistics
    stats[0] = tile_id
    stats[1] = tile
    stats[2] = tile_array_flat_mask.size
    stats[3] = np.mean(tile_array_flat_mask, dtype=np.float64)
    stats[4] = np.median(tile_array_flat_mask)
    stats[5] = np.percentile(tile_array_flat_mask, 10)
    stats[6] = np.percentile(tile_array_flat_mask, 25)
    stats[7] = np.percentile(tile_array_flat_mask, 75)
    stats[8] = np.percentile(tile_array_flat_mask, 90)
    stats[9] = np.amin(tile_array_flat_mask)
    stats[10] = np.amax(tile_array_flat_mask)

    stats_no_brackets = ', '.join(map(str, stats))

    print stats_no_brackets

    # Adds the tile's statistis to the txt file
    with open('{0}_{1}.txt'.format(utilities.tile_stats, utilities.pattern_mangrove_biomass), 'a+') as f:
        f.write(stats_no_brackets + '\r\n')
    f.close()