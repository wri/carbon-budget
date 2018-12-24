from osgeo import gdal
import numpy as np
import subprocess
import sys
sys.path.append('../')
import constants_and_names
import universal_util

# Calculates a range of tile statistics
def create_tile_statistics(tile):

    # Extracts the tile id from the full tile name
    tile_id = universal_util.get_tile_id(tile)

    print "Calculating tile statistics for {0}, tile id {1}...".format(tile, tile_id)

    # Source: http://gis.stackexchange.com/questions/90726
    # Opens raster we're getting statistics on
    focus_tile = gdal.Open(tile)

    # Turns the raster into a numpy array
    tile_array = np.array(focus_tile.GetRasterBand(1).ReadAsArray())

    # Flattens the numpy array to a single dimension
    tile_array_flat = tile_array.flatten()

    # Removes NoData values from the array
    tile_array_flat_mask = tile_array_flat[tile_array_flat != 0]
    tile_array_flat_mask = tile_array_flat_mask[tile_array_flat_mask != -9999]

    ### For converting value/hectare to value/pixel
    # Tile with the area of each pixel in m2
    area_tile = '{0}_{1}.tif'.format(constants_and_names.pattern_pixel_area, tile_id)

    # Output file name
    outname = '{0}_{1}_value_per_pixel.tif'.format(tile_id, tile)

    # Equation argument for converting emissions from per hectare to per pixel.
    # First, multiplies the per hectare emissions by the area of the pixel in m2, then divides by the number of m2 in a hectare.
    calc = '--calc=A*B/{}'.format(constants_and_names.m2_per_ha)

    # Argument for outputting file
    out = '--outfile={}'.format(outname)

    print "Converting {} from /ha to /pixel...".format(tile)
    cmd = ['gdal_calc.py', '-A', tile, '-B', area_tile, calc, out, '--NoDataValue=0', '--co', 'COMPRESS=LZW',
           '--overwrite']
    subprocess.check_call(cmd)
    print "{} converted".format(tile)

    print "Converting value/pixel tile {} to numpy array...".format(tile)
    # Opens raster with value per pixel
    value_per_pixel = gdal.Open(outname)

    # Turns the pixel area raster into a numpy array
    value_per_pixel_array = np.array(value_per_pixel.GetRasterBand(1).ReadAsArray())

    # Flattens the pixel area numpy array to a single dimension
    value_per_pixel_array_flat = value_per_pixel_array.flatten()

    print "Converted {} to numpy array".format(tile)

    # Empty statistics list
    stats = [None] * 12

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
    stats[11] = np.sum(value_per_pixel_array_flat)

    stats_no_brackets = ', '.join(map(str, stats))

    print stats_no_brackets

    # Adds the tile's statistis to the txt file
    with open(constants_and_names.tile_stats, 'a+') as f:
        f.write(stats_no_brackets + '\r\n')
    f.close()