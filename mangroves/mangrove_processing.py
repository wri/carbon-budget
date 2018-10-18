### Creates Hansen-style tiles for aboveground mangrove biomass (Mg/ha) from Lola Fatoyinbo's country
### mangrove data.
### Output tiles conform to the dimensions, resolution, and other properties of Hansen loss tiles.

import utilities
import subprocess
from osgeo import gdal

# Creates mangrove tiles using Hansen tile properties
def create_mangrove_tiles(tile_id):

    print "Getting bounding coordinates for tile", tile_id
    xmin, xmax, ymin, ymax = utilities.coords(tile_id)
    print "  ymax:", ymax, "; ymin:", ymin, "; xmax", xmax, "; xmin:", xmin

    print "Creating tile", tile_id
    out_tile = '{0}_{1}.tif'.format(utilities.mangrove_tile_out, tile_id)
    cmd = ['gdalwarp', '-t_srs', 'EPSG:4326', '-co', 'COMPRESS=LZW', '-tr', '0.00025', '0.00025', '-tap', '-te',
            str(xmin), str(ymin), str(xmax), str(ymax), '-dstnodata', '0', '-overwrite', utilities.mangrove_vrt, out_tile]
    subprocess.check_call(cmd)
    print "  Tile created"

    print "Checking if tile contains any data in it..."
    # Source: http://gis.stackexchange.com/questions/90726
    # Opens raster and chooses band to find min, max
    gtif = gdal.Open(out_tile)
    srcband = gtif.GetRasterBand(1)
    stats = srcband.GetStatistics(True, True)
    print "  Tile stats =  Minimum=%.3f, Maximum=%.3f, Mean=%.3f, StdDev=%.3f" % (stats[0], stats[1], stats[2], stats[3])

    if stats[0] > 0:

        print "  Data found in tile. Copying tile to s3..."
        utilities.upload_final(utilities.mangrove_tile_out, utilities.out_dir, tile_id)
        print "    Tile copied to s3"

    else:

        print "  No data found. Not copying tile."




