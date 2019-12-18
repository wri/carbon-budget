### Creates Hansen-style tiles for aboveground mangrove biomass (Mg/ha) from Lola Fatoyinbo's country
### mangrove data.
### Output tiles conform to the dimensions, resolution, and other properties of Hansen loss tiles.

import utilities
import subprocess
from osgeo import gdal
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Creates mangrove tiles using Hansen tile properties
def create_mangrove_tiles(tile_id):

    print "Getting bounding coordinates for tile", tile_id
    xmin, ymin, xmax, ymax = uu.coords(tile_id)
    print "  ymax:", ymax, "; ymin:", ymin, "; xmax", xmax, "; xmin:", xmin

    print "Creating tile", tile_id
    out_tile = '{0}_{1}.tif'.format(tile_id, cn.pattern_mangrove_biomass_2000)
    cmd = ['gdalwarp', '-t_srs', 'EPSG:4326', '-co', 'COMPRESS=LZW', '-tr', '0.00025', '0.00025', '-tap', '-te',
            str(xmin), str(ymin), str(xmax), str(ymax), '-dstnodata', '0', '-overwrite', utilities.mangrove_vrt, out_tile]
    subprocess.check_call(cmd)
    print "  Tile created"

    print "Checking if {} contains any data...".format(tile_id)
    no_data = uu.local_src_is_empty(out_tile)

    if no_data:

        print "  No data found. Not copying {}.".format(tile_id)

    else:

        print "  Data found in {}. Copying tile to s3...".format(tile_id)
        uu.upload_final(cn.mangrove_biomass_2000_dir, tile_id, cn.pattern_mangrove_biomass_2000)
        print "    Tile copied to s3"






