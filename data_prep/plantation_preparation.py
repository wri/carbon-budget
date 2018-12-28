
import subprocess
from osgeo import gdal
import sys
sys.path.append('../')
import constants_and_names
import universal_util

# Creates mangrove tiles using Hansen tile properties
def create_1x1_tiles(tile_id):

    print "Getting bounding coordinates for tile", tile_id
    xmin, ymin, xmax, ymax = universal_util.coords(tile_id)
    print "  ymax:", ymax, "; ymin:", ymin, "; xmax", xmax, "; xmin:", xmin

    tile_size = abs(int(xmin) - int(xmax))

    for i in range(tile_size):

        print i

        xmin_1x1 = str(xmin) + i
        ymin_1x1 = str(ymin) + i
        xmax_1x1 = str(xmax) + i + 1
        ymax_1x1 = str(ymax) + i + 1

        print "  ymax_1x1:", ymax_1x1, "; ymin_1x1:", ymin_1x1, "; xmax_1x1", xmax_1x1, "; xmin_1x1:", xmin_1x1




# https://gis.stackexchange.com/questions/187224/how-to-use-gdal-rasterize-with-postgis-vector
# gdal_rasterize -tr 0.00025 0.00025 -co COMPRESS = LZW PG:"dbname=ubuntu" -l all_plant col_plant_gdalrasterize.tif -te -80 0 -70 10 -a growth