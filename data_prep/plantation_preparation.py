
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
    print "  xmin:", xmin, "; xmax:", xmax, "; ymin", ymin, "; ymax:", ymax

    x_size = abs(int(xmin) - int(xmax))
    y_size = abs(int(ymin) - int(ymax))

    for x in range(x_size):

        xmin_1x1 = int(xmin) + x
        xmax_1x1 = int(xmin) + x + 1

        for y in range(y_size):

            ymin_1x1 = int(ymin) + y
            ymax_1x1 = int(ymin) + y + 1

            print "x:", x, "y:", y
            print "  xmin_1x1:", xmin_1x1, "; xmax_1x1:", xmax_1x1, "; ymin_1x1", ymin_1x1, "; ymax_1x1:", ymax_1x1




# https://gis.stackexchange.com/questions/187224/how-to-use-gdal-rasterize-with-postgis-vector
# gdal_rasterize -tr 0.00025 0.00025 -co COMPRESS = LZW PG:"dbname=ubuntu" -l all_plant col_plant_gdalrasterize.tif -te -80 0 -70 10 -a growth