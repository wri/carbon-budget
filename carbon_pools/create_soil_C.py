import util
import datetime
import rasterio
import numpy as np
from scipy import stats
import sys
sys.path.append('../')
import universal_util as uu
import constants_and_names as cn


def create_soil_C(tile_id):

    # Start time
    start = datetime.datetime.now()

    print "Getting extent of", tile_id
    xmin, ymin, xmax, ymax = uu.coords(tile_id)

    print "Clipping srtm for", tile_id
    uu.warp_to_Hansen('soil_C.vrt', '{0}_{1}.tif'.format(tile_id, cn.pattern_soil_C_2000), xmin, ymin, xmax, ymax)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_precip)
