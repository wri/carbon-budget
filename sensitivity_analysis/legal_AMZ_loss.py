import glob
import pandas as pd
import datetime
import subprocess
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu


def merge_warp_forest_extent_tiles(tile_id, raw_forest_extent_inputs, out_pattern, dt):

    # Start time
    start = datetime.datetime.now()

    tile = '{0}_{1}.tif'.format(tile_id, out_pattern)

    print tile
    print raw_forest_extent_inputs

    cmd = ['gdal_merge.py', '-o', tile,
           '-co', 'COMPRESS=LZW', '-a_nodata', '0', '-n', '0', '-ot', dt,
           '-ps', cn.Hansen_res, cn.Hansen_res,
           raw_forest_extent_inputs[0], raw_forest_extent_inputs[1], raw_forest_extent_inputs[2],
           raw_forest_extent_inputs[3], raw_forest_extent_inputs[4], raw_forest_extent_inputs[5]]
    print cmd
    subprocess.check_call(cmd)

    print "Checking if {} contains any data...".format(tile)
    no_data = uu.check_for_data(tile)

    if no_data:

        print "  No data found. Deleting {}.".format(tile)
        os.remove(tile)

    else:

        print "  Data found in {}. Warping to Hansen tile...".format(tile)

        print "Getting extent of", tile
        xmin, ymin, xmax, ymax = uu.coords(tile_id)

        uu.warp_to_Hansen(tile, '{0}_{1}.tif'.format(tile_id, cn.pattern_Brazil_forest_extent_2000_processed),
                          xmin, ymin, xmax, ymax, 'Byte')



    uu.end_of_fx_summary(start, tile_id, out_pattern)