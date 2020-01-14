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



    uu.end_of_fx_summary(start, tile_id, out_pattern)