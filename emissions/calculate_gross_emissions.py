import subprocess
import datetime
import os
import glob
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu
import utilities


def calc_emissions(tile_id):

    print "Processing:", tile_id

    start = datetime.datetime.now()

    emissions_tiles_cmd = ['cpp_util/calc_emissions_v3.exe', tile_id]
    subprocess.check_call(emissions_tiles_cmd)

    # # Removes no data pixels from raster-- I HAVEN'T TESTED THIS YET
    # utilities.remove_nodata(tile_id)
    #
    # # Delete input tiles from spot machine. I don't think this is actually deleting the tiles at this point.
    # print 'Deleting tiles from spot machine'
    # utilities.del_tiles(tile_id)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_gross_emis_commod)
