'''
This script calculates the gross emissions in tonnes CO2e/ha for every loss pixel.
'''

import subprocess
import datetime
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu


def calc_emissions(tile_id):

    print "Calculating gross emissions for", tile_id

    start = datetime.datetime.now()

    emissions_tiles_cmd = ['cpp_util/calc_emissions_v3.exe', tile_id]
    subprocess.check_call(emissions_tiles_cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, os.path.join('outdata/',cn.pattern_gross_emis_commod))
