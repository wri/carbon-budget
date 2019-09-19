'''
This script calculates the gross emissions in tonnes CO2e/ha for every loss pixel.
The properties of each pixel determine the appropriate emissions equation, the constants for the equation, and the
carbon pool values that go into the equation.
'''

import subprocess
import datetime
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Calls the function to mask pre-2000 plantations from the loss tiles before calculating emissions from them
def mask_pre_2000_plant(tile_id):

    print "Masking pre-2000 plantations for {}".format(tile_id)

    pre_2000_plant = './cpp_util/{0}_{1}.tif'.format(tile_id, cn.pattern_plant_pre_2000)
    loss_tile = './cpp_util/{}.tif'.format(tile_id)
    out_tile = './cpp_util/{0}_{1}.tif'.format(tile_id, cn.pattern_loss_pre_2000_plant_masked)

    uu.mask_pre_2000_plantation(pre_2000_plant, loss_tile, out_tile, tile_id)


# Calls the c++ script to calculate gross emissions
def calc_emissions(tile_id):

    print "Calculating gross emissions for", tile_id

    start = datetime.datetime.now()

    emissions_tiles_cmd = ['cpp_util/calc_gross_emissions.exe', tile_id]
    subprocess.check_call(emissions_tiles_cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, os.path.join(cn.pattern_gross_emis_commod))
