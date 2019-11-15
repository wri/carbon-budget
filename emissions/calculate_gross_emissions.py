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
def calc_emissions(tile_id, pools, sensit_type):

    print "Calculating gross emissions for", tile_id

    start = datetime.datetime.now()

    # Runs the correct c++ script given the pools selected (biomass+soil or soil_only)
    if (pools == 'biomass_soil') & (sensit_type == 'std'):
        emissions_tiles_cmd = ['cpp_util/calc_gross_emissions_biomass_soil.exe', tile_id]

    elif (pools == 'biomass_soil') & (sensit_type == 'no_shifting_ag'):
        emissions_tiles_cmd = ['cpp_uti/calc_gross_emissions_no_shifting_ag.exe', tile_id]

    elif (pools == 'soil_only') & (sensit_type == 'std'):
        print "this one"
        emissions_tiles_cmd = ['cpp_util/calc_gross_emissions_soil_only.exe', tile_id]

    else:
        raise Exception('Pool and/or sensitivity analysis option not valid')

    subprocess.check_call(emissions_tiles_cmd)


    # Identifies which pattern to use for counting tile completion
    pattern = cn.pattern_gross_emis_commod_biomass_soil
    if pools == 'biomass_soil':
        pattern = pattern

    elif pools == 'soil_only':
        pattern = pattern.replace('biomass_soil', 'soil_only')

    else:
        raise Exception('Pool option not valid')

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, pattern)
