'''
This script creates tiles of soil carbon density, one of the carbon pools.
At this time, mineral soil carbon is for the top 30 cm of soil.
Mangrove soil carbon gets precedence over mineral soil carbon where there is mangrove biomass.
Where there is no mangrove biomass, mineral soil C is used.
Peatland carbon is not recognized or involved in any way.
'''

import datetime
import subprocess
import os
import sys
sys.path.append('../')
import universal_util as uu
import constants_and_names as cn

def create_mangrove_soil_C(tile_id):

    # Start time
    start = datetime.datetime.now()

    if os.path.exists('{0}_{1}.tif'.format(tile_id, cn.pattern_mangrove_biomass_2000)):

        print "Mangrove aboveground biomass tile found for", tile_id

        print "Getting extent of", tile_id
        xmin, ymin, xmax, ymax = uu.coords(tile_id)

        print "Clipping mangrove soil C from mangrove soil vrt for", tile_id
        uu.warp_to_Hansen('mangrove_soil_C.vrt', '{0}_mangrove_full_extent.tif'.format(tile_id), xmin, ymin, xmax, ymax)

        mangrove_soil = '{0}_mangrove_full_extent.tif'.format(tile_id)
        mangrove_biomass = '{0}_{1}.tif'.format(tile_id, cn.pattern_mangrove_biomass_2000)
        outname = '{0}_mangrove_masked_to_mangrove.tif'.format(tile_id)
        out = '--outfile={}'.format(outname)
        calc = '--calc=A*(B>0)'
        datatype = '--type={}'.format('Int16')

        print "Masking mangrove soil to mangrove biomass for", tile_id
        cmd = ['gdal_calc.py', '-A', mangrove_soil, '-B', mangrove_biomass,
               calc, out, '--NoDataValue=0', '--co', 'COMPRESS=DEFLATE', '--overwrite', datatype]
        subprocess.check_call(cmd)

    else:

        print "No mangrove aboveground biomass tile for", tile_id

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, 'mangrove_masked_to_mangrove')



def create_mineral_soil_C(tile_id):

    # Start time
    start = datetime.datetime.now()

    print "Getting extent of", tile_id
    xmin, ymin, xmax, ymax = uu.coords(tile_id)

    print "Clipping mineral soil C for", tile_id
    uu.warp_to_Hansen('mineral_soil_C.vrt', '{0}_{1}.tif'.format(tile_id, 'mineral_soil'), xmin, ymin, xmax, ymax)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_soil_C_full_extent_2000)


def create_combined_soil_C(tile_id):

    # Start time
    start = datetime.datetime.now()

    if os.path.exists('{0}_{1}.tif'.format(tile_id, cn.pattern_mangrove_biomass_2000)):

        print "Mangrove aboveground biomass tile found for", tile_id

        print "Getting extent of", tile_id
        xmin, ymin, xmax, ymax = uu.coords(tile_id)

        print "Clipping combined soil C for", tile_id
        uu.warp_to_Hansen('combined_soil_C.vrt', '{0}_{1}.tif'.format(tile_id, cn.pattern_soil_C_full_extent_2000), xmin, ymin, xmax, ymax)

    else:

        print "No mangrove aboveground biomass tile for", tile_id

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_soil_C_full_extent_2000)
