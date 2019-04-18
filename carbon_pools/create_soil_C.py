import datetime
import subprocess
import sys
sys.path.append('../')
import universal_util as uu
import constants_and_names as cn

def create_mangrove_soil_C(tile_id):

    # Start time
    start = datetime.datetime.now()

    print "Getting extent of", tile_id
    xmin, ymin, xmax, ymax = uu.coords(tile_id)

    print "Clipping mangrove soil C for", tile_id
    uu.warp_to_Hansen('mangrove_soil_C.vrt', '{0}_mangrove.tif'.format(tile_id), xmin, ymin, xmax, ymax)

    mangrove_soil = '{0}_mangrove.tif'.format(tile_id)
    mangrove_biomass = '{0}_{1}.tif'.format(tile_id, cn.pattern_mangrove_biomass_2000)
    outname = '{0}_mangrove_intermediate.tif'.format(tile_id)
    out = '--outfile={}'.format(outname)
    calc = '--calc=A*(B>0)'

    print "Masking mangrove soil to mangrove biomass for", tile_id
    cmd = ['gdal_calc.py', '-A', mangrove_soil, '-B', mangrove_biomass,
           calc, out, '--NoDataValue=0', '--co', 'COMPRESS=LZW', '--overwrite']
    subprocess.check_call(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, 'mangrove_intermediate')



def create_combined_soil_C(tile_id):

    # Start time
    start = datetime.datetime.now()

    print "Getting extent of", tile_id
    xmin, ymin, xmax, ymax = uu.coords(tile_id)

    print "Clipping soil C for", tile_id
    uu.warp_to_Hansen('combined_soil_C.vrt', '{0}_{1}_intermediate.tif'.format(tile_id, cn.pattern_soil_C_full_extent_2000), xmin, ymin, xmax, ymax)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_soil_C_full_extent_2000)
