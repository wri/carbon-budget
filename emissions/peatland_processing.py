### Creates Hansen-style tiles for aboveground mangrove biomass (Mg/ha) from Lola Fatoyinbo's country
### mangrove data.
### Output tiles conform to the dimensions, resolution, and other properties of Hansen loss tiles.

import utilities
import subprocess
import os
import datetime
from osgeo import gdal
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Creates mangrove tiles using Hansen tile properties
def create_peat_mask_tiles(tile_id):

    # Start time
    start = datetime.datetime.now()

    print "Getting bounding coordinates for tile", tile_id
    xmin, ymin, xmax, ymax = uu.coords(tile_id)
    print "  ymax:", ymax, "; ymin:", ymin, "; xmax", xmax, "; xmin:", xmin

    out_tile = '{0}_{1}.tif'.format(tile_id, cn.pattern_peat_mask)

    if ymax > 40 or ymax < -60:

        print "{} is outside CIFOR band. Using SoilGrids250m organic soil mask...".format(tile_id)

        out_intermediate = '{0}_intermediate.tif'.format(tile_id, cn.pattern_peat_mask)

        uu.warp_to_Hansen(cn.soilgrids250_peat_file, out_intermediate, xmin, ymin, xmax, ymax)

        # Carbon gain uses non-mangrove non-planted biomass:carbon ratio
        calc = '--calc=(A>=61)*(A<=65)'
        AGC_accum_outfilearg = '--outfile={}'.format(out_tile)
        cmd = ['gdal_calc.py', '-A', out_intermediate, calc, AGC_accum_outfilearg,
               '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW', '--type=Byte']
        subprocess.check_call(cmd)

        print "{} created.".format(tile_id)

    else:

        print "{} is inside CIFOR band. Using CIFOR/Jukka combination...".format(tile_id)

        cmd = ['gdalwarp', '-t_srs', 'EPSG:4326', '-co', 'COMPRESS=LZW', '-tr', '{}'.format(cn.Hansen_res), '{}'.format(cn.Hansen_res),
               '-tap', '-te', str(xmin), str(ymin), str(xmax), str(ymax),
               '-dstnodata', '0', '-overwrite', '{}'.format(cn.cifor_peat_file), 'jukka_peat.tif', out_tile]

        subprocess.check_call(cmd)
        print "{} created.".format(tile_id)



    print "Checking if {} contains any data...".format(tile_id)
    stats = uu.check_for_data(out_tile)

    if stats[0] > 0:

        print "  Data found in {}. Keeping file...".format(tile_id)

    else:

        print "  No data found. Deleting {}...".format(tile_id)
        os.remove(out_tile)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_peat_mask)




