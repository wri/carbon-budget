'''
This script makes mask tiles of where peat pixels are. Peat is represented by 1s; non-peat is no-data.
Between 40N and 60S, CIFOR peat and Jukka peat (IDN and MYS) are combined to map peat.
Outside that band (>40N, since there are no tiles at >60S), SoilGrids250m is used to mask peat.
Any pixel that is marked as most likely being a histosol subgroup is classified as peat.
'''

import subprocess
import os
import datetime
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def create_peat_mask_tiles(tile_id):

    # Start time
    start = datetime.datetime.now()

    print "Getting bounding coordinates for tile", tile_id
    xmin, ymin, xmax, ymax = uu.coords(tile_id)
    print "  ymax:", ymax, "; ymin:", ymin, "; xmax", xmax, "; xmin:", xmin

    out_tile = '{0}_{1}.tif'.format(tile_id, cn.pattern_peat_mask)

    # If the tile is outside the band covered by the CIFOR peat raster, SoilGrids250m is used
    if ymax > 40 or ymax < -60:

        print "{} is outside CIFOR band. Using SoilGrids250m organic soil mask...".format(tile_id)

        out_intermediate = '{0}_intermediate.tif'.format(tile_id, cn.pattern_peat_mask)

        # Cuts the SoilGrids250m global raster to the focal tile
        uu.warp_to_Hansen(cn.soilgrids250_peat_file, out_intermediate, xmin, ymin, xmax, ymax)

        # Removes all non-histosol sub-groups from the SoilGrids raster.
        # Ideally, this would be done once on the entire SoilGrids raster in the main function but I didn't think of that.
        # Codes 61 through 65 are the histosol subgroups in SoilGrids250.
        calc = '--calc=(A>=61)*(A<=65)'
        AGC_accum_outfilearg = '--outfile={}'.format(out_tile)
        cmd = ['gdal_calc.py', '-A', out_intermediate, calc, AGC_accum_outfilearg,
               '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW', '--type=Byte']
        subprocess.check_call(cmd)

        print "{} created.".format(tile_id)

    # If the tile is inside the band covered by CIFOR, CIFOR is used (and Jukka in the tiles where it occurs).
    # For some reason, the CIFOR raster has a color scheme that makes it symbolized from 0 to 255. This carries
    # over to the output file but that seems like a problem with the output symbology, not the values.
    # gdalinfo shows that the min and max values are 1, as they should be, and it visualizes correctly in ArcMap.
    else:

        print "{} is inside CIFOR band. Using CIFOR/Jukka combination...".format(tile_id)

        # Combines CIFOR and Jukka (if it occurs there)
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




