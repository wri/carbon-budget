### Calculates the net emissions over the study period, with units of Mg CO2/ha on a pixel-by-pixel basis

from subprocess import Popen, PIPE, STDOUT, check_call
import os
import datetime
import rasterio
from shutil import copyfile
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def output_per_pixel(tile_id, input_pattern, output_pattern, sensit_type):

    uu.print_log("Calculating per pixel values for", tile_id)

    # Start time
    start = datetime.datetime.now()

    # Names of the input biomass and TCD tiles
    input_model_tile = '{0}_{1}.tif'.format(tile_id, input_pattern)
    area_tile = 'hanson_2013_area_{}.tif'.format(tile_id)
    output_model_tile = '{0}_{1}.tif'.format(tile_id, output_pattern)

    uu.print_log("Converting {} from Mg CO2/ha to Mg CO2/pixel...".format(input_model_tile))
    # Equation argument for converting emissions from per hectare to per pixel.
    # First, multiplies the per hectare emissions by the area of the pixel in m2, then divides by the number of m2 in a hectare.
    calc = '--calc=A*B/{}'.format(cn.m2_per_ha)
    out = '--outfile={}'.format(output_model_tile)
    cmd = ['gdal_calc.py', '-A', input_model_tile, '-B', area_tile, calc, out, '--NoDataValue=0', '--co', 'COMPRESS=LZW',
           '--overwrite', '--quiet']
    uu.log_subprocess_output_full(cmd)

    uu.print_log("  Per pixel values calculated for {}".format(output_model_tile))

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, output_pattern)