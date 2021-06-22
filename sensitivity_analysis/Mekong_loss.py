import datetime
import numpy as np
import os
import rasterio
from subprocess import Popen, PIPE, STDOUT, check_call
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Replaces the default loss value of 100 with the year of loss for each loss year raster
def recode_tiles(annual_loss):

    uu.print_log("Recoding loss tile by year")

    year = int(annual_loss[-8:-4])
    uu.print_log(year)

    if year < 2001 or year > (2000 + cn.loss_years):

        uu.print_log("Skipping {} because outside of model range".format(year))
        return

    else:

        calc = '--calc={}*(A==100)'.format(int((year-2000)))
        recoded_output = "Mekong_loss_recoded_{}.tif".format(year)
        outfile = '--outfile={}'.format(recoded_output)

        cmd = ['gdal_calc.py', '-A', annual_loss, calc, outfile, '--NoDataValue=0', '--co', 'COMPRESS=DEFLATE', '--quiet']
        # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
        process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
        with process.stdout:
            uu.log_subprocess_output(process.stdout)

def reset_nodata(tile_id):

    uu.print_log("Changing 0 from NoData to actual value for tile", tile_id)

    tile = '{0}_{1}.tif'.format(tile_id, cn.pattern_Mekong_loss_processed)

    cmd = ['gdal_edit.py', '-unsetnodata', tile]

    uu.print_log("Tile processed")