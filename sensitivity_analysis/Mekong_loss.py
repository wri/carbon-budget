import datetime
import numpy as np
import os
import rasterio
import subprocess
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Replaces the default loss value of 100 with the year of loss for each loss year raster
def recode_tiles(annual_loss):

    print("Recoding loss tile by year")

    year = int(annual_loss[-8:-4])
    print(year)

    if year < 2001 or year > (2000 + cn.loss_years):

        print("Skipping {} because outside of model range".format(year))
        return

    else:

        calc = '--calc={}*(A==100)'.format(int((year-2000)))
        recoded_output = "Mekong_loss_recoded_{}.tif".format(year)
        outfile = '--outfile={}'.format(recoded_output)

        cmd = ['gdal_calc.py', '-A', annual_loss, calc, outfile, '--NoDataValue=0', '--co', 'COMPRESS=LZW', '--quiet']
        subprocess.check_call(cmd)

def reset_nodata(tile_id):

    print("Changing 0 from NoData to actual value for tile", tile_id)

    tile = '{0}_{1}.tif'.format(tile_id, cn.pattern_Mekong_loss_processed)

    cmd = ['gdal_edit.py', '-unsetnodata', tile]

    print("Tile processed")