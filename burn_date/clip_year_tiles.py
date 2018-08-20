import os
import subprocess
import sys
import shutil

import utilities

currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
from carbon_pools import get_extent


def clip_year_tiles(tile_year_list):

    tile_id = tile_year_list[0]

    print tile_id

    year = tile_year_list[1]
    vrt_name = "global_vrt_{}_wgs84.vrt".format(year)
    year_tifs_folder = "{}_year_tifs".format(year)

    # get coords of hansen tile
    hansen_tile = utilities.wgetloss(tile_id)
    xmin, ymin, xmax, ymax = utilities.get_extent(tile_id)

    # Removes .tif from the end of the tile id (previously needed but below a problem)
    tile_id = tile_id[0:8]

    # clip vrt to tile extent
    clipped_raster = "ba_{0}_{1}_clipped.tif".format(year, tile_id)
    cmd = ['gdal_translate', '-ot', 'Byte', '-co', 'COMPRESS=LZW', '-a_nodata', '0',
        vrt_name, clipped_raster, '-tr', '.00025', '.00025', '-projwin', str(xmin), str(ymax), str(xmax), str(ymin)]
    subprocess.check_call(cmd)

    # calc year tile values to be equal to year
    calc = '--calc={}*(A>0)'.format(int(year)-2000)
    recoded_output =  "ba_{0}_{1}.tif".format(year, tile_id)
    outfile = '--outfile={}'.format(recoded_output)

    cmd = ['gdal_calc.py', '-A', clipped_raster, calc, outfile, '--NoDataValue=0', '--co', 'COMPRESS=LZW']
    subprocess.check_call(cmd)

    # upload file
    # cmd = ['aws', 's3', 'mv', recoded_output, 's3://gfw-files/sam/carbon_budget/burn_year_10degtiles_modisproj/']    ## previous location
    cmd = ['aws', 's3', 'mv', recoded_output, 's3://gfw2-data/climate/carbon_model/other_emissions_inputs/burn_year/burn_year_10x10_clip/']

    subprocess.check_call(cmd)

    # remove files
    print "Removing files"
    files_to_remove = [clipped_raster, hansen_tile, recoded_output]
    utilities.remove_list_files(files_to_remove)

    print "Done removing individual files. Now removing folder."

    shutil.rmtree(year_tifs_folder)

