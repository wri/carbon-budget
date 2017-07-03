import subprocess
from osgeo import gdal
import utilities
import shutil
import os
import sys

currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
import get_extent


def clip_year_tiles(tile_year_list):

    tile_id = tile_year_list[0]    
    year = tile_year_list[1]
    vrt_name = "global_vrt_{}_wgs84.vrt".format(year)
    year_tifs_folder = "{}_year_tifs".format(year)

    # get coords of hansen tile
    hansen_tile = utilities.wgetloss(tile_id)
    xmin, ymin, xmax, ymax = get_extent.get_extent(hansen_tile)    

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
    cmd = ['aws', 's3', 'mv', recoded_output, 's3://gfw-files/sam/carbon_budget/burn_year_10degtiles_modisproj/']

    #subprocess.check_call(cmd)

    # rm files
    files_to_remove = [clipped_raster, hansen_tile, clipped_raster, recoded_output]
    utilities.remove_list_files(files_to_remove)

    shutil.rmtree(year_tifs_folder)


