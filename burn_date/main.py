import subprocess
import numpy as np
from osgeo import gdal
import utilities
import glob
import os
import sys
import shutil

currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
import get_extent

def process_ba(global_grid_hv):

    for year in range (2006, 2007):

        output_dir = '{0}/{1}/raw/'.format(global_grid_hv, year)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        year_tifs_folder = "{}_year_tifs".format(year)
        # download hdf files
        include = '*A{0}*{1}*'.format(year, global_grid_hv)
        cmd = ['aws', 's3', 'cp', 's3://gfw-files/sam/carbon_budget/burn_raw/', output_dir, '--recursive', '--exclude', "*", '--include', include]  
        #subprocess.check_call(cmd)
        
        hdf_files = glob.glob(output_dir+"*hdf")
        if len(hdf_files) > 0:
        
            array_list = []
            # convert hdf to array
            for hdf in hdf_files:
                print "converting hdf to array"
            	array = utilities.hdf_to_array(hdf)
                array_list.append(array)
   
            # stack arrays, get 1 raster for the year and tile
            print "stacking arrrays"
            stacked_year_array = utilities.stack_arrays(array_list)
            max_stacked_year_array = stacked_year_array.max(0)

            # convert stacked month arrays to 1 raster for the year
            template_hdf = hdf_files[0]
            year_folder ='{0}/{1}/stacked/'.format(global_grid_hv, year)
            if not os.path.exists(year_folder):
                os.makedirs(year_folder)
            print "making raster"
            stacked_year_raster = utilities.array_to_raster(global_grid_hv, year, max_stacked_year_array, template_hdf, year_folder)
            print stacked_year_raster
            # upload to somewhere on s3
            cmd = ['aws', 's3', 'cp', stacked_year_raster, 's3://gfw-files/sam/carbon_budget/burn_year_modisproj/']
            subprocess.check_call(cmd)
            # remove files
        else:
            pass
#for hv in ['h30v08', 'h31v08', 'h32v08', 'h30v09', 'h31v09', 'h32v09', 'h30v10', 'h31v10', 'h32v10']:
#    process_ba(hv)                
def clip_year_tiles(tile_year_list):

    tile_id = tile_year_list[0]    
    year = tile_year_list[1]
    vrt_name = "global_vrt_{}_wgs84.vrt".format(year)
    year_tifs_folder = "{}_year_tifs".format(year)

    # get coords of hansen tile

    # download hanse tile
    hansen_tile = utilities.wgetloss(tile_id)
    #ymax, xmin, ymin, xmax = utilities.coords(tile_id)
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
    #os.remove(clipped_raster)
#clip_year_tiles(["00N_130E",2006])
