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

    for year in range (2000, 2016):

        output_dir = '{0}/{1}/raw/'.format(global_grid_hv, year)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        include = '*A{0}*{1}*'.format(year, global_grid_hv)
        cmd = ['aws', 's3', 'cp', 's3://gfw-files/sam/carbon_budget/burn_raw/', output_dir, '--recursive', '--exclude', "*", '--include', include]  
        subprocess.check_call(cmd)
        
        hdf_files = glob.glob(output_dir+"*hdf")
        if len(hdf_files) > 0:
        
            array_list = []

            for hdf in hdf_files:

                # convert each hdf to a tif
                tif = utilities.hdf_to_tif(hdf)

                array = utilities.raster_to_array(tif)
            
                array_list.append(array)
            
            # stack arrays, get 1 raster for the year and tile
            stacked_year_array = utilities.stack_arrays(array_list)
            max_stacked_year_array = stacked_year_array.max(0)

            # convert stacked month arrays to 1 raster for the year
            rasters = glob.glob("burndate_{0}*_{1}.tif".format(year, global_grid_hv))
            template_raster = rasters[0]
            year_folder ='{0}/{1}/stacked/'.format(global_grid_hv, year)
            if not os.path.exists(year_folder):
                os.makedirs(year_folder)
            
            stacked_year_raster = utilities.array_to_raster(global_grid_hv, year, max_stacked_year_array, template_raster, year_folder)
            proj_com_tif = utilities.set_proj(stacked_year_raster)
            
            # after year raster is stacked, write it to text for vrt creation
            with open('year_list.txt', 'w') as list_of_ba_years:
                list_of_ba_years.write(proj_com_tif + "\n")
        
            # upload to somewhere on s3
            cmd = ['aws', 's3', 'cp', proj_com_tif, 's3://gfw-files/sam/carbon_budget/burn_year/']
            subprocess.check_call(cmd)
            
            
            # remove files
            
            shutil.rmtree(year_folder)
            shutil.rmtree(output_dir)
            burndate_name = "burndate_{0}*_{1}.tif".format(year, global_grid_hv)
            burndate_day_tif = glob.glob(burndate_name)
            for tif in burndate_day_tif:
                os.remove(tif)
        else:
            pass
            
def clip_year_tiles(tile_id):         

    # get coords of hansen tile
    ymax, xmin, ymin, xmax = utilities.coords(tile_id)
    
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
    cmd = ['aws', 's3', 'mv', recoded_output, 's3://gfw-files/sam/carbon_budget/burn_year_10degtiles/']

    subprocess.check_call(cmd)

    # rm files
    os.remove('vrt_files.txt')	
    cmd = ['rm', year_tifs_folder+ "/", '-r']
    subprocess.check_call(cmd)
    os.remove(clipped_raster)

