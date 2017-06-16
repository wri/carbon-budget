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

        utilities.download_ba(global_grid_hv) # ba_2006/day_tiles/h00v00
                        
        # convert ba to array then stack, might be better on memory
        tiles_path = 'ba_{0}/day_tiles/{1}/'.format(year, global_grid_hv)
        
        hdf_files = glob.glob(tiles_path+"*hdf")
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
            year_folder = "ba_{}".format(year)
            stacked_year_raster = utilities.array_to_raster(global_grid_hv, year, max_stacked_year_array, template_raster, year_folder)
            proj_com_tif = utilities.set_proj(stacked_year_raster)
            with open('year_list.txt', 'w') as list_of_ba_years:
                list_of_ba_years.write(proj_com_tif + "\n")
        
            # upload to somewhere on s3
            cmd = ['aws', 's3', 'cp', proj_com_tif, 's3://gfw-files/sam/carbon_budget/burn_year/']
            subprocess.check_call(cmd)
            
            
            # remove files
            
            shutil.rmtree(tiles_path)
            burndate_name = "burndate_{0}*_{1}.tif".format(year, global_grid_hv)
            burndate_day_tif = glob.glob(burndate_name)
            for tif in burndate_day_tif:
                os.remove(tif)
        else:
            pass
	sys.exit()            
        # build a vrt
        vrt_name = "global_vrt_{}.vrt".format(year)
        file_path = "ba_{0}/*{0}*comp.tif".format(year)
        cmd = ['gdalbuildvrt', '-input_file_list', 'year_list.txt', vrt_name]
        subprocess.check_call(cmd)
        
        # clip vrt to hansen tile extent
        tile_id = '10N_110E'
        ymax, xmin, ymin, xmax = utilities.coords(tile_id)
        clipped_raster = "ba_{0}_{1}.tif".format(year, tile_id)
        cmd = ['gdal_translate', '-ot', 'Byte', '-co', 'COMPRESS=LZW', '-a_nodata', '0',
            vrt_name, clipped_raster, '-tr', '.00025', '.00025', '-projwin', str(xmin), str(ymax), str(xmax), str(ymin)]

        subprocess.check_call(cmd) 

        cmd = ['aws', 's3', 'cp', clipped_raster, 's3://gfw-files/sam/carbon_budget/burn_year_10degtiles/']

        subprocess.check_call(cmd)

        os.remove('year_list.txt')	
        comp_tifs = glob.glob('ba_{0}/{0}_{1}_wgs84_comp.tif'.format(year, global_grid_hv))
        for tif in comp_tifs:
            os.remove(tif)
        day_tifs = glob.glob("burndate_*{0}.tif".format(global_grid_hv))
        for daytif in day_tifs:
            os.remove(daytif)

