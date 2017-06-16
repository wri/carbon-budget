import subprocess
import numpy as np
from osgeo import gdal
import utilities
import glob
import os
import sys
import shutil

import multithreaddownload

currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
import get_extent

def process_ba(global_grid_hv):

    for year in range (2001, 2016):

        download_ba(global_grid_hv) # ba_2006/day_tiles/h00v00
                        
        # convert ba to array then stack, might be better on memory
        tiles_path = 'ba_{0}/day_tiles/{1}'.format(year, global_grid_hv)
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
 
            stacked_year_raster = utilities.array_to_raster(global_grid_hv, long_year, max_stacked_year_array, template_raster, year_folder)
            proj_com_tif = utilities.set_proj(stacked_year_raster)
            with open('year_list.txt', 'w') as list_of_ba_years:
                list_of_ba_years.write(proj_com_tif + "\n")
        
            # upload to somewhere on s3
            cmd = ['aws', 's3', 'cp', proj_com_tif, 's3://gfw-files/sam/carbon_budget/burn_year/']
            subprocess.check_call(cmd)
            
            # remove files
            shutil.rmtree(tiles_path)
            
        else:
            pass
            
        # build a vrt
        vrt_name = "global_vrt_{}.vrt".format(long_year)
        file_path = "ba_{0}/*{0}*comp.tif".format(long_year)
        cmd = ['gdalbuildvrt', '-input_file_list', 'year_list.txt', vrt_name]
        subprocess.check_call(cmd)
        
        # clip vrt to hansen tile extent
        tile_id = '10N_110E'
        ymax, xmin, ymin, xmax = utilities.coords(tile_id)
        clipped_raster = "ba_{0}_{1}.tif".format(long_year, tile_id)
        cmd = ['gdal_translate', '-ot', 'Byte', '-co', 'COMPRESS=LZW', '-a_nodata', '0',
            vrt_name, clipped_raster, '-tr', '.00025', '.00025', '-projwin', str(xmin), str(ymax), str(xmax), str(ymin)]

        subprocess.check_call(cmd) 

        cmd = ['aws', 's3', 'cp', clipped_raster, 's3://gfw-files/sam/carbon_budget/burn_year_10degtiles/']

        subprocess.check_call(cmd)

        os.remove('year_list.txt')	
    
'''
# make a list of all the year tifs across windows
windows = glob.glob("win*/*_{}.tif".format(year))
vrt_textfile = "{}_vrtlist.txt".format(year)
print "writing vrt for {}".format(year)

with open (vrt_textfile, "a") as text:
    for w in windows:
        text.write(w + "\n")
text.close()

# build a vrt for that year
if not os.path.exists('vrt/'):
    os.mkdir('vrt/')

vrt_file = 'vrt/{}.vrt'.format(year)
cmd = ['gdalbuildvrt', '-input_file_list', vrt_textfile, vrt_file]
subprocess.check_call(cmd)

# clip each vrt year to the tile
tile_id = '10N_060W'

tile_folder = "{0}/".format(tile_id)
if not os.path.exists(tile_folder):
    os.mkdir(tile_folder)

print "clipping {0} vrt to {1}".format(year, tile_id)        
# clip vrt to tile
clipped_raster_1 = os.path.join(tile_folder, "{0}_{1}_temp.tif".format(tile_id, year))
clipped_raster_2 = os.path.join(tile_folder, "{0}_{1}.tif".format(tile_id, year))

xmin, ymin, xmax, ymax = get_extent.get_extent('{}.tif'.format(tile_id))
coords = ['-projwin', str(xmin), str(ymax), str(xmax), str(ymin)] 

cmd = ['gdal_translate', '-ot', 'Byte', '-co', 'COMPRESS=LZW', '-a_nodata', '0',
        vrt_file, clipped_raster_1, '-tr', '.00025', '.00025', '-projwin', str(xmin), str(ymax), str(xmax), str(ymin)]

subprocess.check_call(cmd)    

cmd = ['gdal_translate', '-ot', 'Byte', '-co', 'COMPRESS=LZW', '-a_nodata', '0',
        clipped_raster_1, clipped_raster_2, '-projwin', str(xmin), str(ymax), str(xmax), str(ymin)]
subprocess.check_call(cmd)
os.remove(clipped_raster_1)
# convert burn year tile to array

# stack arrays, get ne burn years relative to loss years
'''
