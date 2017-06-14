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
'''
ftp://ba1.geog.umd.edu/Collection6/HDF
- 2006
    - 001
        h01v01.tif <------
        h01v02.tif
        h03v01.tif
    - 032
        h01v01.tif <------
        h01v02.tif
        h03v01.tif
    - 060
        h01v01.tif <------
        h01v02.tif
        h03v01.tif
    ....
    - 335
        h01v01.tif <------
        h01v02.tif
        h03v01.tif
- 2007
    - 001
        h01v01.tif
        h01v02.tif
        h03v01.tif
    - 032
        h01v01.tif
        h01v02.tif
        h03v01.tif
    - 060
        h01v01.tif
        h01v02.tif
        h03v01.tif
'''


import get_extent

for year in [6]:

    long_year = 2000 + year
    
    year_folder = "ba_{}".format(long_year)
    
    utilities.makedir(year_folder)
     
    for h in range(29, 31):
    
        for v in range(8, 9):
        
            h, v = utilities.get_hv_format(h, v)
            tile_folder = "day_tiles/h{}v{}/".format(h, v)
            #print "downloading tiles"
            utilities.download_ba(long_year, h, v)
            
            # convert ba to array then stack, might be better on memory
            tiles_path = os.path.join(year_folder, tile_folder)
            hdf_files = glob.glob(tiles_path+"*hdf")
            
            array_list = []

            array_count = 0
            for hdf in hdf_files:
                array_count += 1

                # convert each raster to a tif
                tif = utilities.hdf_to_tif(hdf)

                array = utilities.raster_to_array(tif)\
                
                array_list.append(array)
                
            # stack arrays, get 1 raster for the year and tile
            stacked_year_array = utilities.stack_arrays(array_list)
            max_stacked_year_array = stacked_year_array.max(0)

            # convert stacked month arrays to 1 raster for the year
            rasters = glob.glob("burndate_{0}*_h{1}v{2}.tif".format(long_year, h, v))
            template_raster = rasters[0]
            print "template raster: {}".format(template_raster)
     
            stacked_year_raster = utilities.array_to_raster(h, v, long_year, max_stacked_year_array, template_raster, year_folder)
            proj_com_tif = set_proj(stacked_year_raster)
            
            # upload to somewhere on s3
            cmd = ['aws', 's3', 'cp', proj_com_tif, 's3://gfw-files/sam/carbon_budget/burn_year/']
            subprocess.check_call(cmd)
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
