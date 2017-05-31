import subprocess
import numpy as np
from osgeo import gdal
import utilities
import glob
import os
import sys
currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)

import get_extent
window = '05'
year = 11

# download rasters for window
#utilities.download_ba(window, year)

 # convert month rasters to arrays
array_list = []
year += 2000
rasters = glob.glob("ba_{0}_{1}/*".format(window, year))

for r in rasters:
    print r
    array = utilities.raster_to_array(r)
    array_list.append(array)
# stack month rasters for the year and get max value
stacked_year_array = utilities.stack_arrays(array_list)
max_stacked_year_array = stacked_year_array.max(0)

# convert stacked month arrays to 1 raster for the year
template_raster = rasters[0]
outfolder = "win{0}/".format(window)
if not os.path.exists(outfolder):
    os.mkdir(outfolder)
print "making year window raster"        
utilities.array_to_raster(window, year, max_stacked_year_array, template_raster, outfolder)

#####################################################################################
# make a list of all the year tifs across windows
windows = glob.glob("win*/*tif")
vrt_textfile = "{}_vrtlist.txt".format(year)
print "writing vrt for {}".format(year)

with open (vrt_textfile, "a") as text:
    for w in windows:
        window_year = w.split("_")[1][:4]
	if str(window_year) == str(year):
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
