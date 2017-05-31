import subprocess
import numpy as np
from osgeo import gdal
import utilities
import glob

window = '05'
year = 7

# download rasters for window
#utilities.download_ba(window, year)

# convert month rasters to arrays
array_list = []
year += 2000
rasters = glob.glob("ba_{0}_{1}\*".format(window, year)

for r in rasters:
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
        
utilities.array_to_raster('05', year, max_stacked_year_array, template_raster, outfolder)


'''
 after window has all years in it, repeat for all windows.
 win01
    -> win01_2001.tif
    -> win01_2002.tif
win02
    -> win02_2001.tif
    -> win02_2002.tif

# build vrt per year
vrt/
    2001.vrt
    2002.vrt
    ...
    2015.vrt

# make a list of all the year tifs across windows

windows = glob.glob("win*/*tif")
with open ("2007_vrtlist.txt", "a") as text:
    for w in windows:
        year = w.split(".")[1].strip('A')[:4]
        if year == '2007':
               text.write(w + "\n")
text.close()

# build a vrt for that year
cmd = ['gdalbuildvrt', '-input_file_list', n"2007_vrtlist.txt", 'vrt/2007.vrt']

# clip each vrt year to the tile

# convert burn year tile to array

# stack arrays, get ne burn years relative to loss years
    
start to loop over tiles.
tile_id : 10N_060W
        
# clip each year to tile coords
xmin, ymin, xmax, ymax = get_extent.get_extent('10N_060W.tif')
coords = ['-projwin', str(xmin), str(ymax), str(xmax), str(ymin)] 

base_cmd = ['gdal_translate', '-ot', 'Byte', '-co', 'COMPRESS=LZW', '-a_nodata', '-9999',
        '2001.vrt', clipped_raster, '-tr', '.00025', '.00025']

        clip_cmd = base_cmd + coords
    
'''
################# make 1 raster per window
outfolder = "ba_windows/"
    if not os.path.exists(outfolder):
        os.mkdir(outfolder)

# make list of all the year rasters for the window
year_rasters = glob.glob("{}/*").format(outfolder)

# clip each one to extent of loss tile
tile_id = '10N_060W'
loss_tile = 'loss\{}.tif'.format(tile_id)

# convert rasters to arrays
year_arrays_list = []

for r in year_rasters:
    array = utilities.raster_to_array(r)
    year_arrays_list.append(array)

# stack the arrays
stacked_years_array = utilities.stack_arrays(year_arrays_list)

# get loss year minus 1. if loss is 2008, want to get burn year if burn year is 2007 or 2008
loss_min1 = np.subtract()

"""

# download 2 rasters
cmd = ['wget', '--ftp-user=user', '--ftp-password=burnt_data', 'ftp://ba1.geog.umd.edu/Collection6/TIFF/Win05/2007/MCD64monthly.A2007305.Win05.006.burndate.tif']
subprocess.check_call(cmd)
cmd = ['wget', '--ftp-user=user', '--ftp-password=burnt_data', 'ftp://ba1.geog.umd.edu/Collection6/TIFF/Win05/2007/MCD64monthly.A2007335.Win05.006.burndate.tif']
subprocess.check_call(cmd)

# create 1 raster

# open each as array
ds1 = gdal.Open('MCD64monthly.A2007305.Win05.006.burndate.tif')
array1 = np.array(ds1.GetRasterBand(1).ReadAsArray())

ds2 = gdal.Open('MCD64monthly.A2007335.Win05.006.burndate.tif')
array2 = np.array(ds2.GetRasterBand(1).ReadAsArray())

# stack into one
stack = np.stack((array1, array2))

# get max values
max = stack.max(0)

# write to raster
pixel_size = 

# write to new raster
dst_filename = 'win05_2007.tif'
data = gdal.Open('MCD64monthly.A2007305.Win05.006.burndate.tif', GA_ReadOnly)
geoTransform = data.GetGeoTransform()

"""