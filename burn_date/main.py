import subprocess
import numpy as np
from osgeo import gdal
import utilities

# download rasters for window
utilities.download_ba('05', 7)


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