import gdal
import subprocess
import numpy as np
from osgeo import gdal
currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)

import get_extent

def array_to_raster(window, array, xpix, ypix, xmin, ymax):
    """Array > Raster
    Save a raster from a C order array.

    :param array: ndarray
    """
    dst_filename = 'window_{}.tif'.format(window)


    # You need to get those values like you did.
    x_pixels = 10923  # number of pixels in x
    y_pixels = 5234  # number of pixels in y
    PIXEL_SIZE = 0.00439453125  # size of the pixel...        
    x_min = -81.999999999999957  
    y_max = 12.999999999999995  # x_min & y_max are like the "top left" corner.
    wkt_projection =  'GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]]'

    driver = gdal.GetDriverByName('GTiff')

    dataset = driver.Create(
        dst_filename,
        x_pixels,
        y_pixels,
        1,
        gdal.GDT_Float32, )

    dataset.SetGeoTransform((
        x_min,    # 0
        PIXEL_SIZE,  # 1
        0,                      # 2
        y_max,    # 3
        0,                      # 4
        -PIXEL_SIZE))  

    dataset.SetProjection(wkt_projection)
    dataset.GetRasterBand(1).WriteArray(array)
    dataset.FlushCache()  # Write to disk.
    
def recode_to_year(ba_tif)
    # ba_tif = MCD64monthly.A2007335.Win05.006.burndate.tif
    year = ba_tif.split(".")[1].strip("A")[:4]
    year_int = int(year) - 2000
    calc_str = '--calc="{}*(A>0)"'.format(year_int)
    out_file = '--outfile=ba_{}.tif'.format(year_int)
    cmd = ['gdal_calc.py', '-A', ba_tif, calc_str,'--outfile=year_ba.tif', '--NoDataValue=0']
    
    subprocess.check_call(cmd)
    
def raster_to_array(raster):
    ds = gdal.Open(raster)
    array = np.array(ds.GetRasterBand(1).ReadAsArray())

def stack_arrays(list_of_year_arrays):
    stack = np.stack((list_of_year_arrays))
    max = stack.max(0)
    
    
"""
for w in window:
    for y in years:
        array_list = []
        download oldest image per year, for 1 window
        recode_to_year ->recode burn day to year of raster
        array_list.append(raster_to_array) # convert raster to array, append array to list of arrays)
    # stack arrays, take max values
    max_array = stack_arrays(array_list)
    
    # convert window to 1 raster with values 1 -> 17 for year of burn
    xmin, ymin, xmax, ymax = get_extent.get_extent(raster)
    xsize, ysize = get_size(tif)
    array_to_raster(window, max_array, xsize, ysize, xmin, ymax)
    

"""

    