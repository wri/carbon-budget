import glob
import os
#import gdal
import subprocess
import numpy as np
#from osgeo import gdal
import sys

currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)

import get_extent




def download_ba(window, year):

    year += 2000
    
    ftp_path = 'ftp://ba1.geog.umd.edu/Collection6/TIFF/Win{0}/{1}'.format(window, year)
    
    outfolder = "ba_{0}_{1}/".format(window, year)
    if not os.path.exists(outfolder):
        os.mkdir(outfolder)
    cmd = ['wget', '-r', '--ftp-user=user', '--ftp-password=burnt_data', '--no-directories', '--no-parent', '-A', '*burndate.tif', ftp_path, '-P', outfolder]
    
    print cmd
    
    subprocess.check_call(cmd)
    
    
def raster_to_array(raster):
    ds = gdal.Open(raster)
    array = np.array(ds.GetRasterBand(1).ReadAsArray())
    
    return array



def array_to_raster(window, year, array, raster, outfolder):

    filename = 'win{0}_{1}.tif'.format(window, year)
    dst_filename = os.path.join(outfolder, filename)
    x_pixels, y_pixels = get_extent.get_size(raster)

    pixel_size = get_extent.pixel_size(raster)   
    
    minx, miny, maxx, maxy = get_extent.get_extent(raster)

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
        pixel_size,  # 1
        0,                      # 2
        y_max,    # 3
        0,                      # 4
        -pixel_size))  

    dataset.SetProjection(wkt_projection)
    dataset.GetRasterBand(1).WriteArray(array)
    dataset.FlushCache()  # Write to disk.
    
def recode_to_year(ba_tif, window):
    # ba_tif = MCD64monthly.A2007335.Win05.006.burndate.tif
    year = ba_tif.split(".")[1].strip("A")[:4]
    year_int = int(year) - 2000
    calc_str = '--calc={}*(A>0)'.format(year_int)
    out_file = '--outfile=ba_{}.tif'.format(year_int)
    cmd = ['gdal_calc.py', '-A', ba_tif, calc_str, out_file, '--NoDataValue=0', '--co', 'COMPRESS=LZW']
    print cmd
    subprocess.check_call(cmd)

def stack_arrays(list_of_year_arrays):
    stack = np.stack((list_of_year_arrays))
    


    
"""
a1 = [1,2,3]
a2 = [3,2,1]
lossarray = [1,3,9]
stack = np.stack((a1, a2))
lossarray_min1 = np.subtract(lossarray, 1)
stack_con =(stack >= lossarray_min1) & (stack <= lossarray)
stack_con2 = stack_con * stack
final = stack_con2.max(0)
- then, write it out to a raster. 

"""
