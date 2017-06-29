import glob
import os
import gdal
import subprocess
import numpy as np
from osgeo import gdal
import sys
import shutil 

currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)

import get_extent

def warp84(tif):
    wgs84tif = tif.replace(".tif", "_wgs.tif")
    cmd = ['gdalwarp', '-t_srs', 'EPSG:4326', tif, wgs84tif]
    
    subprocess.check_call(cmd)
    
    return wgs84tif
    
def hdf_to_array(hdf):
    hdf_open = gdal.Open(hdf).GetSubDatasets()
    ds = gdal.Open(hdf_open[0][0])
    array = ds.ReadAsArray()

    return array

def makedir(folder):
    if not os.path.exists(folder):
        os.mkdir(folder)
        
def get_hv_format(h, v):
    
    # get right format 
    if len(str(h)) == 1:
        h = "0{}".format(h)
    if len(str(v)) == 1:
        v = "0{}".format(v)
        
    return h, v
    
def hdf_to_tif(hdf):
    # hdf= MCD64A1.A2006001.h29v08.006.2017017135341.hdf
    year = hdf.split(".")[1].strip("A")[:4]
    day = hdf.split(".")[1].strip("A")[-3:]
    hv = hdf.split(".")[2]

    outtif = 'burndate_{}{}_{}.tif'.format(year, day, hv)
    dirname = os.path.dirname(hdf)
    hdf_w_quotes = '"{}"'.format(hdf)
    hdf_file = 'HDF4_EOS:EOS_GRID:{}:MOD_Grid_Monthly_500m_DB_BA:Burn Date'.format(hdf_w_quotes)
    hdf_file2 = "{}".format(hdf_file)

    cmd = ['gdal_translate', hdf_file2, outtif, '-co', 'COMPRESS=LZW']
    print "converting to tif"    
    subprocess.check_call(cmd)
    return outtif
    
def set_proj(tif):
    set_proj = ['gdal_edit.py', '-a_srs', 'sphere.wkt', tif]
    print "setting projection"
    subprocess.check_call(set_proj)

    proj_tif = tif.replace(".tif", "_wgs84.tif")
    proj_tif_comp = proj_tif.replace("_wgs84.tif", "_wgs84_comp.tif")
    wgs84 = ['gdalwarp', '-t_srs', 'EPSG:4326', '-overwrite', '-tap', '-tr', '.0025', '.0025', '-co', 'COMPRESS=LZW', tif, proj_tif]
    print "projecting"

    subprocess.check_call(wgs84)
    
    # compress again
    compress = ['gdal_translate', '-co', 'COMPRESS=LZW', proj_tif, proj_tif_comp]
    subprocess.check_call(compress)
    os.remove(tif)
    os.remove(proj_tif)
    
    return proj_tif_comp

def wgetloss(tile_id):
    print "download hansen loss tile"
    hansen_tile = '{}_loss.tif'.format(tile_id)
    cmd = ['wget', r'http://glad.geog.umd.edu/Potapov/GFW_2015/tiles/{}.tif'.format(tile_id),
           '-O', hansen_tile]

    subprocess.check_call(cmd)    
    return hansen_tile

def coords(tile_id):
    NS = tile_id.split("_")[0][-1:]
    EW = tile_id.split("_")[1][-1:]

    if NS == 'S':
        ymax =-1*int(tile_id.split("_")[0][:2])
    else:
        ymax = int(str(tile_id.split("_")[0][:2]))
    
    if EW == 'W':
        xmin = -1*int(str(tile_id.split("_")[1][:3]))
    else:
        xmin = int(str(tile_id.split("_")[1][:3]))
        
    
    ymin = str(int(ymax) - 10)
    xmax = str(int(xmin) + 10)
    
    return ymax, xmin, ymin, xmax
    
def download_ba(global_grid_hv, year):
    ftp_path = 'ftp://ba1.geog.umd.edu/Collection6/HDF/{0}/'.format(year)
    outfolder = "ba_{0}/day_tiles/{1}/".format(year, global_grid_hv)
    if not os.path.exists(outfolder):
        os.makedirs(outfolder)
        
    file_name = "*.{}*.*.hdf".format(global_grid_hv)
    cmd = ['wget', '-r', '--ftp-user=user', '--ftp-password=burnt_data', '-A', file_name, '--no-directories', '--no-parent', ftp_path, '-P', outfolder]
    
    subprocess.check_call(cmd)
       
def raster_to_array(raster):
    ds = gdal.Open(raster)
    array = np.array(ds.GetRasterBand(1).ReadAsArray())
    
    return array

def array_to_raster(global_grid_hv, year, array, template_hdf, outfolder):

    filename = '{0}_{1}.tif'.format(year, global_grid_hv)
    dst_filename = os.path.join(outfolder, filename)
    # x_pixels, y_pixels = get_extent.get_size(raster)
    hdf_open = gdal.Open(template_hdf).GetSubDatasets()
    ds = gdal.Open(hdf_open[0][0])
    x_pixels = ds.RasterXSize
    y_pixels = ds.RasterYSize

    geoTransform = ds.GetGeoTransform()
    height = geoTransform[1]
    width = geoTransform[5]
    
    # pixel_size = get_extent.pixel_size(raster)   
    pixel_size = geoTransform[1]
    # minx, miny, maxx, maxy = get_extent.get_extent(raster)

    minx = geoTransform[0]
    maxy = geoTransform[3]
    maxx = minx + geoTransform[1] * ds.RasterXSize
    miny = maxy + geoTransform[5] * ds.RasterYSize
    
    
    #wkt_projection =  'GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]]'
    
    #wkt_projection = 'PROJCS["Sphere_Sinusoidal",GEOGCS["GCS_Sphere",DATUM["D_Sphere",SPHEROID["Sphere",6371000,0]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]],PROJECTION["Sinusoidal"],PARAMETER["False_Easting",0],PARAMETER["False_Northing",0],PARAMETER["Central_Meridian",0],UNIT["Meter",1]]'
    
    wkt_projection = 'PROJCS["Sinusoidal",GEOGCS["GCS_Undefined",DATUM["D_Undefined",SPHEROID["User_Defined_Spheroid",6371007.181,0.0]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.017453292519943295]],PROJECTION["Sinusoidal"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",0.0],UNIT["Meter",1.0]]'
    # http://spatialreference.org/ref/sr-org/6974/
    #wkt_projection = 'PROJCS["MODIS Sinusoidal",GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]],PROJECTION["Sinusoidal"],PARAMETER["false_easting",0.0],PARAMETER["false_northing",0.0],PARAMETER["central_meridian",0.0],PARAMETER["semi_major",6371007.181],PARAMETER["semi_minor",6371007.181],UNIT["m",1.0]]'
    
    
    driver = gdal.GetDriverByName('GTiff')

    dataset = driver.Create(
        dst_filename,
        x_pixels,
        y_pixels,
        1,
        gdal.GDT_Int16, )

    dataset.SetGeoTransform((
        minx,    # 0
        pixel_size,  # 1
        0,                      # 2
        maxy,    # 3
        0,                      # 4
        -pixel_size))  

    dataset.SetProjection(wkt_projection)
    dataset.GetRasterBand(1).WriteArray(array)
    dataset.FlushCache()  # Write to disk.
    
    return dst_filename
    
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
    print "stacking arrays"
    print list_of_year_arrays
    stack = np.stack((list_of_year_arrays))
    
    return stack


  
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
