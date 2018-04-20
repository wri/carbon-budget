import gdal
from gdalconst import GA_ReadOnly

    
def get_size(tif):
    data = gdal.Open(tif, GA_ReadOnly)
    xsize = data.RasterXSize
    ysize = data.RasterYSize

    data = None
    
    return xsize, ysize
    
def pixel_size(tif):
    data = gdal.Open(tif, GA_ReadOnly)
    geoTransform = data.GetGeoTransform()
    height = geoTransform[1]
    width = geoTransform[5]

    data = None
    
    return height