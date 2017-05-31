import gdal
from gdalconst import GA_ReadOnly

def get_extent(tif):
    data = gdal.Open(tif, GA_ReadOnly)
    geoTransform = data.GetGeoTransform()
    minx = geoTransform[0]
    maxy = geoTransform[3]
    maxx = minx + geoTransform[1] * data.RasterXSize
    miny = maxy + geoTransform[5] * data.RasterYSize
    print [minx, miny, maxx, maxy]
    data = None
    return minx, miny, maxx, maxy
    
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