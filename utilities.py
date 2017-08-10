


# def get_extent(tif):
    # data = gdal.Open(tif, GA_ReadOnly)
    # geoTransform = data.GetGeoTransform()
    # minx = geoTransform[0]
    # maxy = geoTransform[3]
    # maxx = minx + geoTransform[1] * data.RasterXSize
    # miny = maxy + geoTransform[5] * data.RasterYSize
    # print [minx, miny, maxx, maxy]
    # data = None
    # return minx, miny, maxx, maxy
    
    

def clip_raster(raster):
    # get extent of a tile
    
    xmin, ymin, xmax, ymax = get_extent(raster)

    coords = ['-projwin', str(xmin), str(ymax), str(xmax), str(ymin)]
    
    xmin_clip = xmin + 5
    xmax_clip = xmin_clip + 0.15
    ymin_clip = ymin + 5
    ymax_clip = ymin_clip + 0.15
    
    clipped_raster = raster.replace(".tif", "_clip.tif")
    cmd = ['gdal_translate', '-ot', 'Byte', '-co', 'COMPRESS=LZW', '-a_nodata', '0',
                raster, clipped_raster, '-projwin', str(xmin_clip), str(ymax_clip), str(xmax_clip), str(ymin_clip)]

    subprocess.check_call(cmd)
clip_raster('10N_100E_litter.tif')