import subprocess
import gdal
from gdalconst import GA_ReadOnly


def download(source):
    cmd = ['aws', 's3', 'cp', source, '.']
    subprocess.check_call(cmd)


def upload(src, dest):
    cmd = ['aws', 's3', 'cp', src, dest]
    subprocess.check_call(cmd)


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


def resample(in_file, out_file):
    resample = ['gdal_translate', '-co', 'COMPRESS=LZW', '-tr', '.00025', '.00025', in_file, out_file]
    subprocess.check_call(resample)

    return out_file


def clip(in_file, out_file,  xmin, ymin, xmax, ymax, extra_param=None):
    cmd = ['gdal_translate', '-projwin', str(xmin), str(ymax), str(xmax), str(ymin), '-co', 'COMPRESS=LZW',
            in_file, out_file]

    if extra_param:
        cmd += extra_param

    subprocess.check_call(cmd)

    return out_file


def rasterize(in_shape, out_tif, xmin, ymin, xmax, ymax, tr=None, ot=None, recode=None, anodata=None):
    cmd = ['gdal_rasterize', '-co', 'COMPRESS=LZW', '-te', str(xmin), str(ymin), str(xmax), str(ymax),
           '-tr', tr, tr, '-ot', ot, '-a', recode, '-a_nodata',
           anodata, in_shape, out_tif]

    subprocess.check_call(cmd)

    return out_tif