import subprocess

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
    
    
def wgetloss(tile_id):
    print "download hansen loss tile"
    loss_tile = '{}_loss.tif'.format(tile_id)
    cmd = ['wget', r'http://glad.geog.umd.edu/Potapov/GFW_2015/tiles/{}.tif'.format(tile_id),
           '-o', loss_tile]

    subprocess.check_call(cmd)
    
    return loss_tile
    
      
def rasterize_shapefile(xmin, ymax, xmax, ymin, shapefile, output_tif, attribute_field):
    layer = shapefile.replace(".shp", "")

    cmd= ['gdal_rasterize', '-te', str(xmin), str(ymin), str(xmax), str(ymax), '-a', attribute_field, '-co', 'COMPRESS=LZW', '-tr', '.0025', '.0025', '-l', layer, shapefile, output_tif]

    subprocess.check_call(cmd)
    
    return output_tif

    
def resample_00025(input_tif, resampled_tif):
    # resample to .00025
    cmd = ['gdal_translate', input_tif, resampled_tif, '-tr', '.00025', '.00025', '-co', 'COMPRESS=LZW']
    subprocess.check_call(cmd)