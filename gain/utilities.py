import subprocess


def wgetloss(tile_id):
    print "download hansen loss tile"
    loss_tile = '{}_loss.tif'.format(tile_id)
    cmd = ['wget', r'http://glad.geog.umd.edu/Potapov/GFW_2015/tiles/{}.tif'.format(tile_id),
           '-O', loss_tile]

    subprocess.check_call(cmd)
    
    return loss_tile
    
      
def rasterize_shapefile(xmin, ymin, xmax, ymax, shapefile, output_tif, attribute_field):

    cmd= ['gdal_rasterize', '-te', xmin, ymin, xmax, ymax, shapefile, output_tif, '-a', attribute_field, '-co', 'COMPRESS=LZW', '-tr', '.0025', '.0025']
    subprocess.check_call(cmd)
    
    return output_tif

    
def resample_00025(input_tif, resampled_tif):
    # resample to .00025
    cmd = ['gdal_translate', input_tif, resampled_tif, '-tr', '.00025', '.00025', '-co', 'COMPRESS=LZW']
    subprocess.check_call(cmd)
