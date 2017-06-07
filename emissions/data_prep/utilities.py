import subprocess
import os

def coords(tile_id):
    ymax = str(tile_id.split("_")[0][:2])
    xmin = str(tile_id.split("_")[1][:3])
    ymin = str(int(ymax) - 10)
    xmax = str(int(xmin) + 10)
    
    return ymax, xmin, ymin, xmax
    
    
def rasterize(shapefile, tile_id):

    print "rasterizing: {}".format(shapefile)
    ymax, xmin, ymin, xmax = coords(tile_id)
    
    d = {'fao_ecozones_bor_tem_tro': 'recode','ifl_2000': 'temp_id','peatland_drainage_proj': 'emisC02ha','gfw_plantations': 'c02emiss'}
    rvalue = d[shapefile]
    
    rasterized_tile = "{0}_res_{1}.tif".format(tile_id, shapefile)
    
    cmd = ['gdal_rasterize', '-co', 'COMPRESS=LZW', '-tr', '0.00025', '0.00025', '-ot',
                         'Byte', '-a', rvalue, '-a_nodata', '0', shapefile + ".shp", rasterized_tile, '-te', xmin, ymin, xmax, ymax]
                      
    subprocess.check_call(cmd)

    return rasterized_tile
 

    print "resampling: {}".format(raster)
    ymax, xmin, ymin, xmax = coords(tile_id)
    
    input_raster = raster + ".tif"
    clipped_raster = '{0}_res_{1}.tif'.format(tile_id, raster)
    
    if raster == "forest_model":
    
        cmd = ['gdal_translate', '-co', 'COMPRESS=LZW', '-a_nodata', '4', '-ot', 'Byte',
                    input_raster, clipped_raster, '-tr', '.00025', '.00025', '-projwin', xmin, ymax, xmax, ymin]
                    
        subprocess.check_call(cmd)
            
    elif raster == 'cifor_peat_mask':
        # cifor raster is large resolution. so, clip to a larger extent than tile, then clip down to tile size
        xmin = str(int(xmin) -.1)
        ymax = str(int(ymax) + .1)
        xmax = str(int(xmax) + .1)
        ymin = str(int(ymin) -.1)
        
        cmd = ['gdal_translate', '-co', 'COMPRESS=LZW', '-a_nodata', '0',
                    input_raster, 'test1.tif', '-tr', '.00025', '.00025', '-projwin', xmin, ymax, xmax, ymin]
        subprocess.check_call(cmd)

        ymax, xmin, ymin, xmax = coords(tile_id)
 
        cmd = ['gdalwarp', '-tr', '.00025', '.00025',  '-co', 'COMPRESS=LZW', '-tap', 'test1.tif', clipped_raster, '-te', xmin, ymin, xmax, ymax]        
        subprocess.check_call(cmd)

    else:
        cmd = ['gdal_translate', '-ot', 'Byte', '-co', 'COMPRESS=LZW', '-a_nodata', '-9999', input_raster, clipped_raster, '-tr', '.00025', '.00025', '-projwin', xmin, ymax, xmax, ymin]

        subprocess.check_call(cmd) 

    return clipped_raster
    