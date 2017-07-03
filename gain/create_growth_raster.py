import os
import sys

currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
import get_extent

import utilities

def create_growth_raster(tile_age):
    tile_id = tile_age[0]
    age = tile_age[1]
    
    shapefile = 'gdam_continent_int_ecozones_oldyoung_att.shp'
    
    hansen_tile = utilities.wgetloss(tile_id)
    print hansen_tile

    
    xmin, ymin, xmax, ymax = get_extent.get_extent(hansen_tile) 
    
    output_tif = tile_id.replace(".tif", "_{}.tif".format(age))
    growth = utilities.rasterize_shapefile(str(xmin), str(ymax), str(xmax), str(ymin), shapefile, output_tif, age)
    
    resampled_tif = growth.replace(".tif", "_res.tif")
    utilities.resample_00025(growth, resampled_tif)
    
    cmd = ['aws', 's3', mv, resampled_tif, 's3://gfw-files/sam/carbon_budget/growth_rasters/']
    subprocess.check_call(cmd)

create_growth_raster(['00N_030E', 'young'])       
