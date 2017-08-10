import os
import sys
import subprocess

import utilities


tile_id_list = ['00N_130E', '00N_040E']

tile_age_list = []

for tile_id in tile_id_list:
    tile_age_list.append([tile_id, 'old'])
    tile_age_list.append([tile_id, 'young'])
    
    

def create_growth_raster(tile_age):
    tile_id = tile_age[0]
    age = tile_age[1]
    
    shapefile = 'gain_test_area.shp'
        
    ymax, xmin, ymin, xmax = utilities.coords(tile_id)
    
    output_tif = "{0}_{1}.tif".format(tile_id, age)
    print output_tif
    growth = utilities.rasterize_shapefile(xmin, ymax, xmax, ymin, shapefile, output_tif, age)
    
    resampled_tif = growth.replace(".tif", "_res.tif")
    utilities.resample_00025(growth, resampled_tif)
    
    cmd = ['aws', 's3', 'mv', resampled_tif, 's3://gfw-files/sam/carbon_budget/growth_rasters/']


