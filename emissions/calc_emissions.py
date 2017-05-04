import subprocess
import datetime
import os
import sys
import pandas as pd

import utilities

currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)

#import get_extent

def calc_emissions(tile_id):
    start = datetime.datetime.now()
    
    print "/n-------TILE ID: {}".format(tile_id)
    
    carbon_pool_files = ['bgc', 'carbon', 'deadwood', 'soil', 'litter']
    
    # download 5 carbon pool files
    #utilities.download(carbon_pool_files, tile_id)

    # download hansen tile
    #utilities.wgetloss(tile_id)

    # get extent of a tile
    xmin, ymin, xmax, ymax = get_extent.get_extent('{}_loss.tif'.format(tile_id))
    coord_list = [str(xmin), str(ymin), str(xmax), str(ymax)]

    # get list of windows intersecting tile
    windows_to_dl = utilities.get_windows_in_tile(tile_id)
    
    # for all files matching Win*, clip, resample, and stack them (all years, months). output 1 file <tileid>_burn.tif
    

    # rasterize shapefiles from one time download
    shapefiles_to_raterize = [{'fao_ecozones_bor_tem_tro': 'recode'}, {'ifl_2000': 'temp_id'}]
    coords = ['-te'] + coord_list
    #rasterized_file = utilities.rasterize_shapefile(shapefiles_to_raterize, tile_id, coords)

    # resample rasters from one time download
    coords = ['-projwin', str(xmin), str(ymax), str(xmax), str(ymin)] 
    rasters_to_resample = ['peatdrainage', 'hwsd_histosoles', 'forest_model', 'climate_zone']
    #resampled_tiles = utilities.resample_raster(rasters_to_resample, tile_id, coords)

    print 'writing emissions tiles'
    emissions_tiles_cmd = ['./calc_emissions.exe', tile_id]
    subprocess.check_call(emissions_tiles_cmd)

    print 'uploading emissions tile to s3'
    upload_emissions = ['aws', 's3', 'cp', emission__tile, 's3://gfw-files/sam/carbon_budget/emissions/']
    #subprocess.check_call(upload_emissions)

    print "deleting intermediate data"
    tiles_to_remove = ['']

    for tile in tiles_to_remove:
        try:
            print "test"
            #os.remove(tile)
        except:
            pass

    print "elapsed time: {}".format(datetime.datetime.now() - start)
