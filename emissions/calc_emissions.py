import subprocess
import datetime
import os
import sys

import utilities

currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)

import get_extent

def calc_emissions(tile_id):
    start = datetime.datetime.now()
    
    print "/n-------TILE ID: {}".format(tile_id)
    
    carbon_pool_files = ['bgc', 'carbon', 'deadwood', 'soil', 'litter']
    
    # download 5 carbon pool files
    utilities.download(carbon_pool_files)

    # get extent of a tile
    xmin, ymin, xmax, ymax = get_extent.get_extent('{}_bgc.tif'.format(tile_id))

    # rasterize shapefiles from one time download
    shapefiles_to_raterize = ['fao_ecozones_bor_temp_tro', 'ifl_2000']
    utilities.rasterize_shapefile(shapefiles_to_raterize)

    # resample rasters from one time download
    rasters_to_resample = ['peatdrainage', 'hwsd_histosoles', 'forest_model', 'climate_zone']
    utilities.resample_raster(rasters_to_resample)

    print 'writing deadwood tile'
    deadwood_tile = '{}_deadwood.tif'.format(tile_id)
    deadwood_tiles_cmd = ['./dead_wood_c_stock.exe', biomass_tile, resampled_ecozone, tile_res_srtm, resample_precip_tile,
                          deadwood_tile]
    #subprocess.check_call(deadwood_tiles_cmd)

    print 'uploading deadwood tile to s3'
    copy_deadwoodtile = ['aws', 's3', 'cp', deadwood_tile, 's3://gfw-files/sam/carbon_budget/deadwood/']
    #subprocess.check_call(copy_deadwoodtile)

    print "deleting intermediate data"
    tiles_to_remove = [deadwood_tile, resample_precip_tile, clipped_precip_tile, biomass_tile, resampled_ecozone, tile_res_srtm, tile_srtm, rasterized_eco_zone_tile]

    for tile in tiles_to_remove:
        try:
            print "test"
            #os.remove(tile)
        except:
            pass

    print "elapsed time: {}".format(datetime.datetime.now() - start)
