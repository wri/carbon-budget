import subprocess
import datetime
import os

import get_extent


def calc_total(tile_id):
    start = datetime.datetime.now()
    print "/n-------TILE ID: {}".format(tile_id)
    list_of_types = ['carbon', 'bgc', 'deadwood', 'litter', 'soil']
    for type in list_of_types:
        file_to_copy = 's3://gfw-files/sam/carbon_budget/{0}/{1}_{0}.tif'.format(type, tile_id)
        copy_file = ['aws', 's3', 'cp', file_to_copy, '.']
        subprocess.check_call(copy_file)
        
    carbon = '{}_carbon.tif'.format(tile_id)
    bgc = '{}_bgc.tif'.format(tile_id)
    deadwood = '{}_deadwood.tif'.format(tile_id)
    litter = '{}_litter.tif'.format(tile_id)
    soil = '{}_soil.tif'.format(tile_id)
    
    print 'writing total c tile'
    total_c_tile = '{}_totalc.tif'.format(tile_id)
    deadwood_tiles_cmd = ['./total_c_stock.exe', carbon, bgc, deadwood, litter,
                          soil, total_c_tile]
    subprocess.check_call(deadwood_tiles_cmd)


    print 'uploading total carbon tile to s3'
    copy_totalc_tile = ['aws', 's3', 'cp', total_c_tile, 's3://gfw-files/sam/carbon_budget/total_carbon/']
    subprocess.check_call(copy_totalc_tile)

    print "deleting intermediate data"
    tiles_to_remove = [carbon,  bgc, deadwood, litter, soil]

    for tile in tiles_to_remove:
        try:
            os.remove(tile)
        except:
            pass

    print "elapsed time: {}".format(datetime.datetime.now() - start)
