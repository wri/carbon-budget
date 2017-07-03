import os
import utilities
import glob
import numpy as np
import subprocess


def hansen_burnyear(tile_id):
    # download the 10x10 deg burn year tiles- 1 for each year- in WGS proj, stack and evaluate
    # to return burn year values on hansen loss pixels within 1 year of loss date

    ## data is in wgs proj, name is wrong
    burn_year_tiles = 's3://gfw-files/sam/carbon_budget/burn_year_10degtiles_modisproj/'
    include = 'ba_*_{}.tif'.format(tile_id)
    burn_tiles_dir = 'burn_tiles'
    if not os.path.exists(burn_tiles_dir):
        os.mkdir(burn_tiles_dir)
    cmd = ['aws', 's3', 'cp', burn_year_tiles, burn_tiles_dir, '--recursive', '--exclude', "*", '--include', include]  
    subprocess.check_call(cmd)
    
    # for each year tile, convert to array and stack them
    array_list = []
    ba_tifs = glob.glob(burn_tiles_dir + '/*{}*'.format(tile_id))
    for ba_tif in ba_tifs:
        print "creating array with {}".format(ba_tif)
        array = utilities.raster_to_array(ba_tif)
        array_list.append(array)
        
    # stack arrays
    print "stacking arrays"
    stacked_year_array = utilities.stack_arrays(array_list)
    
    # download hansen tile
    loss_tile = utilities.wgetloss(tile_id)
    
    # convert hansen tile to array
    print "creating loss year array"
    loss_array = utilities.raster_to_array(loss_tile)
    
    lossarray_min1 = np.subtract(loss_array, 1)
    
    stack_con =(stacked_year_array >= lossarray_min1) & (stacked_year_array <= loss_array)
    stack_con2 = stack_con * stacked_year_array
    lossyear_burn_array = stack_con2.max(0)
    
    # write burn pixels to raster
    outname = '{}_burnyear.tif'.format(tile_id)
    
    utilities.array_to_raster_simple(lossyear_burn_array, outname, loss_tile)
    cmd = ['aws', 's3', 'mv', outname, 's3://gfw-files/sam/carbon_budget/burn_loss_year/']
    subprocess.check_call(cmd)

    # clean up files
    os.remove(loss_tile)
    
tile_list = ['00N_000E', '00N_010E']

for tile_id in tile_list:
    hansen_burnyear(tile_id)
