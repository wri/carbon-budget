import os
import utilities
import glob
import numpy as np
import subprocess
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu


def hansen_burnyear(tile_id):

    tile_id = tile_id[0:8]
    print_log("Processing", tile_id)

    input_tiles = 's3://gfw2-data/climate/carbon_model/other_emissions_inputs/burn_year/20190322/burn_year_10x10_clip/'
    output_tiles = 's3://gfw2-data/climate/carbon_model/other_emissions_inputs/burn_year/20190322/burn_year_with_Hansen_loss/'

    # download the 10x10 deg burn year tiles- 1 for each year- in WGS proj, stack and evaluate
    # to return burn year values on hansen loss pixels within 1 year of loss date

    # data is in wgs proj
    # burn_year_tiles = 's3://gfw-files/sam/carbon_budget/burn_year_10degtiles_modisproj/'  # Previous location
    include = 'ba_*_{}.tif'.format(tile_id)
    burn_tiles_dir = 'burn_tiles'
    if not os.path.exists(burn_tiles_dir):
        os.mkdir(burn_tiles_dir)
    cmd = ['aws', 's3', 'cp', input_tiles, burn_tiles_dir, '--recursive', '--exclude', "*", '--include', include]

    # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        uu.log_subprocess_output(process.stdout)

    # for each year tile, convert to array and stack them
    array_list = []
    ba_tifs = glob.glob(burn_tiles_dir + '/*{}*'.format(tile_id))
    for ba_tif in ba_tifs:
        uu.print_log("creating array with {}".format(ba_tif))
        array = utilities.raster_to_array(ba_tif)
        array_list.append(array)

    # stack arrays
    uu.print_log("stacking arrays")
    stacked_year_array = utilities.stack_arrays(array_list)

    # download hansen tile
    loss_tile = utilities.wgetloss(tile_id)
    uu.print_log(loss_tile)

    # convert hansen tile to array
    uu.print_log("creating loss year array")
    loss_array = utilities.raster_to_array('{}.tif'.format(tile_id))

    lossarray_min1 = np.subtract(loss_array, 1)

    stack_con =(stacked_year_array >= lossarray_min1) & (stacked_year_array <= loss_array)
    stack_con2 = stack_con * stacked_year_array
    lossyear_burn_array = stack_con2.max(0)

    # write burn pixels to raster
    outname = '{}_burnyear.tif'.format(tile_id)

    utilities.array_to_raster_simple(lossyear_burn_array, outname, '{}.tif'.format(tile_id))
    cmd = ['aws', 's3', 'mv', outname, output_tiles]

    # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        uu.log_subprocess_output(process.stdout)

    # clean up files
    os.remove('{}.tif'.format(tile_id))


tile_list = utilities.list_tiles('s3://gfw2-data/forest_change/hansen_2018/')
tile_list = tile_list[1:]
uu.print_log("Tile list: ", tile_list)

for tile_id in tile_list:
    hansen_burnyear(tile_id)
