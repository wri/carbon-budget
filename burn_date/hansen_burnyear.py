import os
import utilities
import glob
import numpy as np
from subprocess import Popen, PIPE, STDOUT, check_call
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu


def hansen_burnyear(tile_id):

    uu.print_log("Processing", tile_id)

    # download the 10x10 deg burn year tiles- 1 for each year- in WGS proj, stack and evaluate
    # to return burn year values on hansen loss pixels within 1 year of loss date
    include = 'ba_*_{}.tif'.format(tile_id)
    burn_tiles_dir = 'burn_tiles'
    if not os.path.exists(burn_tiles_dir):
        os.mkdir(burn_tiles_dir)
    cmd = ['aws', 's3', 'cp', cn.burn_year_warped_to_Hansen_dir, cn.burn_year_dir, '--recursive', '--exclude', "*", '--include', include]
    uu.log_subprocess_output_full(cmd)

    # for each year tile, convert to array and stack them
    array_list = []
    ba_tifs = glob.glob(burn_tiles_dir + '/*{}*'.format(tile_id))
    for ba_tif in ba_tifs:
        uu.print_log("Creating array with {}".format(ba_tif))
        array = utilities.raster_to_array(ba_tif)
        array_list.append(array)

    # stack arrays
    uu.print_log("Stacking arrays")
    stacked_year_array = utilities.stack_arrays(array_list)

    # # download hansen tile
    # loss_tile = utilities.wgetloss(tile_id)
    # uu.print_log(loss_tile)

    # convert hansen tile to array
    uu.print_log("Creating loss year array")
    loss_array = utilities.raster_to_array('{0}_{1}.tif'.format(cn.pattern_loss, tile_id))

    lossarray_min1 = np.subtract(loss_array, 1)

    stack_con =(stacked_year_array >= lossarray_min1) & (stacked_year_array <= loss_array)
    stack_con2 = stack_con * stacked_year_array
    lossyear_burn_array = stack_con2.max(0)

    # write burn pixels to raster
    outname = '{0}_{1}.tif'.format(tile_id, cn.pattern_burn_year)

    utilities.array_to_raster_simple(lossyear_burn_array, outname, '{}.tif'.format(tile_id))


    # cmd = ['aws', 's3', 'mv', outname, cn.burn_year_dir]
    # uu.log_subprocess_output_full(cmd)

    # # clean up files
    # os.remove('{}.tif'.format(tile_id))

    # Only copies to s3 if the tile has data.
    uu.print_log("Checking if {} contains any data...".format(tile_id))
    empty = uu.check_for_data(outname)

    if empty:
        uu.print_log("  No data found. Not copying {}.".format(tile_id))

    else:
        uu.print_log("  Data found in {}. Copying tile to s3...".format(tile_id))
        cmd = ['aws', 's3', 'cp', outname, cn.burn_year_dir]
        uu.log_subprocess_output_full(cmd)
        uu.print_log("    Tile copied to s3")

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_burn_year)

