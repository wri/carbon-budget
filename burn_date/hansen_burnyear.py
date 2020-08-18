import os
import datetime
import utilities
import glob
import numpy as np
from subprocess import Popen, PIPE, STDOUT, check_call
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu


def hansen_burnyear(tile_id):

    # Start time
    start = datetime.datetime.now()

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

    # All of the below is to add metadata tags to the output burn year masks.
    # For some reason, just doing what's at https://rasterio.readthedocs.io/en/latest/topics/tags.html
    # results in the data getting removed.
    # I found it necessary to copy the peat mask and read its windows into a new copy of the file, to which the
    # metadata tags are added. I'm sure there's an easier way to do this but I couldn't figure out how.
    # I know it's very convoluted but I really couldn't figure out how to add the tags without erasing the data.
    # To make it even stranger, adding the tags before the gdal processing seemed to work fine for the non-tropical
    # (SoilGrids) tiles but not for the tropical (CIFOR/Jukka) tiles (i.e. data didn't disappear in the non-tropical
    # tiles if I added the tags before the GDAL steps but the tropical data did disappear).

    copyfile(out_tile_no_tag, out_tile)

    uu.print_log("Adding metadata tags to", tile_id)
    # Opens the output tile, only so that metadata tags can be added
    # Based on https://rasterio.readthedocs.io/en/latest/topics/tags.html
    with rasterio.open(out_tile_no_tag) as out_tile_no_tag_src:

        # Grabs metadata about the tif, like its location/projection/cellsize
        kwargs = out_tile_no_tag_src.meta

        # Grabs the windows of the tile (stripes) so we can iterate over the entire tif without running out of memory
        windows = out_tile_no_tag_src.block_windows(1)

        # Updates kwargs for the output dataset
        kwargs.update(
            driver='GTiff',
            count=1,
            compress='lzw',
            nodata=0
        )

        out_tile_tagged = rasterio.open(out_tile, 'w', **kwargs)

        # Adds metadata tags to the output raster
        uu.add_rasterio_tags(out_tile_tagged, 'std')
        out_tile_tagged.update_tags(
            key='1 = peat. 0 = not peat.')
        out_tile_tagged.update_tags(
            source='Jukka for IDN and MYS; CIFOR for rest of tropics; SoilGrids250 (May 2020) most likely histosol for outside tropics')
        out_tile_tagged.update_tags(
            extent='Full extent of input datasets')

        # Iterates across the windows (1 pixel strips) of the input tile
        for idx, window in windows:
            peat_mask_window = out_tile_no_tag_src.read(1, window=window)

            # Writes the output window to the output
            out_tile_tagged.write_band(1, peat_mask_window, window=window)

    # Otherwise, the untagged version is counted and eventually copied to s3 if it has data in it
    os.remove(out_tile_no_tag)



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

