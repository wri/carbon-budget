import os
import datetime
import rasterio
import utilities
import glob
from shutil import copyfile
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

    # Downloads the burned area tiles for each year
    include = 'ba_*_{}.tif'.format(tile_id)
    burn_tiles_dir = 'burn_tiles'
    if not os.path.exists(burn_tiles_dir):
        os.mkdir(burn_tiles_dir)
    cmd = ['aws', 's3', 'cp', cn.burn_year_warped_to_Hansen_dir, burn_tiles_dir, '--recursive', '--exclude', "*", '--include', include]
    uu.log_subprocess_output_full(cmd)

    # The tiles that are used. out_tile_no_tag is the output before metadata tags are added. out_tile is the output
    # once metadata tags have been added.
    out_tile_no_tag = '{0}_{1}_no_tag.tif'.format(tile_id, cn.pattern_burn_year)
    out_tile = '{0}_{1}.tif'.format(tile_id, cn.pattern_burn_year)
    loss = '{0}_{1}.tif'.format(cn.pattern_loss, tile_id)


    # For each year tile, converts to array and stacks them
    array_list = []
    ba_tifs = glob.glob(burn_tiles_dir + '/*{}*'.format(tile_id))

    # Skips the tile if it has no burned area data for any year
    uu.print_log("There are {0} tiles to stack for {1}".format(len(ba_tifs), tile_id))
    if len(ba_tifs) == 0:
        uu.print_log("Skipping {} because there are no tiles to stack".format(tile_id))
        return


    # NOTE: All of this could pretty easily be done in rasterio. However, Sam's use of GDAL for this still works fine,
    # so I've left it using GDAL.

    for ba_tif in ba_tifs:
        uu.print_log("Creating array with {}".format(ba_tif))
        array = utilities.raster_to_array(ba_tif)
        array_list.append(array)

    # Stacks arrays from each year
    uu.print_log("Stacking arrays for", tile_id)
    stacked_year_array = utilities.stack_arrays(array_list)

    # Converts Hansen tile to array
    uu.print_log("Creating loss year array for", tile_id)
    loss_array = utilities.raster_to_array(loss)

    # Determines what year to assign burned area
    lossarray_min1 = np.subtract(loss_array, 1)

    stack_con =(stacked_year_array >= lossarray_min1) & (stacked_year_array <= loss_array)
    stack_con2 = stack_con * stacked_year_array
    lossyear_burn_array = stack_con2.max(0)

    utilities.array_to_raster_simple(lossyear_burn_array, out_tile_no_tag, loss)

    # Only copies to s3 if the tile has data
    uu.print_log("Checking if {} contains any data...".format(tile_id))
    empty = uu.check_for_data(out_tile_no_tag)

    # Checks output for data. There could be burned area but none of it coincides with tree cover loss,
    # so this is the final check for whether there is any data.
    if empty:
        uu.print_log("  No data found. Not copying {}.".format(tile_id))

        # Without this, the untagged version is counted and eventually copied to s3 if it has data in it
        os.remove(out_tile_no_tag)

        return

    else:
        uu.print_log("  Data found in {}. Adding metadata tags...".format(tile_id))

        # with rasterio.open(out_tile_no_tag, 'r') as src:
        #
        #     profile = src.profile
        #
        # with rasterio.open(out_tile_no_tag, 'w', **profile) as dst:
        #
        #     dst.update_tags(units='year (2001, 2002, 2003...)',
        #                     source='MODIS collection 6 burned area',
        #                     extent='global')


        # All of the below is to add metadata tags to the output burn year masks.
        # For some reason, just doing what's at https://rasterio.readthedocs.io/en/latest/topics/tags.html
        # results in the data getting removed.
        # I found it necessary to copy the desired output and read its windows into a new copy of the file, to which the
        # metadata tags are added. I'm sure there's an easier way to do this but I couldn't figure out how.
        # I know it's very convoluted but I really couldn't figure out how to add the tags without erasing the data.

        copyfile(out_tile_no_tag, out_tile)

        with rasterio.open(out_tile_no_tag) as out_tile_no_tag_src:

            # Grabs metadata about the tif, like its location/projection/cellsize
            kwargs = out_tile_no_tag_src.meta  #### Use profile instead

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
                units='year (2001, 2002, 2003...)')
            out_tile_tagged.update_tags(
                source='MODIS collection 6 burned area')
            out_tile_tagged.update_tags(
                extent='global')

            # Iterates across the windows (1 pixel strips) of the input tile
            for idx, window in windows:
                in_window = out_tile_no_tag_src.read(1, window=window)

                # Writes the output window to the output
                out_tile_tagged.write_band(1, in_window, window=window)

        # Without this, the untagged version is counted and eventually copied to s3 if it has data in it
        os.remove(out_tile_no_tag)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_burn_year)

