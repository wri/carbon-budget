'''
This script creates the three inputs used for creating the carbon emitted_pools besides aboveground carbon.
It takes several hours to run.
'''

import datetime
import rasterio
import numpy as np
from scipy import stats
import sys
sys.path.append('../')
import universal_util as uu
import constants_and_names as cn


def create_input_files(tile_id, no_upload):

    # Start time
    start = datetime.datetime.now()

    uu.print_log("Getting extent of", tile_id)
    xmin, ymin, xmax, ymax = uu.coords(tile_id)


    #### NOTE FOR FUTURE REVISIONS: CHANGE TO USE MP_WARP_TO_HANSEN
    uu.print_log("Clipping srtm for", tile_id)
    uu.warp_to_Hansen('srtm.vrt', '{0}_{1}.tif'.format(tile_id, cn.pattern_elevation), xmin, ymin, xmax, ymax, 'Int16')

    #### NOTE FOR FUTURE REVISIONS: CHANGE TO USE MP_WARP_TO_HANSEN
    uu.print_log("Clipping precipitation for", tile_id)
    uu.warp_to_Hansen('add_30s_precip.tif', '{0}_{1}.tif'.format(tile_id, cn.pattern_precip), xmin, ymin, xmax, ymax, 'Int32')

    uu.print_log("Rasterizing ecozone into boreal-temperate-tropical categories for", tile_id)
    blocksizex = 1024
    blocksizey = 1024
    uu.rasterize('fao_ecozones_bor_tem_tro.shp',
                   "{0}_{1}.tif".format(tile_id, cn.pattern_bor_tem_trop_intermediate),
                        xmin, ymin, xmax, ymax, blocksizex, blocksizey, '.00025', 'Int16', 'recode', '0')

    # Opens boreal/temperate/tropical ecozone tile.
    # Everything from here down is used to assign pixels without boreal-tem-tropical codes to a bor-tem-trop in the 1024x1024 windows.
    bor_tem_trop_src = rasterio.open("{0}_{1}.tif".format(tile_id, cn.pattern_bor_tem_trop_intermediate))

    # Grabs metadata about the tif, like its location/projection/cellsize
    kwargs = bor_tem_trop_src.meta

    # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
    windows = bor_tem_trop_src.block_windows(1)

    # Updates kwargs for the output dataset.
    # Need to update data type to float 32 so that it can handle fractional removal rates
    kwargs.update(
        driver='GTiff',
        count=1,
        compress='DEFLATE',
        nodata=0
    )

    bor_tem_trop_processed = '{0}_{1}.tif'.format(tile_id, cn.pattern_bor_tem_trop_processed)

    # The output file: aboveground carbon density in the year of tree cover loss for pixels with tree cover loss
    dst_bor_tem_trop = rasterio.open(bor_tem_trop_processed, 'w', **kwargs)

    # Iterates across the windows (1024 x 1024 pixel boxes) of the input tile.
    for idx, window in windows:

        # Creates window for input raster
        bor_tem_trop_window = bor_tem_trop_src.read(1, window=window)

        # Turns the 2D array into a 1D array that is n x n long.
        # This makes to easier to remove 0s and find the mode of the remaining bor-tem-tropi codes
        bor_tem_trop_flat = bor_tem_trop_window.flatten()

        # Removes all zeros from the array, leaving just pixels with bor-tem-trop codes
        non_zeros = np.delete(bor_tem_trop_flat, np.where(bor_tem_trop_flat == 0))

        # If there were only pixels without bor-tem-trop codes in the array, the mode is assigned 0
        if non_zeros.size < 1:

            # print "  Window is all 0s"
            mode = 0

        # If there were pixels with bor-tem-trop codes, the mode is the most common code among those in the window
        else:

            mode = stats.mode(non_zeros)[0]
            # print "  Window is not all 0s. Mode is", mode

        # Assigns all pixels without a bor-tem-trop code in that window to that most common code
        bor_tem_trop_window[bor_tem_trop_window == 0] = mode

        # Writes the output window to the output.
        # Although the windows for the input tiles are 1024 x 1024 pixels,
        # the windows for these output files are 40000 x 1 pixels, like all the other tiles in this model,
        # so they should work fine with all the other tiles.
        dst_bor_tem_trop.write_band(1, bor_tem_trop_window, window=window)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_precip, no_upload)
