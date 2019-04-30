import datetime
import subprocess
import rasterio
import numpy as np
from scipy import stats
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def data_prep(tile_id):

    # Start time
    start = datetime.datetime.now()

    print "Getting extent of", tile_id
    xmin, ymin, xmax, ymax = uu.coords(tile_id)

    print "Warping climate zone tile", tile_id
    cmd = ['gdalwarp', '-t_srs', 'EPSG:4326', '-co', 'COMPRESS=LZW', '-tr', str(cn.Hansen_res), str(cn.Hansen_res), '-tap', '-te',
           str(xmin), str(ymin), str(xmax), str(ymax), '-dstnodata', '0', '-ot', 'Byte', '-overwrite',
           '-co', 'TILED=YES', '-co', 'BLOCKXSIZE=1024', '-co', 'BLOCKYSIZE=1024',
           cn.climate_zone_raw, '{0}_{1}.tif'.format(tile_id, "climate_zone_intermediate")]
    subprocess.check_call(cmd)

    print "Re-tiling climate zone for tile", tile_id

    # Opens boreal/temperate/tropical ecozone tile.
    # Everything from here down is used to assign pixels without boreal-tem-tropical codes to a bor-tem-trop in the 1024x1024 windows.
    climate_zone_src = rasterio.open("{0}_{1}.tif".format(tile_id, "climate_zone_intermediate"))

    # Grabs metadata about the tif, like its location/projection/cellsize
    kwargs = climate_zone_src.meta

    # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
    windows = climate_zone_src.block_windows(1)

    # Updates kwargs for the output dataset.
    kwargs.update(
        driver='GTiff',
        count=1,
        compress='lzw',
        nodata=0
    )

    climate_zone_processed = '{0}_{1}.tif'.format(tile_id, cn.pattern_climate_zone)

    # The output file: aboveground carbon density in the year of tree cover loss for pixels with tree cover loss
    dst_climate_zone = rasterio.open(climate_zone_processed, 'w', **kwargs)

    # Iterates across the windows (1024 x 1024 pixel boxes) of the input tile.
    for idx, window in windows:

        # Creates window for input raster
        climate_zone_window = climate_zone_src.read(1, window=window)

        # Turns the 2D array into a 1D array that is n x n long.
        # This makes to easier to remove 0s and find the mode of the remaining bor-tem-tropi codes
        climate_zone_flat = climate_zone_window.flatten()

        # Removes all zeros from the array, leaving just pixels with bor-tem-trop codes
        non_zeros = np.delete(climate_zone_flat, np.where(climate_zone_flat == 0))

        # If there were only pixels without bor-tem-trop codes in the array, the mode is assigned 0
        if non_zeros.size < 1:

            # print "  Window is all 0s"
            mode = 0

        # If there were pixels with bor-tem-trop codes, the mode is the most common code among those in the window
        else:

            mode = stats.mode(non_zeros)[0]
            # print "  Window is not all 0s. Mode is", mode

        # Assigns all pixels without a bor-tem-trop code in that window to that most common code
        climate_zone_window[climate_zone_window == 0] = mode

        # Writes the output window to the output.
        # Although the windows for the input tiles are 1024 x 1024 pixels,
        # the windows for these output files are 40000 x 1 pixels, like all the other tiles in this model,
        # so they should work fine with all the other tiles.
        dst_climate_zone.write_band(1, climate_zone_window, window=window)

        print mode

        sys.quit

    #
    # print "Warping IDN/MYS pre-2000 plantation tile", tile_id
    # uu.warp_to_Hansen('{}.tif'.format(cn.pattern_plant_pre_2000_raw), '{0}_{1}.tif'.format(tile_id, cn.pattern_plant_pre_2000),
    #                                   xmin, ymin, xmax, ymax, 'Byte')
    #
    # print "Warping tree cover loss tile", tile_id
    # uu.warp_to_Hansen('{}.tif'.format(cn.pattern_drivers_raw), '{0}_{1}.tif'.format(tile_id, cn.pattern_drivers), xmin, ymin, xmax, ymax, 'Byte')




    print "Checking if {} contains any data...".format('{0}_{1}.tif'.format(tile_id, cn.pattern_climate_zone))
    tile_stats = uu.check_for_data('{0}_{1}.tif'.format(tile_id, cn.pattern_climate_zone))
    if tile_stats[1] > 0:
        print "  Data found in {}. Keeping tile".format('{0}_{1}.tif'.format(tile_id, cn.pattern_climate_zone))
    else:
        print "  No data found in {}. Deleting.".format('{0}_{1}.tif'.format(tile_id, cn.pattern_climate_zone))
        os.remove('{0}_{1}.tif'.format(tile_id, cn.pattern_climate_zone))

    # print "Checking if {} contains any data...".format('{0}_{1}.tif'.format(tile_id, cn.pattern_plant_pre_2000))
    # tile_stats = uu.check_for_data('{0}_{1}.tif'.format(tile_id, cn.pattern_plant_pre_2000))
    # if tile_stats[1] > 0:
    #     print "  Data found in {}. Keeping tile".format('{0}_{1}.tif'.format(tile_id, cn.pattern_plant_pre_2000))
    # else:
    #     print "  No data found in {}. Deleting.".format('{0}_{1}.tif'.format(tile_id, cn.pattern_plant_pre_2000))
    #     os.remove('{0}_{1}.tif'.format(tile_id, cn.pattern_plant_pre_2000))
    #
    # print "Checking if {} contains any data...".format('{0}_{1}.tif'.format(tile_id, cn.pattern_drivers))
    # tile_stats = uu.check_for_data('{0}_{1}.tif'.format(tile_id, cn.pattern_drivers))
    # if tile_stats[1] > 0:
    #     print "  Data found in {}. Keeping tile".format('{0}_{1}.tif'.format(tile_id, cn.pattern_drivers))
    # else:
    #     print "  No data found in {}. Deleting.".format('{0}_{1}.tif'.format(tile_id, cn.pattern_drivers))
    #     os.remove('{0}_{1}.tif'.format(tile_id, cn.pattern_drivers))

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_drivers)
