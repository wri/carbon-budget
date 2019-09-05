'''
This script processes the inputs for the emissions script that haven't been processed by another script.
At this point, that is: climate zone, Indonesia/Malaysia plantations before 2000, and tree cover loss drivers (TSC drivers).
'''

import datetime
import subprocess
import rasterio
import os
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

    print "Warping IDN/MYS pre-2000 plantation tile", tile_id
    uu.warp_to_Hansen('{}.tif'.format(cn.pattern_plant_pre_2000_raw), '{0}_{1}.tif'.format(tile_id, cn.pattern_plant_pre_2000),
                                      xmin, ymin, xmax, ymax, 'Byte')

    print "Warping tree cover loss tile", tile_id
    uu.warp_to_Hansen('{}.tif'.format(cn.pattern_drivers_raw), '{0}_{1}.tif'.format(tile_id, cn.pattern_drivers), xmin, ymin, xmax, ymax, 'Byte')

    # Makes a 10x10 degree chunk of the global climate zone raster conform to Hansen tile properties.
    # Rather than the usual 40000x1 windows, this creates 1024x1024 windows for filling in missing values (see below).
    # The output of gdalwarp ("climate_zone_intermediate") is not used anywhere else.
    print "Warping climate zone tile", tile_id
    cmd = ['gdalwarp', '-t_srs', 'EPSG:4326', '-co', 'COMPRESS=LZW', '-tr', str(cn.Hansen_res), str(cn.Hansen_res), '-tap', '-te',
           str(xmin), str(ymin), str(xmax), str(ymax), '-dstnodata', '0', '-ot', 'Byte', '-overwrite',
           '-co', 'TILED=YES', '-co', 'BLOCKXSIZE=1024', '-co', 'BLOCKYSIZE=1024',
           cn.climate_zone_raw, '{0}_{1}.tif'.format(tile_id, "climate_zone_intermediate")]
    subprocess.check_call(cmd)

    # Fills in empty pixels in the climate zone raster with whatever value is most common (mode) in its 1024x1024 pixel window.
    # That is, any 1024x1024 processing window that has >=1 climate zone pixel in it will have its empty pixels filled in
    # with whatever value is most common in that window.
    # This extends the climate zone raster out into coastal areas and better covers coasts/islands, meaning that more
    # loss pixels will have climate zone pixels available to them during emissions processing.
    # Everything from here down is used to assign pixels without climate zone to a climate zone in the 1024x1024 windows.
    print "Re-tiling climate zone for tile", tile_id

    # Opens climate zone tile
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

    # Output file name
    climate_zone_processed = '{0}_{1}.tif'.format(tile_id, cn.pattern_climate_zone)

    # The output file: climate zone with empty pixels filled in
    dst_climate_zone = rasterio.open(climate_zone_processed, 'w', **kwargs)

    # Iterates across the windows (1024 x 1024 pixel boxes) of the input tile.
    for idx, window in windows:

        # Creates window for input raster
        climate_zone_window = climate_zone_src.read(1, window=window)

        # Turns the 2D array into a 1D array that is n x n long.
        # This makes to easier to remove 0s and find the mode of the remaining climate zone codes
        climate_zone_flat = climate_zone_window.flatten()

        # Removes all zeros from the array, leaving just pixels with climate zone codes
        non_zeros = np.delete(climate_zone_flat, np.where(climate_zone_flat == 0))

        # If there were only pixels without climate zone codes in the array, the mode is assigned 0
        if non_zeros.size < 1:

            mode = 0

        # If there were pixels with climate zone codes, the mode is the most common code among those in the window
        else:

            mode = stats.mode(non_zeros)[0]

        # Assigns all pixels without a climate zone code in that window to that most common code
        climate_zone_window[climate_zone_window == 0] = mode

        # Writes the output window to the output.
        # Although the windows for the input tiles are 1024 x 1024 pixels,
        # the windows for these output files are 40000 x 1 pixels, like all the other tiles in this model,
        # so they should work fine with all the other tiles.
        dst_climate_zone.write_band(1, climate_zone_window, window=window)


    print "Checking if {} contains any data...".format('{0}_{1}.tif'.format(tile_id, cn.pattern_plant_pre_2000))
    tile_stats = uu.check_for_data('{0}_{1}.tif'.format(tile_id, cn.pattern_plant_pre_2000))
    if tile_stats[1] > 0:
        print "  Data found in {}. Keeping tile".format('{0}_{1}.tif'.format(tile_id, cn.pattern_plant_pre_2000))
    else:
        print "  No data found in {}. Deleting.".format('{0}_{1}.tif'.format(tile_id, cn.pattern_plant_pre_2000))
        os.remove('{0}_{1}.tif'.format(tile_id, cn.pattern_plant_pre_2000))

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_drivers)


def create_primary_tile(tile_id, primary_vrt):

    # Start time
    start = datetime.datetime.now()

    print "Getting extent of", tile_id
    xmin, ymin, xmax, ymax = uu.coords(tile_id)

    primary_tile = '{}_primary.tif'.format(tile_id)

    print "Creating primary forest tile for {}".format(tile_id)

    uu.warp_to_Hansen(primary_vrt, primary_tile, xmin, ymin, xmax, ymax, 'Byte')

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, "primary.tif")

def create_combined_ifl_primary(tile_id):

    # Start time
    start = datetime.datetime.now()

    ifl_tile = '{0}_{1}.tif'.format(tile_id, cn.pattern_ifl)
    primary_tile = '{}_primary.tif'.format(tile_id)

    ifl_primary_tile = '{0}_{1}.tif'.format(tile_id, cn.pattern_ifl_primary)

    print "Getting extent of", tile_id
    xmin, ymin, xmax, ymax = uu.coords(tile_id)

    if ymax <= 30 or ymin >= -30:

        print "{} between 30N and 30S. Using primary forest tile.".format(tile_id)

        os.rename(primary_tile, ifl_primary_tile)

    else:

        print "{} not between 30N and 30S. Using IFL tile.".format(tile_id)

        os.rename(ifl_tile, ifl_primary_tile)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_ifl_primary)

