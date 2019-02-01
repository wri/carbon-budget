###

import datetime
import numpy as np
import rasterio
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu


def mask_biomass(tile_id):

    print "Processing:", tile_id

    # Start time
    start = datetime.datetime.now()

    # Names of the forest age category, continent-ecozone, mangrove biomass, and planted forest tiles
    WHRC_biomass = '{0}_{1}.tif'.format(tile_id, cn.pattern_WHRC_biomass_2000_full_extent)
    mangrove_biomass = '{0}_{1}.tif'.format(tile_id, cn.pattern_mangrove_biomass_2000)
    planted_forest_gain = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_AGC_planted_forest_full_extent)

    biomass_non_mang_non_planted = '{0}_{1}.tif'.format(tile_id, cn.pattern_non_mang_non_planted_biomass_2000)

    print "  Reading input files and creating aboveground and belowground biomass gain rates for {}".format(tile_id)

    # Opens the continent-ecozone and natural forest age category tiles
    WHRC_src = rasterio.open(WHRC_biomass)

    # Grabs metadata about the continent ecozone tile, like its location/projection/cellsize
    kwargs = WHRC_src.meta

    # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
    windows = WHRC_src.block_windows(1)

    # Checks whether there are mangrove or planted forest tiles. If so, they are opened.
    try:
        mangrove_src = rasterio.open(mangrove_biomass)
        print "  Mangrove tile found for {}".format(tile_id)
    except:
        print "  No mangrove tile for {}".format(tile_id)

    try:
        planted_forest_src = rasterio.open(planted_forest_gain)
        print "  Planted forest tile found for {}".format(tile_id)
    except:
        print "  No planted forest tile for {}".format(tile_id)

    # # Updates kwargs for the output dataset.
    # # Need to update data type to float 32 so that it can handle fractional gain rates
    # kwargs.update(
    #     driver='GTiff',
    #     count=1,
    #     compress='lzw',
    #     nodata=0,
    #     nbits=10
    # )

    # The output files, aboveground and belowground biomass gain rates
    dst_WHRC = rasterio.open(biomass_non_mang_non_planted, 'w', **kwargs)

    # Iterates across the windows (1 pixel strips) of the input tiles
    for idx, window in windows:

        # Creates a processing window for each input raster
        WHRC = WHRC_src.read(1, window=window)

        # Converts the continent-ecozone array to float so that the values can be replaced with fractional gain rates
        WHRC_masked = WHRC.astype('float32')

        # If there is a mangrove tile, this masks the mangrove biomass pixels so that only non-mangrove pixels are output
        if os.path.exists(mangrove_biomass):

            # Reads in the mangrove tile's window
            mangrove_AGB = mangrove_src.read(1, window=window)

            # Gets the NoData value of the mangrove biomass tile
            nodata = uu.get_raster_nodata_value(mangrove_biomass)

            # Reclassifies mangrove biomass to 1 or 0 to make a mask of mangrove pixels.
            # Ultimately, only these pixels (ones without mangrove biomass) will get values.
            # I couldn't figure out how to do this without first converting the NoData values to an intermediate value (99)
            mangrove_AGB[mangrove_AGB > nodata] = 99
            mangrove_AGB[mangrove_AGB == nodata] = 1
            mangrove_AGB[mangrove_AGB == 99] = nodata

            # Applies the mask
            WHRC_masked = WHRC_masked * mangrove_AGB

        # If there is a planted forest tile, this masks the planted forest pixels so that only non-planted forest pixels
        # are output.
        # Process is same as for mangroves-- non-planted forest pixels are the only ones output
        if os.path.exists(planted_forest_gain):

            planted_forest = planted_forest_src.read(1, window=window)

            nodata = uu.get_raster_nodata_value(planted_forest_gain)

            planted_forest[planted_forest > nodata] = 99
            planted_forest[planted_forest == nodata] = 1
            planted_forest[planted_forest == 99] = nodata

            WHRC_masked = WHRC_masked * planted_forest

        # Writes the output window to the output file
        dst_WHRC.write_band(1, WHRC_masked, window=window)

    end = datetime.datetime.now()
    elapsed_time = end-start

    print "  Processing time for tile", tile_id, ":", elapsed_time
