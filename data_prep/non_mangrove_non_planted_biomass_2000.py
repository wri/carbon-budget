### Masks out mangrove and planted forest pixels from WHRC biomass 2000 raster so that
### only non-mangrove, non-planted forest pixels are left of the WHRC biomass 2000 raster

import datetime
import rasterio
import os
from shutil import copyfile
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu


def mask_biomass(tile_id, pattern, sensit_type):

    print "Processing:", tile_id

    # Start time
    start = datetime.datetime.now()

    # Names of the input files. Creates the names even if the files don't exist.
    WHRC_biomass = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_WHRC_biomass_2000_unmasked)
    mangrove_biomass = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_mangrove_biomass_2000)
    planted_forest_gain = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_annual_gain_AGC_BGC_planted_forest_unmasked)

    # Name of the output file
    WHRC_biomass_non_mang_non_planted = '{0}_{1}.tif'.format(tile_id, pattern)

    print "Checking if there are mangrove or planted forest tiles for", tile_id

    # Doing this avoids unnecessarily processing biomass tiles
    if os.path.exists(mangrove_biomass) or os.path.exists(planted_forest_gain):

        print "  Mangrove or planted forest tiles found for {}. Masking WHRC biomass using them...".format(tile_id)

        # Opens the unmasked WHRC biomass 2000
        WHRC_src = rasterio.open(WHRC_biomass)

        # Grabs metadata about the unmasked biomass, like its location/projection/cellsize
        kwargs = WHRC_src.meta

        # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
        windows = WHRC_src.block_windows(1)

        # Checks whether there are mangrove or planted forest tiles. If so, they are opened.
        try:
            mangrove_src = rasterio.open(mangrove_biomass)
            print "    Mangrove tile found for {}".format(tile_id)
        except:
            print "    No mangrove tile for {}".format(tile_id)

        try:
            planted_forest_src = rasterio.open(planted_forest_gain)
            print "    Planted forest tile found for {}".format(tile_id)
        except:
            print "    No planted forest tile for {}".format(tile_id)

        # Updates kwargs for the output dataset
        kwargs.update(
            driver='GTiff',
            compress='lzw'
        )

        # The output file, biomass masked by mangroves and planted forests
        dst_WHRC = rasterio.open(WHRC_biomass_non_mang_non_planted, 'w', **kwargs)

        # Iterates across the windows (1 pixel strips) of the input tiles
        for idx, window in windows:

            # Creates a processing window the WHRC raster
            WHRC_masked = WHRC_src.read(1, window=window)

            # print WHRC_masked[0][:20]

            # If there is a mangrove tile, this masks the mangrove biomass pixels so that only non-mangrove pixels are output
            if os.path.exists(mangrove_biomass):

                # Reads in the mangrove tile's window
                mangrove_AGB = mangrove_src.read(1, window=window)

                # print mangrove_AGB[0][:20]

                # Gets the NoData value of the mangrove biomass tile
                nodata = uu.get_raster_nodata_value(mangrove_biomass)

                # Reclassifies mangrove biomass to 1 or 0 to make a mask of mangrove pixels.
                # Ultimately, only these pixels (ones without mangrove biomass) will get values.
                # I couldn't figure out how to do this without first converting the NoData values to an intermediate value (99)
                mangrove_AGB[mangrove_AGB > nodata] = 99
                mangrove_AGB[mangrove_AGB == nodata] = 1
                mangrove_AGB[mangrove_AGB == 99] = nodata

                # print mangrove_AGB[0][:20]

                # Casts the mangrove biomass mask array into int16 so that it can be combined with the WHRC int array
                mangrove_AGB = mangrove_AGB.astype('byte')

                # print mangrove_AGB[0][:20]

                # Applies the mask
                WHRC_masked = WHRC_masked * mangrove_AGB

                # print WHRC_masked[0][:20]

            # If there is a planted forest tile, this masks the planted forest pixels so that only non-planted forest pixels
            # are output.
            # Process is same as for mangroves-- non-planted forest pixels are the only ones output
            if os.path.exists(planted_forest_gain):

                planted_forest = planted_forest_src.read(1, window=window)

                nodata = uu.get_raster_nodata_value(planted_forest_gain)

                planted_forest[planted_forest > nodata] = 99
                planted_forest[planted_forest == nodata] = 1
                planted_forest[planted_forest == 99] = nodata

                planted_forest = planted_forest.astype('int16')

                WHRC_masked = WHRC_masked * planted_forest

            # Writes the output window to the output file
            dst_WHRC.write_band(1, WHRC_masked, window=window)

            # sys.exit()

    # If no mangrove or planted forest tile was found, the original biomass tile is simply duplicated with the output name
    # so it can be copied to s3 with the rest of the outputs.
    else:
        print "  No mangrove or planted forest tile found for {}. Copying tile with output pattern...".format(tile_id)
        copyfile(WHRC_biomass, WHRC_biomass_non_mang_non_planted)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, pattern)
