### This script assigns annual above and belowground biomass gain rates (in the units of IPCC Table 4.9 (currently tonnes
### biomass/ha/yr)) to non-mangrove natural forest pixels.
### The inputs are continent-ecozone tiles and natural forest age category tiles, as well as IPCC Table 4.9, formatted
### for easy ingestion by pandas.
### Essentially, this does some processing of the IPCC gain rate table, then uses it as a dictionary that it applies
### to every pixel in every tile.
### Each continent-ecozone-forest age category combination gets its own code, which matches the codes in the
### processed IPCC table.
### Belowground biomass gain rate is a constant proportion of aboveground biomass gain rate, again according to IPCC tables.

import datetime
import numpy as np
import rasterio
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Necessary to suppress a pandas error later on
np.set_printoptions(threshold=np.nan)

def annual_gain_rate(tile_id, gain_table_dict):

    # Converts the forest age category decision tree output values to the three age categories--
    # 10000: primary forest; 20000: secondary forest > 20 years; 30000: secondary forest <= 20 years
    # These are five digits so they can easily be added to the four digits of the continent-ecozone code to make unique codes
    # for each continent-ecozone-age combination.
    # The key in the dictionary is the forest age category decision tree endpoints.
    age_dict = {0: 0, 1: 20000, 2: 20000, 3: 10000, 4: 30000, 5: 20000, 6: 10000, 7: 30000, 8: 30000, 9: 30000, 10: 30000}

    print "Processing:", tile_id

    # Start time
    start = datetime.datetime.now()

    # Names of the forest age category, continent-ecozone, mangrove biomass, and planted forest tiles
    age_cat = '{0}_{1}.tif'.format(tile_id, cn.pattern_age_cat_natrl_forest)
    cont_eco = '{0}_{1}.tif'.format(tile_id, cn.pattern_cont_eco_processed)
    mangrove_biomass = '{0}_{1}.tif'.format(tile_id, cn.pattern_mangrove_biomass_2000)
    planted_forest_gain = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_AGC_planted_forest_full_extent)

    # Names of the output natural forest gain rate tiles (above and belowground)
    AGB_natrl_forest_gain_rate = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_AGB_natrl_forest)
    BGB_natrl_forest_gain_rate = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_BGB_natrl_forest)

    print "  Reading input files and creating aboveground and belowground biomass gain rates for {}".format(tile_id)

    # Opens the continent-ecozone and natural forest age category tiles
    cont_eco_src = rasterio.open(cont_eco)
    age_cat_src = rasterio.open(age_cat)

    # Grabs metadata about the continent ecozone tile, like its location/projection/cellsize
    kwargs = cont_eco_src.meta

    # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
    windows = cont_eco_src.block_windows(1)

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

    # Updates kwargs for the output dataset.
    # Need to update data type to float 32 so that it can handle fractional gain rates
    kwargs.update(
        driver='GTiff',
        count=1,
        compress='lzw',
        nodata=0,
        dtype='float32'
    )

    # The output files, aboveground and belowground biomass gain rates
    dst_above = rasterio.open(AGB_natrl_forest_gain_rate, 'w', **kwargs)
    dst_below = rasterio.open(BGB_natrl_forest_gain_rate, 'w', **kwargs)

    # Iterates across the windows (1 pixel strips) of the input tiles
    for idx, window in windows:

        # Creates a processing window for each input raster
        cont_eco = cont_eco_src.read(1, window=window)
        age_cat = age_cat_src.read(1, window=window)

        # Recodes the input forest age category array with 10 different decision tree end values into the 3 actual age categories
        age_recode = np.vectorize(age_dict.get)(age_cat)

        # Adds the age category codes to the continent-ecozone codes to create an array of unique continent-ecozone-age codes
        cont_eco_age = cont_eco + age_recode

        # Converts the continent-ecozone array to float so that the values can be replaced with fractional gain rates
        gain_rate_AGB = cont_eco_age.astype('float32')

        # Applies the dictionary of continent-ecozone-age gain rates to the continent-ecozone-age array to
        # get annual gain rates (metric tons aboveground biomass/yr) for each pixel
        for key, value in gain_table_dict.iteritems():
            gain_rate_AGB[gain_rate_AGB == key] = value

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
            gain_rate_AGB = gain_rate_AGB * mangrove_AGB

        # If there is a planted forest tile, this masks the planted forest pixels so that only non-planted forest pixels
        # are output.
        # Process is same as for mangroves-- non-planted forest pixels are the only ones output
        if os.path.exists(planted_forest_gain):

            planted_forest = planted_forest_src.read(1, window=window)

            nodata = uu.get_raster_nodata_value(planted_forest_gain)

            planted_forest[planted_forest > nodata] = 99
            planted_forest[planted_forest == nodata] = 1
            planted_forest[planted_forest == 99] = nodata

            gain_rate_AGB = gain_rate_AGB * planted_forest

        # Writes the output window to the output file
        dst_above.write_band(1, gain_rate_AGB, window=window)

        # Calculates the belowground biomass gain rate
        gain_rate_BGB = gain_rate_AGB * cn.below_to_above_natrl_forest

        # Writes the belowground gain rate output window to the output file
        dst_below.write_band(1, gain_rate_BGB, window=window)

    end = datetime.datetime.now()
    elapsed_time = end-start

    print "  Processing time for tile", tile_id, ":", elapsed_time
