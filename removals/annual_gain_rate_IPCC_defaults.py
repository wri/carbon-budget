import datetime
import numpy as np
import rasterio
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Necessary to suppress a pandas error later on. https://github.com/numpy/numpy/issues/12987
np.set_printoptions(threshold=sys.maxsize)

def annual_gain_rate(tile_id, sensit_type, gain_table_dict, stdev_table_dict, output_pattern_list, no_upload):

    # Converts the forest age category decision tree output values to the three age categories--
    # 10000: primary forest; 20000: secondary forest > 20 years; 30000: secondary forest <= 20 years
    # These are five digits so they can easily be added to the four digits of the continent-ecozone code to make unique codes
    # for each continent-ecozone-age combination.
    # The key in the dictionary is the forest age category decision tree endpoints.
    age_dict = {0: 0, 1: 10000, 2: 20000, 3: 30000}

    uu.print_log("Processing:", tile_id)

    # Start time
    start = datetime.datetime.now()

    # Names of the forest age category and continent-ecozone tiles
    age_cat = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_age_cat_IPCC)
    cont_eco = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_cont_eco_processed)

    # Names of the output natural forest removals rate tiles (above and belowground)
    AGB_IPCC_default_gain_rate = '{0}_{1}.tif'.format(tile_id, output_pattern_list[0])
    BGB_IPCC_default_gain_rate = '{0}_{1}.tif'.format(tile_id, output_pattern_list[1])
    AGB_IPCC_default_gain_stdev = '{0}_{1}.tif'.format(tile_id, output_pattern_list[2])

    uu.print_log("  Creating IPCC default biomass removals rates and standard deviation for {}".format(tile_id))

    # Opens the input tiles if they exist. kips tile if either input doesn't exist.
    try:
        age_cat_src = rasterio.open(age_cat)
        uu.print_log("   Age category tile found for {}".format(tile_id))
    except:
        return uu.print_log("   No age category tile found for {}. Skipping tile.".format(tile_id))

    try:
        cont_eco_src = rasterio.open(cont_eco)
        uu.print_log("   Continent-ecozone tile found for {}".format(tile_id))
    except:
        return uu.print_log("   No continent-ecozone tile found for {}. Skipping tile.".format(tile_id))

    # Grabs metadata about the continent ecozone tile, like its location/projection/cellsize
    kwargs = cont_eco_src.meta

    # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
    windows = cont_eco_src.block_windows(1)

    # Updates kwargs for the output dataset.
    # Need to update data type to float 32 so that it can handle fractional removals rates
    kwargs.update(
        driver='GTiff',
        count=1,
        compress='DEFLATE',
        nodata=0,
        dtype='float32'
    )

    # The output files, aboveground and belowground biomass removals rates
    dst_above = rasterio.open(AGB_IPCC_default_gain_rate, 'w', **kwargs)
    # Adds metadata tags to the output raster
    uu.add_rasterio_tags(dst_above, sensit_type)
    dst_above.update_tags(
        units='megagrams aboveground biomass (AGB or dry matter)/ha/yr')
    dst_above.update_tags(
        source='IPCC Guidelines 2019 refinement, forest section, Table 4.9')
    dst_above.update_tags(
        extent='Full model extent, even though these rates will not be used over the full model extent')

    dst_below = rasterio.open(BGB_IPCC_default_gain_rate, 'w', **kwargs)
    # Adds metadata tags to the output raster
    uu.add_rasterio_tags(dst_below, sensit_type)
    dst_below.update_tags(
        units='megagrams belowground biomass (AGB or dry matter)/ha/yr')
    dst_below.update_tags(
        source='IPCC Guidelines 2019 refinement, forest section, Table 4.9')
    dst_below.update_tags(
        extent='Full model extent, even though these rates will not be used over the full model extent')

    dst_stdev_above = rasterio.open(AGB_IPCC_default_gain_stdev, 'w', **kwargs)
    # Adds metadata tags to the output raster
    uu.add_rasterio_tags(dst_stdev_above, sensit_type)
    dst_stdev_above.update_tags(
        units='standard deviation, in terms of megagrams aboveground biomass (AGB or dry matter)/ha/yr')
    dst_stdev_above.update_tags(
        source='IPCC Guidelines 2019 refinement, forest section, Table 4.9')
    dst_stdev_above.update_tags(
        extent='Full model extent, even though these standard deviations will not be used over the full model extent')

    # Iterates across the windows (1 pixel strips) of the input tiles
    for idx, window in windows:

        # Creates a processing window for each input raster
        try:
            cont_eco_window = cont_eco_src.read(1, window=window)
        except:
            cont_eco_window = np.zeros((window.height, window.width), dtype='uint8')

        try:
            age_cat_window = age_cat_src.read(1, window=window)
        except:
            age_cat_window = np.zeros((window.height, window.width), dtype='uint8')

        # Recodes the input forest age category array with 10 different decision tree end values into the 3 actual age categories
        age_recode = np.vectorize(age_dict.get)(age_cat_window)

        # Adds the age category codes to the continent-ecozone codes to create an array of unique continent-ecozone-age codes
        cont_eco_age = cont_eco_window + age_recode

        ## Aboveground removal factors
        # Converts the continent-ecozone array to float so that the values can be replaced with fractional removals rates
        gain_rate_AGB = cont_eco_age.astype('float32')

        # Applies the dictionary of continent-ecozone-age removals rates to the continent-ecozone-age array to
        # get annual removals rates (metric tons aboveground biomass/yr) for each pixel
        for key, value in gain_table_dict.items():
            gain_rate_AGB[gain_rate_AGB == key] = value

        # Writes the output window to the output file
        dst_above.write_band(1, gain_rate_AGB, window=window)

        ## Belowground removal factors
        # Calculates belowground annual removal rates
        gain_rate_BGB = gain_rate_AGB * cn.below_to_above_non_mang

        # Writes the output window to the output file
        dst_below.write_band(1, gain_rate_BGB, window=window)

        ## Aboveground removal factor standard deviation
        # Converts the continent-ecozone array to float so that the values can be replaced with fractional standard deviations
        gain_stdev_AGB = cont_eco_age.astype('float32')

        # Applies the dictionary of continent-ecozone-age removals rate standard deviations to the continent-ecozone-age array to
        # get annual removals rate standard deviations (metric tons aboveground biomass/yr) for each pixel
        for key, value in stdev_table_dict.items():
            gain_stdev_AGB[gain_stdev_AGB == key] = value

        # Writes the output window to the output file
        dst_stdev_above.write_band(1, gain_stdev_AGB, window=window)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, output_pattern_list[0], no_upload)
