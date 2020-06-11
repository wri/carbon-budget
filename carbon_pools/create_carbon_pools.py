'''
This script creates carbon pools.
For the year 2000, it creates aboveground, belowground, deadwood, litter, and total
carbon pools (soil is created in a separate script but is brought in to create total carbon). These are to the extent
of WHRC and mangrove biomass 2000.

It also creates carbon pools for the year of loss/emissions-- only for pixels that had loss. To do this, it adds
CO2 (carbon) accumulated since 2000 to the C (biomass) 2000 stock, so that the CO2 (carbon) emitted is 2000 + gains
until loss. (For Hansen loss+gain pixels, only the portion of C that is accumulated before loss is included in the
lost carbon (lossyr-1), not the entire carbon gain of the pixel.) Because the emissions year carbon pools depend on
carbon removals, any time the removals model changes, the emissions year carbon pools need to be regenerated.

In both cases (carbon pools in 2000 and in the loss year), BGC, deadwood, and litter are calculated from AGC. Thus,
there are two AGC functions (one for AGC2000 and one for AGC in loss year) but only one function for BGC, deadwood,
litter, and total C (since those are purely functions of the AGC supplied to them).

The carbon pools in 2000 are not used for the model at all; they are purely for illustrative purposes. Only the
emissions year pools are used for the model.

Which carbon pools are being generated (2000 or loss) is controlled through the command line argument --extent (-e).
This extent argument determines which AGC function is used and how the outputs of the other pools' scripts are named.

NOTE: Because there are so many input files, this script needs a machine with extra disk space.
Thus, create a spot machine with extra disk space: spotutil new r4.16xlarge dgibbs_wri --disk_size 1024    (this is the maximum value).
'''

import datetime
import sys
import pandas as pd
import os
import numpy as np
import rasterio
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu


# Creates a dictionary of biomass in belowground, deadwood, and litter pools to aboveground biomass pool
def mangrove_pool_ratio_dict(gain_table_simplified, tropical_dry, tropical_wet,  subtropical):

    # Creates x_pool:aboveground biomass ratio dictionary for the three mangrove types, where the keys correspond to
    # the "mangType" field in the gain rate spreadsheet.
    # If the assignment of mangTypes to ecozones changes, that column in the spreadsheet may need to change and the
    # keys in this dictionary would need to change accordingly.
    # Key 4 is water, so there shouldn't be any mangrove values there.
    type_ratio_dict = {'1': tropical_dry, '2': tropical_wet,
                       '3': subtropical, '4': '100'}
    type_ratio_dict_final = {int(k): float(v) for k, v in list(type_ratio_dict.items())}

    # Applies the x_pool:aboveground biomass ratios for the three mangrove types to the annual aboveground gain rates to
    # create a column of x_pool:AGB
    gain_table_simplified['x_pool_AGB_ratio'] = gain_table_simplified['mangType'].map(type_ratio_dict_final)

    # Converts the continent-ecozone codes and corresponding BGB:AGB to a dictionary
    mang_x_pool_AGB_ratio = pd.Series(gain_table_simplified.x_pool_AGB_ratio.values,
                                   index=gain_table_simplified.gainEcoCon).to_dict()

    # Adds a dictionary entry for where the ecozone-continent code is 0 (not in a continent)
    mang_x_pool_AGB_ratio[0] = 0

    # Converts all the keys (continent-ecozone codes) to float type. Important for matching the raster's values.
    mang_x_pool_AGB_ratio = {float(key): value for key, value in mang_x_pool_AGB_ratio.items()}

    return mang_x_pool_AGB_ratio


'''
This function creates tiles of the aboveground carbon density in 2000 using mangrove and non-mangrove (WHRC) aboveground
biomass density in 2000. Unlike the AGC in emission year function, it uses the full extent (all pixels) of the two input
biomass tiles.
This is not used for the model. It is simply for having information on the carbon stocks in 2000.
'''
def create_2000_AGC(tile_id, pattern, sensit_type):

    # Start time
    start = datetime.datetime.now()

    # Names of the input tiles. Creates the names even if the files don't exist.
    mangrove_biomass_2000 = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_mangrove_biomass_2000)
    natrl_forest_biomass_2000 = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_WHRC_biomass_2000_unmasked)

    # Name of output tile
    all_forests_AGC_2000 = '{0}_{1}.tif'.format(tile_id, pattern)

    print("  Reading input files for {}...".format(tile_id))

    # Opens the input tiles if they exist. Any of these could not exist for a given Hansen tile.
    # Either mangrove biomass or WHRC biomass should exist for each tile, though. Thus, kwargs and windows should be
    # created based on one of those input tiles.
    try:
        mangrove_biomass_2000_src = rasterio.open(mangrove_biomass_2000)
        # Grabs metadata for one of the input tiles, like its location/projection/cellsize
        kwargs = mangrove_biomass_2000_src.meta
        # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
        windows = mangrove_biomass_2000_src.block_windows(1)
        print("Mangrove biomass found for", tile_id)
    except:
        print("No mangrove biomass for", tile_id)

    try:
        natrl_forest_biomass_2000_src = rasterio.open(natrl_forest_biomass_2000)
        # Grabs metadata for one of the input tiles, like its location/projection/cellsize
        kwargs = natrl_forest_biomass_2000_src.meta
        # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
        windows = natrl_forest_biomass_2000_src.block_windows(1)
        print("WHRC biomass found for", tile_id)
    except:
        print("No WHRC biomass found for", tile_id)

    # Updates kwargs for the output dataset.
    # Need to update data type to float 32 so that it can handle fractional gain rates
    kwargs.update(
        driver='GTiff',
        count=1,
        compress='lzw',
        nodata=0,
        dtype='float32'
    )

    # The output file: aboveground carbon density in the year of tree cover loss for pixels with tree cover loss
    dst_AGC_2000 = rasterio.open(all_forests_AGC_2000, 'w', **kwargs)

    print("  Creating aboveground carbon density in 2000 for {}...".format(tile_id))

    # Iterates across the windows (1 pixel strips) of the input tiles
    for idx, window in windows:

        # Populates the output raster's windows with 0s so that pixels without
        # any of the forest types will have 0s
        all_forest_types_AGC_combined = np.zeros((window.height, window.width), dtype='float32')

        # Checks if each forest type exists for the tile. If so, calculates AGC density as AGC in 2000 + AGC accumulation.
        # Initialy does this for all pixles (not just loss pixels)-- loss mask is applied at the very end of the window processing.

        # Mangrove calculation if there is a mangrove biomass tile
        if os.path.exists(mangrove_biomass_2000):

            mangrove_biomass_2000_window = mangrove_biomass_2000_src.read(1, window=window)

            # Adds the mangrove final AGC density values to the ongoing array
            all_forest_types_AGC_combined = all_forest_types_AGC_combined + (mangrove_biomass_2000_window * cn.biomass_to_c_mangrove)

        # Non-mangrove non-planted forest calculation if there is a corresponding C accumulation tile
        if os.path.exists(natrl_forest_biomass_2000):

            natrl_forest_biomass_2000_window = natrl_forest_biomass_2000_src.read(1, window=window)

            # Calculates the aboveground C density in non-mangrove non-planted forest pixels. The masking commands make sure that
            # only WHRC biomass pixels that correspond with non-mangrove non-planted forest pixels are included.
            # (Otherwise, all WHRC biomass pixels would be included in the non-mang non-planted forest calculation, not just
            # the pixels in non-mang non-planted forests.)
            natural_forest_biomass = natrl_forest_biomass_2000_window

            # Masks WHRC biomass where there is non-mangrove non-planted forest. If masked, the masked values are filled with 0s.
            if os.path.exists(mangrove_biomass_2000):
                natural_forest_biomass = np.ma.masked_where(mangrove_biomass_2000_window > 0, natural_forest_biomass)
                natural_forest_biomass = natural_forest_biomass.filled(0)

            # Adds the non-mang non-planted forest final AGC density values to the ongoing array.
            # This may or may not include mangroves or planted forests, depending on what was in the tile
            all_forest_types_AGC_combined = all_forest_types_AGC_combined + (natural_forest_biomass * cn.biomass_to_c_non_mangrove)

        # Converts the output to float32 since float64 is an unnecessary level of precision
        all_forest_types_C_final = all_forest_types_AGC_combined.astype('float32')

        # Writes the output window to the output file
        dst_AGC_2000.write_band(1, all_forest_types_C_final, window=window)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, pattern)


'''
This function creates tiles of the aboveground carbon density in the year in which tree cover loss occurred
using mangrove and non-mangrove (WHRC) aboveground biomass density in 2000 and carbon gain from 2000 until the loss year.
Unlike the AGC in 2000, it outputs values only where there is loss, and the values are carbon in 2000 + gain until loss.
Thus, loss pixels that don't also have gain pixels have all of their carbon accumulation from after 2000 emitted because
all of the carbon accumuluation is assumed to come before the loss happens.
However, pixels that have both loss and gain only emit the portion of the carbon accumulation that occurs before loss.
Therefore, loss+gain pixels only have part of their gross carbon accumulation added to AGC 2000 for all forest types.
This is used for the gross emissions model.
'''
def create_emitted_AGC(tile_id, pattern, sensit_type):

    # Only proceeds with running the function if there is a loss tile. Without a loss tile, there will be no output, so there's
    # no reason to run the function.
    if os.path.exists('{}.tif'.format(tile_id)):
        print("Loss tile found for {}. Processing...".format(tile_id))
        loss_year = '{}.tif'.format(tile_id)
    elif os.path.exists('{}_{}.tif'.format(tile_id, cn.pattern_Brazil_annual_loss_processed)):
        print("Brazil-specific loss tile found for {}. Processing...".format(tile_id))
        loss_year = '{}_{}.tif'.format(tile_id, cn.pattern_Brazil_annual_loss_processed)
    elif os.path.exists('{}_{}.tif'.format(tile_id, cn.pattern_Mekong_loss_processed)):
        print("Mekong-specific loss tile found for {}. Processing...".format(tile_id))
        loss_year = '{}_{}.tif'.format(tile_id, cn.pattern_Mekong_loss_processed)
    else:
        print("No loss tile for {}. Not processing.".format(tile_id))
        return

    # Start time
    start = datetime.datetime.now()

    # Names of the input tiles. Creates the names even if the files don't exist.
    mangrove_biomass_2000 = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_mangrove_biomass_2000)
    mangrove_cumul_AGCO2_gain = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_cumul_gain_AGCO2_mangrove)
    planted_forest_cumul_AGCO2_gain = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_cumul_gain_AGCO2_planted_forest_non_mangrove)
    natrl_forest_cumul_AGCO2_gain = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_cumul_gain_AGCO2_natrl_forest)
    mangrove_annual_gain = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_annual_gain_AGB_mangrove)
    planted_forest_annual_gain = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_annual_gain_AGB_planted_forest_non_mangrove)
    natrl_forest_annual_gain = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_annual_gain_AGB_natrl_forest)

    gain = uu.sensit_tile_rename(sensit_type, cn.pattern_gain, tile_id)
    if sensit_type == 'biomass_swap':
        natrl_forest_biomass_2000 = '{0}_{1}.tif'.format(tile_id, cn.pattern_JPL_unmasked_processed)
    else:
        natrl_forest_biomass_2000 = '{0}_{1}.tif'.format(tile_id, cn.pattern_WHRC_biomass_2000_unmasked)


    print(mangrove_biomass_2000)
    print(planted_forest_cumul_AGCO2_gain)
    print(natrl_forest_biomass_2000)

    # Name of output tile
    all_forests_AGC_emis_year = '{0}_{1}.tif'.format(tile_id, pattern)

    print("  Reading input files for {}...".format(tile_id))

    # Opens the input tiles if they exist. Any of these could not exist for a given Hansen tile.
    # Either mangrove biomass or WHRC biomass should exist for each tile, though. Thus, kwargs and windows should be
    # created based on one of those input tiles.
    try:
        mangrove_biomass_2000_src = rasterio.open(mangrove_biomass_2000)
        mangrove_cumul_AGCO2_gain_src = rasterio.open(mangrove_cumul_AGCO2_gain)
        mangrove_annual_gain_src = rasterio.open(mangrove_annual_gain)
        # Grabs metadata for one of the input tiles, like its location/projection/cellsize
        kwargs = mangrove_biomass_2000_src.meta
        # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
        windows = mangrove_biomass_2000_src.block_windows(1)
        print("Mangrove tile found for", tile_id)
    except:
        print("No mangrove tile for", tile_id)

    try:
        planted_forest_cumul_AGCO2_gain_src = rasterio.open(planted_forest_cumul_AGCO2_gain)
        planted_forest_annual_gain_src = rasterio.open(planted_forest_annual_gain)
        print("Non-mangrove planted carbon accumulation found for", tile_id)
    except:
        print("No non-mangrove planted carbon accumulation for", tile_id)

    try:
        natrl_forest_biomass_2000_src = rasterio.open(natrl_forest_biomass_2000)
        natrl_forest_cumul_AGCO2_gain_src = rasterio.open(natrl_forest_cumul_AGCO2_gain)
        natrl_forest_annual_gain_src = rasterio.open(natrl_forest_annual_gain)
        # Grabs metadata for one of the input tiles, like its location/projection/cellsize
        kwargs = natrl_forest_biomass_2000_src.meta
        # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
        windows = natrl_forest_biomass_2000_src.block_windows(1)
        print("Natural forest found for", tile_id)
    except:
        print("No natural forest found for", tile_id)

    try:
        gain_src = rasterio.open(gain)
    except:
        print("No gain tile found for", tile_id)

    # Due to the check earlier in this function, there should always be a loss year tile
    loss_year_src = rasterio.open(loss_year)

    # Updates kwargs for the output dataset.
    # Need to update data type to float 32 so that it can handle fractional gain rates
    kwargs.update(
        driver='GTiff',
        count=1,
        compress='lzw',
        nodata=0,
        dtype='float32'
    )

    # The output file: aboveground carbon density in the year of tree cover loss for pixels with tree cover loss
    dst_AGC_emis_year = rasterio.open(all_forests_AGC_emis_year, 'w', **kwargs)

    print("  Creating aboveground carbon density in the year of loss for {}...".format(tile_id))

    # Iterates across the windows (1 pixel strips) of the input tiles
    for idx, window in windows:

        # Reads the loss and gain year windows
        loss_year_window = loss_year_src.read(1, window=window)
        gain_window = gain_src.read(1, window=window)

        # Populates the output raster's windows with 0s so that pixels without
        # any of the forest types will have 0s
        all_forest_types_AGC_combined = np.zeros((window.height, window.width), dtype='float32')

        # Creates a mask based on whether the pixels had loss and gain in them. Loss&gain pixels are 1, all else are 0.
        # This is used for all forest types since all forest types use the same loss and gain data.
        # Loss&gain pixels are 1, all else are 0
        loss_gain_mask = np.ma.masked_where(loss_year_window == 0, gain_window).filled(0)

        # Checks if each forest type exists for the tile. If so, calculates AGC density as AGC in 2000 + AGC accumulation
        # (AGC accumulation is converted from AGCO2 accumulation).
        # Initialy does this for all pixles (not just loss pixels)-- loss mask is applied at the very end of the window processing.

        # Mangrove calculation if there is a mangrove biomass tile
        if os.path.exists(mangrove_biomass_2000):

            # Creates windows for the mangrove inputs
            mangrove_biomass_2000_window = mangrove_biomass_2000_src.read(1, window=window)
            mangrove_cumul_AGCO2_gain_window = mangrove_cumul_AGCO2_gain_src.read(1, window=window)
            mangrove_annual_gain_window = mangrove_annual_gain_src.read(1, window=window)

            # Loss pixels that also have gain pixels are treated differently from loss-only pixels.
            # Calculates AGC in emission year for mangrove pixels that don't have gain and loss (excludes loss_gain_mask = 1).
            # To do this, it adds all the accumulated carbon after 2000 to the carbon in 2000 (all accumulated C is emitted).
            mangrove_C_final_non_gain_and_loss = (mangrove_biomass_2000_window * cn.biomass_to_c_mangrove) \
                                                 + (mangrove_cumul_AGCO2_gain_window / cn.c_to_co2)
            mangrove_C_final_non_gain_and_loss_masked = np.ma.masked_where(loss_gain_mask == 1, mangrove_C_final_non_gain_and_loss).filled(0)

            # Calculates AGC in emission year for mangrove pixels that had loss & gain (excludes loss_gain_mask = 0).
            # To do this, it adds only the portion of the gain that occurred before the loss year to the carbon in 2000.
            gain_before_loss = mangrove_annual_gain_window * (loss_year_window - 1)
            mangrove_C_final_gain_and_loss = (mangrove_biomass_2000_window * cn.biomass_to_c_mangrove) \
                                             + (gain_before_loss * cn.biomass_to_c_mangrove)
            mangrove_C_final_gain_and_loss_masked = np.ma.masked_where(loss_gain_mask == 0, mangrove_C_final_gain_and_loss).filled(0)

            # Adds the mangrove final AGC density values to the ongoing array
            all_forest_types_AGC_combined = all_forest_types_AGC_combined \
                                            + mangrove_C_final_non_gain_and_loss_masked \
                                            + mangrove_C_final_gain_and_loss_masked

        # Non-mangrove planted forest calculation if there is a planted forest C accumulation tile
        if os.path.exists(planted_forest_cumul_AGCO2_gain):

            # There are no special planted forest biomass tiles, so planted forests start with the WHRC biomass tiles
            natrl_forest_biomass_2000_window = natrl_forest_biomass_2000_src.read(1, window=window)
            planted_forest_cumul_AGCO2_gain_window = planted_forest_cumul_AGCO2_gain_src.read(1, window=window)
            planted_forest_annual_gain_window = planted_forest_annual_gain_src.read(1, window=window)

            # Loss pixels that also have gain pixels are treated differently from loss-only pixels.
            # Calculates AGC in emission year for non-mangrove planted forest pixels that don't have gain and loss (excludes loss_gain_mask = 1).
            # To do this, it adds all the accumulated carbon after 2000 to the carbon in 2000 (all accumulated C is emitted).
            planted_forest_C_non_gain_and_loss = (natrl_forest_biomass_2000_window * cn.biomass_to_c_non_mangrove) \
                               + (planted_forest_cumul_AGCO2_gain_window / cn.c_to_co2)
            planted_forest_C_non_gain_and_loss_masked = np.ma.masked_where(loss_gain_mask == 1, planted_forest_C_non_gain_and_loss).filled(0)

            # Makes sure that only WHRC biomass pixels that correspond with non-mangrove planted forest pixels are included.
            # (Otherwise, all WHRC biomass pixels would be included in the planted forest calculation, not just the pixels
            # at planted forests.)
            planted_forest_C_non_gain_and_loss_masked_final = \
                np.ma.masked_where(planted_forest_cumul_AGCO2_gain_window == 0, planted_forest_C_non_gain_and_loss_masked).filled(0)

            # Calculates AGC in emission year for non-mangrove planted forest pixels that had loss & gain (excludes loss_gain_mask = 0).
            # To do this, it adds only the portion of the gain that occurred before the loss year to the carbon in 2000.
            gain_before_loss = planted_forest_annual_gain_window * (loss_year_window - 1)
            planted_forest_C_gain_and_loss = (natrl_forest_biomass_2000_window * cn.biomass_to_c_non_mangrove) \
                                             + (gain_before_loss * cn.biomass_to_c_non_mangrove)
            planted_forest_C_gain_and_loss_masked = np.ma.masked_where(loss_gain_mask == 0, planted_forest_C_gain_and_loss).filled(0)

            # Makes sure that only WHRC biomass pixels that correspond with non-mangrove planted forest pixels are included.
            # (Otherwise, all WHRC biomass pixels would be included in the planted forest calculation, not just the pixels
            # at planted forests.)
            planted_forest_C_gain_and_loss_masked_final = \
                np.ma.masked_where(planted_forest_cumul_AGCO2_gain_window == 0, planted_forest_C_gain_and_loss_masked).filled(0)

            # Adds the non-mangrove planted forest final AGC density values to the ongoing array.
            # This will or will not include mangrove values, depending on whether there are mangroves in the tile.
            all_forest_types_AGC_combined = all_forest_types_AGC_combined \
                                            + planted_forest_C_non_gain_and_loss_masked_final \
                                            + planted_forest_C_gain_and_loss_masked_final

        # Non-mangrove non-planted forest calculation if there is a corresponding C accumulation tile
        if os.path.exists(natrl_forest_cumul_AGCO2_gain):

            # Creates windows for the natural forest inputs
            natrl_forest_biomass_2000_window = natrl_forest_biomass_2000_src.read(1, window=window)
            natrl_forest_cumul_AGCO2_gain_window = natrl_forest_cumul_AGCO2_gain_src.read(1, window=window)
            natrl_forest_annual_gain_window = natrl_forest_annual_gain_src.read(1, window=window)

            # Loss pixels that also have gain pixels are treated differently from loss-only pixels.

            # Calculates AGC in emission year for natural forest pixels that don't have gain and loss (i.e.
            # (excludes loss_gain_mask = 1).
            # To do this, it adds all the accumulated carbon after 2000 to the carbon in 2000.
            natural_forest_non_gain_and_loss = (natrl_forest_biomass_2000_window * cn.biomass_to_c_non_mangrove) \
                                                 + (natrl_forest_cumul_AGCO2_gain_window / cn.c_to_co2)
            natural_forest_non_gain_and_loss_masked = np.ma.masked_where(loss_gain_mask == 1, natural_forest_non_gain_and_loss).filled(0)

            # Calculates AGC in emission year for natural forest pixels that had loss & gain (excludes loss_gain_mask = 0).
            # To do this, it adds only the portion of the gain that occurred before the loss year to the carbon in 2000.
            gain_before_loss = natrl_forest_annual_gain_window * (loss_year_window - 1)
            natural_forest_gain_and_loss = (natrl_forest_biomass_2000_window * cn.biomass_to_c_non_mangrove) \
                                             + (gain_before_loss * cn.biomass_to_c_non_mangrove)
            natural_forest_gain_and_loss_masked = np.ma.masked_where(loss_gain_mask == 0, natural_forest_gain_and_loss).filled(0)

            natural_forest_C = natural_forest_non_gain_and_loss_masked + natural_forest_gain_and_loss_masked

            # Calculates the aboveground C density in non-mangrove non-planted forest pixels. The masking commands make sure that
            # only WHRC biomass pixels that correspond with non-mangrove non-planted forest pixels are included.
            # (Otherwise, all WHRC biomass pixels would be included in the non-mang non-planted forest calculation, not just
            # the pixels in non-mang non-planted forests.)
            # Masks WHRC biomass where there is non-mangrove planted forest. If masked, the masked values are filled with 0s.
            if os.path.exists(planted_forest_cumul_AGCO2_gain):
                natural_forest_C = np.ma.masked_where(planted_forest_cumul_AGCO2_gain_window > 0, natural_forest_C)
                natural_forest_C = natural_forest_C.filled(0)

            # Masks WHRC biomass where there is mangrove. If masked, the masked values are filled with 0s.
            if os.path.exists(mangrove_biomass_2000):
                natural_forest_C = np.ma.masked_where(mangrove_biomass_2000_window > 0, natural_forest_C)
                natural_forest_C = natural_forest_C.filled(0)

            # Adds the non-mang non-planted forest final AGC density values to the ongoing array.
            # This may or may not include mangroves or planted forests, depending on what was in the tile
            all_forest_types_AGC_combined = all_forest_types_AGC_combined + natural_forest_C

        # Removes AGC pixels that do not have a loss year and fills with 0s
        all_forest_types_C_final = np.ma.masked_where(loss_year_window == 0, all_forest_types_AGC_combined)
        all_forest_types_C_final = all_forest_types_C_final.filled(0)

        # Converts the output to float32 since float64 is an unnecessary level of precision
        all_forest_types_C_final = all_forest_types_C_final.astype('float32')

        # Writes the output window to the output file
        dst_AGC_emis_year.write_band(1, all_forest_types_C_final, window=window)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_AGC_emis_year)


# Creates belowground carbon tiles (both in 2000 and loss year)
def create_BGC(tile_id, mang_BGB_AGB_ratio, carbon_pool_extent, pattern, sensit_type):

    start = datetime.datetime.now()

    # Names of the input tiles. Creates the names even if the files don't exist.
    # The AGC name depends on whether carbon 2000 or carbon in the emission year is being created.
    # If BGC in the loss year is being created, it uses the loss year AGC tile.
    # If BGC in 2000 is being created, is uses the 2000 AGC tile.
    # The other inputs tiles aren't affected by whether the output is for 2000 or for the loss year.
    mangrove_biomass_2000 = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_mangrove_biomass_2000)
    cont_ecozone = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_cont_eco_processed)
    if carbon_pool_extent == "loss":
        AGC = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_AGC_emis_year)
    if carbon_pool_extent == "2000":
        AGC = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_AGC_2000)

    # Name of output tile
    # The BGC name depends on whether carbon in 2000 or in the emission year is being created.
    BGC = '{0}_{1}.tif'.format(tile_id, pattern)

    print("  Reading input files for {}...".format(tile_id))

    # Both of these tiles should exist and thus be able to be opened
    AGC_src = rasterio.open(AGC)   # This will be either the AGC 2000 or AGC loss year tile
    cont_ecozone_src = rasterio.open(cont_ecozone)

    # Opens the mangrove biomass tile if it exists
    try:
        mangrove_biomass_2000_src = rasterio.open(mangrove_biomass_2000)
        print("Mangrove biomass found for", tile_id)
    except:
        print("No mangrove biomass for", tile_id)

    # Grabs metadata for one of the input tiles, like its location/projection/cellsize
    kwargs = AGC_src.meta
    # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
    windows = AGC_src.block_windows(1)

    # Updates kwargs for the output dataset.
    # Need to update data type to float 32 so that it can handle fractional carbon pools
    kwargs.update(
        driver='GTiff',
        count=1,
        compress='lzw',
        nodata=0,
        dtype='float32'
    )

    # The output file: belowground carbon density
    dst_BGC = rasterio.open(BGC, 'w', **kwargs)

    print("  Creating belowground carbon density for {0} using carbon_pool_extent '{1}'...".format(tile_id, carbon_pool_extent))

    # Iterates across the windows (1 pixel strips) of the input tiles
    for idx, window in windows:

        # Populates the output raster's windows with 0s so that pixels without
        # any of the forest types will have 0s
        BGC_output = np.zeros((window.height, window.width), dtype='float32')

        # Reads in the windows of each input file that definitely exist
        AGC_window = AGC_src.read(1, window=window)
        # print AGC_window[0][30020:30035]
        cont_ecozone_window = cont_ecozone_src.read(1, window=window)
        cont_ecozone_window = cont_ecozone_window.astype('float32')
        # print fao_ecozone_window[0][30020:30035]

        # Mangrove calculation if there is a mangrove biomass tile
        if os.path.exists(mangrove_biomass_2000):

            # Reads in the window for mangrove biomass if it exists
            mangrove_biomass_2000_window = mangrove_biomass_2000_src.read(1, window=window)
            # print mangrove_biomass_2000_window[0][30020:30035]

            # Applies the mangrove BGB:AGB ratios (3 different ratios) to the ecozone raster to create a raster of BGB:AGB ratios
            for key, value in mang_BGB_AGB_ratio.items():
                cont_ecozone_window[cont_ecozone_window == key] = value
            # print fao_ecozone_window[0][30020:30035]

            # Multiplies the AGC in the loss year by the correct mangrove BGB:AGB ratio to get an array of BGC in the loss year
            mangrove_C_final = AGC_window * cont_ecozone_window
            # print mangrove_C_final[0][30020:30035]

            # Masks out non-mangrove pixels and fills the masked values with 0s
            mangrove_C_final = np.ma.masked_where(mangrove_biomass_2000_window == 0, mangrove_C_final)
            mangrove_C_final = mangrove_C_final.filled(0)
            # print mangrove_C_final[0][30020:30035]

            # Applies the non-mangrove BGB:AGB ratio to all AGC pixels
            non_mang_output = AGC_window * cn.below_to_above_non_mang
            # print non_mang_output[0][29930:29950]

            # Masks out mangrove pixels so that only non-mangrove pixels use the non-mangrove BGB:AGB ratio
            non_mang_output_final = np.ma.masked_where(mangrove_biomass_2000_window != 0, non_mang_output)
            # print non_mang_output_final[0][29930:29950]

            # Combines the mangrove and non-mangrove BGC arrays into a single array
            BGC_output = mangrove_C_final + non_mang_output_final
            # print BGC_output[0][29930:29950]

        # If there is no mangrove tile, all AGC pixels are multiplied by the non-mangrove
        # BGB:AGB ratio
        if not os.path.exists(mangrove_biomass_2000):

            BGC_output = AGC_window * cn.below_to_above_non_mang
            # print BGC_output[0][29930:29950]

        # Writes the output window to the output file
        dst_BGC.write_band(1, BGC_output, window=window)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, pattern)


# Creates deadwood carbon tiles (both in 2000 and loss year)
def create_deadwood(tile_id, mang_deadwood_AGB_ratio, carbon_pool_extent, pattern, sensit_type):

    start = datetime.datetime.now()

    # Names of the input tiles. Creates the names even if the files don't exist.
    # The AGC name depends on whether carbon in 2000 or in the emission year is being created.
    # If deadwood in the loss year is being created, it uses the loss year AGC tile.
    # If deadwood in 2000 is being created, is uses the 2000 AGC tile.
    # The other inputs tiles aren't affected by whether the output is for 2000 or for the loss year.
    mangrove_biomass_2000 = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_mangrove_biomass_2000)
    bor_tem_trop = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_bor_tem_trop_processed)
    cont_eco = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_cont_eco_processed)
    precip = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_precip)
    elevation = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_elevation)
    if sensit_type == 'biomass_swap':
        natrl_forest_biomass_2000 = '{0}_{1}.tif'.format(tile_id, cn.pattern_JPL_unmasked_processed)
    else:
        natrl_forest_biomass_2000 = '{0}_{1}.tif'.format(tile_id, cn.pattern_WHRC_biomass_2000_unmasked)
    if carbon_pool_extent == "loss":
        AGC = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_AGC_emis_year)
    if carbon_pool_extent == "2000":
        AGC = uu.sensit_tile_rename(sensit_type, tile_id,  cn.pattern_AGC_2000)

    # Name of output tile
    # The output name depends on whether carbon in 2000 or in the emission year is being created.
    deadwood = '{0}_{1}.tif'.format(tile_id, pattern)

    print("  Reading input files for {}...".format(tile_id))


    # These tiles should exist and thus be able to be opened
    AGC_src = rasterio.open(AGC)    # This will be either the AGC 2000 or AGC loss year tile
    bor_tem_trop_src = rasterio.open(bor_tem_trop)
    cont_ecozone_src = rasterio.open(cont_eco)
    precip_src = rasterio.open(precip)
    elevation_src = rasterio.open(elevation)

    # Opens the mangrove biomass tile if it exists
    try:
        mangrove_biomass_2000_src = rasterio.open(mangrove_biomass_2000)
        print("Mangrove biomass found for", tile_id)
    except:
        print("No mangrove biomass for", tile_id)

    # Opens the WHRC biomass tile if it exists
    try:
        natrl_forest_biomass_2000_src = rasterio.open(natrl_forest_biomass_2000)
        print("WHRC biomass found for", tile_id)
    except:
        print("No WHRC biomass for", tile_id)

    # Grabs metadata for one of the input tiles, like its location/projection/cellsize
    kwargs = AGC_src.meta
    # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
    windows = AGC_src.block_windows(1)

    # Updates kwargs for the output dataset.
    # Need to update data type to float 32 so that it can handle fractional carbon pools
    kwargs.update(
        driver='GTiff',
        count=1,
        compress='lzw',
        nodata=0,
        dtype='float32'
    )

    # The output file: deadwood carbon density
    dst_deadwood = rasterio.open(deadwood, 'w', **kwargs)

    print("  Creating deadwood carbon density for {0} using carbon_pool_extent '{1}'...".format(tile_id, carbon_pool_extent))

    # Iterates across the windows (1 pixel strips) of the input tiles
    for idx, window in windows:

        # Populates the output raster's windows with 0s so that pixels without
        # any of the forest types will have 0s
        deadwood_output = np.zeros((window.height, window.width), dtype='float32')

        # Reads in the window of the AGC emissions in order to determine if there was any loss in thw window
        AGC_window = AGC_src.read(1, window=window)

        # If there was no loss in the window, the window is skipped
        if np.amax(AGC_window) == 0:
            continue

        # These tiles should exist regardless of whether there is mangrove and/or WHRC biomass
        bor_tem_trop_window = bor_tem_trop_src.read(1, window=window)
        cont_ecozone_window = cont_ecozone_src.read(1, window=window)
        cont_ecozone_window = cont_ecozone_window.astype('float32')
        precip_window = precip_src.read(1, window=window)
        elevation_window = elevation_src.read(1, window=window)

        # This allows the script to bypass the few tiles that have mangrove biomass but not WHRC biomass
        if os.path.exists(natrl_forest_biomass_2000):

            # Reads in the windows of each input file that definitely exist
            natrl_forest_biomass_window = natrl_forest_biomass_2000_src.read(1, window=window)

            # The deadwood conversions generally come from here: https://cdm.unfccc.int/methodologies/ARmethodologies/tools/ar-am-tool-12-v3.0.pdf, p. 17-18
            # They depend on the elevation, precipitation, and broad biome category (boreal/temperate/tropical).
            # For some reason, the masks need to be named different variables for each equation.
            # If they all have the same name (e.g., elev_mask and condition_mask are reused), then at least the condition_mask_4
            # equation won't work properly.)

            # Equation for elevation <= 2000, precip <= 1000, bor/temp/trop = 1 (tropical)
            elev_mask_1 = elevation_window <= 2000
            precip_mask_1 = precip_window <= 1000
            ecozone_mask_1 = bor_tem_trop_window == 1
            condition_mask_1 = elev_mask_1 & precip_mask_1 & ecozone_mask_1
            agb_masked_1 = np.ma.array(natrl_forest_biomass_window, mask=np.invert(condition_mask_1))
            deadwood_masked = agb_masked_1 * 0.02 * cn.biomass_to_c_non_mangrove
            deadwood_output = deadwood_output + deadwood_masked.filled(0)

            # Equation for elevation <= 2000, 1000 < precip <= 1600, bor/temp/trop = 1 (tropical)
            elev_mask_2 = elevation_window <= 2000
            precip_mask_2 = (precip_window > 1000) & (precip_window <= 1600)
            ecozone_mask_2 = bor_tem_trop_window == 1
            condition_mask_2 = elev_mask_2 & precip_mask_2 & ecozone_mask_2
            agb_masked_2 = np.ma.array(natrl_forest_biomass_window, mask=np.invert(condition_mask_2))
            deadwood_masked = agb_masked_2 * 0.01 * cn.biomass_to_c_non_mangrove
            deadwood_output = deadwood_output + deadwood_masked.filled(0)

            # Equation for elevation <= 2000, precip > 1600, bor/temp/trop = 1 (tropical)
            elev_mask_3 = elevation_window <= 2000
            precip_mask_3 = precip_window > 1600
            ecozone_mask_3 = bor_tem_trop_window == 1
            condition_mask_3 = elev_mask_3 & precip_mask_3 & ecozone_mask_3
            agb_masked_3 = np.ma.array(natrl_forest_biomass_window, mask=np.invert(condition_mask_3))
            deadwood_masked = agb_masked_3 * 0.06 * cn.biomass_to_c_non_mangrove
            deadwood_output = deadwood_output + deadwood_masked.filled(0)

            # Equation for elevation > 2000, precip = any value, bor/temp/trop = 1 (tropical)
            elev_mask_4 = elevation_window > 2000
            ecozone_mask_4 = bor_tem_trop_window == 1
            condition_mask_4 = elev_mask_4 & ecozone_mask_4
            agb_masked_4 = np.ma.array(natrl_forest_biomass_window, mask=np.invert(condition_mask_4))
            deadwood_masked = agb_masked_4 * 0.07 * cn.biomass_to_c_non_mangrove
            deadwood_output = deadwood_output + deadwood_masked.filled(0)

            # Equation for elevation = any value, precip = any value, bor/temp/trop = 2 or 3 (boreal or temperate)
            ecozone_mask_5 = bor_tem_trop_window != 1
            condition_mask_5 = ecozone_mask_5
            agb_masked_5 = np.ma.array(natrl_forest_biomass_window, mask=np.invert(condition_mask_5))
            deadwood_masked = agb_masked_5 * 0.08 * cn.biomass_to_c_non_mangrove
            deadwood_output = deadwood_output + deadwood_masked.filled(0)

        # Replaces non-mangrove deadwood with special mangrove deadwood values if there is mangrove
        if os.path.exists(mangrove_biomass_2000):

            # Reads in the window for mangrove biomass if it exists
            mangrove_biomass_2000_window = mangrove_biomass_2000_src.read(1, window=window)

            # Applies the mangrove deadwood:AGB ratios (2 different ratios) to the ecozone raster to create a raster of deadwood:AGB ratios
            for key, value in mang_deadwood_AGB_ratio.items():
                cont_ecozone_window[cont_ecozone_window == key] = value

            # Multiplies the AGB in the loss year (2000 for deadwood) by the correct mangrove deadwood:AGB ratio to get an array of deadwood
            mangrove_C_final = mangrove_biomass_2000_window * cont_ecozone_window * cn.biomass_to_c_mangrove

            # Replaces non-mangrove deadwood with mangrove deadwood values
            deadwood_output = np.ma.masked_where(mangrove_biomass_2000_window > 0, deadwood_output)
            deadwood_output = deadwood_output.filled(0)

            # Combines the mangrove and non-mangrove deadwood arrays into a single array
            deadwood_output = mangrove_C_final + deadwood_output

        # Removes deadwood values that did not have tree cover loss
        deadwood_output = np.ma.masked_where(AGC_window == 0, deadwood_output)
        deadwood_output = deadwood_output.filled(0)

        # Necessary for matching the output to the raster datatype
        deadwood_output = deadwood_output.astype('float32')

        # Writes the output window to the output file
        dst_deadwood.write_band(1, deadwood_output, window=window)

        # sys.quit()

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, pattern)


# Creates litter carbon tiles (both in 2000 and loss year)
def create_litter(tile_id, mang_litter_AGB_ratio, carbon_pool_extent, pattern, sensit_type):

    start = datetime.datetime.now()

    # Names of the input tiles. Creates the names even if the files don't exist.
    # The AGC name depends on whether carbon in 2000 or in the emission year is being created.
    # If litter in the loss year is being created, it uses the loss year AGC tile.
    # If litter in 2000 is being created, is uses the 2000 AGC tile.
    # The other inputs tiles aren't affected by whether the output is for 2000 or for the loss year.
    mangrove_biomass_2000 = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_mangrove_biomass_2000)
    bor_tem_trop = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_bor_tem_trop_processed)
    cont_eco = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_cont_eco_processed)
    precip = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_precip)
    elevation = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_elevation)
    if sensit_type == 'biomass_swap':
        natrl_forest_biomass_2000 = '{0}_{1}.tif'.format(tile_id, cn.pattern_JPL_unmasked_processed)
    else:
        natrl_forest_biomass_2000 = '{0}_{1}.tif'.format(tile_id, cn.pattern_WHRC_biomass_2000_unmasked)
    if carbon_pool_extent == "loss":
        AGC = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_AGC_emis_year)
    if carbon_pool_extent == "2000":
        AGC = uu.sensit_tile_rename(sensit_type, tile_id,  cn.pattern_AGC_2000)

    # Name of output tile
    # The output name depends on whether carbon in 2000 or in the emission year is being created.
    litter = '{0}_{1}.tif'.format(tile_id, pattern)

    print("  Reading input files for {}...".format(tile_id))

    # These tiles should exist and thus be able to be opened
    AGC_src = rasterio.open(AGC)
    bor_tem_trop_src = rasterio.open(bor_tem_trop)
    cont_ecozone_src = rasterio.open(cont_eco)
    precip_src = rasterio.open(precip)
    elevation_src = rasterio.open(elevation)

    # Opens the mangrove biomass tile if it exists
    try:
        mangrove_biomass_2000_src = rasterio.open(mangrove_biomass_2000)
        print("Mangrove biomass found for", tile_id)
    except:
        print("No mangrove biomass for", tile_id)

    # Opens the WHRC biomass tile if it exists
    try:
        natrl_forest_biomass_2000_src = rasterio.open(natrl_forest_biomass_2000)
        print("WHRC biomass found for", tile_id)
    except:
        print("No WHRC biomass for", tile_id)

    # Grabs metadata for one of the input tiles, like its location/projection/cellsize
    kwargs = AGC_src.meta
    # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
    windows = AGC_src.block_windows(1)

    # Updates kwargs for the output dataset.
    # Need to update data type to float 32 so that it can handle fractional carbon pools
    kwargs.update(
        driver='GTiff',
        count=1,
        compress='lzw',
        nodata=0,
        dtype='float32'
    )

    # The output file: litter carbon density
    dst_litter = rasterio.open(litter, 'w', **kwargs)

    print("  Creating litter carbon density for {0} using carbon_pool_extent '{1}'...".format(tile_id, carbon_pool_extent))

    # Iterates across the windows (1 pixel strips) of the input tiles
    for idx, window in windows:

        # Populates the output raster's windows with 0s so that pixels without
        # any of the forest types will have 0s
        litter_output = np.zeros((window.height, window.width), dtype='float32')

        # Reads in the window of the AGC emissions in loss year in order to determine if there was any loss in thw window
        AGC_window = AGC_src.read(1, window=window)

        # If there was no loss in the window, the window is skipped
        if np.amax(AGC_window) == 0:
            continue

        # These tiles should exist regardless of whether there is mangrove and/or WHRC biomass
        bor_tem_trop_window = bor_tem_trop_src.read(1, window=window)
        cont_ecozone_window = cont_ecozone_src.read(1, window=window)
        cont_ecozone_window = cont_ecozone_window.astype('float32')
        precip_window = precip_src.read(1, window=window)
        elevation_window = elevation_src.read(1, window=window)

        # This allows the script to bypass the few tiles that have mangrove biomass but not WHRC biomass
        if os.path.exists(natrl_forest_biomass_2000):

            # Reads in the windows of each input file that definitely exist
            natrl_forest_biomass_window = natrl_forest_biomass_2000_src.read(1, window=window)

            # The litter conversions generally come from here: https://cdm.unfccc.int/methodologies/ARmethodologies/tools/ar-am-tool-12-v3.0.pdf, p. 17-18
            # They depend on the elevation, precipitation, and broad biome category (boreal/temperate/tropical).
            # For some reason, the masks need to be named different variables for each equation.
            # If they all have the same name (e.g., elev_mask and condition_mask are reused), then at least the condition_mask_4
            # equation won't work properly.)

            # Equation for elevation <= 2000, precip <= 1000, bor/temp/trop = 1 (tropical)
            elev_mask_1 = elevation_window <= 2000
            precip_mask_1 = precip_window <= 1000
            ecozone_mask_1 = bor_tem_trop_window == 1
            condition_mask_1 = elev_mask_1 & precip_mask_1 & ecozone_mask_1
            agb_masked_1 = np.ma.array(natrl_forest_biomass_window, mask=np.invert(condition_mask_1))
            litter_masked = agb_masked_1 * 0.04 * cn.biomass_to_c_non_mangrove_litter
            litter_output = litter_output + litter_masked.filled(0)

            # Equation for elevation <= 2000, 1000 < precip <= 1600, bor/temp/trop = 1 (tropical)
            elev_mask_2 = elevation_window <= 2000
            precip_mask_2 = (precip_window > 1000) & (precip_window <= 1600)
            ecozone_mask_2 = bor_tem_trop_window == 1
            condition_mask_2 = elev_mask_2 & precip_mask_2 & ecozone_mask_2
            agb_masked_2 = np.ma.array(natrl_forest_biomass_window, mask=np.invert(condition_mask_2))
            litter_masked = agb_masked_2 * 0.01 * cn.biomass_to_c_non_mangrove_litter
            litter_output = litter_output + litter_masked.filled(0)

            # Equation for elevation <= 2000, precip > 1600, bor/temp/trop = 1 (tropical)
            elev_mask_3 = elevation_window <= 2000
            precip_mask_3 = precip_window > 1600
            ecozone_mask_3 = bor_tem_trop_window == 1
            condition_mask_3 = elev_mask_3 & precip_mask_3 & ecozone_mask_3
            agb_masked_3 = np.ma.array(natrl_forest_biomass_window, mask=np.invert(condition_mask_3))
            litter_masked = agb_masked_3 * 0.01 * cn.biomass_to_c_non_mangrove_litter
            litter_output = litter_output + litter_masked.filled(0)

            # Equation for elevation > 2000, precip = any value, bor/temp/trop = 1 (tropical)
            elev_mask_4 = elevation_window > 2000
            ecozone_mask_4 = bor_tem_trop_window == 1
            condition_mask_4 = elev_mask_4 & ecozone_mask_4
            agb_masked_4 = np.ma.array(natrl_forest_biomass_window, mask=np.invert(condition_mask_4))
            litter_masked = agb_masked_4 * 0.01 * cn.biomass_to_c_non_mangrove_litter
            litter_output = litter_output + litter_masked.filled(0)

            # Equation for elevation = any value, precip = any value, bor/temp/trop = 2 or 3 (boreal or temperate)
            ecozone_mask_5 = bor_tem_trop_window != 1
            condition_mask_5 = ecozone_mask_5
            agb_masked_5 = np.ma.array(natrl_forest_biomass_window, mask=np.invert(condition_mask_5))
            litter_masked = agb_masked_5 * 0.04 * cn.biomass_to_c_non_mangrove_litter
            litter_output = litter_output + litter_masked.filled(0)

        # Replaces non-mangrove litter with special mangrove litter values if there is mangrove
        if os.path.exists(mangrove_biomass_2000):

            # Reads in the window for mangrove biomass if it exists
            mangrove_biomass_2000_window = mangrove_biomass_2000_src.read(1, window=window)

            # Applies the mangrove litter:AGB ratios (2 different ratios) to the ecozone raster to create a raster of litter:AGB ratios
            for key, value in mang_litter_AGB_ratio.items():
                cont_ecozone_window[cont_ecozone_window == key] = value

            # Multiplies the AGB in the loss year (2000 for litter) by the correct mangrove litter:AGB ratio to get an array of litter
            mangrove_C_final = mangrove_biomass_2000_window * cont_ecozone_window * cn.biomass_to_c_mangrove

            # Replaces non-mangrove litter with mangrove litter values
            litter_output = np.ma.masked_where(mangrove_biomass_2000_window > 0, litter_output)
            litter_output = litter_output.filled(0)

            # Combines the mangrove and non-mangrove litter arrays into a single array
            litter_output = mangrove_C_final + litter_output

        # Removes litter values that did not have tree cover loss
        litter_output = np.ma.masked_where(AGC_window == 0, litter_output)
        litter_output = litter_output.filled(0)

        # Necessary for matching the output to the raster datatype
        litter_output = litter_output.astype('float32')

        # Writes the output window to the output file
        dst_litter.write_band(1, litter_output, window=window)

        # sys.quit()

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, pattern)


# Creates soil carbon tiles in loss pixels only
def create_soil(tile_id, pattern, sensit_type):

    start = datetime.datetime.now()

    # Names of the input tiles. Creates the names even if the files don't exist.
    soil_full_extent = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_soil_C_full_extent_2000)
    AGC_emis_year = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_AGC_emis_year)

    # Name of output tile
    soil_emis_year = '{0}_{1}.tif'.format(tile_id, pattern)

    print("  Reading input files for {}...".format(tile_id))

    # Both of these tiles should exist and thus be able to be opened
    soil_full_extent_src = rasterio.open(soil_full_extent)
    AGC_emis_year_src = rasterio.open(AGC_emis_year)

    # Grabs metadata for one of the input tiles, like its location/projection/cellsize
    kwargs = AGC_emis_year_src.meta
    # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
    windows = AGC_emis_year_src.block_windows(1)

    # Updates kwargs for the output dataset.
    # Need to update data type to float 32 so that it can handle fractional carbon pools
    kwargs.update(
        driver='GTiff',
        count=1,
        compress='lzw',
        nodata=0,
        dtype='uint16'
    )

    # The output file: belowground carbon denity in the year of tree cover loss for pixels with tree cover loss
    dst_soil_emis_year = rasterio.open(soil_emis_year, 'w', **kwargs)

    print("  Creating soil carbon density for loss pixels in {}...".format(tile_id))

    # Iterates across the windows (1 pixel strips) of the input tiles
    for idx, window in windows:

        # Reads in the windows of each input file that definitely exist
        AGC_emis_year_window = AGC_emis_year_src.read(1, window=window)
        soil_full_extent_window = soil_full_extent_src.read(1, window=window)

        # Removes AGC pixels that do not have a loss year and fills with 0s
        soil_output = np.ma.masked_where(AGC_emis_year_window == 0, soil_full_extent_window)
        soil_output = soil_output.filled(0)

        # Converts the output to float32 since float64 is an unnecessary level of precision
        soil_output = soil_output.astype('uint16')

        # Writes the output window to the output file
        dst_soil_emis_year.write_band(1, soil_output, window=window)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, pattern)


# Creates total carbon tiles (both in 2000 and loss year)
def create_total_C(tile_id, carbon_pool_extent, pattern, sensit_type):

    start = datetime.datetime.now()

    # Names of the input tiles. Creates the names even if the files don't exist.
    # The AGC name depends on whether carbon in 2000 or in the emission year is being created.
    # If litter in the loss year is being created, it uses the loss year AGC tile.
    # If litter in 2000 is being created, is uses the 2000 AGC tile.
    # The other inputs tiles aren't affected by whether the output is for 2000 or for the loss year.
    if carbon_pool_extent == "loss":
        AGC = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_AGC_emis_year)
        BGC = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_BGC_emis_year)
        deadwood = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_deadwood_emis_year_2000)
        litter = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_litter_emis_year_2000)
        soil = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_soil_C_emis_year_2000)
    if carbon_pool_extent == "2000":
        AGC = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_AGC_2000)
        BGC = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_BGC_2000)
        deadwood = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_deadwood_2000)
        litter = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_litter_2000)
        soil = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_soil_C_full_extent_2000)

    # Name of output tile
    # The output name depends on whether carbon in 2000 or in the emission year is being created.
    total_C = '{0}_{1}.tif'.format(tile_id, pattern)

    print("  Reading input files for {}...".format(tile_id))

    # All of these tiles should exist and thus be able to be opened
    AGC_src = rasterio.open(AGC)
    BGC_src = rasterio.open(BGC)
    deadwood_src = rasterio.open(deadwood)
    litter_src = rasterio.open(litter)
    soil_src = rasterio.open(soil)

    # Grabs metadata for one of the input tiles, like its location/projection/cellsize
    kwargs = AGC_src.meta
    # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
    windows = AGC_src.block_windows(1)

    # Updates kwargs for the output dataset.
    # Need to update data type to float 32 so that it can handle fractional carbon pools
    kwargs.update(
        driver='GTiff',
        count=1,
        compress='lzw',
        nodata=0,
        dtype='float32'
    )

    if carbon_pool_extent == "2000":
        kwargs.update(
            bigtiff='YES'
        )

    # The output file: total carbon density
    dst_total_C = rasterio.open(total_C, 'w', **kwargs)

    print("  Creating total carbon density for {0} using carbon_pool_extent '{1}'...".format(tile_id, carbon_pool_extent))

    # Iterates across the windows (1 pixel strips) of the input tiles
    for idx, window in windows:

        # Reads in the windows of each input file that definitely exist
        AGC_window = AGC_src.read(1, window=window)
        BGC_window = BGC_src.read(1, window=window)
        deadwood_window = deadwood_src.read(1, window=window)
        litter_window = litter_src.read(1, window=window)
        soil_window = soil_src.read(1, window=window)

        total_C_output = AGC_window + BGC_window + deadwood_window + litter_window + soil_window

        # Converts the output to float32 since float64 is an unnecessary level of precision
        total_C_output = total_C_output.astype('float32')

        # Writes the output window to the output file
        dst_total_C.write_band(1, total_C_output, window=window)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, pattern)
