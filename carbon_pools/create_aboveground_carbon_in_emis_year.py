'''
This script create tiles of the aboveground carbon density in the year in which tree cover loss occurred
using mangrove and non-mangrove (WHRC) aboveground biomass density in 2000 and carbon gain from 2000 until the loss year.
Unlike the AGC in 2000, it outputs values only where there is loss, and the values are carbon in 2000 + gain until loss.
Thus, loss pixels that don't also have gain pixels have all of their carbon accumulation from after 2000 emitted because
all of the carbon accumuluation is assumed to come before the loss happens.
However, pixels that have both loss and gain only emit the portion of the carbon accumulation that occurs before loss.
Therefore, loss+gain pixels only have part of their gross carbon accumulation added to AGC 2000 for all forest types.
This is used for the gross emissions model.
'''

import datetime
import sys
import os
import numpy as np
import rasterio
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def create_emitted_AGC(tile_id, pattern, sensit_type):

    # Only proceeds with running the function if there is a loss tile. Without a loss tile, there will be no output, so there's
    # no reason to run the function.
    if os.path.exists('{}.tif'.format(tile_id)):
        print "Loss tile found for {}. Processing...".format(tile_id)
    else:
        print "No loss tile for {}. Not processing.".format(tile_id)
        return

    # Start time
    start = datetime.datetime.now()

    # Names of the input tiles. Creates the names even if the files don't exist.
    mangrove_biomass_2000 = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_mangrove_biomass_2000, 'false')
    natrl_forest_biomass_2000 = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_WHRC_biomass_2000_unmasked, 'false')
    mangrove_cumul_AGCO2_gain = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_cumul_gain_AGCO2_mangrove, 'true')
    planted_forest_cumul_AGCO2_gain = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_cumul_gain_AGCO2_planted_forest_non_mangrove, 'true')
    natrl_forest_cumul_AGCO2_gain = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_cumul_gain_AGCO2_natrl_forest, 'true')
    mangrove_annual_gain = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_annual_gain_AGB_mangrove, 'true')
    planted_forest_annual_gain = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_annual_gain_AGB_planted_forest_non_mangrove, 'true')
    natrl_forest_annual_gain = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_annual_gain_AGB_natrl_forest, 'true')
    loss_year = uu.sensit_tile_rename(sensit_type, tile_id, '', 'false')
    gain = uu.sensit_tile_rename(sensit_type, cn.pattern_gain, tile_id, 'false')

    # Name of output tile
    all_forests_AGC_emis_year = '{0}_{1}.tif'.format(tile_id, pattern)

    print "  Reading input files for {}...".format(tile_id)

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
        print "Mangrove tile found for", tile_id
    except:
        print "No mangrove tile for", tile_id

    try:
        planted_forest_cumul_AGCO2_gain_src = rasterio.open(planted_forest_cumul_AGCO2_gain)
        planted_forest_annual_gain_src = rasterio.open(planted_forest_annual_gain)
        print "Non-mangrove planted carbon accumulation found for", tile_id
    except:
        print "No non-mangrove planted carbon accumulation for", tile_id

    try:
        natrl_forest_biomass_2000_src = rasterio.open(natrl_forest_biomass_2000)
        natrl_forest_cumul_AGCO2_gain_src = rasterio.open(natrl_forest_cumul_AGCO2_gain)
        natrl_forest_annual_gain_src = rasterio.open(natrl_forest_annual_gain)
        # Grabs metadata for one of the input tiles, like its location/projection/cellsize
        kwargs = natrl_forest_biomass_2000_src.meta
        # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
        windows = natrl_forest_biomass_2000_src.block_windows(1)
        print "Natural forest found for", tile_id
    except:
        print "No natural forest found for", tile_id

    try:
        gain_src = rasterio.open(gain)
    except:
        print "No gain tile found for", tile_id

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

    print "  Creating aboveground carbon density in the year of loss for {}...".format(tile_id)

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
                                             + (gain_before_loss / cn.c_to_co2)
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
                                             + (gain_before_loss / cn.c_to_co2)
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
            # Calculates AGC in emission year for natural forest pixels that don't have gain and loss (excludes loss_gain_mask = 1).
            # To do this, it adds all the accumulated carbon after 2000 to the carbon in 2000.
            natural_forest_non_gain_and_loss = (natrl_forest_biomass_2000_window * cn.biomass_to_c_non_mangrove) \
                                                 + (natrl_forest_cumul_AGCO2_gain_window / cn.c_to_co2)
            natural_forest_non_gain_and_loss_masked = np.ma.masked_where(loss_gain_mask == 1, natural_forest_non_gain_and_loss).filled(0)

            # Calculates AGC in emission year for natural forest pixels that had loss & gain (excludes loss_gain_mask = 0).
            # To do this, it adds only the portion of the gain that occurred before the loss year to the carbon in 2000.
            gain_before_loss = natrl_forest_annual_gain_window * (loss_year_window - 1)
            natural_forest_gain_and_loss = (natrl_forest_biomass_2000_window * cn.biomass_to_c_non_mangrove) \
                                             + (gain_before_loss / cn.c_to_co2)
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