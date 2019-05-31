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
    type_ratio_dict_final = {int(k): float(v) for k, v in type_ratio_dict.items()}

    # Applies the x_pool:aboveground biomass ratios for the three mangrove types to the annual aboveground gain rates to
    # create a column of x_pool:AGB
    gain_table_simplified['x_pool_AGB_ratio'] = gain_table_simplified['mangType'].map(type_ratio_dict_final)

    # Converts the continent-ecozone codes and corresponding BGB:AGB to a dictionary
    mang_x_pool_AGB_ratio = pd.Series(gain_table_simplified.x_pool_AGB_ratio.values,
                                   index=gain_table_simplified.gainEcoCon).to_dict()

    # Adds a dictionary entry for where the ecozone-continent code is 0 (not in a continent)
    mang_x_pool_AGB_ratio[0] = 0

    # Converts all the keys (continent-ecozone codes) to float type. Important for matching the raster's values.
    mang_x_pool_AGB_ratio = {float(key): value for key, value in mang_x_pool_AGB_ratio.iteritems()}

    return mang_x_pool_AGB_ratio

def create_BGC(tile_id, mang_BGB_AGB_ratio, extent):

    start = datetime.datetime.now()

    # Names of the input tiles. Creates the names even if the files don't exist.
    # The AGC name depends on whether carbon in 2000 or in the emission year is being created.
    # If BGC in the loss year is being created, it uses the loss year AGC tile.
    # If BGC in 2000 is being created, is uses the 2000 AGC tile.
    # The other inputs tiles aren't affected by whether the output is for 2000 or for the loss year.
    mangrove_biomass_2000 = '{0}_{1}.tif'.format(tile_id, cn.pattern_mangrove_biomass_2000)
    cont_ecozone = '{0}_{1}.tif'.format(tile_id, cn.pattern_cont_eco_processed)
    if extent == "loss":
        AGC = '{0}_{1}.tif'.format(tile_id, cn.pattern_AGC_emis_year)
    if extent == "full":
        AGC = '{0}_{1}.tif'.format(tile_id, cn.pattern_AGC_2000)

    # Name of output tile
    # The BGC name depends on whether carbon in 2000 or in the emission year is being created.
    if extent == "loss":
        BGC = '{0}_{1}.tif'.format(tile_id, cn.pattern_BGC_emis_year)
        pattern_BGC = cn.pattern_BGC_emis_year
    if extent == "full":
        BGC = '{0}_{1}.tif'.format(tile_id, cn.pattern_BGC_2000)
        pattern_BGC = cn.pattern_BGC_2000

    print "  Reading input files for {}...".format(tile_id)

    # Opens the mangrove biomass tile if it exists
    try:
        mangrove_biomass_2000_src = rasterio.open(mangrove_biomass_2000)
        print "Mangrove biomass found for", tile_id
    except:
        print "No mangrove biomass for", tile_id

    # Both of these tiles should exist and thus be able to be opened
    AGC_src = rasterio.open(AGC)
    cont_ecozone_src = rasterio.open(cont_ecozone)

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

    # The output file: belowground carbon denity in the year of tree cover loss for pixels with tree cover loss
    dst_BGC = rasterio.open(BGC, 'w', **kwargs)

    print "  Creating belowground carbon density in the year of loss for {}...".format(tile_id)

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
            for key, value in mang_BGB_AGB_ratio.iteritems():
                cont_ecozone_window[cont_ecozone_window == key] = value
            # print fao_ecozone_window[0][30020:30035]

            # Multiplies the AGC in the loss year by the correct mangrove BGB:AGB ratio to get an array of BGC in the loss year
            mangrove_C_final = AGC_window * cont_ecozone_window
            # print mangrove_C_final[0][30020:30035]

            # Masks out non-mangrove pixels and fills the masked values with 0s
            mangrove_C_final = np.ma.masked_where(mangrove_biomass_2000_window == 0, mangrove_C_final)
            mangrove_C_final = mangrove_C_final.filled(0)
            # print mangrove_C_final[0][30020:30035]

            # Applies the non-mangrove BGB:AGB ratio to all AGC in emissions year pixels
            non_mang_output = AGC_window * cn.below_to_above_non_mang
            # print non_mang_output[0][29930:29950]

            # Masks out mangrove pixels so that only non-mangrove pixels use the non-mangrove BGB:AGB ratio
            non_mang_output_final = np.ma.masked_where(mangrove_biomass_2000_window != 0, non_mang_output)
            # print non_mang_output_final[0][29930:29950]

            # Combines the mangrove and non-mangrove BGC arrays into a single array
            BGC_output = mangrove_C_final + non_mang_output_final
            # print BGC_output[0][29930:29950]

        # If there is no mangrove tile, all AGC in emissions year pixels are multiplied by the non-mangrove
        # BGB:AGB ratio
        if not os.path.exists(mangrove_biomass_2000):

            BGC_output = AGC_window * cn.below_to_above_non_mang
            # print BGC_output[0][29930:29950]

        # Writes the output window to the output file
        dst_BGC.write_band(1, BGC_output, window=window)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, pattern_BGC)

def create_deadwood(tile_id, mang_deadwood_AGB_ratio, extent):

    start = datetime.datetime.now()

    # Names of the input tiles. Creates the names even if the files don't exist.
    # The AGC name depends on whether carbon in 2000 or in the emission year is being created.
    # If deadwood in the loss year is being created, it uses the loss year AGC tile.
    # If deadwood in 2000 is being created, is uses the 2000 AGC tile.
    # The other inputs tiles aren't affected by whether the output is for 2000 or for the loss year.
    mangrove_biomass_2000 = '{0}_{1}.tif'.format(tile_id, cn.pattern_mangrove_biomass_2000)
    WHRC_biomass_2000 = '{0}_{1}.tif'.format(tile_id, cn.pattern_WHRC_biomass_2000_unmasked)
    bor_tem_trop = '{0}_{1}.tif'.format(tile_id, cn.pattern_bor_tem_trop_processed)
    cont_eco = '{0}_{1}.tif'.format(tile_id, cn.pattern_cont_eco_processed)
    precip = '{0}_{1}.tif'.format(tile_id, cn.pattern_precip)
    elevation = '{0}_{1}.tif'.format(tile_id, cn.pattern_elevation)
    if extent == "loss":
        AGC = '{0}_{1}.tif'.format(tile_id, cn.pattern_AGC_emis_year)
    if extent == "full":
        AGC = '{0}_{1}.tif'.format(tile_id, cn.pattern_AGC_2000)

    # Name of output tile
    # The output name depends on whether carbon in 2000 or in the emission year is being created.
    if extent == "loss":
        deadwood = '{0}_{1}.tif'.format(tile_id, cn.pattern_deadwood_emis_year_2000)
        pattern_deadwood = cn.pattern_deadwood_emis_year_2000
    if extent == "full":
        deadwood = '{0}_{1}.tif'.format(tile_id, cn.pattern_deadwood_2000)
        pattern_deadwood = cn.pattern_deadwood_2000

    print "  Reading input files for {}...".format(tile_id)

    # Opens the mangrove biomass tile if it exists
    try:
        mangrove_biomass_2000_src = rasterio.open(mangrove_biomass_2000)
        print "Mangrove biomass found for", tile_id
    except:
        print "No mangrove biomass for", tile_id

    # Opens the WHRC biomass tile if it exists
    try:
        WHRC_biomass_2000_src = rasterio.open(WHRC_biomass_2000)
        print "WHRC biomass found for", tile_id
    except:
        print "No WHRC biomass for", tile_id

    # These tiles should exist and thus be able to be opened
    AGC_src = rasterio.open(AGC)
    bor_tem_trop_src = rasterio.open(bor_tem_trop)
    cont_ecozone_src = rasterio.open(cont_eco)
    precip_src = rasterio.open(precip)
    elevation_src = rasterio.open(elevation)

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

    # The output file: belowground carbon density in the year of tree cover loss for pixels with tree cover loss
    dst_deadwood = rasterio.open(deadwood, 'w', **kwargs)

    print "  Creating deadwood carbon density in the year of loss for {}...".format(tile_id)

    # Iterates across the windows (1 pixel strips) of the input tiles
    for idx, window in windows:

        # Populates the output raster's windows with 0s so that pixels without
        # any of the forest types will have 0s
        deadwood_output = np.zeros((window.height, window.width), dtype='float32')

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
        if os.path.exists(WHRC_biomass_2000):

            # Reads in the windows of each input file that definitely exist
            WHRC_biomass_window = WHRC_biomass_2000_src.read(1, window=window)

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
            agb_masked_1 = np.ma.array(WHRC_biomass_window, mask=np.invert(condition_mask_1))
            deadwood_masked = agb_masked_1 * 0.02 * cn.biomass_to_c_non_mangrove
            deadwood_output = deadwood_output + deadwood_masked.filled(0)

            # Equation for elevation <= 2000, 1000 < precip <= 1600, bor/temp/trop = 1 (tropical)
            elev_mask_2 = elevation_window <= 2000
            precip_mask_2 = (precip_window > 1000) & (precip_window <= 1600)
            ecozone_mask_2 = bor_tem_trop_window == 1
            condition_mask_2 = elev_mask_2 & precip_mask_2 & ecozone_mask_2
            agb_masked_2 = np.ma.array(WHRC_biomass_window, mask=np.invert(condition_mask_2))
            deadwood_masked = agb_masked_2 * 0.01 * cn.biomass_to_c_non_mangrove
            deadwood_output = deadwood_output + deadwood_masked.filled(0)

            # Equation for elevation <= 2000, precip > 1600, bor/temp/trop = 1 (tropical)
            elev_mask_3 = elevation_window <= 2000
            precip_mask_3 = precip_window > 1600
            ecozone_mask_3 = bor_tem_trop_window == 1
            condition_mask_3 = elev_mask_3 & precip_mask_3 & ecozone_mask_3
            agb_masked_3 = np.ma.array(WHRC_biomass_window, mask=np.invert(condition_mask_3))
            deadwood_masked = agb_masked_3 * 0.06 * cn.biomass_to_c_non_mangrove
            deadwood_output = deadwood_output + deadwood_masked.filled(0)

            # Equation for elevation > 2000, precip = any value, bor/temp/trop = 1 (tropical)
            elev_mask_4 = elevation_window > 2000
            ecozone_mask_4 = bor_tem_trop_window == 1
            condition_mask_4 = elev_mask_4 & ecozone_mask_4
            agb_masked_4 = np.ma.array(WHRC_biomass_window, mask=np.invert(condition_mask_4))
            deadwood_masked = agb_masked_4 * 0.07 * cn.biomass_to_c_non_mangrove
            deadwood_output = deadwood_output + deadwood_masked.filled(0)

            # Equation for elevation = any value, precip = any value, bor/temp/trop = 2 or 3 (boreal or temperate)
            ecozone_mask_5 = bor_tem_trop_window != 1
            condition_mask_5 = ecozone_mask_5
            agb_masked_5 = np.ma.array(WHRC_biomass_window, mask=np.invert(condition_mask_5))
            deadwood_masked = agb_masked_5 * 0.08 * cn.biomass_to_c_non_mangrove
            deadwood_output = deadwood_output + deadwood_masked.filled(0)

        # Replaces non-mangrove deadwood with special mangrove deadwood values if there is mangrove
        if os.path.exists(mangrove_biomass_2000):

            # Reads in the window for mangrove biomass if it exists
            mangrove_biomass_2000_window = mangrove_biomass_2000_src.read(1, window=window)

            # Applies the mangrove deadwood:AGB ratios (2 different ratios) to the ecozone raster to create a raster of deadwood:AGB ratios
            for key, value in mang_deadwood_AGB_ratio.iteritems():
                cont_ecozone_window[cont_ecozone_window == key] = value

            # Multiplies the AGB in the loss year (2000 for deadwood) by the correct mangrove deadwood:AGB ratio to get an array of deadwood in the loss year (2000)
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
    uu.end_of_fx_summary(start, tile_id, pattern_deadwood)

def create_litter(tile_id, mang_litter_AGB_ratio, extent):

    start = datetime.datetime.now()

    # Names of the input tiles. Creates the names even if the files don't exist.
    # The AGC name depends on whether carbon in 2000 or in the emission year is being created.
    # If litter in the loss year is being created, it uses the loss year AGC tile.
    # If litter in 2000 is being created, is uses the 2000 AGC tile.
    # The other inputs tiles aren't affected by whether the output is for 2000 or for the loss year.
    mangrove_biomass_2000 = '{0}_{1}.tif'.format(tile_id, cn.pattern_mangrove_biomass_2000)
    WHRC_biomass_2000 = '{0}_{1}.tif'.format(tile_id, cn.pattern_WHRC_biomass_2000_unmasked)
    bor_tem_trop = '{0}_{1}.tif'.format(tile_id, cn.pattern_bor_tem_trop_processed)
    cont_eco = '{0}_{1}.tif'.format(tile_id, cn.pattern_cont_eco_processed)
    precip = '{0}_{1}.tif'.format(tile_id, cn.pattern_precip)
    elevation = '{0}_{1}.tif'.format(tile_id, cn.pattern_elevation)
    if extent == "loss":
        AGC = '{0}_{1}.tif'.format(tile_id, cn.pattern_AGC_emis_year)
    if extent == "full":
        AGC = '{0}_{1}.tif'.format(tile_id, cn.pattern_AGC_2000)

    # Name of output tile
    # The output name depends on whether carbon in 2000 or in the emission year is being created.
    if extent == "loss":
        litter = '{0}_{1}.tif'.format(tile_id, cn.pattern_litter_emis_year_2000)
        pattern_litter = cn.pattern_litter_emis_year_2000
    if extent == "full":
        litter = '{0}_{1}.tif'.format(tile_id, cn.pattern_litter_2000)
        pattern_litter = cn.pattern_litter_2000

    print "  Reading input files for {}...".format(tile_id)

    # Opens the mangrove biomass tile if it exists
    try:
        mangrove_biomass_2000_src = rasterio.open(mangrove_biomass_2000)
        print "Mangrove biomass found for", tile_id
    except:
        print "No mangrove biomass for", tile_id

    # Opens the WHRC biomass tile if it exists
    try:
        WHRC_biomass_2000_src = rasterio.open(WHRC_biomass_2000)
        print "WHRC biomass found for", tile_id
    except:
        print "No WHRC biomass for", tile_id

    # These tiles should exist and thus be able to be opened
    AGC_src = rasterio.open(AGC)
    bor_tem_trop_src = rasterio.open(bor_tem_trop)
    cont_ecozone_src = rasterio.open(cont_eco)
    precip_src = rasterio.open(precip)
    elevation_src = rasterio.open(elevation)

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

    # The output file: belowground carbon density in the year of tree cover loss for pixels with tree cover loss
    dst_litter = rasterio.open(litter, 'w', **kwargs)

    print "  Creating litter carbon density in the year of loss for {}...".format(tile_id)

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
        if os.path.exists(WHRC_biomass_2000):

            # Reads in the windows of each input file that definitely exist
            WHRC_biomass_window = WHRC_biomass_2000_src.read(1, window=window)

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
            agb_masked_1 = np.ma.array(WHRC_biomass_window, mask=np.invert(condition_mask_1))
            litter_masked = agb_masked_1 * 0.04 * cn.biomass_to_c_non_mangrove_litter
            litter_output = litter_output + litter_masked.filled(0)

            # Equation for elevation <= 2000, 1000 < precip <= 1600, bor/temp/trop = 1 (tropical)
            elev_mask_2 = elevation_window <= 2000
            precip_mask_2 = (precip_window > 1000) & (precip_window <= 1600)
            ecozone_mask_2 = bor_tem_trop_window == 1
            condition_mask_2 = elev_mask_2 & precip_mask_2 & ecozone_mask_2
            agb_masked_2 = np.ma.array(WHRC_biomass_window, mask=np.invert(condition_mask_2))
            litter_masked = agb_masked_2 * 0.01 * cn.biomass_to_c_non_mangrove_litter
            litter_output = litter_output + litter_masked.filled(0)

            # Equation for elevation <= 2000, precip > 1600, bor/temp/trop = 1 (tropical)
            elev_mask_3 = elevation_window <= 2000
            precip_mask_3 = precip_window > 1600
            ecozone_mask_3 = bor_tem_trop_window == 1
            condition_mask_3 = elev_mask_3 & precip_mask_3 & ecozone_mask_3
            agb_masked_3 = np.ma.array(WHRC_biomass_window, mask=np.invert(condition_mask_3))
            litter_masked = agb_masked_3 * 0.01 * cn.biomass_to_c_non_mangrove_litter
            litter_output = litter_output + litter_masked.filled(0)

            # Equation for elevation > 2000, precip = any value, bor/temp/trop = 1 (tropical)
            elev_mask_4 = elevation_window > 2000
            ecozone_mask_4 = bor_tem_trop_window == 1
            condition_mask_4 = elev_mask_4 & ecozone_mask_4
            agb_masked_4 = np.ma.array(WHRC_biomass_window, mask=np.invert(condition_mask_4))
            litter_masked = agb_masked_4 * 0.01 * cn.biomass_to_c_non_mangrove_litter
            litter_output = litter_output + litter_masked.filled(0)

            # Equation for elevation = any value, precip = any value, bor/temp/trop = 2 or 3 (boreal or temperate)
            ecozone_mask_5 = bor_tem_trop_window != 1
            condition_mask_5 = ecozone_mask_5
            agb_masked_5 = np.ma.array(WHRC_biomass_window, mask=np.invert(condition_mask_5))
            litter_masked = agb_masked_5 * 0.04 * cn.biomass_to_c_non_mangrove_litter
            litter_output = litter_output + litter_masked.filled(0)

        # Replaces non-mangrove litter with special mangrove litter values if there is mangrove
        if os.path.exists(mangrove_biomass_2000):

            # Reads in the window for mangrove biomass if it exists
            mangrove_biomass_2000_window = mangrove_biomass_2000_src.read(1, window=window)

            # Applies the mangrove litter:AGB ratios (2 different ratios) to the ecozone raster to create a raster of litter:AGB ratios
            for key, value in mang_litter_AGB_ratio.iteritems():
                cont_ecozone_window[cont_ecozone_window == key] = value

            # Multiplies the AGB in the loss year (2000 for litter) by the correct mangrove litter:AGB ratio to get an array of litter in the loss year (2000)
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
    uu.end_of_fx_summary(start, tile_id, pattern_litter)


def create_soil(tile_id):

    start = datetime.datetime.now()

    # Names of the input tiles. Creates the names even if the files don't exist.
    soil_full_extent = '{0}_{1}.tif'.format(tile_id, cn.pattern_soil_C_full_extent_2000)
    AGC_emis_year = '{0}_{1}.tif'.format(tile_id, cn.pattern_AGC_emis_year)

    # Name of output tile
    soil_emis_year = '{0}_{1}.tif'.format(tile_id, cn.pattern_soil_C_emis_year_2000)

    print "  Reading input files for {}...".format(tile_id)

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

    print "  Creating soil carbon density in the year of loss for loss pixels in {}...".format(tile_id)

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
    uu.end_of_fx_summary(start, tile_id, cn.pattern_BGC_emis_year)

def create_total_C(tile_id):

    start = datetime.datetime.now()

    # Names of the input tiles. Creates the names even if the files don't exist.
    AGC = '{0}_{1}.tif'.format(tile_id, cn.pattern_AGC_emis_year)
    BGC = '{0}_{1}.tif'.format(tile_id, cn.pattern_BGC_emis_year)
    deadwood = '{0}_{1}.tif'.format(tile_id, cn.pattern_deadwood_emis_year_2000)
    litter = '{0}_{1}.tif'.format(tile_id, cn.pattern_litter_emis_year_2000)
    soil = '{0}_{1}.tif'.format(tile_id, cn.pattern_soil_C_emis_year_2000)

    # Name of output tile
    total_C_emis_year = '{0}_{1}.tif'.format(tile_id, cn.pattern_total_C_emis_year)

    print "  Reading input files for {}...".format(tile_id)

    # Both of these tiles should exist and thus be able to be opened
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

    # The output file: belowground carbon denity in the year of tree cover loss for pixels with tree cover loss
    dst_total_C = rasterio.open(total_C_emis_year, 'w', **kwargs)

    print "  Creating total carbon density in the year of loss for loss pixels in {}...".format(tile_id)

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
    uu.end_of_fx_summary(start, tile_id, cn.pattern_total_C_emis_year)
