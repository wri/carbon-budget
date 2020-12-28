import datetime
import sys
import pandas as pd
import os
import numpy as np
import rasterio
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu


# Creates a dictionary of biomass in belowground, deadwood, and litter emitted_pools to aboveground biomass pool
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


# Creates aboveground carbon emitted_pools in 2000 and/or the year of loss (loss pixels only)
def create_AGC(tile_id, sensit_type, carbon_pool_extent):

    # Start time
    start = datetime.datetime.now()

    # Names of the input tiles. Creates the names even if the files don't exist.
    removal_forest_type = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_removal_forest_type)
    mangrove_biomass_2000 = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_mangrove_biomass_2000)
    gain = uu.sensit_tile_rename(sensit_type, cn.pattern_gain, tile_id)
    annual_gain_AGC = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_annual_gain_AGC_all_types)
    cumul_gain_AGCO2 = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_cumul_gain_AGCO2_all_types)

    # Biomass tile name depends on the sensitivity analysis
    if sensit_type == 'biomass_swap':
        natrl_forest_biomass_2000 = '{0}_{1}.tif'.format(tile_id, cn.pattern_JPL_unmasked_processed)
        uu.print_log("Using JPL biomass tile for {} sensitivity analysis".format(sensit_type))
    else:
        natrl_forest_biomass_2000 = '{0}_{1}.tif'.format(tile_id, cn.pattern_WHRC_biomass_2000_unmasked)
        uu.print_log("Using WHRC biomass tile for {} sensitivity analysis".format(sensit_type))

    uu.print_log("  Reading input files for {}...".format(tile_id))

    # Loss tile name depends on the sensitivity analysis
    if sensit_type == 'legal_Amazon_loss':
        uu.print_log("    Brazil-specific loss tile found for {}".format(tile_id))
        loss_year = '{}_{}.tif'.format(tile_id, cn.pattern_Brazil_annual_loss_processed)
    elif os.path.exists('{}_{}.tif'.format(tile_id, cn.pattern_Mekong_loss_processed)):
        uu.print_log("    Mekong-specific loss tile found for {}".format(tile_id))
        loss_year = '{}_{}.tif'.format(tile_id, cn.pattern_Mekong_loss_processed)
    else:
        uu.print_log("    Hansen loss tile found for {}".format(tile_id))
        loss_year = '{0}_{1}.tif'.format(cn.pattern_loss, tile_id)

    # This input should exist
    removal_forest_type_src = rasterio.open(removal_forest_type)

    # Opens the input tiles if they exist
    try:
        annual_gain_AGC_src = rasterio.open(annual_gain_AGC)
        uu.print_log("    Aboveground removal factor tile found for", tile_id)
    except:
        uu.print_log("    No aboveground removal factor tile for", tile_id)

    try:
        cumul_gain_AGCO2_src = rasterio.open(cumul_gain_AGCO2)
        uu.print_log("    Gross aboveground removal tile found for", tile_id)
    except:
        uu.print_log("    No gross aboveground removal tile for", tile_id)

    try:
        mangrove_biomass_2000_src = rasterio.open(mangrove_biomass_2000)
        uu.print_log("    Mangrove tile found for", tile_id)
    except:
        uu.print_log("    No mangrove tile for", tile_id)

    try:
        natrl_forest_biomass_2000_src = rasterio.open(natrl_forest_biomass_2000)
        uu.print_log("    Biomass found for", tile_id)
    except:
        uu.print_log("    No biomass found for", tile_id)

    try:
        gain_src = rasterio.open(gain)
        uu.print_log("    Gain tile found for", tile_id)
    except:
        uu.print_log("    No gain tile found for", tile_id)

    try:
        loss_year_src = rasterio.open(loss_year)
        uu.print_log("    Loss tile found for", tile_id)
    except:
        uu.print_log("    No loss tile found for", tile_id)


    # Grabs the windows of a tile to iterate over the entire tif without running out of memory
    windows = removal_forest_type_src.block_windows(1)

    # Grabs metadata for one of the input tiles, like its location/projection/cellsize
    kwargs = removal_forest_type_src.meta

    # Updates kwargs for the output dataset.
    # Need to update data type to float 32 so that it can handle fractional carbon
    kwargs.update(
        driver='GTiff',
        count=1,
        compress='lzw',
        nodata=0,
        dtype='float32'
    )


    # The output files: aboveground carbon density in 2000 and in the year of loss. Creates names and rasters to write to.
    if '2000' in carbon_pool_extent:
        output_pattern_list = [cn.pattern_AGC_2000]
        if sensit_type != 'std':
            output_pattern_list = uu.alter_patterns(sensit_type, output_pattern_list)
        AGC_2000 = '{0}_{1}.tif'.format(tile_id, output_pattern_list[0])
        dst_AGC_2000 = rasterio.open(AGC_2000, 'w', **kwargs)
        # Adds metadata tags to the output raster
        uu.add_rasterio_tags(dst_AGC_2000, sensit_type)
        dst_AGC_2000.update_tags(
            units='megagrams aboveground carbon (AGC)/ha')
        dst_AGC_2000.update_tags(
            source='WHRC (if standard model) or JPL (if biomass swap sensitivity analysis) and mangrove AGB (Simard et al. 2018)')
        dst_AGC_2000.update_tags(
            extent='aboveground biomass in 2000 (WHRC if standard model, JPL if biomass_swap sensitivity analysis) and mangrove AGB. Mangrove AGB has precedence.')
    if 'loss' in carbon_pool_extent:
        output_pattern_list = [cn.pattern_AGC_emis_year]
        if sensit_type != 'std':
            output_pattern_list = uu.alter_patterns(sensit_type, output_pattern_list)
        AGC_emis_year = '{0}_{1}.tif'.format(tile_id, output_pattern_list[0])
        dst_AGC_emis_year = rasterio.open(AGC_emis_year, 'w', **kwargs)
        # Adds metadata tags to the output raster
        uu.add_rasterio_tags(dst_AGC_emis_year, sensit_type)
        dst_AGC_emis_year.update_tags(
            units='megagrams aboveground carbon (AGC)/ha')
        dst_AGC_emis_year.update_tags(
            source='WHRC (if standard model) or JPL (if biomass_swap sensitivity analysis) and mangrove AGB (Simard et al. 2018). Gross removals added to AGC2000 to get AGC in loss year.')
        dst_AGC_emis_year.update_tags(
            extent='tree cover loss pixels within model extent')


    uu.print_log("  Creating aboveground carbon density for {0} using carbon_pool_extent '{1}'...".format(tile_id, carbon_pool_extent))

    # Iterates across the windows (1 pixel strips) of the input tiles
    for idx, window in windows:

        # Reads the input tiles' windows. For windows from tiles that may not exist, an array of all 0s is created.
        removal_forest_type_window = removal_forest_type_src.read(1, window=window)
        try:
            annual_gain_AGC_window = annual_gain_AGC_src.read(1, window=window)
        except:
            annual_gain_AGC_window = np.zeros((window.height, window.width), dtype='float32')
        try:
            cumul_gain_AGCO2_window = cumul_gain_AGCO2_src.read(1, window=window)
        except:
            cumul_gain_AGCO2_window = np.zeros((window.height, window.width), dtype='float32')
        try:
            loss_year_window = loss_year_src.read(1, window=window)
        except:
            loss_year_window = np.zeros((window.height, window.width), dtype='uint8')
        try:
            gain_window = gain_src.read(1, window=window)
        except:
            gain_window = np.zeros((window.height, window.width), dtype='uint8')
        try:
            mangrove_biomass_2000_window = mangrove_biomass_2000_src.read(1, window=window)
        except:
            mangrove_biomass_2000_window = np.zeros((window.height, window.width), dtype='uint8')
        try:
            natrl_forest_biomass_2000_window = natrl_forest_biomass_2000_src.read(1, window=window)
        except:
            natrl_forest_biomass_2000_window = np.zeros((window.height, window.width), dtype='uint8')


        # Creates aboveground carbon density in 2000. Where mangrove biomass is found, it is used. Otherwise, WHRC or JPL AGB is used.
        # This is necessary for calculating AGC in emissions year.
        agc_2000_window = np.where(mangrove_biomass_2000_window != 0,
                                   mangrove_biomass_2000_window * cn.biomass_to_c_mangrove,
                                   natrl_forest_biomass_2000_window * cn.biomass_to_c_non_mangrove
                                   ).astype('float32')

        # Only writes AGC2000 window to raster if user asked for carbon emitted_pools in 2000
        if '2000' in carbon_pool_extent:
            dst_AGC_2000.write_band(1, agc_2000_window, window=window)


        # From here on, AGC in the year of emissions is being calculated
        if 'loss' in carbon_pool_extent:

            # Limits the AGC to the model extent
            agc_2000_model_extent_window = np.where(removal_forest_type_window > 0, agc_2000_window, 0)
            # print(agc_2000_model_extent_window[0][0:5])

            # Creates a mask based on whether the pixels had loss and gain in them. Loss&gain pixels are 1, all else are 0.
            # This is used to determine how much post-2000 carbon gain to add to AGC2000 pixels.
            loss_gain_mask = np.ma.masked_where(loss_year_window == 0, gain_window).filled(0)

            # Loss pixels that also have gain pixels are treated differently from loss-only pixels.
            # Calculates AGC in emission year for pixels that don't have gain and loss (excludes loss_gain_mask = 1).
            # To do this, it adds all the accumulated carbon after 2000 to the carbon in 2000 (all accumulated C is emitted).
            AGC_emis_year_non_loss_and_gain = agc_2000_model_extent_window + (cumul_gain_AGCO2_window / cn.c_to_co2)
            # print(AGC_emis_year_non_loss_and_gain[0][0:5])
            AGC_emis_year_non_loss_and_gain_masked = np.ma.masked_where(loss_gain_mask == 1, AGC_emis_year_non_loss_and_gain).filled(0)
            # print(AGC_emis_year_non_loss_and_gain_masked[0][0:5])


            # Calculates AGC in emission year for pixels that had loss & gain (excludes loss_gain_mask = 0).
            # To do this, it adds only the portion of the gain that occurred before the loss year to the carbon in 2000.
            gain_before_loss = annual_gain_AGC_window * (loss_year_window - 1)
            AGC_emis_year_loss_and_gain = agc_2000_model_extent_window + gain_before_loss
            AGC_emis_year_loss_and_gain_masked = np.ma.masked_where(loss_gain_mask == 0, AGC_emis_year_loss_and_gain).filled(0)
            # print(AGC_emis_year_loss_and_gain_masked[0][0:5])

            # Adds the loss year pixels that had loss&gain to those that didn't have loss&gain.
            # Each pixel falls into only one of those categories.
            AGC_emis_year_all = AGC_emis_year_non_loss_and_gain_masked + AGC_emis_year_loss_and_gain_masked
            # print(AGC_emis_year_all[0][0:5])

            # Limits output to only pixels that had tree cover loss.
            AGC_emis_year_all = np.where(loss_year_window > 0, AGC_emis_year_all, 0)
            # print(AGC_emis_year_all[0][0:5])

            # Converts the output to float32 since float64 is an unnecessary level of precision
            AGC_emis_year_all = AGC_emis_year_all.astype('float32')
            # print(AGC_emis_year_all[0][0:5])

            # Writes AGC in emissions year to raster
            dst_AGC_emis_year.write_band(1, AGC_emis_year_all, window=window)



    # Prints information about the tile that was just processed
    if 'loss' in carbon_pool_extent:
        uu.end_of_fx_summary(start, tile_id, cn.pattern_AGC_emis_year)
    else:
        uu.end_of_fx_summary(start, tile_id, cn.pattern_AGC_2000)


# Creates belowground carbon tiles (both in 2000 and loss year)
def create_BGC(tile_id, mang_BGB_AGB_ratio, carbon_pool_extent, sensit_type):

    start = datetime.datetime.now()

    # Names of the input tiles
    removal_forest_type = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_removal_forest_type)
    cont_ecozone = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_cont_eco_processed)

    # For BGC 2000, opens AGC, names the output tile, creates the output tile
    if '2000' in carbon_pool_extent:
        AGC_2000 = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_AGC_2000)
        AGC_2000_src = rasterio.open(AGC_2000)
        kwargs = AGC_2000_src.meta
        kwargs.update(driver='GTiff', count=1, compress='lzw', nodata=0)
        windows = AGC_2000_src.block_windows(1)
        output_pattern_list = [cn.pattern_BGC_2000]
        if sensit_type != 'std':
            output_pattern_list = uu.alter_patterns(sensit_type, output_pattern_list)
        BGC_2000 = '{0}_{1}.tif'.format(tile_id, output_pattern_list[0])
        dst_BGC_2000 = rasterio.open(BGC_2000, 'w', **kwargs)
        # Adds metadata tags to the output raster
        uu.add_rasterio_tags(dst_BGC_2000, sensit_type)
        dst_BGC_2000.update_tags(
            units='megagrams belowground carbon (BGC)/ha')
        dst_BGC_2000.update_tags(
            source='WHRC (if standard model) or JPL (if biomass_swap sensitivity analysis) and mangrove AGB (Simard et al. 2018). AGC:BGC for mangrove and non-mangrove forests applied.')
        dst_BGC_2000.update_tags(
            extent='aboveground biomass in 2000 (WHRC if standard model, JPL if biomass_swap sensitivity analysis) and mangrove AGB. Mangrove AGB has precedence.')

    # For BGC in emissions year, opens AGC, names the output tile, creates the output tile
    if 'loss' in carbon_pool_extent:
        AGC_emis_year = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_AGC_emis_year)
        AGC_emis_year_src = rasterio.open(AGC_emis_year)
        kwargs = AGC_emis_year_src.meta
        kwargs.update(driver='GTiff', count=1, compress='lzw', nodata=0)
        windows = AGC_emis_year_src.block_windows(1)
        output_pattern_list = [cn.pattern_BGC_emis_year]
        if sensit_type != 'std':
            output_pattern_list = uu.alter_patterns(sensit_type, output_pattern_list)
        BGC_emis_year = '{0}_{1}.tif'.format(tile_id, output_pattern_list[0])
        dst_BGC_emis_year = rasterio.open(BGC_emis_year, 'w', **kwargs)
        # Adds metadata tags to the output raster
        uu.add_rasterio_tags(dst_BGC_emis_year, sensit_type)
        dst_BGC_emis_year.update_tags(
            units='megagrams belowground carbon (BGC)/ha')
        dst_BGC_emis_year.update_tags(
            source='WHRC (if standard model) or JPL (if biomass_swap sensitivity analysis) and mangrove AGB (Simard et al. 2018). Gross removals added to AGC2000 to get AGC in loss year. AGC:BGC for mangrove and non-mangrove forests applied.')
        dst_BGC_emis_year.update_tags(
            extent='tree cover loss pixels within model extent')


    uu.print_log("  Reading input files for {}...".format(tile_id))

    # Opens inputs that are used regardless of whether calculating BGC2000 or BGC in emissions year
    try:
        cont_ecozone_src = rasterio.open(cont_ecozone)
        uu.print_log("    Continent-ecozone tile found for", tile_id)
    except:
        uu.print_log("    No Continent-ecozone tile found for", tile_id)

    try:
        removal_forest_type_src = rasterio.open(removal_forest_type)
        uu.print_log("    Removal forest type tile found for", tile_id)
    except:
        uu.print_log("    No Removal forest type tile found for", tile_id)

    uu.print_log("  Creating belowground carbon density for {0} using carbon_pool_extent '{1}'...".format(tile_id, carbon_pool_extent))

    # Iterates across the windows (1 pixel strips) of the input tiles
    for idx, window in windows:

        # Creates windows from inputs that are used regardless of whether calculating BGC2000 or BGC in emissions year
        try:
            cont_ecozone_window = cont_ecozone_src.read(1, window=window).astype('float32')
        except:
            cont_ecozone_window = np.zeros((window.height, window.width), dtype='float32')

        try:
            removal_forest_type_window = removal_forest_type_src.read(1, window=window)
        except:
            removal_forest_type_window = np.zeros((window.height, window.width))

        # Applies the mangrove BGB:AGB ratios (3 different ratios) to the ecozone raster to create a raster of BGB:AGB ratios
        for key, value in mang_BGB_AGB_ratio.items():
            cont_ecozone_window[cont_ecozone_window == key] = value

        # Calculates BGC2000 from AGC2000
        if '2000' in carbon_pool_extent:
            AGC_2000_window = AGC_2000_src.read(1, window=window)

            # Applies mangrove-specific AGB:BGB ratios by ecozone (ratio applies to AGC:BGC as well)
            mangrove_BGC_2000 = np.where(removal_forest_type_window == cn.mangrove_rank, AGC_2000_window * cont_ecozone_window, 0)
            # Applies non-mangrove AGB:BGB ratio to all non-mangrove pixels
            non_mangrove_BGC_2000 = np.where(removal_forest_type_window != cn.mangrove_rank, AGC_2000_window * cn.below_to_above_non_mang, 0)
            # Combines mangrove and non-mangrove pixels
            BGC_2000_window = mangrove_BGC_2000 + non_mangrove_BGC_2000

            dst_BGC_2000.write_band(1, BGC_2000_window, window=window)

        # Calculates BGC in emissions year from AGC in emissions year
        if 'loss' in carbon_pool_extent:
            AGC_emis_year_window = AGC_emis_year_src.read(1, window=window)

            mangrove_BGC_emis_year = np.where(removal_forest_type_window == cn.mangrove_rank, AGC_emis_year_window * cont_ecozone_window, 0)
            non_mangrove_BGC_emis_year = np.where(removal_forest_type_window != cn.mangrove_rank, AGC_emis_year_window * cn.below_to_above_non_mang, 0)
            BGC_emis_year_window = mangrove_BGC_emis_year + non_mangrove_BGC_emis_year

            dst_BGC_emis_year.write_band(1, BGC_emis_year_window, window=window)


    # Prints information about the tile that was just processed
    if 'loss' in carbon_pool_extent:
        uu.end_of_fx_summary(start, tile_id, cn.pattern_BGC_emis_year)
    else:
        uu.end_of_fx_summary(start, tile_id, cn.pattern_BGC_2000)


# Creates deadwood and litter carbon tiles (in 2000 and/or in loss year)
def create_deadwood_litter(tile_id, mang_deadwood_AGB_ratio, mang_litter_AGB_ratio, carbon_pool_extent, sensit_type):

    start = datetime.datetime.now()

    # Names of the input tiles. Creates the names even if the files don't exist.
    mangrove_biomass_2000 = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_mangrove_biomass_2000)
    bor_tem_trop = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_bor_tem_trop_processed)
    cont_eco = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_cont_eco_processed)
    precip = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_precip)
    elevation = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_elevation)
    if sensit_type == 'biomass_swap':
        natrl_forest_biomass_2000 = '{0}_{1}.tif'.format(tile_id, cn.pattern_JPL_unmasked_processed)
        uu.print_log("Using JPL biomass tile for {} sensitivity analysis".format(sensit_type))
    else:
        natrl_forest_biomass_2000 = '{0}_{1}.tif'.format(tile_id, cn.pattern_WHRC_biomass_2000_unmasked)
        uu.print_log("Using WHRC biomass tile for {} sensitivity analysis".format(sensit_type))

    # For deadwood and litter 2000, opens AGC, names the output tiles, creates the output tiles
    if '2000' in carbon_pool_extent:
        AGC_2000 = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_AGC_2000)
        AGC_2000_src = rasterio.open(AGC_2000)
        kwargs = AGC_2000_src.meta
        kwargs.update(driver='GTiff', count=1, compress='lzw', nodata=0)
        windows = AGC_2000_src.block_windows(1)
        output_pattern_list = [cn.pattern_deadwood_2000, cn.pattern_litter_2000]
        if sensit_type != 'std':
            output_pattern_list = uu.alter_patterns(sensit_type, output_pattern_list)
        deadwood_2000 = '{0}_{1}.tif'.format(tile_id, output_pattern_list[0])
        litter_2000 = '{0}_{1}.tif'.format(tile_id, output_pattern_list[1])
        dst_deadwood_2000 = rasterio.open(deadwood_2000, 'w', **kwargs)
        dst_litter_2000 = rasterio.open(litter_2000, 'w', **kwargs)
        # Adds metadata tags to the output raster
        uu.add_rasterio_tags(dst_deadwood_2000, sensit_type)
        dst_deadwood_2000.update_tags(
            units='megagrams deadwood carbon/ha')
        dst_deadwood_2000.update_tags(
            source='WHRC (if standard model) or JPL (if biomass swap sensitivity analysis) and mangrove AGB (Simard et al. 2018). AGC:deadwood carbon for mangrove and non-mangrove forests applied.')
        dst_deadwood_2000.update_tags(
            extent='aboveground biomass in 2000 (WHRC if standard model, JPL if biomass_swap sensitivity analysis) and mangrove AGB. Mangrove AGB has precedence.')
        # Adds metadata tags to the output raster
        uu.add_rasterio_tags(dst_litter_2000, sensit_type)
        dst_litter_2000.update_tags(
            units='megagrams litter carbon/ha')
        dst_litter_2000.update_tags(
            source='WHRC (if standard model) or JPL (if biomass swap sensitivity analysis) and mangrove AGB (Simard et al. 2018). AGC:litter carbon for mangrove and non-mangrove forests applied.')
        dst_litter_2000.update_tags(
            extent='aboveground biomass in 2000 (WHRC if standard model, JPL if biomass_swap sensitivity analysis) and mangrove AGB. Mangrove AGB has precedence.')

    # For deadwood and litter in emissions year, opens AGC, names the output tiles, creates the output tiles
    if 'loss' in carbon_pool_extent:
        AGC_emis_year = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_AGC_emis_year)
        AGC_emis_year_src = rasterio.open(AGC_emis_year)
        kwargs = AGC_emis_year_src.meta
        kwargs.update(driver='GTiff', count=1, compress='lzw', nodata=0)
        windows = AGC_emis_year_src.block_windows(1)

        output_pattern_list = [cn.pattern_deadwood_emis_year_2000, cn.pattern_litter_emis_year_2000]
        if sensit_type != 'std':
            output_pattern_list = uu.alter_patterns(sensit_type, output_pattern_list)
        deadwood_emis_year = '{0}_{1}.tif'.format(tile_id, output_pattern_list[0])
        litter_emis_year = '{0}_{1}.tif'.format(tile_id, output_pattern_list[1])
        dst_deadwood_emis_year = rasterio.open(deadwood_emis_year, 'w', **kwargs)
        dst_litter_emis_year = rasterio.open(litter_emis_year, 'w', **kwargs)
        # Adds metadata tags to the output raster
        uu.add_rasterio_tags(dst_deadwood_emis_year, sensit_type)
        dst_deadwood_emis_year.update_tags(
            units='megagrams deadwood carbon/ha')
        dst_deadwood_emis_year.update_tags(
            source='WHRC (if standard model) or JPL (if biomass_swap sensitivity analysis) and mangrove AGB (Simard et al. 2018). Gross removals added to AGC2000 to get AGC in loss year. AGC:litter carbon for mangrove and non-mangrove forests applied.')
        dst_deadwood_emis_year.update_tags(
            extent='tree cover loss pixels within model extent')
        # Adds metadata tags to the output raster
        uu.add_rasterio_tags(dst_litter_emis_year, sensit_type)
        dst_litter_emis_year.update_tags(
            units='megagrams litter carbon/ha')
        dst_litter_emis_year.update_tags(
            source='WHRC (if standard model) or JPL (if biomass_swap sensitivity analysis) and mangrove AGB (Simard et al. 2018). Gross removals added to AGC2000 to get AGC in loss year. AGC:litter carbon for mangrove and non-mangrove forests applied.')
        dst_litter_emis_year.update_tags(
            extent='tree cover loss pixels within model extent')

    uu.print_log("  Reading input files for {}...".format(tile_id))

    try:
        precip_src = rasterio.open(precip)
        uu.print_log("    Precipitation tile found for", tile_id)
    except:
        uu.print_log("    No precipitation tile biomass for", tile_id)

    try:
        elevation_src = rasterio.open(elevation)
        uu.print_log("    Elevation tile found for", tile_id)
    except:
        uu.print_log("    No elevation tile biomass for", tile_id)

    # Opens the mangrove biomass tile if it exists
    try:
        bor_tem_trop_src = rasterio.open(bor_tem_trop)
        uu.print_log("    Boreal/temperate/tropical tile found for", tile_id)
    except:
        uu.print_log("    No boreal/temperate/tropical tile biomass for", tile_id)

    # Opens the mangrove biomass tile if it exists
    try:
        mangrove_biomass_2000_src = rasterio.open(mangrove_biomass_2000)
        uu.print_log("    Mangrove biomass found for", tile_id)
    except:
        uu.print_log("    No mangrove biomass for", tile_id)

    # Opens the WHRC/JPL biomass tile if it exists
    try:
        natrl_forest_biomass_2000_src = rasterio.open(natrl_forest_biomass_2000)
        uu.print_log("    Biomass found for", tile_id)
    except:
        uu.print_log("    No biomass for", tile_id)

    # Opens the continent-ecozone tile if it exists
    try:
        cont_ecozone_src = rasterio.open(cont_eco)
        uu.print_log("    Continent-ecozone tile found for", tile_id)
    except:
        uu.print_log("    No Continent-ecozone tile found for", tile_id)

    uu.print_log("  Creating deadwood and litter carbon density for {0} using carbon_pool_extent '{1}'...".format(tile_id, carbon_pool_extent))

    # Iterates across the windows (1 pixel strips) of the input tiles
    for idx, window in windows:

        # Populates the output raster's windows with 0s so that pixels without
        # any of the forest types will have 0s.
        # Starts with deadwood and litter at the extent of AGB2000.
        # Script later clips to loss extent.
        deadwood_2000_output = np.zeros((window.height, window.width), dtype='float32')
        litter_2000_output = np.zeros((window.height, window.width), dtype='float32')

        # Clips deadwood and litter outputs to AGC2000 and AGC in emissions year extents.
        # # Note that deadwood and litter 2000 should already be at the extent of AGC2000 and not actually need
        # # clipping to AGC2000; I'm doing that just as a formality. It feels more complete.
        # try:
        #     AGC_2000_window = AGC_2000_src.read(1, window=window)
        # except:
        #     AGC_2000_window = np.zeros((window.height, window.width), dtype='float32')
        try:
            AGC_emis_year_window = AGC_emis_year_src.read(1, window=window)
        except:
            AGC_emis_year_window = np.zeros((window.height, window.width), dtype='float32')
        try:
            cont_ecozone_window = cont_ecozone_src.read(1, window=window).astype('float32')
        except:
            cont_ecozone_window = np.zeros((window.height, window.width), dtype='float32')
        try:
            bor_tem_trop_window = bor_tem_trop_src.read(1, window=window)
        except:
            bor_tem_trop_window = np.zeros((window.height, window.width))
        try:
            precip_window = precip_src.read(1, window=window)
        except:
            precip_window = np.zeros((window.height, window.width))
        try:
            elevation_window = elevation_src.read(1, window=window)
        except:
            elevation_window = np.zeros((window.height, window.width))

        # This allows the script to bypass the few tiles that have mangrove biomass but not WHRC biomass
        if os.path.exists(natrl_forest_biomass_2000):

            # Reads in the windows of each input file that definitely exist
            natrl_forest_biomass_window = natrl_forest_biomass_2000_src.read(1, window=window)

            # The deadwood and litter conversions generally come from here: https://cdm.unfccc.int/methodologies/ARmethodologies/tools/ar-am-tool-12-v3.0.pdf, p. 17-18
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
            deadwood_2000_output = deadwood_2000_output + deadwood_masked.filled(0)
            litter_masked = agb_masked_1 * 0.04 * cn.biomass_to_c_non_mangrove_litter
            litter_2000_output = litter_2000_output + litter_masked.filled(0)


            # Equation for elevation <= 2000, 1000 < precip <= 1600, bor/temp/trop = 1 (tropical)
            elev_mask_2 = elevation_window <= 2000
            precip_mask_2 = (precip_window > 1000) & (precip_window <= 1600)
            ecozone_mask_2 = bor_tem_trop_window == 1
            condition_mask_2 = elev_mask_2 & precip_mask_2 & ecozone_mask_2
            agb_masked_2 = np.ma.array(natrl_forest_biomass_window, mask=np.invert(condition_mask_2))
            deadwood_masked = agb_masked_2 * 0.01 * cn.biomass_to_c_non_mangrove
            deadwood_2000_output = deadwood_2000_output + deadwood_masked.filled(0)
            litter_masked = agb_masked_2 * 0.01 * cn.biomass_to_c_non_mangrove_litter
            litter_2000_output = litter_2000_output + litter_masked.filled(0)

            # Equation for elevation <= 2000, precip > 1600, bor/temp/trop = 1 (tropical)
            elev_mask_3 = elevation_window <= 2000
            precip_mask_3 = precip_window > 1600
            ecozone_mask_3 = bor_tem_trop_window == 1
            condition_mask_3 = elev_mask_3 & precip_mask_3 & ecozone_mask_3
            agb_masked_3 = np.ma.array(natrl_forest_biomass_window, mask=np.invert(condition_mask_3))
            deadwood_masked = agb_masked_3 * 0.06 * cn.biomass_to_c_non_mangrove
            deadwood_2000_output = deadwood_2000_output + deadwood_masked.filled(0)
            litter_masked = agb_masked_3 * 0.01 * cn.biomass_to_c_non_mangrove_litter
            litter_2000_output = litter_2000_output + litter_masked.filled(0)

            # Equation for elevation > 2000, precip = any value, bor/temp/trop = 1 (tropical)
            elev_mask_4 = elevation_window > 2000
            ecozone_mask_4 = bor_tem_trop_window == 1
            condition_mask_4 = elev_mask_4 & ecozone_mask_4
            agb_masked_4 = np.ma.array(natrl_forest_biomass_window, mask=np.invert(condition_mask_4))
            deadwood_masked = agb_masked_4 * 0.07 * cn.biomass_to_c_non_mangrove
            deadwood_2000_output = deadwood_2000_output + deadwood_masked.filled(0)
            litter_masked = agb_masked_4 * 0.01 * cn.biomass_to_c_non_mangrove_litter
            litter_2000_output = litter_2000_output + litter_masked.filled(0)

            # Equation for elevation = any value, precip = any value, bor/temp/trop = 2 or 3 (boreal or temperate)
            ecozone_mask_5 = bor_tem_trop_window != 1
            condition_mask_5 = ecozone_mask_5
            agb_masked_5 = np.ma.array(natrl_forest_biomass_window, mask=np.invert(condition_mask_5))
            deadwood_masked = agb_masked_5 * 0.08 * cn.biomass_to_c_non_mangrove
            deadwood_2000_output = deadwood_2000_output + deadwood_masked.filled(0)
            litter_masked = agb_masked_5 * 0.04 * cn.biomass_to_c_non_mangrove_litter
            litter_2000_output = litter_2000_output + litter_masked.filled(0)

            deadwood_2000_output = deadwood_2000_output.astype('float32')
            litter_2000_output = litter_2000_output.astype('float32')

        # Replaces non-mangrove deadwood and litter with special mangrove deadwood and litter values if there is mangrove
        if os.path.exists(mangrove_biomass_2000):

            # Reads in the window for mangrove biomass if it exists
            mangrove_biomass_2000_window = mangrove_biomass_2000_src.read(1, window=window)

            # Applies the mangrove deadwood:AGB ratios (2 different ratios) to the ecozone raster to create a raster of deadwood:AGB ratios
            for key, value in mang_deadwood_AGB_ratio.items():
                cont_ecozone_window[cont_ecozone_window == key] = value

            # Multiplies the AGB in the loss year (2000 for deadwood) by the correct mangrove deadwood:AGB ratio to get an array of deadwood
            mangrove_C_final = mangrove_biomass_2000_window * cont_ecozone_window * cn.biomass_to_c_mangrove

            # Replaces non-mangrove deadwood with mangrove deadwood values
            deadwood_2000_output = np.ma.masked_where(mangrove_biomass_2000_window > 0, deadwood_2000_output)
            deadwood_2000_output = deadwood_2000_output.filled(0)

            # Combines the mangrove and non-mangrove deadwood arrays into a single array
            deadwood_2000_output = mangrove_C_final + deadwood_2000_output
            deadwood_2000_output.astype('float32')

            # # Masks the deadwood 2000 to the AGC2000 extent. This shouldn't actually change the extent of the deadwood at all.
            # # Just doing it because it feels more complete.
            # deadwood_2000_output = np.where(AGC_2000_window > 0, deadwood_2000_output, 0).astype('float32')


            # Same as above but for litter
            try:
                cont_ecozone_window = cont_ecozone_src.read(1, window=window).astype('float32')
            except:
                cont_ecozone_window = np.zeros((window.height, window.width), dtype='float32')

            # Applies the mangrove deadwood:AGB ratios (2 different ratios) to the ecozone raster to create a raster of deadwood:AGB ratios
            for key, value in mang_litter_AGB_ratio.items():
                cont_ecozone_window[cont_ecozone_window == key] = value

            mangrove_C_final = mangrove_biomass_2000_window * cont_ecozone_window * cn.biomass_to_c_mangrove

            litter_2000_output = np.ma.masked_where(mangrove_biomass_2000_window > 0, litter_2000_output)
            litter_2000_output = litter_2000_output.filled(0)

            litter_2000_output = mangrove_C_final + litter_2000_output
            litter_2000_output.astype('float32')

            # litter_2000_output = np.where(AGC_2000_window > 0, litter_2000_output, 0).astype('float32')

        # Only writes deadwood and litter 2000 to rasters if output in 2000 is desired
        if '2000' in carbon_pool_extent:

            # Writes deadwood and litter 2000 to rasters
            dst_deadwood_2000.write_band(1, deadwood_2000_output, window=window)
            dst_litter_2000.write_band(1, litter_2000_output, window=window)


        # Only if calculating carbon emitted_pools in emissions year are deadwood and litter clipped to AGC emissions year pixels.
        # Important to use AGC_emis_year_window extent and not loss years because AGC_emis_year_extent is already
        # clipped to the model extent, whereas some loss pixels are outside the extent of the model.
        if 'loss' in carbon_pool_extent:

            deadwood_emis_year_output = np.where(AGC_emis_year_window > 0, deadwood_2000_output, 0).astype('float32')
            litter_emis_year_output = np.where(AGC_emis_year_window > 0, litter_2000_output, 0).astype('float32')

            # Writes the output window to the output file
            dst_deadwood_emis_year.write_band(1, deadwood_emis_year_output, window=window)
            dst_litter_emis_year.write_band(1, litter_emis_year_output, window=window)


    # Prints information about the tile that was just processed
    if 'loss' in carbon_pool_extent:
        uu.end_of_fx_summary(start, tile_id, cn.pattern_deadwood_emis_year_2000)
    else:
        uu.end_of_fx_summary(start, tile_id, cn.pattern_deadwood_2000)


# Creates soil carbon tiles in loss pixels only
def create_soil_emis_extent(tile_id, pattern, sensit_type):

    start = datetime.datetime.now()

    # Names of the input tiles. Creates the names even if the files don't exist.
    soil_full_extent = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_soil_C_full_extent_2000)
    AGC_emis_year = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_AGC_emis_year)

    if os.path.exists(soil_full_extent) & os.path.exists(AGC_emis_year):
        uu.print_log("Soil C 2000 and loss found for {}. Proceeding with soil C in loss extent.".format(tile_id))
    else:
        return uu.print_log("Soil C 2000 and/or loss not found for {}. Skipping soil C in loss extent.".format(tile_id))

    # Name of output tile
    soil_emis_year = '{0}_{1}.tif'.format(tile_id, pattern)

    uu.print_log("  Reading input files for {}...".format(tile_id))

    # Both of these tiles should exist and thus be able to be opened
    soil_full_extent_src = rasterio.open(soil_full_extent)
    AGC_emis_year_src = rasterio.open(AGC_emis_year)

    # Grabs metadata for one of the input tiles, like its location/projection/cellsize
    kwargs = AGC_emis_year_src.meta
    # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
    windows = AGC_emis_year_src.block_windows(1)

    # Updates kwargs for the output dataset.
    # Need to update data type to float 32 so that it can handle fractional carbon emitted_pools
    kwargs.update(
        driver='GTiff',
        count=1,
        compress='lzw',
        nodata=0,
        dtype='uint16'
    )

    # The output file: belowground carbon denity in the year of tree cover loss for pixels with tree cover loss
    dst_soil_emis_year = rasterio.open(soil_emis_year, 'w', **kwargs)

    # Adds metadata tags to the output raster
    uu.add_rasterio_tags(dst_soil_emis_year, sensit_type)
    dst_soil_emis_year.update_tags(
        units='megagrams soil carbon/ha')
    dst_soil_emis_year.update_tags(
        source='ISRIC SoilGrids250 (May 2020 update) soil organic carbon stock data. 0-30 cm data.')
    dst_soil_emis_year.update_tags(
        extent='tree cover loss pixels')

    uu.print_log("  Creating soil carbon density for loss pixels in {}...".format(tile_id))

    # Iterates across the windows (1 pixel strips) of the input tiles
    for idx, window in windows:

        # Reads in the windows of each input file that definitely exist
        AGC_emis_year_window = AGC_emis_year_src.read(1, window=window)
        soil_full_extent_window = soil_full_extent_src.read(1, window=window)

        # Removes AGC pixels that do not have a loss year and fills with 0s
        soil_output = np.ma.masked_where(AGC_emis_year_window == 0, soil_full_extent_window)
        soil_output = soil_output.filled(0)

        # Converts the output to uint16 since the soil C density is integers
        soil_output = soil_output.astype('uint16')

        # Writes the output window to the output file
        dst_soil_emis_year.write_band(1, soil_output, window=window)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, pattern)


# Creates total carbon tiles (both in 2000 and loss year)
def create_total_C(tile_id, carbon_pool_extent, sensit_type):

    start = datetime.datetime.now()

    # Names of the input tiles. Creates the names even if the files don't exist.
    # The AGC name depends on whether carbon in 2000 or in the emission year is being created.
    # If litter in the loss year is being created, it uses the loss year AGC tile.
    # If litter in 2000 is being created, is uses the 2000 AGC tile.
    # The other inputs tiles aren't affected by whether the output is for 2000 or for the loss year.
    if '2000' in carbon_pool_extent:
        AGC_2000 = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_AGC_2000)
        BGC_2000 = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_BGC_2000)
        deadwood_2000 = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_deadwood_2000)
        litter_2000 = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_litter_2000)
        soil_2000 = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_soil_C_full_extent_2000)
        AGC_2000_src = rasterio.open(AGC_2000)
        BGC_2000_src = rasterio.open(BGC_2000)
        deadwood_2000_src = rasterio.open(deadwood_2000)
        litter_2000_src = rasterio.open(litter_2000)
        try:
            soil_2000_src = rasterio.open(soil_2000)
            uu.print_log("   Soil C 2000 tile found for", tile_id)
        except:
            uu.print_log("    No soil C 2000 tile found for", tile_id)

        kwargs = AGC_2000_src.meta
        kwargs.update(driver='GTiff', count=1, compress='lzw', nodata=0)
        windows = AGC_2000_src.block_windows(1)
        output_pattern_list = [cn.pattern_total_C_2000]
        if sensit_type != 'std':
            output_pattern_list = uu.alter_patterns(sensit_type, output_pattern_list)
        total_C_2000 = '{0}_{1}.tif'.format(tile_id, output_pattern_list[0])
        dst_total_C_2000 = rasterio.open(total_C_2000, 'w', **kwargs)
        # Adds metadata tags to the output raster
        uu.add_rasterio_tags(dst_total_C_2000, sensit_type)
        dst_total_C_2000.update_tags(
            units='megagrams total (all emitted_pools) carbon/ha')
        dst_total_C_2000.update_tags(
            source='AGC, BGC, deadwood carbon, litter carbon, and soil carbon')
        dst_total_C_2000.update_tags(
            extent='aboveground biomass in 2000 (WHRC if standard model, JPL if biomass_swap sensitivity analysis), mangrove AGB, and soil carbon. Mangrove AGB has precedence.')


    if 'loss' in carbon_pool_extent:
        AGC_emis_year = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_AGC_emis_year)
        BGC_emis_year = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_BGC_emis_year)
        deadwood_emis_year = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_deadwood_emis_year_2000)
        litter_emis_year = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_litter_emis_year_2000)
        soil_emis_year = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_soil_C_emis_year_2000)
        AGC_emis_year_src = rasterio.open(AGC_emis_year)
        BGC_emis_year_src = rasterio.open(BGC_emis_year)
        deadwood_emis_year_src = rasterio.open(deadwood_emis_year)
        litter_emis_year_src = rasterio.open(litter_emis_year)
        try:
            soil_emis_year_src = rasterio.open(soil_emis_year)
            uu.print_log("   Soil C emission year tile found for", tile_id)
        except:
            uu.print_log("    No soil C emission year tile found for", tile_id)

        kwargs = AGC_emis_year_src.meta
        kwargs.update(driver='GTiff', count=1, compress='lzw', nodata=0)
        windows = AGC_emis_year_src.block_windows(1)
        output_pattern_list = [cn.pattern_total_C_emis_year]
        if sensit_type != 'std':
            output_pattern_list = uu.alter_patterns(sensit_type, output_pattern_list)
        total_C_emis_year = '{0}_{1}.tif'.format(tile_id, output_pattern_list[0])
        dst_total_C_emis_year = rasterio.open(total_C_emis_year, 'w', **kwargs)
        # Adds metadata tags to the output raster
        uu.add_rasterio_tags(dst_total_C_emis_year, sensit_type)
        dst_total_C_emis_year.update_tags(
            units='megagrams total (all emitted_pools) carbon/ha')
        dst_total_C_emis_year.update_tags(
            source='AGC, BGC, deadwood carbon, litter carbon, and soil carbon')
        dst_total_C_emis_year.update_tags(
            extent='tree cover loss pixels within model extent')


    uu.print_log("  Creating total carbon density for {0} using carbon_pool_extent '{1}'...".format(tile_id, carbon_pool_extent))

    # Iterates across the windows (1 pixel strips) of the input tiles
    for idx, window in windows:

        if '2000' in carbon_pool_extent:

            # Reads in the windows of each input file that definitely exist
            AGC_2000_window = AGC_2000_src.read(1, window=window)
            BGC_2000_window = BGC_2000_src.read(1, window=window)
            deadwood_2000_window = deadwood_2000_src.read(1, window=window)
            litter_2000_window = litter_2000_src.read(1, window=window)
            try:
                soil_2000_window = soil_2000_src.read(1, window=window)
            except:
                soil_2000_window = np.zeros((window.height, window.width))

            total_C_2000_window = AGC_2000_window + BGC_2000_window + deadwood_2000_window + litter_2000_window + soil_2000_window

            # Converts the output to float32 since float64 is an unnecessary level of precision
            total_C_2000_window = total_C_2000_window.astype('float32')

            # Writes the output window to the output file
            dst_total_C_2000.write_band(1, total_C_2000_window, window=window)


        if 'loss' in carbon_pool_extent:

            # Reads in the windows of each input file that definitely exist
            AGC_emis_year_window = AGC_emis_year_src.read(1, window=window)
            BGC_emis_year_window = BGC_emis_year_src.read(1, window=window)
            deadwood_emis_year_window = deadwood_emis_year_src.read(1, window=window)
            litter_emis_year_window = litter_emis_year_src.read(1, window=window)
            try:
                soil_emis_year_window = soil_emis_year_src.read(1, window=window)
            except:
                soil_emis_year_window = np.zeros((window.height, window.width))

            total_C_emis_year_window = AGC_emis_year_window + BGC_emis_year_window + deadwood_emis_year_window + litter_emis_year_window + soil_emis_year_window

            # Converts the output to float32 since float64 is an unnecessary level of precision
            total_C_emis_year_window = total_C_emis_year_window.astype('float32')

            # Writes the output window to the output file
            dst_total_C_emis_year.write_band(1, total_C_emis_year_window, window=window)


    # Prints information about the tile that was just processed
    if 'loss' in carbon_pool_extent:
        uu.end_of_fx_summary(start, tile_id, cn.pattern_total_C_emis_year)
    else:
        uu.end_of_fx_summary(start, tile_id, cn.pattern_total_C_2000)
