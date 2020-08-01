

import datetime
import numpy as np
import os
import rasterio
import logging
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def annual_gain_rate_AGB_BGB_all_forest_types(tile_id, sensit_type):

    uu.print_log("Mapping forest type for removal and AGB removal rate:", tile_id)

    # Start time
    start = datetime.datetime.now()

    # Names of the input tiles
    model_extent = '{0}_{1}.tif'.format(tile_id, cn.pattern_model_extent)
    mangrove_agb = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_AGB_mangrove)
    mangrove_bgb = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_BGB_mangrove)
    europe = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_annual_gain_AGC_BGC_natrl_forest_Europe)
    plantations = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_annual_gain_AGC_BGC_planted_forest_unmasked)
    us = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_annual_gain_AGC_BGC_natrl_forest_US)
    young = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_annual_gain_AGC_natrl_forest_young)
    ipcc_default = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_annual_gain_AGB_IPCC_defaults)

    # Names of the output tiles
    removal_forest_type = '{0}_{1}.tif'.format(tile_id, cn.pattern_removal_forest_type)
    annual_gain_AGB_all_forest_types = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_AGB_all_types)
    annual_gain_BGB_all_forest_types = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_AGB_all_types)

    # Opens biomass tile
    with rasterio.open(model_extent) as model_extent_src:

        # Grabs metadata about the tif, like its location/projection/cellsize
        kwargs = model_extent_src.meta

        # Grabs the windows of the tile (stripes) so we can iterate over the entire tif without running out of memory
        windows = model_extent_src.block_windows(1)

        # Updates kwargs for the output dataset
        kwargs.update(
            driver='GTiff',
            count=1,
            compress='lzw',
            nodata=0
        )

        # Checks whether there are mangrove or planted forest tiles. If so, they are opened.
        try:
            mangrove_agb_src = rasterio.open(mangrove_agb)
            mangrove_bgb_src = rasterio.open(mangrove_bgb)
            uu.print_log("    Mangrove tile found for {}".format(tile_id))
        except:
            uu.print_log("    No mangrove tile for {}".format(tile_id))

        try:
            europe_src = rasterio.open(europe)
            uu.print_log("    Europe removal factor tile found for {}".format(tile_id))
        except:
            uu.print_log("    No Europe removal factor tile for {}".format(tile_id))

        try:
            plantations_src = rasterio.open(plantations)
            uu.print_log("    Planted forest tile found for {}".format(tile_id))
        except:
            uu.print_log("    No planted forest tile for {}".format(tile_id))

        try:
            us_src = rasterio.open(us)
            uu.print_log("    US removal factor tile found for {}".format(tile_id))
        except:
            uu.print_log("    No US removal factor tile for {}".format(tile_id))

        try:
            young_src = rasterio.open(young)
            uu.print_log("    Young forest removal factor tile found for {}".format(tile_id))
        except:
            uu.print_log("    No young forest removal factor tile for {}".format(tile_id))

        try:
            ipcc_default_src = rasterio.open(ipcc_default)
            uu.print_log("    IPCC default removal rate tile found for {}".format(tile_id))
        except:
            uu.print_log("    IPCC default removal rate tile for {}".format(tile_id))

        # Opens the output tile, giving it the arguments of the input tiles
        removal_forest_type_dst = rasterio.open(removal_forest_type, 'w', **kwargs)

        # Updates kwargs for the removal rate outputs-- just need to change datatype
        kwargs.update(dtype='float32')

        annual_gain_AGB_all_forest_types_dst = rasterio.open(annual_gain_AGB_all_forest_types, 'w', **kwargs)
        # annual_gain_BGB_all_forest_types_dst = rasterio.open(annual_gain_BGB_all_forest_types, 'w', **kwargs)

        uu.print_log("  Creating removal model forest type tile, AGB removal factor tile, and BGB removal factor tile for {}".format(tile_id))

        # Iterates across the windows (1 pixel strips) of the input tile
        for idx, window in windows:

            model_extent_window = model_extent_src.read(1, window=window)

            removal_forest_type_window = np.zeros((window.height, window.width), dtype='uint8')
            annual_gain_AGB_all_forest_types_window = np.zeros((window.height, window.width), dtype='float32')
            # annual_gain_BGB_all_forest_types_window = np.zeros((window.height, window.width), dtype='float32')

            try:
                ipcc_default_rate_window = ipcc_default_src.read(1, window=window)
                removal_forest_type_window = np.where(ipcc_default_rate_window != 0, cn.old_natural_rank, removal_forest_type_window).astype('uint8')
                annual_gain_AGB_all_forest_types_window = np.where(ipcc_default_rate_window != 0,
                                                                   ipcc_default_rate_window,
                                                                   annual_gain_AGB_all_forest_types_window).astype('float32')
            except:
                pass

            try: # young_rate_window uses > because of the weird NaN in the tiles. If != is used, the young rate NaN overwrite the IPCC arrays
                young_rate_window = young_src.read(1, window=window)
                removal_forest_type_window = np.where(young_rate_window > 0, cn.young_natural_rank, removal_forest_type_window).astype('uint8')
                annual_gain_AGB_all_forest_types_window = np.where(young_rate_window > 0,
                                                                   young_rate_window / cn.biomass_to_c_non_mangrove,
                                                                   annual_gain_AGB_all_forest_types_window).astype('float32')
            except:
                pass

            try:
                us_rate_window = us_src.read(1, window=window)
                removal_forest_type_window = np.where(us_rate_window != 0, cn.US_rank, removal_forest_type_window).astype('uint8')
                annual_gain_AGB_all_forest_types_window = np.where(us_rate_window != 0,
                                                                   us_rate_window / cn.biomass_to_c_non_mangrove / (1 + cn.below_to_above_non_mang),
                                                                   annual_gain_AGB_all_forest_types_window).astype('float32')
            except:
                pass

            try:
                plantations_rate_window = plantations_src.read(1, window=window)
                removal_forest_type_window = np.where(plantations_rate_window != 0, cn.planted_forest_rank, removal_forest_type_window).astype('uint8')
                annual_gain_AGB_all_forest_types_window = np.where(plantations_rate_window != 0,
                                                                   plantations_rate_window / cn.biomass_to_c_non_mangrove / (1 + cn.below_to_above_non_mang),
                                                                   annual_gain_AGB_all_forest_types_window).astype('float32')
            except:
                pass

            try:
                europe_rate_window = europe_src.read(1, window=window)
                removal_forest_type_window = np.where(europe_rate_window != 0, cn.europe_rank, removal_forest_type_window).astype('uint8')
                annual_gain_AGB_all_forest_types_window = np.where(europe_rate_window != 0,
                                                                   europe_rate_window / cn.biomass_to_c_non_mangrove / (1 + cn.below_to_above_non_mang),
                                                                   annual_gain_AGB_all_forest_types_window).astype('float32')
            except:
                pass

            try:
                mangroves_rate_window = mangrove_agb_src.read(1, window=window)
                removal_forest_type_window = np.where(mangroves_rate_window != 0, cn.mangrove_rank, removal_forest_type_window).astype('uint8')
                annual_gain_AGB_all_forest_types_window = np.where(mangroves_rate_window != 0,
                                                                   mangroves_rate_window,
                                                                   annual_gain_AGB_all_forest_types_window).astype('float32')
            except:
                pass

            removal_forest_type_window = np.where(model_extent_window == 1, removal_forest_type_window, 0)
            annual_gain_AGB_all_forest_types_window = np.where(model_extent_window == 1, annual_gain_AGB_all_forest_types_window, 0)
            # annual_gain_BGB_all_forest_types_window = np.where(model_extent_window == 1, annual_gain_BGB_all_forest_types_window, 0)


            # Writes the outputs window to the output files
            removal_forest_type_dst.write_band(1, removal_forest_type_window, window=window)
            annual_gain_AGB_all_forest_types_dst.write_band(1, annual_gain_AGB_all_forest_types_window, window=window)
            # annual_gain_BGB_all_forest_types_dst.write_band(1, annual_gain_BGB_all_forest_types_window, window=window)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_removal_forest_type)