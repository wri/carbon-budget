### This script combines the annual gain rate tiles from different forest types (non-mangrove natural forests, mangroves,
### plantations) into combined tiles. It does the same for cumulative CO2 gain over the study period (above and belowground).

import datetime
import os
import rasterio
import sys
import numpy as np
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def gain_merge(tile_id, output_pattern_list, sensit_type):

    uu.print_log("Calculating annual biomass and cumulative CO2 removals for all forest types:", tile_id)

    # Start time
    start = datetime.datetime.now()

    # Names of the annual gain rate and cumulative gain tiles for mangroves.
    # These names are created even if the tile doesn't have any mangroves.
    annual_gain_AGB_mangrove = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_annual_gain_AGB_mangrove)
    cumul_gain_AGCO2_mangrove = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_cumul_gain_AGCO2_mangrove)
    annual_gain_BGB_mangrove = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_annual_gain_BGB_mangrove)
    cumul_gain_BGCO2_mangrove = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_cumul_gain_BGCO2_mangrove)

    # Names of the annual gain rate and cumulative gain tiles for non-mangrove planted forests
    # These names are created even if the tile doesn't have any planted forests.
    annual_gain_AGB_planted_forest = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_annual_gain_AGB_planted_forest_non_mangrove)
    cumul_gain_AGCO2_planted_forest = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_cumul_gain_AGCO2_planted_forest_non_mangrove)
    annual_gain_BGB_planted_forest = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_annual_gain_BGB_planted_forest_non_mangrove)
    cumul_gain_BGCO2_planted_forest = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_cumul_gain_BGCO2_planted_forest_non_mangrove)

    # Names of the annual gain rate and cumulative gain tiles for non-mangrove non-planted forests
    # These names are created even if the tile doesn't have any non-mangrove non-planted forests.
    annual_gain_AGB_natrl_forest = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_annual_gain_AGB_IPCC_defaults)
    cumul_gain_AGCO2_natrl_forest = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_cumul_gain_AGCO2_natrl_forest)
    annual_gain_BGB_natrl_forest = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_annual_gain_BGB_natrl_forest)
    cumul_gain_BGCO2_natrl_forest = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_cumul_gain_BGCO2_natrl_forest)

    # If there is a mangrove tile, it is read into rasterio.
    # Uses the annual AGB gain rate as a proxy for the other three tiles; if there's an annual AGB tile, there will
    # definitely be an annual BGB tile and there will almost certainly be cumulative AGC and BGC tiles.
    # (The only reason there wouldn't be cumulative AGC and BGC tiles is if all the annual AGB/BGB pixels in the tile
    # had a gain year count of 1, so there was no accumulation. That is highly unlikely but theoretically possible.)
    if os.path.exists(annual_gain_AGB_mangrove):
        uu.print_log("{} has mangroves.".format(tile_id))

        gain_AGB_mangrove_src = rasterio.open(annual_gain_AGB_mangrove)
        gain_BGB_mangrove_src = rasterio.open(annual_gain_BGB_mangrove)
        gain_AGCO2_mangrove_src = rasterio.open(cumul_gain_AGCO2_mangrove)
        gain_BGCO2_mangrove_src = rasterio.open(cumul_gain_BGCO2_mangrove)

        # Creates windows and and keyword args out of the mangrove tile.
        # It does this in case the mangrove tile is the only input tile and this is the only source of
        # window and kwarg info for the output.
        windows = gain_AGB_mangrove_src.block_windows(1)
        kwargs = gain_AGB_mangrove_src.meta

    # Same as above but for non-mangrove planted forests
    if os.path.exists(annual_gain_AGB_planted_forest):
        uu.print_log("{} has non-mangrove planted forest.".format(tile_id))

        gain_AGB_planted_forest_src = rasterio.open(annual_gain_AGB_planted_forest)
        gain_BGB_planted_forest_src = rasterio.open(annual_gain_BGB_planted_forest)
        gain_AGCO2_planted_forest_src = rasterio.open(cumul_gain_AGCO2_planted_forest)
        gain_BGCO2_planted_forest_src = rasterio.open(cumul_gain_BGCO2_planted_forest)

        windows = gain_AGB_planted_forest_src.block_windows(1)
        kwargs = gain_AGB_planted_forest_src.meta

    # Same as above except for non-mangrove non-planted forests
    if os.path.exists(annual_gain_AGB_natrl_forest):
        uu.print_log("{} has non-mangrove, non-planted forest.".format(tile_id))

        gain_AGB_natrl_forest_src = rasterio.open(annual_gain_AGB_natrl_forest)
        gain_BGB_natrl_forest_src = rasterio.open(annual_gain_BGB_natrl_forest)
        gain_AGCO2_natrl_forest_src = rasterio.open(cumul_gain_AGCO2_natrl_forest)
        gain_BGCO2_natrl_forest_src = rasterio.open(cumul_gain_BGCO2_natrl_forest)

        windows = gain_AGB_natrl_forest_src.block_windows(1)
        kwargs = gain_AGB_natrl_forest_src.meta

    # Updates kwargs for the output dataset.
    kwargs.update(
        driver='GTiff',
        count=1,
        compress='lzw',
        nodata=0,
        dtype='float32'
    )

    # Creates the output data: annual AGB+BGB gain for all forest types and
    # cumulative AGC and BGC gain for all forest types
    annual_out = '{0}_{1}.tif'.format(tile_id, output_pattern_list[0])
    dst_annual = rasterio.open(annual_out, 'w', **kwargs)

    cumul_out = '{0}_{1}.tif'.format(tile_id, output_pattern_list[1])
    dst_cumul = rasterio.open(cumul_out, 'w', **kwargs)

    # Iterates through the windows created above
    for idx, window in windows:

        # Populates the output rasters' windows with 0s so that pixels without
        # any of the forest types will have 0s
        dst_annual_window = np.zeros((window.height, window.width), dtype='float32')
        dst_cumul_window = np.zeros((window.height, window.width), dtype='float32')

        # If there is a mangrove tile, this reates a numpy array for the four mangrove inputs
        if os.path.exists(annual_gain_AGB_mangrove):
            gain_AGB_mangrove = gain_AGB_mangrove_src.read(1, window=window)
            gain_BGB_mangrove = gain_BGB_mangrove_src.read(1, window=window)
            gain_AGCO2_mangrove = gain_AGCO2_mangrove_src.read(1, window=window)
            gain_BGCO2_mangrove = gain_BGCO2_mangrove_src.read(1, window=window)

            # Adds the AGB and BGB mangrove arrays to the base array.
            # Likewise with the cumulative carbon arrays.
            dst_annual_window = gain_AGB_mangrove + gain_BGB_mangrove
            dst_cumul_window = gain_AGCO2_mangrove + gain_BGCO2_mangrove

        # Same as above but for non-mangrove planted forests, except that the planted
        # forest values are added to the mangrove values, if there are any
        if os.path.exists(annual_gain_AGB_planted_forest):
            gain_AGB_planted = gain_AGB_planted_forest_src.read(1, window=window)
            gain_BGB_planted = gain_BGB_planted_forest_src.read(1, window=window)
            gain_AGCO2_planted = gain_AGCO2_planted_forest_src.read(1, window=window)
            gain_BGCO2_planted = gain_BGCO2_planted_forest_src.read(1, window=window)

            dst_annual_window = dst_annual_window + gain_AGB_planted + gain_BGB_planted
            dst_cumul_window = dst_cumul_window + gain_AGCO2_planted + gain_BGCO2_planted

        # Same as above except for non-mangrove non-planted forests, except that the
        # natural forest values are added to the planted forest and/or mangrove values,
        # if there are any
        if os.path.exists(annual_gain_AGB_natrl_forest):
            gain_AGB_natrl = gain_AGB_natrl_forest_src.read(1, window=window)
            gain_BGB_natrl = gain_BGB_natrl_forest_src.read(1, window=window)
            gain_AGCO2_natrl = gain_AGCO2_natrl_forest_src.read(1, window=window)
            gain_BGCO2_natrl = gain_BGCO2_natrl_forest_src.read(1, window=window)

            dst_annual_window = dst_annual_window + gain_AGB_natrl + gain_BGB_natrl
            dst_cumul_window = dst_cumul_window + gain_AGCO2_natrl + gain_BGCO2_natrl


        # Writes the two output arrays to the output rasters
        dst_annual.write_band(1, dst_annual_window, window=window)
        dst_cumul.write_band(1, dst_cumul_window, window=window)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, output_pattern_list[0])