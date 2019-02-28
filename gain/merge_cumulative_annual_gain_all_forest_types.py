### This script combines the annual gain rate tiles from different forest types (non-mangrove natural forests, mangroves,
### plantations, into combined tiles. It does the same for cumulative gain over the study period.

import datetime
import os
import rasterio
import sys
import numpy as np
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def gain_merge(tile_id):

    print "Processing:", tile_id

    # Start time
    start = datetime.datetime.now()

    # Names of the annual gain rate and cumulative gain tiles for mangroves.
    # These names are created even if the tile doesn't have any mangroves.
    annual_gain_AGB_mangrove = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_AGB_mangrove)
    cumul_gain_AGC_mangrove = '{0}_{1}.tif'.format(tile_id, cn.pattern_cumul_gain_AGC_mangrove)
    annual_gain_BGB_mangrove = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_BGB_mangrove)
    cumul_gain_BGC_mangrove = '{0}_{1}.tif'.format(tile_id, cn.pattern_cumul_gain_BGC_mangrove)

    # Names of the annual gain rate and cumulative gain tiles for non-mangrove planted forests
    # These names are created even if the tile doesn't have any planted forests.
    annual_gain_AGB_planted_forest = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_AGB_planted_forest_non_mangrove)
    cumul_gain_AGC_planted_forest = '{0}_{1}.tif'.format(tile_id, cn.pattern_cumul_gain_AGC_planted_forest_non_mangrove)
    annual_gain_BGB_planted_forest = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_BGB_planted_forest_non_mangrove)
    cumul_gain_BGC_planted_forest = '{0}_{1}.tif'.format(tile_id, cn.pattern_cumul_gain_BGC_planted_forest_non_mangrove)

    # Names of the annual gain rate and cumulative gain tiles for non-mangrove non-planted forests
    # These names are created even if the tile doesn't have any non-mangrove non-planted forests.
    annual_gain_AGB_natrl_forest = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_AGB_natrl_forest)
    cumul_gain_AGC_natrl_forest = '{0}_{1}.tif'.format(tile_id, cn.pattern_cumul_gain_AGC_natrl_forest)
    annual_gain_BGB_natrl_forest = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_BGB_natrl_forest)
    cumul_gain_BGC_natrl_forest = '{0}_{1}.tif'.format(tile_id, cn.pattern_cumul_gain_BGC_natrl_forest)

    # If there is a mangrove tile, it is read into rasterio.
    # Uses the annual AGB gain rate as a proxy for the other three tiles; if there's an annual AGB tile, there will
    # definitely be an annual BGB tile and there will almost certainly be cumulative AGC and BGC tiles.
    # (The only reason there wouldn't be cumulative AGC and BGC tiles is if all the annual AGB/BGB pixels in the tile
    # had a gain year count of 1, so there was no accumulation. That is highly unlikely but theoretically possible.)
    if os.path.exists(annual_gain_AGB_mangrove):
        print "{} has mangroves.".format(tile_id)

        gain_AGB_mangrove_src = rasterio.open(annual_gain_AGB_mangrove)
        gain_BGB_mangrove_src = rasterio.open(annual_gain_BGB_mangrove)
        gain_AGC_mangrove_src = rasterio.open(cumul_gain_AGC_mangrove)
        gain_BGC_mangrove_src = rasterio.open(cumul_gain_BGC_mangrove)

        # Creates windows and and keyword args out of the mangrove tile.
        # It does this in case the mangrove tile is the only input tile and this is the only source of
        # window and kwarg info for the output.
        windows = gain_AGB_mangrove_src.block_windows(1)
        kwargs = gain_AGB_mangrove_src.meta

    # Same as above but for non-mangrove planted forests
    if os.path.exists(annual_gain_AGB_planted_forest):
        print "{} has non-mangrove planted forest.".format(tile_id)

        gain_AGB_planted_forest_src = rasterio.open(annual_gain_AGB_planted_forest)
        gain_BGB_planted_forest_src = rasterio.open(annual_gain_BGB_planted_forest)
        gain_AGC_planted_forest_src = rasterio.open(cumul_gain_AGC_planted_forest)
        gain_BGC_planted_forest_src = rasterio.open(cumul_gain_BGC_planted_forest)

        windows = gain_AGB_planted_forest_src.block_windows(1)
        kwargs = gain_AGB_planted_forest_src.meta

    # Same as above except for non-mangrove non-planted forests
    if os.path.exists(annual_gain_AGB_natrl_forest):
        print "{} has non-mangrove, non-planted forest.".format(tile_id)

        gain_AGB_natrl_forest_src = rasterio.open(annual_gain_AGB_natrl_forest)
        gain_BGB_natrl_forest_src = rasterio.open(annual_gain_BGB_natrl_forest)
        gain_AGC_natrl_forest_src = rasterio.open(cumul_gain_AGC_natrl_forest)
        gain_BGC_natrl_forest_src = rasterio.open(cumul_gain_BGC_natrl_forest)

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
    annual_out = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_combo)
    dst_annual = rasterio.open(annual_out, 'w', **kwargs)

    cumul_out = '{0}_{1}.tif'.format(tile_id, cn.pattern_cumul_gain_combo)
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
            gain_AGC_mangrove = gain_AGC_mangrove_src.read(1, window=window)
            gain_BGC_mangrove = gain_BGC_mangrove_src.read(1, window=window)

            # Adds the AGB and BGB mangrove arrays to the base array.
            # Likewise with the cumulative carbon arrays.
            dst_annual_window = gain_AGB_mangrove + gain_BGB_mangrove
            dst_cumul_window = gain_AGC_mangrove + gain_BGC_mangrove

        # Same as above but for non-mangrove planted forests, except that the planted
        # forest values are added to the mangrove values, if there are any
        if os.path.exists(annual_gain_AGB_planted_forest):
            gain_AGB_planted = gain_AGB_planted_forest_src.read(1, window=window)
            gain_BGB_planted = gain_BGB_planted_forest_src.read(1, window=window)
            gain_AGC_planted = gain_AGC_planted_forest_src.read(1, window=window)
            gain_BGC_planted = gain_BGC_planted_forest_src.read(1, window=window)

            dst_annual_window = dst_annual_window + gain_AGB_planted + gain_BGB_planted
            dst_cumul_window = dst_cumul_window + gain_AGC_planted + gain_BGC_planted

        # Same as above except for non-mangrove non-planted forests, except that the
        # natural forest values are added to the planted forest and/or mangrove values,
        # if there are any
        if os.path.exists(annual_gain_AGB_natrl_forest):
            gain_AGB_natrl = gain_AGB_natrl_forest_src.read(1, window=window)
            gain_BGB_natrl = gain_BGB_natrl_forest_src.read(1, window=window)
            gain_AGC_natrl = gain_AGC_natrl_forest_src.read(1, window=window)
            gain_BGC_natrl = gain_BGC_natrl_forest_src.read(1, window=window)

            dst_annual_window = dst_annual_window + gain_AGB_natrl + gain_BGB_natrl
            dst_cumul_window = dst_cumul_window + gain_AGC_natrl + gain_BGC_natrl


        # Writes the two output arrays to the output rasters
        dst_annual.write_band(1, dst_annual_window, window=window)
        dst_cumul.write_band(1, dst_cumul_window, window=window)

    # # These tiles need to be listed in this particular order because of how they are iterated through below.
    # in_tiles = [annual_gain_AGB_natrl_forest, cumul_gain_AGC_natrl_forest, annual_gain_AGB_mangrove,  cumul_gain_AGC_mangrove,
    #             annual_gain_BGB_natrl_forest, cumul_gain_BGC_natrl_forest, annual_gain_BGB_mangrove,  cumul_gain_BGC_mangrove]
    #
    # out_tiles = ['{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_combo), '{0}_{1}.tif'.format(tile_id, cn.pattern_cumul_gain_combo)]
    #
    #
    # # Levels are the annual gain rate and cumulative gain
    # for level in range(0, 2):
    #
    #     # Checks if this tile has any mangroves
    #     if os.path.exists('{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_AGB_mangrove)):
    #
    #         print "{0} has mangroves. Adding tiles for level {1}.".format(tile_id, level)
    #
    #         # Opens first input tile
    #         with rasterio.open(in_tiles[level+0]) as gain_AGB_natrl_forest_src:
    #
    #             # Grabs metadata about the tif, like its location/projection/cellsize
    #             kwargs = gain_AGB_natrl_forest_src.meta
    #
    #             # Grabs the windows of the tile (stripes) so we can iterate over the entire tif without running out of memory
    #             windows = gain_AGB_natrl_forest_src.block_windows(1)
    #
    #             # Opens second input tile
    #             with rasterio.open(in_tiles[level+4]) as gain_BGB_natrl_forest_src:
    #                 # Opens third input tile
    #                 with rasterio.open(in_tiles[level+2]) as gain_AGB_mangrove_src:
    #                     # Opens fourth input tile
    #                     with rasterio.open(in_tiles[level+6]) as gain_BGB_mangrove_src:
    #                         # Updates kwargs for the output dataset
    #                         kwargs.update(
    #                             driver='GTiff',
    #                             count=1,
    #                             compress='lzw',
    #                             nodata=0
    #                         )
    #
    #                         # Opens the output tile, giving it the arguments of the input tiles
    #                         with rasterio.open(out_tiles[level], 'w', **kwargs) as dst:
    #
    #                             # Iterates across the windows (1 pixel strips) of the input tile
    #                             for idx, window in windows:
    #
    #                                 # Creates windows for each input tile
    #                                 AGB_natrl_forest = gain_AGB_natrl_forest_src.read(1, window=window)
    #                                 BGB_natrl_forest = gain_BGB_natrl_forest_src.read(1, window=window)
    #                                 AGB_mangrove = gain_AGB_mangrove_src.read(1, window=window)
    #                                 BGB_mangrove = gain_BGB_mangrove_src.read(1, window=window)
    #
    #                                 # Adds all the input tiles together to get the combined values
    #                                 dst_data = AGB_natrl_forest + BGB_natrl_forest + AGB_mangrove + BGB_mangrove
    #
    #                                 dst.write_band(1, dst_data, window=window)
    #
    #     else:
    #
    #         print "{0} does not have mangroves. Adding tiles for level {1}.".format(tile_id, level)
    #
    #         # Opens first input tile
    #         with rasterio.open(in_tiles[level+0]) as gain_AGB_natrl_forest_src:
    #
    #             # Grabs metadata about the tif, like its location/projection/cellsize
    #             kwargs = gain_AGB_natrl_forest_src.meta
    #
    #             # Grabs the windows of the tile (stripes) so we can iterate over the entire tif without running out of memory
    #             windows = gain_AGB_natrl_forest_src.block_windows(1)
    #
    #             # Opens second input tile
    #             with rasterio.open(in_tiles[level + 4]) as gain_BGB_natrl_forest_src:
    #                 # Updates kwargs for the output dataset
    #                 kwargs.update(
    #                     driver='GTiff',
    #                     count=1,
    #                     compress='lzw',
    #                     nodata=0
    #                 )
    #
    #                 # Opens the output tile, giving it the arguments of the input tiles
    #                 with rasterio.open(out_tiles[level], 'w', **kwargs) as dst:
    #
    #                     # Iterates across the windows (1 pixel strips) of the input tile
    #                     for idx, window in windows:
    #
    #                         # Creates windows for each input tile
    #                         AGB_natrl_forest = gain_AGB_natrl_forest_src.read(1, window=window)
    #                         BGB_natrl_forest = gain_BGB_natrl_forest_src.read(1, window=window)
    #
    #                         # Adds all the input tiles together to get the combined values
    #                         dst_data = AGB_natrl_forest + BGB_natrl_forest
    #
    #                         dst.write_band(1, dst_data, window=window)
    #
    # utilities.upload_final(cn.annual_gain_combo_dir, tile_id, cn.pattern_annual_gain_combo)
    # utilities.upload_final(cn.cumul_gain_combo_dir, tile_id, cn.pattern_cumul_gain_combo)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_annual_gain_combo)