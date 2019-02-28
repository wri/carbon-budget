### This script combines the annual gain rate tiles from different forest types (non-mangrove natural forests, mangroves,
### plantations, into combined tiles. It does the same for cumulative gain over the study period.

import datetime
import os
import rasterio
import sys
import numpy as np
sys.path.append('../')
import constants_and_names as cn

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
    annual_gain_AGB_planted_forest = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_AGB_planted_forest_non_mangrove)
    cumul_gain_AGC_planted_forest = '{0}_{1}.tif'.format(tile_id, cn.pattern_cumul_gain_AGC_planted_forest_non_mangrove)
    annual_gain_BGB_planted_forest = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_BGB_planted_forest_non_mangrove)
    cumul_gain_BGC_planted_forest = '{0}_{1}.tif'.format(tile_id, cn.pattern_cumul_gain_BGC_planted_forest_non_mangrove)

    # Names of the annual gain rate and cumulative gain tiles for non-mangrove non-planted forests
    annual_gain_AGB_natrl_forest = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_AGB_natrl_forest)
    cumul_gain_AGC_natrl_forest = '{0}_{1}.tif'.format(tile_id, cn.pattern_cumul_gain_AGC_natrl_forest)
    annual_gain_BGB_natrl_forest = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_BGB_natrl_forest)
    cumul_gain_BGC_natrl_forest = '{0}_{1}.tif'.format(tile_id, cn.pattern_cumul_gain_BGC_natrl_forest)

    # These tiles need to be listed in this particular order because of how they are iterated through below.
    in_tiles = [annual_gain_AGB_natrl_forest, cumul_gain_AGC_natrl_forest, annual_gain_AGB_mangrove,  cumul_gain_AGC_mangrove,
                annual_gain_BGB_natrl_forest, cumul_gain_BGC_natrl_forest, annual_gain_BGB_mangrove,  cumul_gain_BGC_mangrove]

    out_tiles = ['{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_combo), '{0}_{1}.tif'.format(tile_id, cn.pattern_cumul_gain_combo)]

    if os.path.exists(annual_gain_AGB_mangrove):
        print "{} has mangroves.".format(tile_id)

        gain_AGB_mangrove_src = rasterio.open(annual_gain_AGB_mangrove)
        gain_BGB_mangrove_src = rasterio.open(annual_gain_BGB_mangrove)

        windows = gain_AGB_mangrove_src.block_windows(1)
        kwargs = gain_AGB_mangrove_src.meta

    if os.path.exists(annual_gain_AGB_planted_forest):
        print "{} has non-mangrove planted forest.".format(tile_id)

        gain_AGB_planted_forest_src = rasterio.open(annual_gain_AGB_planted_forest)
        gain_BGB_planted_forest_src = rasterio.open(annual_gain_BGB_planted_forest)

        windows = gain_AGB_planted_forest_src.block_windows(1)
        kwargs = gain_AGB_planted_forest_src.meta

    if os.path.exists(annual_gain_AGB_natrl_forest):
        print "{} has non-mangrove non-planted forest.".format(tile_id)

        gain_AGB_natrl_forest_src = rasterio.open(annual_gain_AGB_natrl_forest)
        gain_BGB_natrl_forest_src = rasterio.open(annual_gain_BGB_natrl_forest)

        windows = gain_AGB_natrl_forest_src.block_windows(1)
        kwargs = gain_AGB_natrl_forest_src.meta

    # Updates kwargs for the output dataset.
    # Need to update data type to float 32 so that it can handle fractional gain rates
    kwargs.update(
        driver='GTiff',
        count=1,
        compress='lzw',
        nodata=0,
        dtype='float32'
    )

    annual_out = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_combo)
    dst_annual = rasterio.open(annual_out, 'w', **kwargs)

    for idx, window in windows:

        dst_data = np.zeros((window.height, window.width), dtype='float32')

        if os.path.exists(annual_gain_AGB_mangrove):
            gain_AGB_mangrove = gain_AGB_mangrove_src.read(1, window=window)
            gain_BGB_mangrove = gain_BGB_mangrove_src.read(1, window=window)

            dst_data = gain_AGB_mangrove + gain_BGB_mangrove

            print dst_data

        if os.path.exists(annual_gain_AGB_planted_forest):
            gain_AGB_planted = gain_AGB_planted_forest_src.read(1, window=window)
            gain_BGB_planted = gain_AGB_planted_forest_src.read(1, window=window)

            dst_data = gain_AGB_planted + gain_BGB_planted

            print dst_data

        if os.path.exists(annual_gain_AGB_natrl_forest):
            gain_AGB_natrl = gain_AGB_natrl_forest_src.read(1, window=window)
            gain_BGB_natrl = gain_AGB_natrl_forest_src.read(1, window=window)

            dst_data = gain_AGB_natrl + gain_BGB_natrl

            print dst_data

        print dst_data

        dst_annual.write_band(1, dst_data, window=window)

        os.quit()

        # dst_annual.write_band(1, gain_rate_AGB, window=window)



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

    end = datetime.datetime.now()
    elapsed_time = end-start

    print "  Processing time for tile", tile_id, ":", elapsed_time