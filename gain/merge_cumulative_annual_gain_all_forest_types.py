### This script combines the annual gain rate tiles from different forest types (non-mangrove natural forests, mangroves,
### plantations, into combined tiles. It does the same for cumulative gain over the study period.

import utilities
import datetime
import subprocess
import os
import numpy as np
import rasterio

def gain_merge(tile_id):

    print "Processing:", tile_id

    # Start time
    start = datetime.datetime.now()

    # Names of the annual gain rate and cumulative gain tiles for non-mangrove natural forests
    annual_gain_AGB_natrl_forest = '{0}_{1}.tif'.format(utilities.pattern_annual_gain_AGB_natrl_forest, tile_id)
    cumul_gain_AGC_natrl_forest = '{0}_{1}.tif'.format(utilities.pattern_cumul_gain_AGC_natrl_forest, tile_id)
    annual_gain_BGB_natrl_forest = '{0}_{1}.tif'.format(utilities.pattern_annual_gain_BGB_natrl_forest, tile_id)
    cumul_gain_BGC_natrl_forest = '{0}_{1}.tif'.format(utilities.pattern_cumul_gain_BGC_natrl_forest, tile_id)

    annual_gain_AGB_mangrove = '{0}_{1}.tif'.format(utilities.pattern_annual_gain_AGB_mangrove, tile_id)
    cumul_gain_AGC_mangrove = '{0}_{1}.tif'.format(utilities.pattern_cumul_gain_AGC_mangrove, tile_id)
    annual_gain_BGB_mangrove = '{0}_{1}.tif'.format(utilities.pattern_annual_gain_BGB_mangrove, tile_id)
    cumul_gain_BGC_mangrove = '{0}_{1}.tif'.format(utilities.pattern_cumul_gain_BGC_mangrove, tile_id)

    tiles_in = [annual_gain_AGB_natrl_forest, annual_gain_AGB_mangrove, cumul_gain_AGC_natrl_forest, cumul_gain_AGC_mangrove,
                annual_gain_BGB_natrl_forest, annual_gain_BGB_mangrove, cumul_gain_BGC_natrl_forest, cumul_gain_BGC_mangrove]

    if os.path.exists('{0}_{1}.tif'.format(utilities.pattern_annual_gain_AGB_mangrove, tile_id)):

        print "{} has mangroves".format(tile_id)

        # Opens first input tile
        with rasterio.open(annual_gain_AGB_natrl_forest) as annual_gain_AGB_natrl_forest_src:

            # Grabs metadata about the tif, like its location/projection/cellsize
            kwargs = annual_gain_AGB_natrl_forest_src.meta

            # Grabs the windows of the tile (stripes) so we can iterate over the entire tif without running out of memory
            windows = annual_gain_AGB_natrl_forest_src.block_windows(1)

            # Opens second input tile
            with rasterio.open(annual_gain_BGB_natrl_forest) as annual_gain_BGB_natrl_forest_src:
                # Opens third input tile
                with rasterio.open(annual_gain_AGB_mangrove) as annual_gain_AGB_mangrove_src:
                    # Opens fourth input tile
                    with rasterio.open(annual_gain_BGB_mangrove) as annual_gain_BGB_mangrove_src:
                        # Updates kwargs for the output dataset
                        kwargs.update(
                            driver='GTiff',
                            count=1,
                            compress='lzw',
                            nodata=0
                        )

                        # Opens the output tile, giving it the arguments of the input tiles
                        with rasterio.open('{0}_{1}.tif'.format(utilities.pattern_annual_gain_combo, tile_id),
                                           'w', **kwargs) as dst:

                            # Iterates across the windows (1 pixel strips) of the input tile
                            for idx, window in windows:

                                # Creates windows for each input tile
                                annual_AGB_natrl_forest = annual_gain_AGB_natrl_forest_src.read(1, window=window)
                                annual_BGB_natrl_forest = annual_gain_BGB_natrl_forest_src.read(1, window=window)
                                annual_AGB_mangrove = annual_gain_AGB_mangrove_src.read(1, window=window)
                                annual_BGB_mangrove = annual_gain_BGB_mangrove_src.read(1, window=window)

                                # # Create a 0s array for the output
                                # dst_data = np.zeros((window.height, window.width), dtype='float32')

                                # Adds all the input tiles together to get the combined values
                                dst_data = annual_AGB_natrl_forest + annual_BGB_natrl_forest + annual_AGB_mangrove + annual_BGB_mangrove

                                dst.write_band(1, dst_data, window=window)

    else:

        print "{} does not have mangroves".format(tile_id)

        # Opens first input tile
        with rasterio.open(annual_gain_AGB_natrl_forest) as annual_gain_AGB_natrl_forest_src:

            # Grabs metadata about the tif, like its location/projection/cellsize
            kwargs = annual_gain_AGB_natrl_forest_src.meta

            # Grabs the windows of the tile (stripes) so we can iterate over the entire tif without running out of memory
            windows = annual_gain_AGB_natrl_forest_src.block_windows(1)

            # Opens second input tile
            with rasterio.open(annual_gain_BGB_natrl_forest) as annual_gain_BGB_natrl_forest_src:
                # Updates kwargs for the output dataset
                kwargs.update(
                    driver='GTiff',
                    count=1,
                    compress='lzw',
                    nodata=0
                )

                # Opens the output tile, giving it the arguments of the input tiles
                with rasterio.open('{0}_{1}.tif'.format(utilities.pattern_annual_gain_combo, tile_id),
                                   'w', **kwargs) as dst:

                    # Iterates across the windows (1 pixel strips) of the input tile
                    for idx, window in windows:

                        # Creates windows for each input tile
                        annual_AGB_natrl_forest = annual_gain_AGB_natrl_forest_src.read(1, window=window)
                        annual_BGB_natrl_forest = annual_gain_BGB_natrl_forest_src.read(1, window=window)

                        # # Create a 0s array for the output
                        # dst_data = np.zeros((window.height, window.width), dtype='float32')

                        # Adds all the input tiles together to get the combined values
                        dst_data = annual_AGB_natrl_forest + annual_BGB_natrl_forest

                        dst.write_band(1, dst_data, window=window)

        # Names of the annual gain rate and cumulative gain tiles for mangroves


    #     tiles_out = [annual_gain_AGB_natrl_forest_reclass, annual_gain_AGB_mangrove_reclass, cumul_gain_AGC_natrl_forest_reclass, cumul_gain_AGC_mangrove_reclass,
    #                       annual_gain_BGB_natrl_forest_reclass, annual_gain_BGB_mangrove_reclass, cumul_gain_BGC_natrl_forest_reclass, cumul_gain_BGC_mangrove_reclass]
    #
    #     # Removes the nodata values in the tiles because having nodata values kept gdal_calc from properly summing values.
    #     # The gdal_calc expression didn't know how to evaluate nodata values, so I had to remove them.
    #     print "  Removing nodata values in all annual and cumulative tiles for {}".format(tile_id)
    #     for in_tile, out_tile in zip(tiles_in, tiles_out):
    #
    #         cmd = ['gdal_translate', '-a_nodata', 'none', in_tile, out_tile]
    #         subprocess.check_call(cmd)
    #
    #     print "Combining annual above and belowground biomass gain rate tiles from different forest types for {}".format(tile_id)
    #     biomass_rate_sum_calc = '--calc=A+B+C+D'
    #     rate_sum_outfilename = '{0}_{1}.tif'.format(utilities.pattern_annual_gain_combo, tile_id)
    #     rate_sum_outfilearg = '--outfile={}'.format(rate_sum_outfilename)
    #     cmd = ['gdal_calc.py', '-A', annual_gain_AGB_natrl_forest_reclass, '-B', annual_gain_AGB_mangrove_reclass, '-C', annual_gain_BGB_natrl_forest_reclass, '-D', annual_gain_BGB_mangrove_reclass,
    #            biomass_rate_sum_calc, rate_sum_outfilearg, '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW']
    #     subprocess.check_call(cmd)
    #
    #     print "Combining cumulative above and belowground carbon gain tiles from different forest types for {}".format(tile_id)
    #     biomass_rate_sum_calc = '--calc=A+B+C+D'
    #     rate_sum_outfilename = '{0}_{1}.tif'.format(utilities.pattern_cumul_gain_combo, tile_id)
    #     rate_sum_outfilearg = '--outfile={}'.format(rate_sum_outfilename)
    #     cmd = ['gdal_calc.py', '-A', cumul_gain_AGC_natrl_forest_reclass, '-B', cumul_gain_AGC_mangrove_reclass, '-C', cumul_gain_BGC_natrl_forest_reclass, '-D', cumul_gain_BGC_mangrove_reclass,
    #            biomass_rate_sum_calc, rate_sum_outfilearg, '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW']
    #     subprocess.check_call(cmd)
    #
    #     # Delete intermediate tiles that take up memory
    #     for tile in tiles_out:
    #         os.remove(tile)
    #
    # else:
    #
    #     print "{} does not have mangroves".format(tile_id)
    #
    #     print "Combining annual above and belowground biomass gain rate tiles from different forest types for {}".format(tile_id)
    #     biomass_rate_sum_calc = '--calc=A+B'
    #     rate_sum_outfilename = '{0}_{1}.tif'.format(utilities.pattern_annual_gain_combo, tile_id)
    #     rate_sum_outfilearg = '--outfile={}'.format(rate_sum_outfilename)
    #     cmd = ['gdal_calc.py', '-A', annual_gain_AGB_natrl_forest, '-B', annual_gain_BGB_natrl_forest,
    #            biomass_rate_sum_calc, rate_sum_outfilearg, '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW']
    #     subprocess.check_call(cmd)
    #
    #     print "Combining cumulative above and belowground carbon gain tiles from different forest types for {}".format(tile_id)
    #     biomass_rate_sum_calc = '--calc=A+B'
    #     rate_sum_outfilename = '{0}_{1}.tif'.format(utilities.pattern_cumul_gain_combo, tile_id)
    #     rate_sum_outfilearg = '--outfile={}'.format(rate_sum_outfilename)
    #     cmd = ['gdal_calc.py', '-A', cumul_gain_AGC_natrl_forest, '-B', cumul_gain_BGC_natrl_forest,
    #            biomass_rate_sum_calc, rate_sum_outfilearg, '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW']
    #     subprocess.check_call(cmd)

    utilities.upload_final(utilities.pattern_annual_gain_combo, utilities.annual_gain_combo_dir, tile_id)
    # utilities.upload_final(utilities.pattern_cumul_gain_combo, utilities.cumul_gain_combo_dir, tile_id)

    end = datetime.datetime.now()
    elapsed_time = end-start

    print "  Processing time for tile", tile_id, ":", elapsed_time