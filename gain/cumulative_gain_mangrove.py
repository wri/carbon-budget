### This script calculates the cumulative above and belowground carbon gain in mangrove forest pixels from 2001-2015.
### It multiplies the annual biomass gain rate by the number of years of gain by the biomass-to-carbon conversion.

import utilities
import datetime
import numpy as np
import rasterio
import subprocess


def cumulative_gain(tile_id):

    print "Processing:", tile_id

    # Start time
    start = datetime.datetime.now()

    # Names of the annual gain rate and gain year count tiles
    gain_rate_AGB = '{0}_{1}.tif'.format(utilities.pattern_annual_gain_AGB_mangrove, tile_id)
    gain_rate_BGB = '{0}_{1}.tif'.format(utilities.pattern_annual_gain_BGB_mangrove, tile_id)
    gain_year_count = '{0}_{1}.tif'.format(utilities.pattern_gain_year_count_mangrove, tile_id)

    accum_AGC = '{0}_{1}.tif'.format(utilities.pattern_cumul_gain_AGC_mangrove, tile_id)
    accum_BGC = '{0}_{1}.tif'.format(utilities.pattern_cumul_gain_BGC_mangrove, tile_id)

    dict = {gain_rate_AGB: accum_AGC, gain_rate_BGB, accum_BGC}

    print "  Reading input files and calculating cumulative gain for mangrove tile {}".format(tile_id)

    accum_calc = '--calc=A*B*{}'.format(utilities.biomass_to_c)
    accum_outfilename = '{0}_{1}.tif'.format(utilities.pattern_cumul_gain_AGC_mangrove, tile_id)
    accum_outfilearg = '--outfile={}'.format(accum_outfilename)
    cmd = ['gdal_calc.py', '-A', gain_rate_AGB, '-B', gain_year_count, accum_calc, accum_outfilearg, '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW']
    subprocess.check_call(cmd)

    # # Opens continent-ecozone tile
    # with rasterio.open(gain_rate) as gain_rate_src:
    #
    #     # Grabs metadata about the tif, like its location/projection/cellsize
    #     kwargs = gain_rate_src.meta
    #
    #     # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
    #     windows = gain_rate_src.block_windows(1)
    #
    #     # Opens gain year count tile
    #     with rasterio.open(gain_year_count) as gain_year_count_src:
    #
    #         # Updates kwargs for the output dataset.
    #         # Need to update data type to float 32 so that it can handle fractional gain rates
    #         kwargs.update(
    #             driver='GTiff',
    #             count=1,
    #             compress='lzw',
    #             nodata=0
    #         )
    #
    #         # Opens the output tile, giving it the arguments of the input tiles
    #         with rasterio.open('{0}_{1}.tif'.format(utilities.pattern_cumul_gain_AGC_mangrove, tile_id), 'w', **kwargs) as dst:
    #
    #             # Iterates across the windows (1 pixel strips) of the input tile
    #             for idx, window in windows:
    #
    #                 # Creates windows for each input raster
    #                 gain_rate = gain_rate_src.read(1, window=window)
    #                 gain_year_count = gain_year_count_src.read(1, window=window)
    #
    #                 # Multiplies the annual gain rate by the number of years with gain by the biomass to carbon conversion
    #                 dst_data = gain_rate * gain_year_count * utilities.biomass_to_c
    #
    #                 # Writes the output window to the output
    #                 dst.write_band(1, dst_data, window=window)

    utilities.upload_final(utilities.pattern_cumul_gain_AGC_mangrove, utilities.cumul_gain_AGC_mangrove_dir, tile_id)

    end = datetime.datetime.now()
    elapsed_time = end-start

    print "  Processing time for tile", tile_id, ":", elapsed_time