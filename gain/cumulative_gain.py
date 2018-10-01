### This script calculates the cumulative carbon gain in natural forest pixels from 2001-2015.
### It multiplies the annual biomass gain rate by the number of years of gain by the biomass-to-carbon conversion

import utilities
import datetime
import numpy as np
import rasterio


def cumulative_gain(tile_id):

    upload_dir = 's3://gfw2-data/climate/carbon_model/cumulative_gain/20181001/'

    print "Processing:", tile_id

    # Start time
    start = datetime.datetime.now()

    # Names of the forest age category and continent-ecozone tiles
    gain_rate = 'annual_gain_rate_{}.tif'.format(tile_id)
    gain_year_count = 'gain_year_count_{}.tif'.format(tile_id)

    print "  Reading input files and calculating cumulative gain"

    # Opens continent-ecozone tile
    with rasterio.open(gain_rate) as gain_rate_src:

        # Grabs metadata about the tif, like its location/projection/cellsize
        kwargs = gain_rate_src.meta

        # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
        windows = gain_rate_src.block_windows(1)

        # Opens age category tile
        with rasterio.open(gain_year_count) as gain_year_count_src:

            # Updates kwargs for the output dataset.
            # Need to update data type to float 32 so that it can handle fractional gain rates
            kwargs.update(
                driver='GTiff',
                count=1,
                compress='lzw',
                nodata=0
            )

            # Opens the output tile, giving it the arguments of the input tiles
            with rasterio.open('aboveground_C_gain_{}.tif'.format(tile_id), 'w', **kwargs) as dst:

                # Iterates across the windows (1 pixel strips) of the input tile
                for idx, window in windows:

                    # Creates windows for each input raster
                    gain_rate = gain_rate_src.read(1, window=window)
                    gain_year_count = gain_year_count_src.read(1, window=window)

                    # Multiplies the annual gain rate by the number of years with gain by the biomass to carbon conversion
                    dst_data = gain_rate * gain_year_count * utilities.biomass_to_c

                    # Writes the output window to the output
                    dst.write_band(1, dst_data, window=window)

    pattern = 'aboveground_C_gain'

    utilities.upload_final(pattern, upload_dir, tile_id)

    end = datetime.datetime.now()
    elapsed_time = end-start

    print "  Processing time for tile", tile_id, ":", elapsed_time