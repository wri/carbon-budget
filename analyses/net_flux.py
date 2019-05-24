### Calculates the net emissions over the study period, with units of CO2/ha on a pixel-by-pixel basis

import utilities
import os
import datetime
import rasterio
import subprocess
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def net_calc(tile_id):

    print "Calculating net emissions for", tile_id

    # Start time
    start = datetime.datetime.now()

    # Names of the annual gain rate and cumulative gain tiles for non-mangrove natural forests
    gain_in = '{0}_{1}.tif'.format(tile_id, cn.pattern_cumul_gain_combo)
    loss_in = '{0}_{1}.tif'.format(tile_id, cn.pattern_gross_emis_all_drivers)

    # Output net emissions file
    net_emis = '{0}_{1}.tif'.format(tile_id, cn.pattern_net_flux)

    # Opens cumulative gain input tile
    with rasterio.open(gain_in) as gain_src:

        # Grabs metadata about the tif, like its location/projection/cellsize
        kwargs = gain_src.meta

        # Grabs the windows of the tile (stripes) so we can iterate over the entire tif without running out of memory
        windows = gain_src.block_windows(1)

        # Opens loss tile
        with rasterio.open(loss_in) as loss_src:
            kwargs.update(
                driver='GTiff',
                count=1,
                compress='lzw',
                nodata=0,
                dtype='float32'
            )

            # Opens the output tile, giving it the arguments of the input tiles
            with rasterio.open(net_emis, 'w', **kwargs) as dst:

                # Iterates across the windows (1 pixel strips) of the input tile
                for idx, window in windows:

                    # Creates windows for each input tile
                    gain = gain_src.read(1, window=window)
                    loss = loss_src.read(1, window=window)

                    # Converts gain from C to CO2 and subtracts that from loss
                    dst_data = loss - (gain*cn.c_to_co2)

                    dst.write_band(1, dst_data, window=window)

    # Need to include these or the spot machine will run out of memory otherwise
    os.remove(gain_in)
    os.remove(loss_in)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, os.path.join(cn.pattern_gross_emis_commod))