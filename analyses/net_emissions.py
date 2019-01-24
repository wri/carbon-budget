### Calculates the net emissions over the study period, with units of CO2/ha on a pixel-by-pixel basis

import utilities
import os
import datetime
import rasterio
import subprocess
import sys
sys.path.append('../')
import constants_and_names as cn

def net_calc(tile_id):

    print "Calculating net emissions for", tile_id

    # Start time
    start = datetime.datetime.now()

    # Names of the annual gain rate and cumulative gain tiles for non-mangrove natural forests
    gain_in = '{0}_{1}.tif'.format(cn.pattern_cumul_gain_combo, tile_id)
    loss_in = '{0}_{1}.tif'.format(tile_id, cn.pattern_gross_emissions)

    # Emissions nodata values are currently -9999, which messes up the net calculation. This converts the
    # emissions nodata values to nothing so that they aren't part of the net calculation.
    print "Removing nodata values in emissions tile", tile_id
    loss_nodata = '{0}_{1}_without_nodata.tif'.format(tile_id, cn.pattern_gross_emissions)
    cmd = ['gdalwarp', '-srcnodata', '-9999', '-dstnodata', 'none', '-overwrite', loss_in, loss_nodata]
    subprocess.check_call(cmd)

    # Output net emissions file
    net_emis = '{0}_{1}.tif'.format(tile_id, cn.pattern_net_flux)

    # Opens cumulative gain input tile
    with rasterio.open(gain_in) as gain_src:

        # Grabs metadata about the tif, like its location/projection/cellsize
        kwargs = gain_src.meta

        # Grabs the windows of the tile (stripes) so we can iterate over the entire tif without running out of memory
        windows = gain_src.block_windows(1)

        # Opens loss tile
        ######## Making nodata = -9999 outputs the wrong results
        with rasterio.open(loss_nodata) as loss_src:
            kwargs.update(
                driver='GTiff',
                count=1,
                compress='lzw',
                nodata=0
            )

            # Opens the output tile, giving it the arguments of the input tiles
            with rasterio.open(net_emis, 'w', **kwargs) as dst:

                # Iterates across the windows (1 pixel strips) of the input tile
                for idx, window in windows:

                    # Creates windows for each input tile
                    gain = gain_src.read(1, window=window)
                    loss = loss_src.read(1, window=window)

                    # Converts gain from C to CO2 and subtracts that from loss
                    dst_data = loss - gain*cn.c_to_co2

                    dst.write_band(1, dst_data, window=window)

    end = datetime.datetime.now()
    elapsed_time = end-start

    # Need to include these or the spot machine will run out of memory otherwise
    os.remove(gain_in)
    os.remove(loss_in)
    os.remove(loss_nodata)

    print "  Processing time for tile", tile_id, ":", elapsed_time