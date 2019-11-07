### Calculates the net emissions over the study period, with units of CO2/ha on a pixel-by-pixel basis

import os
import datetime
import rasterio
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def net_calc(tile_id, pattern, sensit_type):

    print "Calculating net flux for", tile_id

    # Start time
    start = datetime.datetime.now()

    # Names of the gain and emissions tiles
    gain_in = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_cumul_gain_AGCO2_BGCO2_all_types, 'true')
    loss_in = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil, 'false')

    # Output net emissions file
    net_flux = '{0}_{1}.tif'.format(tile_id, pattern)

    # Opens cumulative gain input tile
    gain_src = rasterio.open(gain_in)

    # Grabs metadata about the tif, like its location/projection/cellsize
    kwargs = gain_src.meta

    # Grabs the windows of the tile (stripes) so we can iterate over the entire tif without running out of memory
    windows = gain_src.block_windows(1)

    # Opens loss tile
    loss_src = rasterio.open(loss_in)

    kwargs.update(
        driver='GTiff',
        count=1,
        compress='lzw',
        nodata=0,
        dtype='float32'
    )

    # Opens the output tile, giving it the arguments of the input tiles
    net_flux_dst = rasterio.open(net_flux, 'w', **kwargs)

    # Iterates across the windows (1 pixel strips) of the input tile
    for idx, window in windows:

        # Creates windows for each input tile
        gain = gain_src.read(1, window=window)
        loss = loss_src.read(1, window=window)

        # Subtracts gain that from loss
        dst_data = loss - gain

        net_flux_dst.write_band(1, dst_data, window=window)

    # Need to include these or the spot machine will run out of memory otherwise
    os.remove(gain_in)
    os.remove(loss_in)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, pattern)