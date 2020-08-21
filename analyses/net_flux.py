### Calculates the net emissions over the study period, with units of Mg CO2/ha on a pixel-by-pixel basis

import os
import datetime
import rasterio
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def net_calc(tile_id, pattern, sensit_type):

    uu.print_log("Calculating net flux for", tile_id)

    # Start time
    start = datetime.datetime.now()

    # Names of the gain and emissions tiles
    gain_in = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_cumul_gain_AGCO2_BGCO2_all_types)
    loss_in = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil)

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

    # Adds metadata tags to the output raster
    uu.add_rasterio_tags(net_flux_dst, sensit_type)
    net_flux_dst.update_tags(
        units='Mg CO2e/ha over model duration (2001-20{})'.format(cn.loss_years))
    net_flux_dst.update_tags(
        source='Gross emissions - gross removals')
    net_flux_dst.update_tags(
        extent='Model extent')
    net_flux_dst.update_tags(
        scale='Negative values are net sinks. Positive values are net sources.')

    # Iterates across the windows (1 pixel strips) of the input tile
    for idx, window in windows:

        # Creates windows for each input tile
        gain_window = gain_src.read(1, window=window)
        loss_window = loss_src.read(1, window=window)

        # Subtracts gain that from loss
        dst_data = loss_window - gain_window

        net_flux_dst.write_band(1, dst_data, window=window)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, pattern)