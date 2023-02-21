"""
Function to create net flux tiles
"""

import datetime
import numpy as np
import rasterio
import sys
from memory_profiler import profile

sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

@profile
def net_calc(tile_id, pattern):
    """
    Creates net GHG flux tile set
    :param tile_id: tile to be processed, identified by its tile id
    :param pattern: pattern for output tile names
    :return: 1 tile with net GHG flux (gross emissions minus gross removals).
        Units: Mg CO2e/ha over the model period
    """

    uu.print_log("Calculating net flux for", tile_id)

    # Start time
    start = datetime.datetime.now()

    # Names of the removals and emissions tiles
    removals_in = uu.sensit_tile_rename(cn.SENSIT_TYPE, tile_id, cn.pattern_cumul_gain_AGCO2_BGCO2_all_types)
    emissions_in = uu.sensit_tile_rename(cn.SENSIT_TYPE, tile_id, cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil)

    # Output net emissions file
    net_flux = uu.make_tile_name(tile_id, pattern)

    try:
        removals_src = rasterio.open(removals_in)
        # Grabs metadata about the tif, like its location/projection/cellsize
        kwargs = removals_src.meta
        # Grabs the windows of the tile (stripes) so we can iterate over the entire tif without running out of memory
        windows = removals_src.block_windows(1)
        uu.print_log(f'   Gross removals tile found for {removals_in}')
    except rasterio.errors.RasterioIOError:
        uu.print_log(f'   No gross removals tile found for {removals_in}')

    try:
        emissions_src = rasterio.open(emissions_in)
        # Grabs metadata about the tif, like its location/projection/cellsize
        kwargs = emissions_src.meta
        # Grabs the windows of the tile (stripes) so we can iterate over the entire tif without running out of memory
        windows = emissions_src.block_windows(1)
        uu.print_log(f'   Gross emissions tile found for {emissions_in}')
    except rasterio.errors.RasterioIOError:
        uu.print_log(f'   No gross emissions tile found for {emissions_in}')

    # Skips the tile if there is neither a gross emissions nor a gross removals tile.
    # This should only occur for biomass_swap sensitivity analysis, which gets its net flux tile list from
    # the JPL tile list (some tiles of which have neither emissions nor removals), rather than the union of
    # emissions and removals tiles.
    try:
        kwargs.update(
            driver='GTiff',
            count=1,
            compress='DEFLATE',
            nodata=0,
            dtype='float32'
        )
    except rasterio.errors.RasterioIOError:
        uu.print_log(f'No gross emissions or gross removals for {tile_id}. Skipping tile.')
        return

    # Opens the output tile, giving it the arguments of the input tiles
    net_flux_dst = rasterio.open(net_flux, 'w', **kwargs)

    # Adds metadata tags to the output raster
    uu.add_universal_metadata_rasterio(net_flux_dst)
    net_flux_dst.update_tags(
        units=f'Mg CO2e/ha over model duration (2001-20{cn.loss_years})')
    net_flux_dst.update_tags(
        source='Gross emissions - gross removals')
    net_flux_dst.update_tags(
        extent='Model extent')
    net_flux_dst.update_tags(
        scale='Negative values are net sinks. Positive values are net sources.')

    uu.check_memory()

    # Iterates across the windows (1 pixel strips) of the input tile
    for idx, window in windows:

        # Creates windows for each input tile
        try:
            removals_window = removals_src.read(1, window=window).astype('float32')
        except UnboundLocalError:
            removals_window = np.zeros((window.height, window.width)).astype('float32')
        try:
            emissions_window = emissions_src.read(1, window=window).astype('float32')
        except UnboundLocalError:
            emissions_window = np.zeros((window.height, window.width)).astype('float32')

        # Subtracts removals from emissions to calculate net flux (negative is net sink, positive is net source)
        dst_data = emissions_window - removals_window

        net_flux_dst.write_band(1, dst_data, window=window)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, pattern)
