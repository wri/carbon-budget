import datetime
import numpy as np
import os
import rasterio
import logging
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def model_extent(tile_id, pattern, sensit_type):

    # I don't know why, but this needs to be here and not just in mp_model_extent
    os.chdir(cn.docker_base_dir)

    uu.print_log("Delineating model extent:", tile_id)

    # Start time
    start = datetime.datetime.now()

    # Names of the input tiles
    mangrove = '{0}_{1}.tif'.format(tile_id, cn.pattern_mangrove_biomass_2000)
    gain = '{0}_{1}.tif'.format(cn.pattern_gain, tile_id)
    tcd = '{0}_{1}.tif'.format(cn.pattern_tcd, tile_id)
    pre_2000_plantations = '{0}_{1}.tif'.format(tile_id, cn.pattern_plant_pre_2000)

    # Biomass tile name depends on the sensitivity analysis
    if sensit_type == 'biomass_swap':
        biomass = '{0}_{1}.tif'.format(tile_id, cn.pattern_JPL_unmasked_processed)
        uu.print_log("Using JPL biomass tile for {} sensitivity analysis".format(sensit_type))
    else:
        biomass = '{0}_{1}.tif'.format(tile_id, cn.pattern_WHRC_biomass_2000_unmasked)
        uu.print_log("Using WHRC biomass tile for {} sensitivity analysis".format(sensit_type))

    out_tile = '{0}_{1}.tif'.format(tile_id, pattern)

    # Opens biomass tile
    with rasterio.open(tcd) as tcd_src:

        # Grabs metadata about the tif, like its location/projection/cellsize
        kwargs = tcd_src.meta

        # Grabs the windows of the tile (stripes) so we can iterate over the entire tif without running out of memory
        windows = tcd_src.block_windows(1)

        # Updates kwargs for the output dataset
        kwargs.update(
            driver='GTiff',
            count=1,
            compress='lzw',
            nodata=0
        )

        # Checks whether each input tile exists
        try:
            mangroves_src = rasterio.open(mangrove)
            uu.print_log("  Mangrove tile found for {}".format(tile_id))
        except:
            uu.print_log("  No mangrove tile found for {}".format(tile_id))

        try:
            gain_src = rasterio.open(gain)
            uu.print_log("  Gain tile found for {}".format(tile_id))
        except:
            uu.print_log("  No gain tile found for {}".format(tile_id))

        try:
            biomass_src = rasterio.open(biomass)
            uu.print_log("  Biomass tile found for {}".format(tile_id))
        except:
            uu.print_log("  No biomass tile found for {}".format(tile_id))

        try:
            pre_2000_plantations_src = rasterio.open(pre_2000_plantations)
            uu.print_log("  Pre-2000 plantation tile found for {}".format(tile_id))
        except:
            uu.print_log("  No pre-2000 plantation tile found for {}".format(tile_id))


        # Opens the output tile, giving it the metadata of the input tiles
        dst = rasterio.open(out_tile, 'w', **kwargs)

        # Adds metadata tags to the output raster
        uu.add_rasterio_tags(dst, sensit_type)
        dst.update_tags(
            units='unitless. 1 = in model extent. 0 = not in model extent')
        if sensit_type == 'biomass_swap':
            dst.update_tags(
                source='Pixels with ((Hansen 2000 tree cover AND NASA JPL AGB2000) OR Hansen gain OR mangrove biomass 2000) NOT pre-2000 plantations')
        else:
            dst.update_tags(
                source='Pixels with ((Hansen 2000 tree cover AND WHRC AGB2000) OR Hansen gain OR mangrove biomass 2000) NOT pre-2000 plantations')
        dst.update_tags(
            extent='Full model extent. This defines which pixels are included in the model.')


        uu.print_log("  Creating model extent for {}".format(tile_id))

        # Iterates across the windows (1 pixel strips) of the input tile
        for idx, window in windows:

            # Tries to create a window (array) for each input tile.
            # If the tile does exist, it creates an array of the values in the window
            # If the tile does not exist, it creates an array of 0s.
            try:
                mangrove_window = mangroves_src.read(1, window=window).astype('uint8')
            except:
                mangrove_window = np.zeros((window.height, window.width), dtype=int)
            try:
                gain_window = gain_src.read(1, window=window)
            except:
                gain_window = np.zeros((window.height, window.width), dtype=int)
            try:
                biomass_window = biomass_src.read(1, window=window)
            except:
                biomass_window = np.zeros((window.height, window.width), dtype=int)
            try:
                tcd_window = tcd_src.read(1, window=window)
            except:
                tcd_window = np.zeros((window.height, window.width), dtype=int)
            try:
                pre_2000_plantations_window = pre_2000_plantations_src.read(1, window=window)
            except:
                pre_2000_plantations_window = np.zeros((window.height, window.width), dtype=int)

            # Array of pixels that have both biomass and tree cover density
            tcd_with_biomass_window = np.where((biomass_window > 0) & (tcd_window > 0), 1, 0)
            # Array of pixels with (biomass AND tcd) OR mangrove biomass OR Hansen gain
            forest_extent = np.where((tcd_with_biomass_window == 1) | (mangrove_window > 1) | (gain_window == 1), 1, 0)

            # Array of pixels with (biomass AND tcd) OR mangrove biomass OR Hansen gain WITHOUT pre-2000 plantations
            forest_extent = np.where((forest_extent == 1) & (pre_2000_plantations_window == 0), 1, 0).astype('uint8')

            # Writes the output window to the output
            dst.write_band(1, forest_extent, window=window)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, pattern)