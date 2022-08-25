import datetime
import numpy as np
import os
import rasterio
import logging
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# @uu.counter
def model_extent(tile_id, pattern):

    # I don't know why, but this needs to be here and not just in mp_model_extent
    os.chdir(cn.docker_base_dir)

    uu.print_log(f'Delineating model extent: {tile_id}')

    # Start time
    start = datetime.datetime.now()

    # Names of the input tiles
    mangrove = f'{tile_id}_{cn.pattern_mangrove_biomass_2000}.tif'
    gain = f'{cn.pattern_gain}_{tile_id}.tif'
    pre_2000_plantations = f'{tile_id}_{cn.pattern_plant_pre_2000}.tif'

    # Tree cover tile name depends on the sensitivity analysis.
    # PRODES extent 2000 stands in for Hansen TCD
    if cn.SENSIT_TYPE == 'legal_Amazon_loss':
        tcd = f'{tile_id}_{cn.pattern_Brazil_forest_extent_2000_processed}.tif'
        uu.print_log(f'Using PRODES extent 2000 tile {tile_id} for {cn.SENSIT_TYPE} sensitivity analysis')
    else:
        tcd = f'{cn.pattern_tcd}_{tile_id}.tif'
        uu.print_log(f'Using Hansen tcd tile {tile_id} for {cn.SENSIT_TYPE} model run')

    # Biomass tile name depends on the sensitivity analysis
    if cn.SENSIT_TYPE == 'biomass_swap':
        biomass = f'{tile_id}_{cn.pattern_JPL_unmasked_processed}.tif'
        uu.print_log(f'Using JPL biomass tile {tile_id} for {cn.SENSIT_TYPE} sensitivity analysis')
    else:
        biomass = f'{tile_id}_{cn.pattern_WHRC_biomass_2000_unmasked}.tif'
        uu.print_log(f'Using WHRC biomass tile {tile_id} for {cn.SENSIT_TYPE} model run')

    out_tile = f'{tile_id}_{pattern}.tif'

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
            compress='DEFLATE',
            nodata=0
        )

        # Checks whether each input tile exists
        try:
            mangroves_src = rasterio.open(mangrove)
            uu.print_log(f'  Mangrove tile found for {tile_id}')
        except:
            uu.print_log(f'  No mangrove tile found for {tile_id}')

        try:
            gain_src = rasterio.open(gain)
            uu.print_log(f'  Gain tile found for {tile_id}')
        except:
            uu.print_log(f'  No gain tile found for {tile_id}')

        try:
            biomass_src = rasterio.open(biomass)
            uu.print_log(f'  Biomass tile found for {tile_id}')
        except:
            uu.print_log(f'  No biomass tile found for {tile_id}')

        try:
            pre_2000_plantations_src = rasterio.open(pre_2000_plantations)
            uu.print_log(f'  Pre-2000 plantation tile found for {tile_id}')
        except:
            uu.print_log(f'  No pre-2000 plantation tile found for {tile_id}')


        # Opens the output tile, giving it the metadata of the input tiles
        dst = rasterio.open(out_tile, 'w', **kwargs)

        # Adds metadata tags to the output raster
        uu.add_universal_metadata_rasterio(dst)
        dst.update_tags(
            units='unitless. 1 = in model extent. 0 = not in model extent')
        if cn.SENSIT_TYPE == 'biomass_swap':
            dst.update_tags(
                source='Pixels with ((Hansen 2000 tree cover AND NASA JPL AGB2000) OR Hansen gain OR mangrove biomass 2000) NOT pre-2000 plantations')
        else:
            dst.update_tags(
                source='Pixels with ((Hansen 2000 tree cover AND WHRC AGB2000) OR Hansen gain OR mangrove biomass 2000) NOT pre-2000 plantations')
        dst.update_tags(
            extent='Full model extent. This defines which pixels are included in the model.')


        uu.print_log(f'  Creating model extent for {tile_id}')

        uu.check_memory()

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

            # For all moel types except legal_Amazon_loss sensitivity analysis
            if cn.SENSIT_TYPE != 'legal_Amazon_loss':

                # Array of pixels with (biomass AND tcd) OR mangrove biomass OR Hansen gain
                forest_extent = np.where((tcd_with_biomass_window == 1) | (mangrove_window > 1) | (gain_window == 1), 1, 0)

                # extent now WITHOUT pre-2000 plantations
                forest_extent = np.where((forest_extent == 1) & (pre_2000_plantations_window == 0), 1, 0).astype('uint8')

            # For legal_Amazon_loss sensitivity analysis
            else:
                # Array of pixels with (biomass AND tcd) OR mangrove biomass.
                # Does not include mangrove or Hansen gain pixels that are outside PRODES 2000 forest extent
                forest_extent = tcd_with_biomass_window.astype('uint8')


            # Writes the output window to the output
            dst.write_band(1, forest_extent, window=window)



    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, pattern)