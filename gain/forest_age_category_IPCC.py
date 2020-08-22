import datetime
import numpy as np
import os
import rasterio
import logging
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def forest_age_category(tile_id, gain_table_dict, pattern, sensit_type):

    uu.print_log("Assigning forest age categories:", tile_id)

    # Start time
    start = datetime.datetime.now()

    # Gets the bounding coordinates of each tile. Needed to determine if the tile is in the tropics (within 30 deg of the equator)
    xmin, ymin, xmax, ymax = uu.coords(tile_id)
    uu.print_log("  ymax:", ymax)

    # Default value is that the tile is not in the tropics
    tropics = 0

    # Criteria for assigning a tile to the tropics
    if (ymax > -30) & (ymax <= 30) :

        tropics = 1

    uu.print_log("  Tile in tropics:", tropics)

    # Names of the input tiles
    gain = '{0}_{1}.tif'.format(cn.pattern_gain, tile_id)
    model_extent = '{0}_{1}.tif'.format(tile_id, cn.pattern_model_extent)
    ifl_primary = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_ifl_primary)
    biomass = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_WHRC_biomass_2000_unmasked)
    cont_eco = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_cont_eco_processed)

    if sensit_type == 'Mekong_loss':
        loss = '{}_{}.tif'.format(tile_id, cn.pattern_Mekong_loss_processed)
    else:
        loss = '{}_{}.tif'.format(cn.pattern_loss, tile_id)

    uu.print_log("  Assigning age categories")

    # Opens biomass tile
    with rasterio.open(model_extent) as model_extent_src:

        # Grabs metadata about the tif, like its location/projection/cellsize
        kwargs = model_extent_src.meta

        # Grabs the windows of the tile (stripes) so we can iterate over the entire tif without running out of memory
        windows = model_extent_src.block_windows(1)

        # continent-ecozone is necessary for this script to work. If it does not exist, tile is skipped.
        try:
            cont_eco_src = rasterio.open(cont_eco)
            uu.print_log("   Continent-ecozone tile found for {}".format(tile_id))
        except:
            uu.print_log("    No continent-ecozone tile found for {}".format(tile_id))

        try:
            gain_src = rasterio.open(gain)
            uu.print_log("  Gain tile found for {}".format(tile_id))
        except:
            uu.print_log("    No gain tile found for {}".format(tile_id))

        try:
            biomass_src = rasterio.open(biomass)
            uu.print_log("   WHRC biomass tile found for {}".format(tile_id))
        except:
            uu.print_log("    No WHRC biomass tile found for {}".format(tile_id))

        try:
            loss_src = rasterio.open(loss)
            uu.print_log("   Loss tile found for {}".format(tile_id))
        except:
            uu.print_log("    No loss tile found for {}".format(tile_id))

        try:
            ifl_primary_src = rasterio.open(ifl_primary)
            uu.print_log("   IFL-primary forest tile found for {}".format(tile_id))
        except:
            uu.print_log("    No IFL-primary forest tile found for {}".format(tile_id))

        # Updates kwargs for the output dataset
        kwargs.update(
            driver='GTiff',
            count=1,
            compress='lzw',
            nodata=0
        )

        # Opens the output tile, giving it the arguments of the input tiles
        dst = rasterio.open('{0}_{1}.tif'.format(tile_id, pattern), 'w', **kwargs)

        # Adds metadata tags to the output raster
        uu.add_rasterio_tags(dst, sensit_type)
        dst.update_tags(
            key='1: young (<20 year) secondary forest; 2: old (>20 year) secondary forest; 3: primary forest or IFL')
        dst.update_tags(
            source='Decision tree that uses Hansen gain and loss, IFL/primary forest extent, and aboveground biomass to assign an age category')
        dst.update_tags(
            extent='Full model extent, even though these age categories will not be used over the full model extent. They apply to just the rates from IPCC defaults.')


        # Iterates across the windows (1 pixel strips) of the input tile
        for idx, window in windows:

            # Creates windows for each input raster. Only model_extent_src is guaranteed to exist
            model_extent_window = model_extent_src.read(1, window=window)

            try:
                loss_window = loss_src.read(1, window=window).astype('uint8')
            except:
                loss_window = np.zeros((window.height, window.width), dtype=int)

            try:
                gain_window = gain_src.read(1, window=window).astype('uint8')
            except:
                gain_window = np.zeros((window.height, window.width), dtype=int)

            try:
                cont_eco_window = cont_eco_src.read(1, window=window).astype('uint8')
            except:
                cont_eco_window = np.zeros((window.height, window.width), dtype=int)

            try:
                biomass_window = biomass_src.read(1, window=window).astype('uint8')
            except:
                biomass_window = np.zeros((window.height, window.width), dtype=int)

            try:
                ifl_primary_window = ifl_primary_src.read(1, window=window).astype('uint8')
            except:
                ifl_primary_window = np.zeros((window.height, window.width), dtype=int)

            # Creates a numpy array that has the <=20 year secondary forest growth rate x 20
            # based on the continent-ecozone code of each pixel (the dictionary).
            # This is used to assign pixels to the correct age category.
            gain_20_years = np.vectorize(gain_table_dict.get)(cont_eco_window)*20

            # Create a 0s array for the output
            dst_data = np.zeros((window.height, window.width), dtype='uint8')

            # Logic tree for assigning age categories begins here
            # Code 1 = young (<20 years) secondary forest, code 2 = old (>20 year) secondary forest, code 3 = primary forest
            # model_extent_window ensures that there is both biomass and tree cover in 2000 OR mangroves OR tree cover gain
            # WITHOUT pre-2000 plantations

            # No change pixels- no loss or gain
            if tropics == 0:

                dst_data[np.where((model_extent_window > 0) & (gain_window == 0) & (loss_window == 0))] = 2

            if tropics == 1:

                dst_data[np.where((model_extent_window > 0) & (gain_window == 0) & (loss_window == 0) & (ifl_primary_window != 1))] = 2
                dst_data[np.where((model_extent_window > 0) & (gain_window == 0) & (loss_window == 0) & (ifl_primary_window == 1))] = 3

            # Loss-only pixels
            dst_data[np.where((model_extent_window > 0) & (gain_window == 0) & (loss_window > 0) & (ifl_primary_window != 1) & (biomass_window <= gain_20_years))] = 1
            dst_data[np.where((model_extent_window > 0) & (gain_window == 0) & (loss_window > 0) & (ifl_primary_window != 1) & (biomass_window > gain_20_years))] = 2
            dst_data[np.where((model_extent_window > 0) & (gain_window == 0) & (loss_window > 0) & (ifl_primary_window ==1))] = 3

            # Gain-only pixels
            # If there is gain, the pixel doesn't need biomass or canopy cover. It just needs to be outside of plantations and mangroves.
            # The role of model_extent_window here is to exclude the pre-2000 plantations.
            dst_data[np.where((model_extent_window > 0) & (gain_window == 1) & (loss_window == 0))] = 1

            # Pixels with loss and gain
            # If there is gain with loss, the pixel doesn't need biomass or canopy cover. It just needs to be outside of plantations and mangroves.
            # The role of model_extent_window here is to exclude the pre-2000 plantations.
            dst_data[np.where((model_extent_window > 0) & (gain_window == 1) & (loss_window > (cn.gain_years)))] = 1
            dst_data[np.where((model_extent_window > 0) & (gain_window == 1) & (loss_window > 0) & (loss_window <= (cn.gain_years/2)))] = 1
            dst_data[np.where((model_extent_window > 0) & (gain_window == 1) & (loss_window > (cn.gain_years/2)) & (loss_window <= cn.gain_years))] = 1

            # Writes the output window to the output
            dst.write_band(1, dst_data, window=window)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, pattern)