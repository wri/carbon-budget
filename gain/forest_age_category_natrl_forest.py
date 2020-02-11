### This script creates tiles of natural non-mangrove forest age category according to a decision tree.
### The age categories are: <= 20 year old secondary forest, >20 year old secondary forest, and primary forest.
### The decision tree uses several input tiles, including IFL status, gain, and loss.
### Downloading all of these tiles can take awhile.
### The decision tree is implemented as a series of numpy array statements rather than as nested if statements or gdal_calc operations.
### The output tiles have 10 possible values, each value representing an end of the decision tree.
### These 10 values map to the three forest age categories.
### The forest age category tiles are inputs for assigning gain rates to pixels.

import datetime
import numpy as np
import os
import rasterio
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def forest_age_category(tile_id, gain_table_dict, pattern, sensit_type):

    print "Assigning forest age categories:", tile_id

    # Start time
    start = datetime.datetime.now()

    # Gets the bounding coordinates of each tile. Needed to determine if the tile is in the tropics (within 30 deg of the equator)
    xmin, ymin, xmax, ymax = uu.coords(tile_id)
    print "  ymax:", ymax

    # Default value is that the tile is not in the tropics
    tropics = 0

    # Criteria for assigning a tile to the tropics
    if (ymax > -30) & (ymax <= 30) :

        tropics = 1

    print "  Tile in tropics:", tropics

    # Names of the input tiles
    gain = '{0}_{1}.tif'.format(cn.pattern_gain, tile_id)
    tcd = '{0}_{1}.tif'.format(cn.pattern_tcd, tile_id)
    ifl_primary = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_ifl_primary)
    biomass = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_WHRC_biomass_2000_non_mang_non_planted)
    cont_eco = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_cont_eco_processed)
    plantations = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_planted_forest_type_unmasked)
    mangroves = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_mangrove_biomass_2000)

    if sensit_type == 'Mekong_loss':
        loss = '{}_{}.tif'.format(tile_id, cn.pattern_Mekong_loss_processed)
    else:
        loss = '{}.tif'.format(tile_id)

    print "  Reading input files and evaluating conditions"

    # Opens biomass tile
    with rasterio.open(loss) as loss_src:

        # Grabs metadata about the tif, like its location/projection/cellsize
        kwargs = loss_src.meta

        # Grabs the windows of the tile (stripes) so we can iterate over the entire tif without running out of memory
        windows = loss_src.block_windows(1)

        # Opens gain tile
        gain_src = rasterio.open(gain)
        ifl_primary_src = rasterio.open(ifl_primary)
        cont_eco_src = rasterio.open(cont_eco)
        biomass_src = rasterio.open(biomass)
        extent_src = rasterio.open(tcd)

        # Checks whether there are mangrove or planted forest tiles. If so, they are opened.
        try:
            plantations_src = rasterio.open(plantations)
            print "    Planted forest tile found for {}".format(tile_id)
        except:
            print "    No planted forest tile for {}".format(tile_id)

        try:
            mangroves_src = rasterio.open(mangroves)
            print "    Mangrove tile found for {}".format(tile_id)
        except:
            print "    No mangrove tile for {}".format(tile_id)


        # Updates kwargs for the output dataset
        kwargs.update(
            driver='GTiff',
            count=1,
            compress='lzw',
            nodata=0
        )

        # Opens the output tile, giving it the arguments of the input tiles
        dst = rasterio.open('{0}_{1}.tif'.format(tile_id, pattern), 'w', **kwargs)

        # Iterates across the windows (1 pixel strips) of the input tile
        for idx, window in windows:

            # Creates windows for each input raster
            loss_window = loss_src.read(1, window=window)
            gain_window = gain_src.read(1, window=window)
            tcd_window = extent_src.read(1, window=window)
            ifl_primary_window = ifl_primary_src.read(1, window=window)
            cont_eco_window = cont_eco_src.read(1, window=window)
            biomass_window = biomass_src.read(1, window=window)

            # Creates a numpy array that has the <=20 year secondary forest growth rate x 20
            # based on the continent-ecozone code of each pixel (the dictionary).
            # This is used to assign pixels to the correct age category.
            gain_20_years = np.vectorize(gain_table_dict.get)(cont_eco_window)*20

            # Create a 0s array for the output
            dst_data = np.zeros((window.height, window.width), dtype='uint8')

            # Logic tree for assigning age categories begins here
            # No change pixels- no loss or gain
            # If there is no change, biomass (with mangroves and planted forests masked out)
            # and canopy cover are required to include the pixel in the model
            if tropics == 0:

                dst_data[np.where((biomass_window > 0) & (tcd_window > 0) & (gain_window == 0) & (loss_window == 0))] = 1

            if tropics == 1:

                dst_data[np.where((biomass_window > 0) & (tcd_window > 0) & (gain_window == 0) & (loss_window == 0) & (ifl_primary_window != 1))] = 2
                dst_data[np.where((biomass_window > 0) & (tcd_window > 0) & (gain_window == 0) & (loss_window == 0) & (ifl_primary_window == 1))] = 3

            # Loss-only pixels
            # If there is only loss, biomass (with mangroves and planted forests masked out) is required to include the pixel in the model.
            dst_data[np.where((biomass_window > 0) & (gain_window == 0) & (loss_window > 0) & (ifl_primary_window != 1) & (biomass_window <= gain_20_years))] = 4
            dst_data[np.where((biomass_window > 0) & (gain_window == 0) & (loss_window > 0) & (ifl_primary_window != 1) & (biomass_window > gain_20_years))] = 5
            dst_data[np.where((biomass_window > 0) & (gain_window == 0) & (loss_window > 0) & (ifl_primary_window ==1))] = 6

            # Since the gain-only and loss-and-gain pixels are supposed to exclude mangroves and planted forests.
            # Need separate conditions to do that since not every tile has mangroves and/or plantations
            if os.path.exists(mangroves) & os.path.exists(plantations):

                plantations_window = plantations_src.read(1, window=window)
                mangroves_window = mangroves_src.read(1, window=window)

                # Gain-only pixels
                # If there is gain, the pixel doesn't need biomass or canopy cover. It just needs to be outside of plantations and mangroves.
                dst_data[np.where((plantations_window == 0) & (mangroves_window == 0) & (gain_window == 1) & (loss_window == 0))] = 7

                # Pixels with loss and gain
                # If there is gain with loss, the pixel doesn't need biomass or canopy cover. It just needs to be outside of plantations and mangroves.
                dst_data[np.where((plantations_window == 0) & (mangroves_window == 0) & (gain_window == 1) & (loss_window >= 13))] = 8
                dst_data[np.where((plantations_window == 0) & (mangroves_window == 0) & (gain_window == 1) & (loss_window > 0) & (loss_window <= 6))] = 9
                dst_data[np.where((plantations_window == 0) & (mangroves_window == 0) & (gain_window == 1) & (loss_window > 6) & (loss_window < 13))] = 10

            elif os.path.exists(mangroves):

                mangroves_window = mangroves_src.read(1, window=window)

                # Gain-only pixels
                # If there is gain, the pixel doesn't need biomass or canopy cover. It just needs to be outside of plantations and mangroves.
                dst_data[np.where((mangroves_window == 0) & (gain_window == 1) & (loss_window == 0))] = 7

                # Pixels with loss and gain
                # If there is gain with loss, the pixel doesn't need biomass or canopy cover. It just needs to be outside of plantations and mangroves.
                dst_data[np.where((mangroves_window == 0) & (gain_window == 1) & (loss_window >= 13))] = 8
                dst_data[np.where((mangroves_window == 0) & (gain_window == 1) & (loss_window > 0) & (loss_window <= 6))] = 9
                dst_data[np.where((mangroves_window == 0) & (gain_window == 1) & (loss_window > 6) & (loss_window < 13))] = 10

            elif os.path.exists(plantations):

                plantations_window = plantations_src.read(1, window=window)

                # Gain-only pixels
                # If there is gain, the pixel doesn't need biomass or canopy cover. It just needs to be outside of plantations and mangroves.
                dst_data[np.where((plantations_window == 0) & (gain_window == 1) & (loss_window == 0))] = 7

                # Pixels with loss and gain
                # If there is gain with loss, the pixel doesn't need biomass or canopy cover. It just needs to be outside of plantations and mangroves.
                dst_data[np.where((plantations_window == 0) & (gain_window == 1) & (loss_window >= 13))] = 8
                dst_data[np.where((plantations_window == 0) & (gain_window == 1) & (loss_window > 0) & (loss_window <= 6))] = 9
                dst_data[np.where((plantations_window == 0) & (gain_window == 1) & (loss_window > 6) & (loss_window < 13))] = 10

            else:

                # Gain-only pixels
                # If there is gain, the pixel doesn't need biomass or canopy cover. It just needs to be outside of plantations and mangroves.
                dst_data[np.where((gain_window == 1) & (loss_window == 0))] = 7

                # Pixels with loss and gain
                # If there is gain with loss, the pixel doesn't need biomass or canopy cover. It just needs to be outside of plantations and mangroves.
                dst_data[np.where((gain_window == 1) & (loss_window >= 13))] = 8
                dst_data[np.where((gain_window == 1) & (loss_window > 0) & (loss_window <= 6))] = 9
                dst_data[np.where((gain_window == 1) & (loss_window > 6) & (loss_window < 13))] = 10

            # Writes the output window to the output
            dst.write_band(1, dst_data, window=window)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, pattern)