import numpy as np
import datetime
import rasterio
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu


def legal_Amazon_forest_age_category(tile_id, sensit_type, output_pattern):

    # Start time
    start = datetime.datetime.now()

    loss = '{0}_{1}.tif'.format(tile_id, cn.pattern_Brazil_annual_loss_processed)
    gain = '{0}_{1}.tif'.format(cn.pattern_gain, tile_id)
    extent = '{0}_{1}.tif'.format(tile_id, cn.pattern_Brazil_forest_extent_2000_processed)
    biomass = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_WHRC_biomass_2000_non_mang_non_planted)
    plantations = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_planted_forest_type_unmasked)
    mangroves = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_mangrove_biomass_2000)

    # Opens biomass tile
    with rasterio.open(loss) as loss_src:

        # Grabs metadata about the tif, like its location/projection/cellsize
        kwargs = loss_src.meta

        # Grabs the windows of the tile (stripes) so we can iterate over the entire tif without running out of memory
        windows = loss_src.block_windows(1)

        # Opens tiles
        gain_src = rasterio.open(gain)
        extent_src = rasterio.open(extent)
        biomass_src = rasterio.open(biomass)

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
        dst = rasterio.open('{0}_{1}.tif'.format(tile_id, output_pattern), 'w', **kwargs)

        # Iterates across the windows (1 pixel strips) of the input tile
        for idx, window in windows:

            # Creates windows for each input raster
            loss_window = loss_src.read(1, window=window)
            gain_window = gain_src.read(1, window=window)
            extent_window = extent_src.read(1, window=window)
            biomass_window = biomass_src.read(1, window=window)

            # Create a 0s array for the output
            dst_data = np.zeros((window.height, window.width), dtype='Byte')

            # No change pixels (no loss or gain)
            dst_data[np.where((biomass_window > 0) & (extent_window == 1) & (loss_window == 0))] = 3  # primary forest

            # Loss-only pixels
            dst_data[np.where((biomass_window > 0) & (extent_window == 1) & (loss_window > 0))] = 6   # primary forest

            # Loss-and-gain pixels
            dst_data[np.where((extent_window == 1) & (gain_window == 1) & (loss_window > 0))] = 8   # young secondary forest

            # if os.path.exists(mangroves):
            #     mangroves_window = mangroves_src.read(1, window=window)
            #     dst_data = np.ma.masked_where(mangroves_window == 0, dst_data).filled(0)
            #
            # if os.path.exists(plantations):
            #     plantations_window = plantations_src.read(1, window=window)
            #     dst_data = np.ma.masked_where(plantations_window == 0, dst_data).filled(0)

            # Writes the output window to the output
            dst.write_band(1, dst_data, window=window)

            # # Since the gain-only and loss-and-gain pixels are supposed to exclude mangroves and planted forests.
            # # Need separate conditions to do that since not every tile has mangroves and/or plantations
            # if os.path.exists(mangroves) & os.path.exists(plantations):
            #
            #     plantations_window = plantations_src.read(1, window=window)
            #     mangroves_window = mangroves_src.read(1, window=window)
            #
            #     # Pixels with loss and gain
            #     # If there is gain with loss, the pixel doesn't need biomass or canopy cover. It just needs to be outside of plantations and mangroves.
            #     dst_data[np.where((extent_window == 1) & (plantations_window == 0) & (mangroves_window == 0)
            #                       & (gain_window == 1) & (loss_window > 0))] = 8
            #
            # elif os.path.exists(mangroves):
            #
            #     mangroves_window = mangroves_src.read(1, window=window)
            #
            #     # Pixels with loss and gain
            #     # If there is gain with loss, the pixel doesn't need biomass or canopy cover. It just needs to be outside of plantations and mangroves.
            #     dst_data[np.where((extent_window == 1) & (mangroves_window == 0) & (gain_window == 1) & (loss_window > 0))] = 8
            #
            # elif os.path.exists(plantations):
            #
            #     plantations_window = plantations_src.read(1, window=window)
            #
            #     # Pixels with loss and gain
            #     # If there is gain with loss, the pixel doesn't need biomass or canopy cover. It just needs to be outside of plantations and mangroves.
            #     dst_data[np.where((extent_window == 1) & (plantations_window == 0) & (gain_window == 1) & (loss_window > 0))] = 8
            #
            # else:
            #
            #     # Pixels with loss and gain
            #     # If there is gain with loss, the pixel doesn't need biomass or canopy cover. It just needs to be outside of plantations and mangroves.
            #     dst_data[np.where((extent_window == 1) & (gain_window == 1) & (loss_window > 0))] = 8


    uu.end_of_fx_summary(start, tile_id, output_pattern)