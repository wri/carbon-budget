import datetime
import numpy as np
import os
import rasterio
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Creates Hansen tiles out of FIA region shapefile
def prep_FIA_regions(tile_id):

    print "Creating Hansen tile for FIA regions"

    # Start time
    start = datetime.datetime.now()

    print "Getting extent of", tile_id
    xmin, ymin, xmax, ymax = uu.coords(tile_id)


    print "Rasterizing FIA region shapefile", tile_id
    uu.rasterize('{}.shp'.format(cn.name_FIA_regions_raw[:-4]),
                   "{0}_{1}.tif".format(tile_id, cn.pattern_FIA_regions_processed),
                        xmin, ymin, xmax, ymax, '.00025', 'Byte', 'regionCode', '0')

    print "Checking if {} contains any data...".format(tile_id)
    no_data = uu.check_for_data("{0}_{1}.tif".format(tile_id, cn.pattern_FIA_regions_processed))

    if no_data:

        print "  No data found. Deleting {}.".format(tile_id)
        os.remove("{0}_{1}.tif".format(tile_id, cn.pattern_FIA_regions_processed))

    else:

        print "  Data found in {}. Copying tile to s3...".format(tile_id)
        uu.upload_final(cn.FIA_regions_processed_dir, tile_id, cn.pattern_FIA_regions_processed)
        print "    Tile copied to s3"

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_FIA_regions_processed)


def US_removal_rate_calc(tile_id, gain_table_dict, pattern, sensit_type):

    print "Assigning US removal rates:", tile_id

    # Start time
    start = datetime.datetime.now()

    # Names of the input tiles
    gain = '{0}_{1}.tif'.format(cn.pattern_gain, tile_id)
    annual_gain_standard = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_AGB_natrl_forest)
    US_age_cat = '{0}_{1}.tif'.format(tile_id, cn.pattern_US_forest_age_cat_processed)
    US_forest_group = '{0}_{1}.tif'.format(tile_id, cn.pattern_FIA_forest_group_processed)
    US_region = '{0}_{1}.tif'.format(tile_id, cn.pattern_FIA_regions_processed)

    print "  Reading input files and evaluating conditions"

    # Opens biomass tile
    with rasterio.open(annual_gain_standard) as annual_gain_standard_src:

        # Grabs metadata about the tif, like its location/projection/cellsize
        kwargs = annual_gain_standard_src.meta

        # Grabs the windows of the tile (stripes) so we can iterate over the entire tif without running out of memory
        windows = annual_gain_standard_src.block_windows(1)

        # Opens gain tile
        gain_src = rasterio.open(gain)
        US_age_cat_src = rasterio.open(US_age_cat)
        US_forest_group_src = rasterio.open(US_forest_group)
        US_region_src = rasterio.open(US_region)

        # Opens the output tile, giving it the arguments of the input tiles
        dst = rasterio.open('{0}_{1}.tif'.format(tile_id, pattern), 'w', **kwargs)

        # Iterates across the windows (1 pixel strips) of the input tile
        for idx, window in windows:

            # Creates windows for each input raster
            annual_gain_standard_window = annual_gain_standard_src.read(1, window=window)
            gain_window = gain_src.read(1, window=window)
            US_age_cat_window = US_age_cat_src.read(1, window=window)
            US_forest_group_window = US_forest_group_src.read(1, window=window)
            US_region_window = US_region_src.read(1, window=window)

            # Create a 0s array for the output
            dst_data = np.zeros((window.height, window.width), dtype='float32')

            age_cat_masked_window = np.ma.masked_where(annual_gain_standard_window == 0, US_age_cat_window).filled(0)
            US_forest_group_masked_window = np.ma.masked_where(annual_gain_standard_window == 0, US_forest_group_window).filled(0)
            US_region_masked_window = np.ma.masked_where(annual_gain_standard_window == 0, US_region_window).filled(0)

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