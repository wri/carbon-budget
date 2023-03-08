### Creates tiles of annual aboveground and belowground biomass removals rates for mangroves using IPCC Wetlands Supplement Table 4.4 rates.
### Its inputs are the continent-ecozone tiles, mangrove biomass tiles (for locations of mangroves), and the IPCC
### removals rate table.

import datetime
import numpy as np
import os
import rasterio
import sys

import constants_and_names as cn
import universal_util as uu

# Necessary to suppress a pandas error later on. https://github.com/numpy/numpy/issues/12987
np.set_printoptions(threshold=sys.maxsize)

def annual_gain_rate(tile_id, output_pattern_list, gain_above_dict, gain_below_dict, stdev_dict):

    uu.print_log("Processing:", tile_id)

    # Start time
    start = datetime.datetime.now()

    # This is only needed for testing, when a list of tiles might include ones without mangroves.
    # When the full model is being run, only tiles with mangroves are included.
    mangrove_biomass_tile_list = uu.tile_list_s3(cn.mangrove_biomass_2000_dir)
    if tile_id not in mangrove_biomass_tile_list:
        uu.print_log("{} does not contain mangroves. Skipping tile.".format(tile_id))
        return

    # Name of the input files
    mangrove_biomass = uu.sensit_tile_rename(cn.SENSIT_TYPE, tile_id, cn.pattern_mangrove_biomass_2000)
    cont_eco = uu.sensit_tile_rename(cn.SENSIT_TYPE, tile_id, cn.pattern_cont_eco_processed)

    # Names of the output aboveground and belowground mangrove removals rate tiles
    AGB_gain_rate = '{0}_{1}.tif'.format(tile_id, output_pattern_list[0])
    BGB_gain_rate = '{0}_{1}.tif'.format(tile_id, output_pattern_list[1])
    AGB_gain_stdev = '{0}_{1}.tif'.format(tile_id, output_pattern_list[2])

    uu.print_log("  Reading input files and creating aboveground and belowground biomass removals rates for {}".format(tile_id))

    cont_eco_src = rasterio.open(cont_eco)
    mangrove_AGB_src = rasterio.open(mangrove_biomass)

    # Grabs metadata about the tif, like its location/projection/cellsize
    kwargs = cont_eco_src.meta

    # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
    windows = cont_eco_src.block_windows(1)

    # Updates kwargs for the output dataset.
    # Need to update data type to float 32 so that it can handle fractional removals rates
    kwargs.update(
        driver='GTiff',
        count=1,
        compress='DEFLATE',
        nodata=0,
        dtype='float32'
    )

    dst_above = rasterio.open(AGB_gain_rate, 'w', **kwargs)
    # Adds metadata tags to the output raster
    uu.add_universal_metadata_rasterio(dst_above)
    dst_above.update_tags(
        units='megagrams aboveground biomass (AGB or dry matter)/ha/yr')
    dst_above.update_tags(
        source='IPCC Guidelines, 2013 Coastal Wetlands Supplement, Table 4.4')
    dst_above.update_tags(
        extent='Simard et al. 2018, based on Giri et al. 2011 (Global Ecol. Biogeogr.) mangrove extent')

    dst_below = rasterio.open(BGB_gain_rate, 'w', **kwargs)
    # Adds metadata tags to the output raster
    uu.add_universal_metadata_rasterio(dst_below)
    dst_below.update_tags(
        units='megagrams belowground biomass (BGB or dry matter)/ha/yr')
    dst_below.update_tags(
        source='IPCC Guidelines, 2013 Coastal Wetlands Supplement, Table 4.4')
    dst_below.update_tags(
        extent='Simard et al. 2018, based on Giri et al. 2011 (Global Ecol. Biogeogr.) mangrove extent')

    dst_stdev_above = rasterio.open(AGB_gain_stdev, 'w', **kwargs)
    # Adds metadata tags to the output raster
    uu.add_universal_metadata_rasterio(dst_stdev_above)
    dst_stdev_above.update_tags(
        units='standard deviation, in terms of megagrams aboveground biomass (AGB or dry matter)/ha/yr')
    dst_stdev_above.update_tags(
        source='IPCC Guidelines, 2013 Coastal Wetlands Supplement, Table 4.4')
    dst_stdev_above.update_tags(
        extent='Simard et al. 2018, based on Giri et al. 2011 (Global Ecol. Biogeogr.) mangrove extent')

    # Iterates across the windows (1 pixel strips) of the input tile
    for idx, window in windows:

        # Creates windows for each input raster
        cont_eco = cont_eco_src.read(1, window=window)
        mangrove_AGB = mangrove_AGB_src.read(1, window=window)

        # Converts the continent-ecozone array to float so that the values can be replaced with fractional removals rates.
        # Creates two copies: one for aboveground removals and one for belowground removals.
        # Creating only one copy of the cont_eco raster made it so that belowground removals rates weren't being
        # written correctly for some reason.
        cont_eco_above = cont_eco.astype('float32')
        cont_eco_below = cont_eco.astype('float32')
        cont_eco_stdev = cont_eco.astype('float32')

        # Reclassifies mangrove biomass to 1 or 0 to make a mask of mangrove pixels.
        # Ultimately, only these pixels (ones with mangrove biomass) will get values.
        mangrove_AGB[mangrove_AGB > 0] = 1


        # Applies the dictionary of continent-ecozone aboveground removals rates to the continent-ecozone array to
        # get annual aboveground removals rates (metric tons aboveground biomass/yr) for each pixel
        for key, value in gain_above_dict.items():
            cont_eco_above[cont_eco_above == key] = value

        # Masks out pixels without mangroves, leaving removals rates in only pixels with mangroves
        dst_above_data = cont_eco_above * mangrove_AGB

        # Writes the output window to the output
        dst_above.write_band(1, dst_above_data, window=window)


        # Same as above but for belowground removals rates
        for key, value in gain_below_dict.items():
            cont_eco_below[cont_eco_below == key] = value

        dst_below_data = cont_eco_below * mangrove_AGB

        dst_below.write_band(1, dst_below_data, window=window)


        # Applies the dictionary of continent-ecozone aboveground removals rate standard deviations to the continent-ecozone array to
        # get annual aboveground removals rate standard deviations (metric tons aboveground biomass/yr) for each pixel
        for key, value in stdev_dict.items():
            cont_eco_stdev[cont_eco_stdev == key] = value

        # Masks out pixels without mangroves, leaving removals rates in only pixels with mangroves
        dst_stdev = cont_eco_stdev * mangrove_AGB

        # Writes the output window to the output
        dst_stdev_above.write_band(1, dst_stdev, window=window)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, output_pattern_list[0])




