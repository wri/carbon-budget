### Creates tiles of annual aboveground and belowground biomass gain rates for mangroves using IPCC Wetlands Supplement Table 4.4 rates.
### Its inputs are the continent-ecozone tiles, mangrove biomass tiles (for locations of mangroves), and the IPCC
### gain rate table.

import datetime
import numpy as np
import os
import rasterio
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Necessary to suppress a pandas error later on
np.set_printoptions(threshold=np.nan)

def annual_gain_rate(tile_id, sensit_type, output_pattern_list, gain_above_dict, gain_below_dict):

    print "Processing:", tile_id

    # Start time
    start = datetime.datetime.now()

    # Name of the input files
    mangrove_biomass = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_mangrove_biomass_2000)
    cont_eco = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_cont_eco_processed)
    pre_2000_plant = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_plant_pre_2000)

    # Names of the output aboveground and belowground mangrove gain rate tiles
    AGB_gain_rate = '{0}_{1}.tif'.format(tile_id, output_pattern_list[0])
    BGB_gain_rate = '{0}_{1}.tif'.format(tile_id, output_pattern_list[1])

    uu.mask_pre_2000_plantation(pre_2000_plant, mangrove_biomass, mangrove_biomass, tile_id)

    print "  Reading input files and creating aboveground and belowground biomass gain rates for {}".format(tile_id)

    cont_eco_src = rasterio.open(cont_eco)
    mangrove_AGB_src = rasterio.open(mangrove_biomass)

    # Grabs metadata about the tif, like its location/projection/cellsize
    kwargs = cont_eco_src.meta

    # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
    windows = cont_eco_src.block_windows(1)

    # Updates kwargs for the output dataset.
    # Need to update data type to float 32 so that it can handle fractional gain rates
    kwargs.update(
        driver='GTiff',
        count=1,
        compress='lzw',
        nodata=0,
        dtype='float32'
    )

    dst_above = rasterio.open(AGB_gain_rate, 'w', **kwargs)
    dst_below = rasterio.open(BGB_gain_rate, 'w', **kwargs)

    # Iterates across the windows (1 pixel strips) of the input tile
    for idx, window in windows:

        # Creates windows for each input raster
        cont_eco = cont_eco_src.read(1, window=window)
        mangrove_AGB = mangrove_AGB_src.read(1, window=window)

        # Converts the continent-ecozone array to float so that the values can be replaced with fractional gain rates.
        # Creates two copies: one for aboveground gain and one for belowground gain.
        # Creating only one copy of the cont_eco raster made it so that belowground gain rates weren't being
        # written correctly for some reason.
        cont_eco_above = cont_eco.astype('float32')
        cont_eco_below = cont_eco.astype('float32')

        # Reclassifies mangrove biomass to 1 or 0 to make a mask of mangrove pixels.
        # Ultimately, only these pixels (ones with mangrove biomass) will get values.
        mangrove_AGB[mangrove_AGB > 0] = 1

        # Applies the dictionary of continent-ecozone aboveground gain rates to the continent-ecozone array to
        # get annual aboveground gain rates (metric tons aboveground biomass/yr) for each pixel
        for key, value in gain_above_dict.iteritems():
            cont_eco_above[cont_eco_above == key] = value

        # Masks out pixels without mangroves, leaving gain rates in only pixels with mangroves
        dst_above_data = cont_eco_above * mangrove_AGB

        # Writes the output window to the output
        dst_above.write_band(1, dst_above_data, window=window)


        # Same as above but for belowground gain rates
        for key, value in gain_below_dict.iteritems():
            cont_eco_below[cont_eco_below == key] = value

        dst_below_data = cont_eco_below * mangrove_AGB

        dst_below.write_band(1, dst_below_data, window=window)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, output_pattern_list[0])




