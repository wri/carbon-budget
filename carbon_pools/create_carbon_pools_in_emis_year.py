import datetime
import sys
import subprocess
import os
import numpy as np
import rasterio
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def create_BGC(tile_id, mang_BGB_AGB_ratio):

    start = datetime.datetime.now()

    # Names of the input tiles. Creates the names even if the files don't exist.
    mangrove_biomass_2000 = '{0}_{1}.tif'.format(tile_id, cn.pattern_mangrove_biomass_2000)
    AGC_emis_year = '{0}_{1}.tif'.format(tile_id, cn.pattern_AGC_emis_year)
    fao_ecozone = '{0}_{1}.tif'.format(tile_id, cn.pattern_cont_eco_processed)

    # Name of output tile
    BGC_emis_year = '{0}_{1}.tif'.format(tile_id, cn.pattern_BGC_emis_year)

    print "  Reading input files for {}...".format(tile_id)

    # Opens the input tiles if they exist. Any of these could not exist for a given Hansen tile.
    # Either mangrove biomass or WHRC biomass should exist for each tile, though. Thus, kwargs and windows should be
    # created based on one of those input tiles.
    try:
        mangrove_biomass_2000_src = rasterio.open(mangrove_biomass_2000)
        print "Mangrove biomass found for", tile_id
    except:
        print "No mangrove biomass for", tile_id

    AGC_emis_year_src = rasterio.open(AGC_emis_year)
    fao_ecozone_src = rasterio.open(fao_ecozone)

    # Grabs metadata for one of the input tiles, like its location/projection/cellsize
    kwargs = AGC_emis_year_src.meta
    # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
    windows = AGC_emis_year_src.block_windows(1)

    # Updates kwargs for the output dataset.
    # Need to update data type to float 32 so that it can handle fractional gain rates
    kwargs.update(
        driver='GTiff',
        count=1,
        compress='lzw',
        nodata=0,
        dtype='float32'
    )

    # The output file: aboveground carbon density in the year of tree cover loss for pixels with tree cover loss
    dst_BGC_emis_year = rasterio.open(BGC_emis_year, 'w', **kwargs)

    print "  Creating belowground carbon density in the year of loss for {}...".format(tile_id)

    # Iterates across the windows (1 pixel strips) of the input tiles
    for idx, window in windows:

        # Populates the output raster's windows with 0s so that pixels without
        # any of the forest types will have 0s
        BGC_output = np.zeros((window.height, window.width), dtype='float32')

        # Checks if each forest type exists for the tile. If so, calculates AGC density as AGC in 2000 + AGC accumulation.
        # Initialy does this for all pixles (not just loss pixels)-- loss mask is applied at the very end of the window processing.

        AGC_emis_year_window = AGC_emis_year_src.read(1, window=window)
        print AGC_emis_year_window[0][30020:30035]
        fao_ecozone_window = fao_ecozone_src.read(1, window=window)
        fao_ecozone_window = fao_ecozone_window.astype('float32')
        print fao_ecozone_window[0][30020:30035]


        # Mangrove calculation if there is a mangrove biomass tile
        if os.path.exists(mangrove_biomass_2000):

            mangrove_biomass_2000_window = mangrove_biomass_2000_src.read(1, window=window)

            print mangrove_biomass_2000_window[0][30020:30035]

            for key, value in mang_BGB_AGB_ratio.iteritems():
                fao_ecozone_window[fao_ecozone_window == key] = value

            print fao_ecozone_window[0][30020:30035]

            mangrove_C_final = AGC_emis_year_window * fao_ecozone_window

            print mangrove_C_final[0][30020:30035]

            mangrove_C_final = np.ma.masked_where(mangrove_biomass_2000_window == 0, mangrove_C_final)

            print mangrove_C_final[0][30020:30035]

            BGC_output = BGC_output + mangrove_C_final

            print BGC_output[0][30020:30035]

            sys.quit()


        # BGC_output = BGC_output * cn.below_to_above_natrl_forest

    # Writes the output window to the output file
    dst_BGC_emis_year.write_band(1, BGC_output, window=window)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_BGC_emis_year)
