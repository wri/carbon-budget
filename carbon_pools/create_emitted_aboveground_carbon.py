import datetime
import sys
import numpy as np
import rasterio
import os
import subprocess
import glob
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def create_emitted_AGC(tile_id):

    # Start time
    start = datetime.datetime.now()

    # Names of the input tiles
    mangrove_biomass_2000 = '{0}_{1}.tif'.format(tile_id, cn.pattern_mangrove_biomass_2000)
    natrl_forest_biomass_2000 = '{0}_{1}.tif'.format(tile_id, cn.pattern_WHRC_biomass_2000_unmasked)
    mangrove_cumul_AGC_gain = '{0}_{1}.tif'.format(tile_id, cn.pattern_cumul_gain_AGC_mangrove)
    planted_forest_cumul_AGC_gain = '{0}_{1}.tif'.format(tile_id, cn.pattern_cumul_gain_AGC_planted_forest_non_mangrove)
    natrl_forest_cumul_AGC_gain = '{0}_{1}.tif'.format(tile_id, cn.pattern_cumul_gain_AGC_natrl_forest)
    loss_year = '{0}.tif'.format(tile_id)

    # Name of output tile
    all_forests_AGC_emis_year = '{0}_{1}.tif'.format(tile_id, cn.pattern_AGC_emis_year)


    print "  Reading input files and creating aboveground carbon in the year of loss for for {}".format(tile_id)

    # Opens the input tiles
    mangrove_biomass_2000_src = rasterio.open(mangrove_biomass_2000)
    natrl_forest_biomass_2000_src = rasterio.open(natrl_forest_biomass_2000)
    mangrove_cumul_AGC_gain_src = rasterio.open(mangrove_cumul_AGC_gain)
    planted_forest_cumul_AGC_gain_src = rasterio.open(planted_forest_cumul_AGC_gain)
    natrl_forest_cumul_AGC_gain_src = rasterio.open(natrl_forest_cumul_AGC_gain)
    loss_year_src = rasterio.open(loss_year)

    # Grabs metadata about the continent ecozone tile, like its location/projection/cellsize
    kwargs = natrl_forest_biomass_2000_src.meta

    # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
    windows = natrl_forest_biomass_2000_src.block_windows(1)

    # Updates kwargs for the output dataset.
    # Need to update data type to float 32 so that it can handle fractional gain rates
    kwargs.update(
        driver='GTiff',
        count=1,
        compress='lzw',
        nodata=0,
        dtype='float32'
    )

    # The output file: aboveground carbon density in the year of emission
    dst_AGC_emis_year = rasterio.open(all_forests_AGC_emis_year, 'w', **kwargs)

    # Iterates across the windows (1 pixel strips) of the input tiles
    for idx, window in windows:

        # Creates a processing window for each input raster
        mangrove_biomass_2000_window = mangrove_biomass_2000_src.read(1, window=window)
        # print mangrove_biomass_2000_window[[0]]
        natrl_forest_biomass_2000_window = natrl_forest_biomass_2000_src.read(1, window=window)
        # print natrl_forest_biomass_2000_window[[0]]
        mangrove_cumul_AGC_gain_window = mangrove_cumul_AGC_gain_src.read(1, window=window)
        # print mangrove_cumul_AGC_gain_window[[0]]
        planted_forest_cumul_AGC_gain_window = planted_forest_cumul_AGC_gain_src.read(1, window=window)
        # print planted_forest_cumul_AGC_gain_window[[0]]
        natrl_forest_cumul_AGC_gain_window = natrl_forest_cumul_AGC_gain_src.read(1, window=window)
        # print natrl_forest_cumul_AGC_gain_window[[0]]
        loss_year_window = loss_year_src.read(1, window=window)
        # print loss_year_window[[0]]

        mangrove_C_final = (mangrove_biomass_2000_window * cn.biomass_to_c_mangrove) + mangrove_cumul_AGC_gain_window
        print mangrove_C_final[[0]]

        planted_forest = np.ma.masked_where(planted_forest_cumul_AGC_gain_window == 0, natrl_forest_biomass_2000_window)
        print planted_forest

        planted_forest_C_final = (natrl_forest_biomass_2000_window * cn.biomass_to_c_natrl_forest) + planted_forest_cumul_AGC_gain_window
        print planted_forest_C_final[[0]]

        natural_forest_C = (natrl_forest_biomass_2000_window * cn.biomass_to_c_natrl_forest) + natrl_forest_cumul_AGC_gain_window
        print natural_forest_C[[0]]

        natural_forest_C_final = np.ma.masked_where(planted_forest_C_final==0, natural_forest_C)
        # natural_forest_C_final = natural_forest_C[planted_forest_C_final == 0]
        print natural_forest_C_final[[0]]

        all_forest_types_C_final = mangrove_C_final + planted_forest_C_final + natural_forest_C_final
        print all_forest_types_C_final[[0]]

        all_forest_types_C_final = all_forest_types_C_final[loss_year_window > 0]
        print all_forest_types_C_final[[0]]

        # Writes the output window to the output file
        dst_AGC_emis_year.write_band(1, all_forest_types_C_final, window=window)

        sys.quit()

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_AGC_emis_year)