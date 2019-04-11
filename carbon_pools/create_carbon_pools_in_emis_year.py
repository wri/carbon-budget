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
    cont_ecozone = '{0}_{1}.tif'.format(tile_id, cn.pattern_cont_eco_processed)

    # Name of output tile
    BGC_emis_year = '{0}_{1}.tif'.format(tile_id, cn.pattern_BGC_emis_year)

    print "  Reading input files for {}...".format(tile_id)

    # Opens the mangrove biomass tile if it exists
    try:
        mangrove_biomass_2000_src = rasterio.open(mangrove_biomass_2000)
        print "Mangrove biomass found for", tile_id
    except:
        print "No mangrove biomass for", tile_id

    # Both of these tiles should exist and thus be able to be opened
    AGC_emis_year_src = rasterio.open(AGC_emis_year)
    cont_ecozone_src = rasterio.open(cont_ecozone)

    # Grabs metadata for one of the input tiles, like its location/projection/cellsize
    kwargs = AGC_emis_year_src.meta
    # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
    windows = AGC_emis_year_src.block_windows(1)

    # Updates kwargs for the output dataset.
    # Need to update data type to float 32 so that it can handle fractional carbon pools
    kwargs.update(
        driver='GTiff',
        count=1,
        compress='lzw',
        nodata=0,
        dtype='float32'
    )

    # The output file: belowground carbon density in the year of tree cover loss for pixels with tree cover loss
    dst_BGC_emis_year = rasterio.open(BGC_emis_year, 'w', **kwargs)

    print "  Creating belowground carbon density in the year of loss for {}...".format(tile_id)

    # Iterates across the windows (1 pixel strips) of the input tiles
    for idx, window in windows:

        # Populates the output raster's windows with 0s so that pixels without
        # any of the forest types will have 0s
        BGC_output = np.zeros((window.height, window.width), dtype='float32')

        # Reads in the windows of each input file that definitely exist
        AGC_emis_year_window = AGC_emis_year_src.read(1, window=window)
        # print AGC_emis_year_window[0][30020:30035]
        cont_ecozone_window = cont_ecozone_src.read(1, window=window)
        cont_ecozone_window = cont_ecozone_window.astype('float32')
        # print fao_ecozone_window[0][30020:30035]

        # Mangrove calculation if there is a mangrove biomass tile
        if os.path.exists(mangrove_biomass_2000):

            # Reads in the window for mangrove biomass if it exists
            mangrove_biomass_2000_window = mangrove_biomass_2000_src.read(1, window=window)
            # print mangrove_biomass_2000_window[0][30020:30035]

            # Applies the mangrove BGB:AGB ratios (3 different ratios) to the ecozone raster to create a raster of BGB:AGB ratios
            for key, value in mang_BGB_AGB_ratio.iteritems():
                cont_ecozone_window[cont_ecozone_window == key] = value
            # print fao_ecozone_window[0][30020:30035]

            # Multiplies the AGC in the loss year by the correct mangrove BGB:AGB ratio to get an array of BGC in the loss year
            mangrove_C_final = AGC_emis_year_window * cont_ecozone_window
            # print mangrove_C_final[0][30020:30035]

            # Masks out non-mangrove pixels and fills the masked values with 0s
            mangrove_C_final = np.ma.masked_where(mangrove_biomass_2000_window == 0, mangrove_C_final)
            mangrove_C_final = mangrove_C_final.filled(0)
            # print mangrove_C_final[0][30020:30035]

            # Applies the non-mangrove BGB:AGB ratio to all AGC in emissions year pixels
            non_mang_output = AGC_emis_year_window * cn.below_to_above_non_mang
            # print non_mang_output[0][29930:29950]

            # Masks out mangrove pixels so that only non-mangrove pixels use the non-mangrove BGB:AGB ratio
            non_mang_output_final = np.ma.masked_where(mangrove_biomass_2000_window != 0, non_mang_output)
            # print non_mang_output_final[0][29930:29950]

            # Combines the mangrove and non-mangrove BGC arrays into a single array
            BGC_output = mangrove_C_final + non_mang_output_final
            # print BGC_output[0][29930:29950]

            # sys.quit()

        # If there is no mangrove tile, all AGC in emissions year pixels are multiplied by the non-mangrove
        # BGB:AGB ratio
        if not os.path.exists(mangrove_biomass_2000):

            BGC_output = AGC_emis_year_window * cn.below_to_above_non_mang
            # print BGC_output[0][29930:29950]

        # Writes the output window to the output file
        dst_BGC_emis_year.write_band(1, BGC_output, window=window)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_BGC_emis_year)

def create_deadwood(tile_id):

    start = datetime.datetime.now()

    # Names of the input tiles. Creates the names even if the files don't exist.
    mangrove_biomass_2000 = '{0}_{1}.tif'.format(tile_id, cn.pattern_mangrove_biomass_2000)
    WHRC_biomass_2000 = '{0}_{1}.tif'.format(tile_id, cn.pattern_WHRC_biomass_2000_unmasked)
    AGC_emis_year = '{0}_{1}.tif'.format(tile_id, cn.pattern_AGC_emis_year)
    fao_ecozone = '{0}_{1}.tif'.format(tile_id, cn.pattern_cont_eco_processed)
    precip = '{0}_{1}.tif'.format(tile_id, cn.pattern_precip)
    elevation = '{0}_{1}.tif'.format(tile_id, cn.pattern_elevation)

    # Name of output tile
    deadwood_2000 = '{0}_{1}.tif'.format(tile_id, cn.pattern_deadwood_emis_year_2000)

    print "  Reading input files for {}...".format(tile_id)

    # Opens the mangrove biomass tile if it exists
    try:
        mangrove_biomass_2000_src = rasterio.open(mangrove_biomass_2000)
        print "Mangrove biomass found for", tile_id
    except:
        print "No mangrove biomass for", tile_id

    # These tiles should exist and thus be able to be opened
    AGC_emis_year_src = rasterio.open(AGC_emis_year)
    fao_ecozone_src = rasterio.open(fao_ecozone)
    WHRC_biomass_2000_src = rasterio.open(WHRC_biomass_2000)
    precip_src = rasterio.open(precip)
    elevation_src = rasterio.open(elevation)

    # Grabs metadata for one of the input tiles, like its location/projection/cellsize
    kwargs = AGC_emis_year_src.meta
    # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
    windows = AGC_emis_year_src.block_windows(1)

    # Updates kwargs for the output dataset.
    # Need to update data type to float 32 so that it can handle fractional carbon pools
    kwargs.update(
        driver='GTiff',
        count=1,
        compress='lzw',
        nodata=0,
        dtype='float32'
    )

    # The output file: belowground carbon density in the year of tree cover loss for pixels with tree cover loss
    dst_deadwood_2000 = rasterio.open(deadwood_2000, 'w', **kwargs)

    print "  Creating deadwood carbon density in the year of loss for {}...".format(tile_id)

    # Iterates across the windows (1 pixel strips) of the input tiles
    for idx, window in windows:

        # Populates the output raster's windows with 0s so that pixels without
        # any of the forest types will have 0s
        deadwood_output = np.zeros((window.height, window.width), dtype='float32')

        AGC_emis_year_window = AGC_emis_year_src.read(1, window=window)

        # if np.amax(AGC_emis_year_window) == 0:
        #
        #     continue

        # Reads in the windows of each input file that definitely exist
        WHRC_biomass_window = WHRC_biomass_2000_src.read(1, window=window)
        # print AGC_emis_year_window[0][30020:30035]
        ecozone_window = fao_ecozone_src.read(1, window=window)
        precip_window = precip_src.read(1, window=window)
        elevation_window = elevation_src.read(1, window=window)
        # print fao_ecozone_window[0][30020:30035]

        # # Mangrove calculation if there is a mangrove biomass tile
        # if os.path.exists(mangrove_biomass_2000):
        #
        #     # Reads in the window for mangrove biomass if it exists
        #     mangrove_biomass_2000_window = mangrove_biomass_2000_src.read(1, window=window)
        #     # print mangrove_biomass_2000_window[0][30020:30035]
        #
        #     # Multiplies the AGC in the loss year by the correct mangrove BGB:AGB ratio to get an array of BGC in the loss year
        #     mangrove_C_final = AGC_emis_year_window * ecozone_window
        #     # print mangrove_C_final[0][30020:30035]
        #
        #     # Masks out non-mangrove pixels and fills the masked values with 0s
        #     mangrove_C_final = np.ma.masked_where(mangrove_biomass_2000_window == 0, mangrove_C_final)
        #     mangrove_C_final = mangrove_C_final.filled(0)
        #     # print mangrove_C_final[0][30020:30035]
        #
        #     # Applies the non-mangrove BGB:AGB ratio to all AGC in emissions year pixels
        #     non_mang_output = AGC_emis_year_window * cn.below_to_above_non_mang
        #     # print non_mang_output[0][29930:29950]
        #
        #     # Masks out mangrove pixels so that only non-mangrove pixels use the non-mangrove BGB:AGB ratio
        #     non_mang_output_final = np.ma.masked_where(mangrove_biomass_2000_window != 0, non_mang_output)
        #     # print non_mang_output_final[0][29930:29950]
        #
        #     # Combines the mangrove and non-mangrove BGC arrays into a single array
        #     BGC_output = mangrove_C_final + non_mang_output_final
        #     # print BGC_output[0][29930:29950]
        #
        #     # sys.quit()
        #
        # # If there is no mangrove tile, all AGC in emissions year pixels are multiplied by the non-mangrove
        # # BGB:AGB ratio
        # if not os.path.exists(mangrove_biomass_2000):
        #
        #     BGC_output = AGC_emis_year_window * cn.below_to_above_non_mang
        #     # print BGC_output[0][29930:29950]

        # The deadwood conversions generally come from here: https://cdm.unfccc.int/methodologies/ARmethodologies/tools/ar-am-tool-12-v3.0.pdf, p. 17-18

        elev_mask = elevation_window < -9999
        ecozone_mask = ecozone_window == 2
        condition_mask = elev_mask & ecozone_mask
        agb_masked = np.ma.array(WHRC_biomass_window, mask=np.invert(condition_mask))
        deadwood_masked = agb_masked * 0.08 * cn.biomass_to_c_natrl_forest
        deadwood_output = deadwood_output + deadwood_masked.filled(0)
        # print deadwood_masked[0][0:10]
        # print deadwood_output[0][0:10]

        elev_mask = elevation_window < 2000
        precip_mask = precip_window < 1000
        ecozone_mask = ecozone_window == 1
        condition_mask = elev_mask & precip_mask & ecozone_mask
        agb_masked = np.ma.array(WHRC_biomass_window, mask=np.invert(condition_mask))
        deadwood_masked = agb_masked * 0.02 * cn.biomass_to_c_natrl_forest
        deadwood_output = deadwood_output + deadwood_masked.filled(0)
        # print deadwood_masked[0][0:10]
        # print deadwood_output[0][0:10]

        elev_mask = elevation_window < 2000
        precip_mask = (precip_window > 1000) & (precip_window < 1600)
        ecozone_mask = ecozone_window == 1
        condition_mask = elev_mask & precip_mask & ecozone_mask
        agb_masked = np.ma.array(WHRC_biomass_window, mask=np.invert(condition_mask))
        deadwood_masked = agb_masked * 0.01 * cn.biomass_to_c_natrl_forest
        deadwood_output = deadwood_output + deadwood_masked.filled(0)
        # print deadwood_masked[0][0:10]
        # print deadwood_output[0][0:10]

        elev_mask = elevation_window < 2000
        precip_mask = precip_window > 1600
        ecozone_mask = ecozone_window == 1
        condition_mask = elev_mask & precip_mask & ecozone_mask
        agb_masked = np.ma.array(WHRC_biomass_window, mask=np.invert(condition_mask))
        deadwood_masked = agb_masked * 0.06 * cn.biomass_to_c_natrl_forest
        deadwood_output = deadwood_output + deadwood_masked.filled(0)
        # print deadwood_masked[0][0:10]
        # print deadwood_output[0][0:10]

        elev_mask = elevation_window > 2000
        ecozone_mask = ecozone_window == 1
        condition_mask = elev_mask & ecozone_mask
        agb_masked = np.ma.array(WHRC_biomass_window, mask=np.invert(condition_mask))
        deadwood_masked = agb_masked * 0.07 * cn.biomass_to_c_natrl_forest
        deadwood_output = deadwood_output + deadwood_masked.filled(0)
        # print deadwood_masked[0][0:10]
        # print deadwood_output[0][0:10]

        ecozone_mask = ecozone_window != 1
        condition_mask = ecozone_mask
        agb_masked = np.ma.array(WHRC_biomass_window, mask=np.invert(condition_mask))
        deadwood_masked = agb_masked * 0.08 * cn.biomass_to_c_natrl_forest
        deadwood_output = deadwood_output + deadwood_masked.filled(0)
        # print deadwood_masked[0][0:10]
        # print deadwood_output[0][0:10]

        # deadwood_output = np.ma.masked_where(AGC_emis_year_window == 0, deadwood_output)
        # deadwood_output = deadwood_output.filled(0)

        deadwood_output = deadwood_output.astype('float32')
        # print deadwood_output[0][0:10]

        # Writes the output window to the output file
        dst_deadwood_2000.write_band(1, deadwood_output, window=window)

        # sys.quit()

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_BGC_emis_year)