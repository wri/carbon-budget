'''
This script creates maps of model outputs at roughly 5km resolution (0.05x0.05 degrees), where each output pixel
represents the total value in the pixel (not the density) (hence, the aggregated results).
This is currently set up for annual removal rate, gross removals, gross emissions, and net flux.
It iterates through all the model outputs that are supplied.
The rewindowed pixel area tiles, tcd, Hansen gain, and mangrove biomass tiles must already be created and in s3
(created using mp_rewindow_tiles.py).
First, this script rewindows the model output into 200x200 (0.05x0.05 degree) windows, instead of the native
40000x1 pixel windows.
Then it calculates the per pixel value for each model output pixel and sums those values within each 0.05x0.05 degree
aggregated pixel.
It converts emissions, removals, and net flux from totals over the model period to annual values.
For sensitivity analysis runs, it only processes outputs which actually have a sensitivity analysis version.
The user has to supply a tcd threshold for which forest pixels to include in the results. Defaults to cn.canopy_threshold.
For sensitivity analysis, the s3 folder with the aggregations for the standard model must be specified.
sample command: python mp_aggregate_results_to_4_km.py -tcd 30 -t no_shifting_ag -sagg s3://gfw2-data/climate/carbon_model/0_4deg_output_aggregation/biomass_soil/standard/20200901/net_flux_Mt_CO2e_biomass_soil_per_year_tcd30_0_4deg_modelv1_2_0_std_20200901.tif
'''


import numpy as np
from subprocess import Popen, PIPE, STDOUT, check_call
import os
import rasterio
from rasterio.transform import from_origin
import datetime
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Converts the existing (per ha) values to per pixel values (e.g., emissions/ha to emissions/pixel)
# and sums those values in each 160x160 pixel window.
# The sum for each 160x160 pixel window is stored in a 2D array, which is then converted back into a raster at
# 0.1x0.1 degree resolution (approximately 10m in the tropics).
# Each pixel in that raster is the sum of the 30m pixels converted to value/pixel (instead of value/ha).
# The 0.1x0.1 degree tile is output.
def aggregate(tile, thresh, sensit_type, no_upload):

    # start time
    start = datetime.datetime.now()

    # Extracts the tile id, tile type, and bounding box for the tile
    tile_id = uu.get_tile_id(tile)
    tile_type = uu.get_tile_type(tile)
    xmin, ymin, xmax, ymax = uu.coords(tile_id)

    # Name of inputs
    focal_tile_rewindow = '{0}_{1}_rewindow.tif'.format(tile_id, tile_type)
    pixel_area_rewindow = '{0}_{1}.tif'.format(cn.pattern_pixel_area_rewindow, tile_id)
    tcd_rewindow = '{0}_{1}.tif'.format(cn.pattern_tcd_rewindow, tile_id)
    gain_rewindow = '{0}_{1}.tif'.format(cn.pattern_gain_rewindow, tile_id)
    mangrove_rewindow = '{0}_{1}.tif'.format(tile_id, cn.pattern_mangrove_biomass_2000_rewindow)

    # Opens input tiles for rasterio
    in_src = rasterio.open(focal_tile_rewindow)
    pixel_area_src = rasterio.open(pixel_area_rewindow)
    tcd_src = rasterio.open(tcd_rewindow)
    gain_src = rasterio.open(gain_rewindow)

    try:
        mangrove_src = rasterio.open(mangrove_rewindow)
        uu.print_log("    Mangrove tile found for {}".format(tile_id))
    except:
        uu.print_log("    No mangrove tile found for {}".format(tile_id))

    uu.print_log("  Converting {} to per-pixel values...".format(tile))

    # Grabs the windows of the tile (stripes) in order to iterate over the entire tif without running out of memory
    windows = in_src.block_windows(1)

    #2D array in which the 0.05x0.05 deg aggregated sums will be stored
    sum_array = np.zeros([250,250], 'float32')

    out_raster = "{0}_{1}_0_4deg.tif".format(tile_id, tile_type)

    # Iterates across the windows (160x160 30m pixels) of the input tile
    for idx, window in windows:

        # Creates windows for each input tile
        in_window = in_src.read(1, window=window)
        pixel_area_window = pixel_area_src.read(1, window=window)
        tcd_window = tcd_src.read(1, window=window)
        gain_window = gain_src.read(1, window=window)

        try:
            mangrove_window = mangrove_src.read(1, window=window)
        except:
            mangrove_window = np.zeros((window.height, window.width), dtype='uint8')

        # Applies the tree cover density threshold to the 30x30m pixels
        if thresh > 0:

            # QCed this line before publication and then again afterwards in response to question from Lena Schulte-Uebbing at Wageningen Uni.
            in_window = np.where((tcd_window > thresh) | (gain_window == 1) | (mangrove_window != 0), in_window, 0)

        # Calculates the per-pixel value from the input tile value (/ha to /pixel)
        per_pixel_value = in_window * pixel_area_window / cn.m2_per_ha

        # Sums the pixels to create a total value for the 0.1x0.1 deg pixel
        non_zero_pixel_sum = np.sum(per_pixel_value)

        # Stores the resulting value in the array
        sum_array[idx[0], idx[1]] = non_zero_pixel_sum

    # Converts the annual carbon gain values annual gain in megatonnes and makes negative (because removals are negative)
    if cn.pattern_annual_gain_AGC_all_types in tile_type:
        sum_array = sum_array / cn.tonnes_to_megatonnes * -1

    # Converts the cumulative CO2 gain values to annualized CO2 in megatonnes and makes negative (because removals are negative)
    if cn.pattern_cumul_gain_AGCO2_BGCO2_all_types in tile_type:
        sum_array = sum_array / cn.loss_years / cn.tonnes_to_megatonnes * -1

    # # Converts the cumulative gross emissions CO2 only values to annualized gross emissions CO2e in megatonnes
    # if cn.pattern_gross_emis_co2_only_all_drivers_biomass_soil in tile_type:
    #     sum_array = sum_array / cn.loss_years / cn.tonnes_to_megatonnes
    #
    # # Converts the cumulative gross emissions non-CO2 values to annualized gross emissions CO2e in megatonnes
    # if cn.pattern_gross_emis_non_co2_all_drivers_biomass_soil in tile_type:
    #     sum_array = sum_array / cn.loss_years / cn.tonnes_to_megatonnes

    # Converts the cumulative gross emissions all gases CO2e values to annualized gross emissions CO2e in megatonnes
    if cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil in tile_type:
        sum_array = sum_array / cn.loss_years / cn.tonnes_to_megatonnes

    # Converts the cumulative net flux CO2 values to annualized net flux CO2 in megatonnes
    if cn.pattern_net_flux in tile_type:
        sum_array = sum_array / cn.loss_years / cn.tonnes_to_megatonnes

    uu.print_log("  Creating aggregated tile for {}...".format(tile))

    # Converts array to the same output type as the raster that is created below
    sum_array = np.float32(sum_array)

    # Creates a tile at 0.04x0.04 degree resolution (approximately 10x10 km in the tropics) where the values are
    # from the 2D array created by rasterio above
    # https://gis.stackexchange.com/questions/279953/numpy-array-to-gtiff-using-rasterio-without-source-raster
    with rasterio.open(out_raster, 'w',
                                driver='GTiff', compress='lzw', nodata='0', dtype='float32', count=1,
                                height=250, width=250,
                                crs='EPSG:4326', transform=from_origin(xmin,ymax,0.04,0.04)) as aggregated:
        aggregated.write(sum_array, 1)
        ### I don't know why, but update_tags() is adding the tags to the raster but not saving them.
        ### That is, the tags are printed but not showing up when I do gdalinfo on the raster.
        ### Instead, I'm using gdal_edit
        # print(aggregated)
        # aggregated.update_tags(a="1")
        # print(aggregated.tags())
        # uu.add_rasterio_tags(aggregated, sensit_type)
        # print(aggregated.tags())
        # if cn.pattern_annual_gain_AGC_all_types in tile_type:
        #     aggregated.update_tags(units='Mg aboveground carbon/pixel, where pixels are 0.04x0.04 degrees)',
        #                     source='per hectare version of the same model output, aggregated from 0.00025x0.00025 degree pixels',
        #                     extent='Global',
        #                     treecover_density_threshold='{0} (only model pixels with canopy cover > {0} are included in aggregation'.format(thresh))
        # if cn.pattern_cumul_gain_AGCO2_BGCO2_all_types:
        #     aggregated.update_tags(units='Mg CO2/yr/pixel, where pixels are 0.04x0.04 degrees)',
        #                     source='per hectare version of the same model output, aggregated from 0.00025x0.00025 degree pixels',
        #                     extent='Global',
        #                     treecover_density_threshold='{0} (only model pixels with canopy cover > {0} are included in aggregation'.format(thresh))
        # # if cn.pattern_gross_emis_co2_only_all_drivers_biomass_soil in tile_type:
        # #     aggregated.update_tags(units='Mg CO2e/yr/pixel, where pixels are 0.04x0.04 degrees)',
        # #                     source='per hectare version of the same model output, aggregated from 0.00025x0.00025 degree pixels',
        # #                     extent='Global', gases_included='CO2 only',
        # #                     treecover_density_threshold = '{0} (only model pixels with canopy cover > {0} are included in aggregation'.format(thresh))
        # # if cn.pattern_gross_emis_non_co2_all_drivers_biomass_soil in tile_type:
        # #     aggregated.update_tags(units='Mg CO2e/yr/pixel, where pixels are 0.04x0.04 degrees)',
        # #                     source='per hectare version of the same model output, aggregated from 0.00025x0.00025 degree pixels',
        # #                     extent='Global', gases_included='CH4, N20',
        # #                     treecover_density_threshold='{0} (only model pixels with canopy cover > {0} are included in aggregation'.format(thresh))
        # if cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil in tile_type:
        #     aggregated.update_tags(units='Mg CO2e/yr/pixel, where pixels are 0.04x0.04 degrees)',
        #                     source='per hectare version of the same model output, aggregated from 0.00025x0.00025 degree pixels',
        #                     extent='Global',
        #                     treecover_density_threshold='{0} (only model pixels with canopy cover > {0} are included in aggregation'.format(thresh))
        # if cn.pattern_net_flux in tile_type:
        #     aggregated.update_tags(units='Mg CO2e/yr/pixel, where pixels are 0.04x0.04 degrees)',
        #                     scale='Negative values are net sinks. Positive values are net sources.',
        #                     source='per hectare version of the same model output, aggregated from 0.00025x0.00025 degree pixels',
        #                     extent='Global',
        #                     treecover_density_threshold='{0} (only model pixels with canopy cover > {0} are included in aggregation'.format(thresh))
        # print(aggregated.tags())
        # aggregated.close()

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, '{}_0_4deg'.format(tile_type), no_upload)


# Calculates the percent difference between the standard model's net flux output
# and the sensitivity model's net flux output
def percent_diff(std_aggreg_flux, sensit_aggreg_flux, sensit_type, no_upload):

    # start time
    start = datetime.datetime.now()
    date = datetime.datetime.now()
    date_formatted = date.strftime("%Y_%m_%d")

    uu.print_log(sensit_aggreg_flux)
    uu.print_log(std_aggreg_flux)

    # This produces errors about dividing by 0. As far as I can tell, those are fine. It's just trying to divide NoData
    # pixels by NoData pixels, and it doesn't affect the output.
    # For model v1.2.0, this kept producing incorrect values for the biomass_swap analysis. I don't know why. I ended
    # up just using raster calculator in ArcMap to create the percent diff raster for biomass_swap. It worked
    # fine for all the other analyses, though (including legal_Amazon_loss).
    # Maybe that divide by 0 is throwing off other values now.
    perc_diff_calc = '--calc=(A-B)/absolute(B)*100'
    perc_diff_outfilename = '{0}_{1}_{2}.tif'.format(cn.pattern_aggreg_sensit_perc_diff, sensit_type, date_formatted)
    perc_diff_outfilearg = '--outfile={}'.format(perc_diff_outfilename)
    # cmd = ['gdal_calc.py', '-A', sensit_aggreg_flux, '-B', std_aggreg_flux, perc_diff_calc, perc_diff_outfilearg,
    #        '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW', '--quiet']
    cmd = ['gdal_calc.py', '-A', sensit_aggreg_flux, '-B', std_aggreg_flux, perc_diff_calc, perc_diff_outfilearg,
           '--overwrite', '--co', 'COMPRESS=LZW', '--quiet']
    uu.log_subprocess_output_full(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, 'global', sensit_aggreg_flux, no_upload)


# Maps where the sources stay sources, sinks stay sinks, sources become sinks, and sinks become sources
def sign_change(std_aggreg_flux, sensit_aggreg_flux, sensit_type, no_upload):

    # start time
    start = datetime.datetime.now()

    # Date for the output raster name
    date = datetime.datetime.now()
    date_formatted = date.strftime("%Y_%m_%d")

    # Opens the standard net flux output in rasterio
    with rasterio.open(std_aggreg_flux) as std_src:

        kwargs = std_src.meta

        windows = std_src.block_windows(1)

        # Opens the sensitivity analysis net flux output in rasterio
        sensit_src = rasterio.open(sensit_aggreg_flux)

        # Creates the sign change raster
        dst = rasterio.open('{0}_{1}_{2}.tif'.format(cn.pattern_aggreg_sensit_sign_change, sensit_type, date_formatted), 'w', **kwargs)

        # Adds metadata tags to the output raster
        uu.add_rasterio_tags(dst, sensit_type)
        dst.update_tags(
            key='1=stays net source. 2=stays net sink. 3=changes from net source to net sink. 4=changes from net sink to net source.')
        dst.update_tags(
            source='Comparison of net flux at 0.04x0.04 degrees from standard model to net flux from {} sensitivity analysis'.format(sensit_type))
        dst.update_tags(
            extent='Global')

        # Iterates through the windows in the standard net flux output
        for idx, window in windows:

            std_window = std_src.read(1, window=window)
            sensit_window = sensit_src.read(1, window=window)

            # Defaults the sign change output raster to 0
            dst_data = np.zeros((window.height, window.width), dtype='Float32')

            # Assigns the output value based on the signs (source, sink) of the standard and sensitivity analysis.
            # No option has both windows equaling 0 because that results in the NoData values getting assigned whatever
            # output corresponds to that
            # (e.g., if dst_data[np.where((sensit_window >= 0) & (std_window >= 0))] = 1, NoData values (0s) would become 1s.
            dst_data[np.where((sensit_window > 0) & (std_window >= 0))] = 1   # stays net source
            dst_data[np.where((sensit_window < 0) & (std_window < 0))] = 2    # stays net sink
            dst_data[np.where((sensit_window >= 0) & (std_window < 0))] = 3   # changes from sink to source
            dst_data[np.where((sensit_window < 0) & (std_window >= 0))] = 4   # changes from source to sink

            dst.write_band(1, dst_data, window=window)


    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, 'global', sensit_aggreg_flux, no_upload)
