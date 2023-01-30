"""

"""

import numpy as np
import os
import rasterio
from rasterio.transform import from_origin
import datetime
import sys

import constants_and_names as cn
import universal_util as uu


def supplementary_outputs(tile_id, input_pattern, output_patterns):

    # start time
    start = datetime.datetime.now()

    # Extracts the tile id, tile type, and bounding box for the tile
    tile_id = uu.get_tile_id(tile_id)

    # Names of inputs
    focal_tile = f'{tile_id}_{input_pattern}.tif'
    pixel_area = f'{cn.pattern_pixel_area}_{tile_id}.tif'
    tcd = f'{cn.pattern_tcd}_{tile_id}.tif'
    gain = f'{cn.pattern_gain}_{tile_id}.tif'
    mangrove = f'{tile_id}_{cn.pattern_mangrove_biomass_2000}.tif'

    # Names of outputs.
    # Requires that output patterns be listed in main script in the correct order for here
    # (currently, per pixel full extent, per hectare forest extent, per pixel forest extent).
    per_pixel_full_extent = f'{tile_id}_{output_patterns[0]}.tif'
    per_hectare_forest_extent = f'{tile_id}_{output_patterns[1]}.tif'
    per_pixel_forest_extent = f'{tile_id}_{output_patterns[2]}.tif'

    # Opens input tiles for rasterio
    in_src = rasterio.open(focal_tile)
    # Grabs metadata about the tif, like its location/projection/cellsize
    kwargs = in_src.meta
    # Grabs the windows of the tile (stripes) so we can iterate over the entire tif without running out of memory
    windows = in_src.block_windows(1)

    pixel_area_src = rasterio.open(pixel_area)
    tcd_src = rasterio.open(tcd)
    gain_src = rasterio.open(gain)

    try:
        mangrove_src = rasterio.open(mangrove)
        uu.print_log(f'    Mangrove tile found for {tile_id}')
    except:
        uu.print_log(f'    No mangrove tile found for {tile_id}')

    uu.print_log(f'  Creating outputs for {focal_tile}...')

    kwargs.update(
        driver='GTiff',
        count=1,
        compress='DEFLATE',
        nodata=0,
        dtype='float32'
    )

    # Opens output tiles, giving them the arguments of the input tiles
    per_pixel_full_extent_dst = rasterio.open(per_pixel_full_extent, 'w', **kwargs)
    per_hectare_forest_extent_dst = rasterio.open(per_hectare_forest_extent, 'w', **kwargs)
    per_pixel_forest_extent_dst = rasterio.open(per_pixel_forest_extent, 'w', **kwargs)

    # Adds metadata tags to the output rasters

    uu.add_universal_metadata_rasterio(per_pixel_full_extent_dst)
    per_pixel_full_extent_dst.update_tags(
        units=f'Mg CO2e/pixel over model duration (2001-20{cn.loss_years})')
    per_pixel_full_extent_dst.update_tags(
        source='per hectare full model extent tile')
    per_pixel_full_extent_dst.update_tags(
        extent='Full model extent: ((TCD2000>0 AND WHRC AGB2000>0) OR Hansen gain=1 OR mangrove AGB2000>0) NOT IN pre-2000 plantations')

    uu.add_universal_metadata_rasterio(per_hectare_forest_extent_dst)
    per_hectare_forest_extent_dst.update_tags(
        units=f'Mg CO2e/hectare over model duration (2001-20{cn.loss_years})')
    per_hectare_forest_extent_dst.update_tags(
        source='per hectare full model extent tile')
    per_hectare_forest_extent_dst.update_tags(
        extent='Forest extent: ((TCD2000>30 AND WHRC AGB2000>0) OR Hansen gain=1 OR mangrove AGB2000>0) NOT IN pre-2000 plantations')

    uu.add_universal_metadata_rasterio(per_pixel_forest_extent_dst)
    per_pixel_forest_extent_dst.update_tags(
        units=f'Mg CO2e/pixel over model duration (2001-20{cn.loss_years})')
    per_pixel_forest_extent_dst.update_tags(
        source='per hectare forest model extent tile')
    per_pixel_forest_extent_dst.update_tags(
        extent='Forest extent: ((TCD2000>30 AND WHRC AGB2000>0) OR Hansen gain=1 OR mangrove AGB2000>0) NOT IN pre-2000 plantations')

    if "net_flux" in focal_tile:
        per_pixel_full_extent_dst.update_tags(
            scale='Negative values are net sinks. Positive values are net sources.')
        per_hectare_forest_extent_dst.update_tags(
            scale='Negative values are net sinks. Positive values are net sources.')
        per_pixel_forest_extent_dst.update_tags(
            scale='Negative values are net sinks. Positive values are net sources.')

    uu.check_memory()

    # Iterates across the windows of the input tiles
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

        # Output window for per pixel full extent raster
        dst_window_per_pixel_full_extent = in_window * pixel_area_window / cn.m2_per_ha

        # Output window for per hectare forest extent raster
        # QCed this line before publication and then again afterwards in response to question from Lena Schulte-Uebbing at Wageningen Uni.
        dst_window_per_hectare_forest_extent = np.where((tcd_window > cn.canopy_threshold) | (gain_window == 1) | (mangrove_window != 0), in_window, 0)

        # Output window for per pixel forest extent raster
        dst_window_per_pixel_forest_extent = dst_window_per_hectare_forest_extent * pixel_area_window / cn.m2_per_ha

        # Writes arrays to output raster
        per_pixel_full_extent_dst.write_band(1, dst_window_per_pixel_full_extent, window=window)
        per_hectare_forest_extent_dst.write_band(1, dst_window_per_hectare_forest_extent, window=window)
        per_pixel_forest_extent_dst.write_band(1, dst_window_per_pixel_forest_extent, window=window)

    uu.print_log(f'  Output tiles created for {tile_id}...')

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, output_patterns[0])


# Converts the existing (per ha) values to per pixel values (e.g., emissions/ha to emissions/pixel)
# and sums those values in each 160x160 pixel window.
# The sum for each 160x160 pixel window is stored in a 2D array, which is then converted back into a raster at
# 0.1x0.1 degree resolution (approximately 10m in the tropics).
# Each pixel in that raster is the sum of the 30m pixels converted to value/pixel (instead of value/ha).
# The 0.1x0.1 degree tile is output.
def aggregate(tile_id, download_pattern_name):

    # start time
    start = datetime.datetime.now()

    print(download_pattern_name)

    # Name of inputs
    focal_tile_rewindowed = f'{tile_id}_{download_pattern_name}_rewindow.tif'

    xmin, ymin, xmax, ymax = uu.coords(focal_tile_rewindowed)

    in_src = rasterio.open(focal_tile_rewindowed)

    # Grabs the windows of the tile (stripes) in order to iterate over the entire tif without running out of memory
    windows = in_src.block_windows(1)

    # 2D array (250x250 cells) in which the 0.04x0.04 deg aggregated sums will be stored.
    # sum_array = np.zeros([int(cn.tile_width/cn.agg_pixel_window),int(cn.tile_width/cn.agg_pixel_window)], 'float32')
    sum_array = np.zeros([250, 250], 'float32')

    out_raster = f'{tile_id}_{download_pattern_name}_0_04deg.tif'

    uu.check_memory()

    # Iterates across the windows (160x160 30m pixels) of the input tile
    for idx, window in windows:

        # Creates windows for each input tile
        in_window = in_src.read(1, window=window)

        # Sums the pixels to create a total value for the 0.04x0.04 deg pixel
        non_zero_pixel_sum = np.sum(in_window)

        # Stores the resulting value in the array
        sum_array[idx[0], idx[1]] = non_zero_pixel_sum


    # Converts the annual carbon removals values annual removals in megatonnes and makes negative (because removals are negative)
    if cn.pattern_annual_gain_AGC_all_types in download_pattern_name:
        sum_array = sum_array / cn.tonnes_to_megatonnes * -1

    # Converts the cumulative CO2 removals values to annualized CO2 in megatonnes and makes negative (because removals are negative)
    if cn.pattern_cumul_gain_AGCO2_BGCO2_all_types in download_pattern_name:
        sum_array = sum_array / cn.loss_years / cn.tonnes_to_megatonnes * -1

    # # Converts the cumulative gross emissions CO2 only values to annualized gross emissions CO2e in megatonnes
    # if cn.pattern_gross_emis_co2_only_all_drivers_biomass_soil in download_pattern_name:
    #     sum_array = sum_array / cn.loss_years / cn.tonnes_to_megatonnes
    #
    # # Converts the cumulative gross emissions non-CO2 values to annualized gross emissions CO2e in megatonnes
    # if cn.pattern_gross_emis_non_co2_all_drivers_biomass_soil in download_pattern_name:
    #     sum_array = sum_array / cn.loss_years / cn.tonnes_to_megatonnes

    # Converts the cumulative gross emissions all gases CO2e values to annualized gross emissions CO2e in megatonnes
    if cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil in download_pattern_name:
        sum_array = sum_array / cn.loss_years / cn.tonnes_to_megatonnes

    # Converts the cumulative net flux CO2 values to annualized net flux CO2 in megatonnes
    if cn.pattern_net_flux in download_pattern_name:
        sum_array = sum_array / cn.loss_years / cn.tonnes_to_megatonnes

    uu.print_log(f'  Creating aggregated tile for {tile_id}...')

    # Converts array to the same output type as the raster that is created below
    sum_array = np.float32(sum_array)

    # Creates a tile at 0.04x0.04 degree resolution (approximately 10x10 km in the tropics) where the values are
    # from the 2D array created by rasterio above
    # https://gis.stackexchange.com/questions/279953/numpy-array-to-gtiff-using-rasterio-without-source-raster
    with rasterio.open(out_raster, 'w',
                                driver='GTiff', compress='DEFLATE', nodata='0', dtype='float32', count=1,
                                height=250, width=250,
                                crs='EPSG:4326', transform=from_origin(xmin,ymax,0.04,0.04)) as aggregated:
        aggregated.write(sum_array, 1)
        ### I don't know why, but update_tags() is adding the tags to the raster but not saving them.
        ### That is, the tags are printed but not showing up when I do gdalinfo on the raster.
        ### Instead, I'm using gdal_edit
        # print(aggregated)
        # aggregated.update_tags(a="1")
        # print(aggregated.tags())
        # uu.add_rasterio_tags(aggregated)
        # print(aggregated.tags())
        # if cn.pattern_annual_gain_AGC_all_types in download_pattern_name:
        #     aggregated.update_tags(units='Mg aboveground carbon/pixel, where pixels are 0.04x0.04 degrees)',
        #                     source='per hectare version of the same model output, aggregated from 0.00025x0.00025 degree pixels',
        #                     extent='Global',
        #                     treecover_density_threshold='{0} (only model pixels with canopy cover > {0} are included in aggregation'.format(thresh))
        # if cn.pattern_cumul_gain_AGCO2_BGCO2_all_types:
        #     aggregated.update_tags(units='Mg CO2/yr/pixel, where pixels are 0.04x0.04 degrees)',
        #                     source='per hectare version of the same model output, aggregated from 0.00025x0.00025 degree pixels',
        #                     extent='Global',
        #                     treecover_density_threshold='{0} (only model pixels with canopy cover > {0} are included in aggregation'.format(thresh))
        # # if cn.pattern_gross_emis_co2_only_all_drivers_biomass_soil in download_pattern_name:
        # #     aggregated.update_tags(units='Mg CO2e/yr/pixel, where pixels are 0.04x0.04 degrees)',
        # #                     source='per hectare version of the same model output, aggregated from 0.00025x0.00025 degree pixels',
        # #                     extent='Global', gases_included='CO2 only',
        # #                     treecover_density_threshold = '{0} (only model pixels with canopy cover > {0} are included in aggregation'.format(thresh))
        # # if cn.pattern_gross_emis_non_co2_all_drivers_biomass_soil in download_pattern_name:
        # #     aggregated.update_tags(units='Mg CO2e/yr/pixel, where pixels are 0.04x0.04 degrees)',
        # #                     source='per hectare version of the same model output, aggregated from 0.00025x0.00025 degree pixels',
        # #                     extent='Global', gases_included='CH4, N20',
        # #                     treecover_density_threshold='{0} (only model pixels with canopy cover > {0} are included in aggregation'.format(thresh))
        # if cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil in download_pattern_name:
        #     aggregated.update_tags(units='Mg CO2e/yr/pixel, where pixels are 0.04x0.04 degrees)',
        #                     source='per hectare version of the same model output, aggregated from 0.00025x0.00025 degree pixels',
        #                     extent='Global',
        #                     treecover_density_threshold='{0} (only model pixels with canopy cover > {0} are included in aggregation'.format(thresh))
        # if cn.pattern_net_flux in download_pattern_name:
        #     aggregated.update_tags(units='Mg CO2e/yr/pixel, where pixels are 0.04x0.04 degrees)',
        #                     scale='Negative values are net sinks. Positive values are net sources.',
        #                     source='per hectare version of the same model output, aggregated from 0.00025x0.00025 degree pixels',
        #                     extent='Global',
        #                     treecover_density_threshold='{0} (only model pixels with canopy cover > {0} are included in aggregation'.format(thresh))
        # print(aggregated.tags())
        # aggregated.close()

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, f'{download_pattern_name}_0_04deg')