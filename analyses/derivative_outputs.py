"""
Final step of the flux model. This creates various derivative outputs which are used on the GFW platform and for
supplemental analyses. Derivative outputs for gross emissions, gross removals, and net flux at 0.00025x0.000025 deg
resolution for full model extent (all pixels included in mp_model_extent.py):
1. Full extent flux Mg per pixel at 0.00025x0.00025 deg (all pixels included in mp_model_extent.py)
2. Forest extent flux Mg per hectare at 0.00025x0.00025 deg (forest extent defined below)
3. Forest extent flux Mg per pixel at 0.00025x0.00025 deg (forest extent defined below)
4. Forest extent flux Mt at 0.04x0.04 deg (aggregated output, ~ 4x4 km at equator)
For sensitivity analyses only:
5. Percent difference between standard model and sensitivity analysis for aggregated map
6. Pixels with sign changes between standard model and sensitivity analysis for aggregated map

The forest extent outputs are for sharing with partners because they limit the model to just the relevant pixels
(those within forests, as defined below).
Forest extent is defined in the methods section of Harris et al. 2021 Nature Climate Change:
within the model extent, pixels that have TCD>30 OR Hansen gain OR mangrove biomass.
More formally, forest extent is:
((TCD2000>30 AND WHRC AGB2000>0) OR Hansen gain=1 OR mangrove AGB2000>0) NOT IN pre-2000 plantations.
The WHRC AGB2000 condition was set in mp_model_extent.py, so it doesn't show up here.
"""

import numpy as np
import os
import rasterio
from rasterio.transform import from_origin
import datetime
import sys

import constants_and_names as cn
import universal_util as uu


def forest_extent_per_pixel_outputs(tile_id, input_pattern, output_patterns):
    """
    Creates derivative outputs at 0.00025x0.00025 deg resolution
    :param tile_id: tile to be processed, identified by its tile id
    :param input_pattern: pattern for input tile
    :param output_patterns: patterns for output tile names (list of patterns because three derivative outputs)
    :return: Three tiles: full extent Mg per pixel, forest extent Mg per hectare, forest extent Mg per pixel
    """

    # start time
    start = datetime.datetime.now()

    # Names of inputs
    focal_tile = f'{tile_id}_{input_pattern}.tif'
    pixel_area = f'{cn.pattern_pixel_area}_{tile_id}.tif'
    tcd = f'{cn.pattern_tcd}_{tile_id}.tif'
    gain = f'{tile_id}_{cn.pattern_gain_ec2}.tif'
    mangrove = f'{tile_id}_{cn.pattern_mangrove_biomass_2000}.tif'
    pre_2000_plantations = f'{tile_id}_{cn.pattern_plant_pre_2000}.tif'

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

    try:
        gain_src = rasterio.open(gain)
        uu.print_log(f'    Gain tile found for {tile_id}')
    except:
        uu.print_log(f'    Gain tile not found for {tile_id}')

    try:
        mangrove_src = rasterio.open(mangrove)
        uu.print_log(f'    Mangrove tile found for {tile_id}')
    except:
        uu.print_log(f'    Mangrove tile not found for {tile_id}')

    try:
        pre_2000_plantations_src = rasterio.open(pre_2000_plantations)
        uu.print_log(f'  Pre-2000 plantation tile found for {tile_id}')
    except rasterio.errors.RasterioIOError:
        uu.print_log(f'  Pre-2000 plantation tile not found for {tile_id}')

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
        extent='Full model extent: ((TCD2000>0 AND WHRC AGB2000>0) OR Hansen gain=1 OR mangrove AGB2000>0)')

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

        try:
            gain_window = gain_src.read(1, window=window)
        except:
            gain_window = np.zeros((window.height, window.width), dtype='uint8')

        try:
            mangrove_window = mangrove_src.read(1, window=window)
        except:
            mangrove_window = np.zeros((window.height, window.width), dtype='uint8')

        try:
            pre_2000_plantations_window = pre_2000_plantations_src.read(1, window=window)
        except UnboundLocalError:
            pre_2000_plantations_window = np.zeros((window.height, window.width), dtype=int)

        # Output window for per pixel full extent raster
        dst_window_per_pixel_full_extent = in_window * pixel_area_window / cn.m2_per_ha

        # Output window for per hectare forest extent raster
        # QCed this line before publication and then again afterwards in response to question from Lena Schulte-Uebbing at Wageningen Uni.
        dst_window_per_hectare_forest_extent = \
            np.where(((tcd_window > cn.canopy_threshold) | (gain_window == 1) | (mangrove_window != 0)) & (pre_2000_plantations_window == 0), in_window, 0)

        # Output window for per pixel forest extent raster
        dst_window_per_pixel_forest_extent = dst_window_per_hectare_forest_extent * pixel_area_window / cn.m2_per_ha

        # Writes arrays to output raster
        per_pixel_full_extent_dst.write_band(1, dst_window_per_pixel_full_extent, window=window)
        per_hectare_forest_extent_dst.write_band(1, dst_window_per_hectare_forest_extent, window=window)
        per_pixel_forest_extent_dst.write_band(1, dst_window_per_pixel_forest_extent, window=window)

    uu.print_log(f'  Output tiles created for {tile_id}...')

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, output_patterns[0])


def aggregate_within_tile(tile_id, download_pattern_name):
    """
    Aggregates 0.00025x0.00025 deg per pixel forest extent raster to 0.04x0.04 deg raster
    :param tile_id: tile to be processed, identified by its tile id
    :param download_pattern_name: pattern for input tile, in this case the forest extent per-pixel version
    :return: Raster with values aggregated to Mt per 0.04x0.04 deg cells
    """

    # start time
    start = datetime.datetime.now()

    # Name of inputs
    focal_tile_rewindowed = f'{tile_id}_{download_pattern_name}_rewindow.tif'

    xmin, ymin, xmax, ymax = uu.coords(focal_tile_rewindowed)

    try:
        in_src = rasterio.open(focal_tile_rewindowed)
        uu.print_log(f'   Tile found for {tile_id}. Rewindowing.')
    except rasterio.errors.RasterioIOError:
        uu.print_log(f'   Tile not found for {tile_id}. Skipping tile.')
        return

    # Grabs the windows of the tile (stripes) in order to iterate over the entire tif without running out of memory
    windows = in_src.block_windows(1)

    # 2D array (250x250 cells) in which the 0.04x0.04 deg aggregated sums will be stored.
    sum_array = np.zeros([int(cn.tile_width/cn.agg_pixel_window),int(cn.tile_width/cn.agg_pixel_window)], 'float32')

    out_raster = f'{tile_id}_{download_pattern_name}_{cn.agg_pixel_res_filename}deg.tif'

    uu.check_memory()

    # Iterates across the windows (160x160 30m pixels) of the input tile
    for idx, window in windows:

        # Creates windows for each input tile
        in_window = in_src.read(1, window=window)

        # Sums the pixels to create a total value for the 0.04x0.04 deg pixel
        non_zero_pixel_sum = np.sum(in_window)

        # Stores the resulting value in the array
        sum_array[idx[0], idx[1]] = non_zero_pixel_sum


    # Converts the cumulative CO2 removals values to annualized CO2 in megatonnes and makes negative (because removals are negative)
    # [0:15] limits the pattern to the part of the download_pattern_name shared by the full extent per-hectare version
    # and the forest extent per-pixel version. It's hacky.
    if cn.pattern_cumul_gain_AGCO2_BGCO2_all_types[0:15] in download_pattern_name:
        sum_array = sum_array / cn.loss_years / cn.tonnes_to_megatonnes * -1

    # Converts the cumulative gross emissions all gases CO2e values to annualized gross emissions CO2e in megatonnes.
    # [0:15] limits the pattern to the part of the download_pattern_name shared by the full extent per-hectare version
    # and the forest extent per-pixel version. It's hacky.
    if cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil[0:15] in download_pattern_name:
        sum_array = sum_array / cn.loss_years / cn.tonnes_to_megatonnes

    # Converts the cumulative net flux CO2 values to annualized net flux CO2 in megatonnes.
    # [0:15] limits the pattern to the part of the download_pattern_name shared by the full extent per-hectare version
    # and the forest extent per-pixel version. It's hacky.
    if cn.pattern_net_flux[0:15] in download_pattern_name:
        sum_array = sum_array / cn.loss_years / cn.tonnes_to_megatonnes

    uu.print_log(f'  Creating aggregated tile for {tile_id}...')

    # Converts array to the same output type as the raster that is created below
    sum_array = np.float32(sum_array)

    # Creates a tile at 0.04x0.04 degree resolution (approximately 10x10 km in the tropics) where the values are
    # from the 2D array created by rasterio above
    # https://gis.stackexchange.com/questions/279953/numpy-array-to-gtiff-using-rasterio-without-source-raster
    with rasterio.open(out_raster, 'w',
                                driver='GTiff', compress='DEFLATE', nodata='0', dtype='float32', count=1,
                                height=int(cn.tile_width/cn.agg_pixel_window),
                                width=int(cn.tile_width/cn.agg_pixel_window),
                                crs='EPSG:4326',
                                transform=from_origin(xmin,ymax,cn.agg_pixel_res,cn.agg_pixel_res)) as aggregated:
        aggregated.write(sum_array, 1)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, f'{download_pattern_name}_{cn.agg_pixel_res_filename}deg')


def aggregate_tiles(basic_pattern, per_pixel_forest_pattern):
    """
    Aggregates all 0.04x0.04 deg resolution 10x10 deg tiles into a global 0.04x0.04 deg map
    :param basic_pattern: pattern for per hectare full extent tiles (used as basis for aggregated output file name)
    :param per_pixel_forest_pattern: pattern for per pixel forest extent tiles
    :return: global aggregated 0.04x0.04 deg map with fluxes of Mt/year/pixel
    """

    # Makes a vrt of all the output 10x10 tiles (0.04 degree resolution)
    out_vrt = f'{per_pixel_forest_pattern}_{cn.agg_pixel_res_filename}deg.vrt'
    os.system(f'gdalbuildvrt -tr {str(cn.agg_pixel_res)} {str(cn.agg_pixel_res)} {out_vrt} *{per_pixel_forest_pattern}_{cn.agg_pixel_res_filename}deg.tif')

    # Creates the output name for the aggregated map
    out_aggregated_pattern = uu.name_aggregated_output(basic_pattern)
    uu.print_log(f'Aggregated raster pattern is {out_aggregated_pattern}')

    # Produces a single raster of all the 10x10 tiles
    cmd = ['gdalwarp', '-t_srs', "EPSG:4326", '-overwrite', '-dstnodata', '0', '-co', 'COMPRESS=DEFLATE',
           '-tr', str(cn.agg_pixel_res), str(cn.agg_pixel_res),
           out_vrt, f'{out_aggregated_pattern}.tif']
    uu.log_subprocess_output_full(cmd)

    # Adds metadata tags to output rasters
    uu.add_universal_metadata_gdal(f'{out_aggregated_pattern}.tif')

    # Units are different for annual removal factor, so metadata has to reflect that
    if 'annual_removal_factor' in out_aggregated_pattern:
        cmd = ['gdal_edit.py',
               '-mo', f'units=Mg aboveground carbon/yr/pixel, where pixels are {cn.agg_pixel_res}x{cn.agg_pixel_res} degrees',
               '-mo',
               'source=per hectare version of the same model output, aggregated from 0.00025x0.00025 degree pixels',
               '-mo', 'extent=Global',
               '-mo', 'scale=negative values are removals',
               '-mo',
               f'treecover_density_threshold={cn.canopy_threshold} (only model pixels with canopy cover > {cn.canopy_threshold} are included in aggregation',
               f'{out_aggregated_pattern}.tif']
        uu.log_subprocess_output_full(cmd)

    else:
        cmd = ['gdal_edit.py',
               '-mo', f'units=Mg CO2e/yr/pixel, where pixels are {cn.agg_pixel_res}x{cn.agg_pixel_res} degrees',
               '-mo',
               'source=per hectare version of the same model output, aggregated from 0.00025x0.00025 degree pixels',
               '-mo', 'extent=Global',
               '-mo',
               f'treecover_density_threshold={cn.canopy_threshold} (only model pixels with canopy cover > {cn.canopy_threshold} are included in aggregation',
               f'{out_aggregated_pattern}.tif']
        uu.log_subprocess_output_full(cmd)