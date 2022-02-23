'''
Script to create three supplementary tiled outputs for each main model output (gross emissions, gross removals, net flux),
which are already in per hectare values for full model extent:
1. per pixel values for full model extent (all pixels included in model extent)
2. per hectare values for forest extent (within the model extent, pixels that have TCD>30 OR Hansen gain OR mangrove biomass)
3. per pixel values for forest extent
The forest extent outputs are for sharing with partners because they limit the model to just the relevant pixels
(those within forests).
Forest extent is defined in the methods section of Harris et al. 2021 Nature Climate Change.
It is roughly implemented in mp_model_extent.py but using TCD>0 rather thant TCD>30. Here, the TCD>30 requirement
is implemented instead as a subset of the full model extent pixels.
Forest extent is: ((TCD2000>30 AND WHRC AGB2000>0) OR Hansen gain=1 OR mangrove AGB2000>0) NOT IN pre-2000 plantations.
The WHRC AGB2000 and pre-2000 plantations conditions were set in mp_model_extent.py, so they don't show up here.
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

def create_supplementary_outputs(tile_id, input_pattern, output_patterns, sensit_type, no_upload):

    # start time
    start = datetime.datetime.now()

    # Extracts the tile id, tile type, and bounding box for the tile
    tile_id = uu.get_tile_id(tile_id)

    # Names of inputs
    focal_tile = '{0}_{1}.tif'.format(tile_id, input_pattern)
    pixel_area = '{0}_{1}.tif'.format(cn.pattern_pixel_area, tile_id)
    tcd = '{0}_{1}.tif'.format(cn.pattern_tcd, tile_id)
    gain = '{0}_{1}.tif'.format(cn.pattern_gain, tile_id)
    mangrove = '{0}_{1}.tif'.format(tile_id, cn.pattern_mangrove_biomass_2000)

    # Names of outputs.
    # Requires that output patterns be listed in main script in the correct order for here
    # (currently, per pixel full extent, per hectare forest extent, per pixel forest extent).
    per_pixel_full_extent = '{0}_{1}.tif'.format(tile_id, output_patterns[0])
    per_hectare_forest_extent = '{0}_{1}.tif'.format(tile_id, output_patterns[1])
    per_pixel_forest_extent = '{0}_{1}.tif'.format(tile_id, output_patterns[2])

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
        uu.print_log("    Mangrove tile found for {}".format(tile_id))
    except:
        uu.print_log("    No mangrove tile found for {}".format(tile_id))

    uu.print_log("  Creating outputs for {}...".format(focal_tile))

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

    uu.add_rasterio_tags(per_pixel_full_extent_dst, sensit_type)
    per_pixel_full_extent_dst.update_tags(
        units='Mg CO2e/pixel over model duration (2001-20{})'.format(cn.loss_years))
    per_pixel_full_extent_dst.update_tags(
        source='per hectare full model extent tile')
    per_pixel_full_extent_dst.update_tags(
        extent='Full model extent: ((TCD2000>0 AND WHRC AGB2000>0) OR Hansen gain=1 OR mangrove AGB2000>0) NOT IN pre-2000 plantations')

    uu.add_rasterio_tags(per_hectare_forest_extent_dst, sensit_type)
    per_hectare_forest_extent_dst.update_tags(
        units='Mg CO2e/hectare over model duration (2001-20{})'.format(cn.loss_years))
    per_hectare_forest_extent_dst.update_tags(
        source='per hectare full model extent tile')
    per_hectare_forest_extent_dst.update_tags(
        extent='Forest extent: ((TCD2000>30 AND WHRC AGB2000>0) OR Hansen gain=1 OR mangrove AGB2000>0) NOT IN pre-2000 plantations')

    uu.add_rasterio_tags(per_pixel_forest_extent_dst, sensit_type)
    per_pixel_forest_extent_dst.update_tags(
        units='Mg CO2e/pixel over model duration (2001-20{})'.format(cn.loss_years))
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

    uu.print_log("  Output tiles created for {}...".format(tile_id))

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, output_patterns[0], no_upload)