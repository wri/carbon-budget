'''
This script creates maps of model outputs at roughly 10km resolution (0.1x0.1 degrees), where each output pixel
represents the total value in the pixel (not the density) (hence, the aggregated results).
It iterates through all the model outputs that are supplied.
First, it rewindows the model output, pixel area tile, and tcd tile into 400x400 (0.1x0.1 degree) windows, instead of the native
40000x1 pixel windows.
Then it calculates the per pixel value for each model output pixel and sums those values within each 0.1x0.1 degree
aggregated pixel.
It converts cumulative carbon gain to CO2 gain per year, converts cumulative CO2 flux to CO2 flux per year, and
converts cumulative gross CO2 emissions to gross CO2 emissions per year.
The user has to supply a tcd threshold for which forest pixels to include in the results.
'''

import numpy as np
import subprocess
import os
import rasterio
from rasterio.transform import from_origin
import datetime
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Converts the 10x10 degree Hansen tiles that are in windows of 40000x1 pixels to windows of 400x400 pixels,
# which is the resolution of the output tiles. This will allow the 30x30 m pixels in each window to be summed.
def rewindow(tile):

    # start time
    start = datetime.datetime.now()

    print "Rewindowing {} to 400x400 pixel windows (0.1 degree x 0.1 degree)...". format(tile)

    # Extracts the tile id, tile type, and bounding box for the tile
    tile_id = uu.get_tile_id(tile)
    tile_type = uu.get_tile_type(tile)
    xmin, ymin, xmax, ymax = uu.coords(tile_id)

    # Raster name for 400x400 pixel tiles (intermediate output)
    input_rewindow = '{0}_{1}_rewindow.tif'.format(tile_id, tile_type)
    area_tile = '{0}_{1}.tif'.format(cn.pattern_pixel_area, tile_id)
    pixel_area_rewindow = '{0}_{1}_rewindow.tif'.format(cn.pattern_pixel_area, tile_id)
    tcd_tile = '{0}_{1}.tif'.format(cn.pattern_tcd, tile_id)
    tcd_rewindow = '{0}_{1}_rewindow.tif'.format(cn.pattern_tcd, tile_id)

    # Only rewindows the necessary files if they haven't already been processed (just in case
    # this was run on the spot machine before)

    if not os.path.exists(input_rewindow):

        # Converts the tile of interest to the 400x400 pixel windows
        cmd = ['gdalwarp', '-co', 'COMPRESS=LZW', '-overwrite',
               '-te', str(xmin), str(ymin), str(xmax), str(ymax), '-tap',
               '-tr', str(cn.Hansen_res), str(cn.Hansen_res),
               '-co', 'TILED=YES', '-co', 'BLOCKXSIZE=400', '-co', 'BLOCKYSIZE=400',
               tile, input_rewindow]
        subprocess.check_call(cmd)

    if not os.path.exists(tcd_rewindow):

        # Converts the tcd tile to the 400x400 pixel windows
        cmd = ['gdalwarp', '-co', 'COMPRESS=LZW', '-overwrite', '-dstnodata', '0',
               '-te', str(xmin), str(ymin), str(xmax), str(ymax), '-tap',
               '-tr', str(cn.Hansen_res), str(cn.Hansen_res),
               '-co', 'TILED=YES', '-co', 'BLOCKXSIZE=400', '-co', 'BLOCKYSIZE=400',
               tcd_tile, tcd_rewindow]
        subprocess.check_call(cmd)

    else:

        print "Canopy cover for {} already rewindowed.".format(tile_id)

    if not os.path.exists(pixel_area_rewindow):

        # Converts the pixel area tile to the 400x400 pixel windows
        cmd = ['gdalwarp', '-co', 'COMPRESS=LZW', '-overwrite', '-dstnodata', '0',
               '-te', str(xmin), str(ymin), str(xmax), str(ymax), '-tap',
               '-tr', str(cn.Hansen_res), str(cn.Hansen_res),
               '-co', 'TILED=YES', '-co', 'BLOCKXSIZE=400', '-co', 'BLOCKYSIZE=400',
               area_tile, pixel_area_rewindow]
        subprocess.check_call(cmd)

    else:

        print "Pixel area for {} already rewindowed.".format(tile_id)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, '{}_rewindow'.format(tile_type))


# Converts the existing (per ha) values to per pixel values (e.g., emissions/ha to emissions/pixel)
# and sums those values in each 400x400 pixel window.
# The sum for each 400x400 pixel window is stored in a 2D array, which is then converted back into a raster at
# 0.1x0.1 degree resolution (approximately 10m in the tropics).
# Each pixel in that raster is the sum of the 30m pixels converted to value/pixel (instead of value/ha).
# The 0.1x0.1 degree tile is output.
def aggregate(tile, thresh):

    # start time
    start = datetime.datetime.now()

    # Extracts the tile id, tile type, and bounding box for the tile
    tile_id = uu.get_tile_id(tile)
    tile_type = uu.get_tile_type(tile)
    xmin, ymin, xmax, ymax = uu.coords(tile_id)

    print "  Converting {} to per-pixel values...".format(tile)

    # Name of inputs
    focal_tile_rewindow = '{0}_{1}_rewindow.tif'.format(tile_id, tile_type)
    pixel_area_rewindow = '{0}_{1}_rewindow.tif'.format(cn.pattern_pixel_area, tile_id)
    tcd_rewindow = '{0}_{1}_rewindow.tif'.format(cn.pattern_tcd, tile_id)

    # Opens input tiles for rasterio
    in_src = rasterio.open(focal_tile_rewindow)
    pixel_area_src = rasterio.open(pixel_area_rewindow)
    tcd_src = rasterio.open(tcd_rewindow)

    # Grabs the windows of the tile (stripes) in order to iterate over the entire tif without running out of memory
    windows = in_src.block_windows(1)

    #2D array in which the 0.1x0.1 deg aggregated sums will be stored
    sum_array = np.zeros([100,100], 'float32')

    # Iterates across the windows (400x400 30m pixels) of the input tile
    for idx, window in windows:

        # Creates windows for each input tile
        in_window = in_src.read(1, window=window)
        pixel_area_window = pixel_area_src.read(1, window=window)
        tcd_window = tcd_src.read(1, window=window)

        # Applies the tree cover density threshold to the 30x30m pixels
        if thresh > 0:

            in_window = np.ma.masked_where(tcd_window < thresh, in_window)
            in_window = in_window.filled(0)

        # Calculates the per-pixel value from the input tile value (/ha to /pixel)
        per_pixel_value = in_window * pixel_area_window / cn.m2_per_ha

        # Sums the pixels to create a total value for the 0.1x0.1 deg pixel
        non_zero_pixel_sum = np.sum(per_pixel_value)

        # Stores the resulting value in the array
        sum_array[idx[0], idx[1]] = non_zero_pixel_sum

    # Converts the annual biomass gain values annual gain in megatonnes and makes negative (because removals are negative)
    if cn.pattern_annual_gain_AGB_BGB_all_types in tile_type:
        sum_array = sum_array / cn.tonnes_to_megatonnes * -1

    # Converts the cumulative CO2 gain values to annualized CO2 in megatonnes and makes negative (because removals are negative)
    if cn.pattern_cumul_gain_AGCO2_BGCO2_all_types in tile_type:
        sum_array = sum_array / cn.loss_years / cn.tonnes_to_megatonnes * -1

    # Converts the cumulative net flux CO2 values to annualized net flux CO2 in megatonnes
    if cn.pattern_net_flux in tile_type:
        sum_array = sum_array / cn.loss_years / cn.tonnes_to_megatonnes

    # Converts the cumulative gross emissions all gases CO2e values to annualized gross emissions CO2e in megatonnes
    if cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil in tile_type:
        sum_array = sum_array / cn.loss_years / cn.tonnes_to_megatonnes

    # Converts the cumulative gross emissions all gases CO2e values to annualized gross emissions CO2e in megatonnes
    if cn.pattern_gross_emis_co2_only_all_drivers_biomass_soil in tile_type:
        sum_array = sum_array / cn.loss_years / cn.tonnes_to_megatonnes

    # Converts the cumulative gross emissions all gases CO2e values to annualized gross emissions CO2e in megatonnes
    if cn.pattern_gross_emis_non_co2_all_drivers_biomass_soil in tile_type:
        sum_array = sum_array / cn.loss_years / cn.tonnes_to_megatonnes

    print "  Creating aggregated tile for {}...".format(tile)

    # Converts array to the same output type as the raster that is created below
    sum_array = np.float32(sum_array)

    # Creates a tile at 0.1x0.1 degree resolution (approximately 10x10 km in the tropics) where the values are
    # from the 2D array created by rasterio above
    # https://gis.stackexchange.com/questions/279953/numpy-array-to-gtiff-using-rasterio-without-source-raster
    aggregated = rasterio.open("{0}_{1}_10km.tif".format(tile_id, tile_type), 'w',
                                driver='GTiff', compress='lzw', nodata='0', dtype='float32', count=1,
                                height=100, width=100,
                                crs='EPSG:4326', transform=from_origin(xmin,ymax,0.1,0.1))
    aggregated.write(sum_array,1)
    aggregated.close()

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, tile_type)


# Calculates the percent difference between the standard model's net flux output
# and the sensitivity model's net flux output
def percent_diff(std_aggreg_flux, sensit_aggreg_flux, sensit_type):

    # start time
    start = datetime.datetime.now()
    date = datetime.datetime.now()
    date_formatted = date.strftime("%Y_%m_%d")

    # CO2 gain uses non-mangrove non-planted biomass:carbon ratio
    # This produces errors about dividing by 0. As far as I can tell, those are fine. It's just trying to divide NoData
    # pixels by NoData pixels, and it doesn't affect the output.
    perc_diff_calc = '--calc=(A-B)/B*100'.format(sensit_aggreg_flux, std_aggreg_flux)
    perc_diff_outfilename = '{0}_{1}_{2}.tif'.format(cn.pattern_aggreg_sensit_perc_diff, sensit_type, date_formatted)
    perc_diff_outfilearg = '--outfile={}'.format(perc_diff_outfilename)
    cmd = ['gdal_calc.py', '-A', sensit_aggreg_flux, '-B', std_aggreg_flux, perc_diff_calc, perc_diff_outfilearg,
           '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW']
    subprocess.check_call(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, 'global', sensit_aggreg_flux)


# Maps where the sources stay sources, sinks stay sinks, sources become sinks, and sinks become sources
def sign_change(std_aggreg_flux, sensit_aggreg_flux, sensit_type):

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

        # Iterates through the windows in the standard net flux output
        for idx, window in windows:

            std_window = std_src.read(1, window=window)
            sensit_window = sensit_src.read(1, window=window)

            # Defaults the sign change output raster to 0
            dst_data = np.zeros((window.height, window.width), dtype='Float64')

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
    uu.end_of_fx_summary(start, 'global', sensit_aggreg_flux)