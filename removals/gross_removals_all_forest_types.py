"""
Function to create gross removals tiles
"""

import datetime
import rasterio

import constants_and_names as cn
import universal_util as uu

def gross_removals_all_forest_types(tile_id, output_pattern_list):
    """
    Calculates cumulative aboveground carbon dioxide removals in mangroves
    :param tile_id: tile to be processed, identified by its tile id
    :param output_pattern_list: pattern for output tile names
    :return: 3 tiles: gross aboveground removals, belowground removals, aboveground+belowground removals
        Units: Mg CO2/ha over entire model period.
    """

    uu.print_log(f'Calculating cumulative CO2 removals: {tile_id}')

    # Start time
    start = datetime.datetime.now()

    # Names of the input tiles, modified according to sensitivity analysis
    gain_rate_AGC = uu.sensit_tile_rename(cn.SENSIT_TYPE, tile_id, cn.pattern_annual_gain_AGC_all_types)
    gain_rate_BGC = uu.sensit_tile_rename(cn.SENSIT_TYPE, tile_id, cn.pattern_annual_gain_BGC_all_types)
    gain_year_count = uu.sensit_tile_rename(cn.SENSIT_TYPE, tile_id, cn.pattern_gain_year_count)

    # Names of the output removal tiles
    cumulative_gain_AGCO2 = f'{tile_id}_{output_pattern_list[0]}.tif'
    cumulative_gain_BGCO2 = f'{tile_id}_{output_pattern_list[1]}.tif'
    cumulative_gain_AGCO2_BGCO2 = f'{tile_id}_{output_pattern_list[2]}.tif'

    # Opens the input tiles if they exist. If one of the inputs doesn't exist,
    try:
        gain_rate_AGC_src = rasterio.open(gain_rate_AGC)
        uu.print_log(f'    Aboveground removal factor tile found for {tile_id}')
    except rasterio.errors.RasterioIOError:
        uu.print_log(f'    No aboveground removal factor tile found for {tile_id}. Not creating gross removals.')
    try:
        gain_rate_BGC_src = rasterio.open(gain_rate_BGC)
        uu.print_log(f'    Belowground removal factor tile found for {tile_id}')
    except rasterio.errors.RasterioIOError:
        uu.print_log(f'    No belowground removal factor tile found for {tile_id}. Not creating gross removals.')
    try:
        gain_year_count_src = rasterio.open(gain_year_count)
        uu.print_log(f'    Gain year count tile found for {tile_id}')
    except rasterio.errors.RasterioIOError:
        uu.print_log(f'    No gain year count tile found for {tile_id}. Not creating gross removals.')


    # Grabs metadata for an input tile
    kwargs = gain_rate_AGC_src.meta

    # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
    windows = gain_rate_AGC_src.block_windows(1)

    # Updates kwargs for the output dataset.
    kwargs.update(
        driver='GTiff',
        count=1,
        compress='DEFLATE',
        nodata=0
    )

    # The output files: aboveground gross removals, belowground gross removals, above+belowground gross removals. Adds metadata tags
    cumulative_gain_AGCO2_dst = rasterio.open(cumulative_gain_AGCO2, 'w', **kwargs)
    uu.add_universal_metadata_rasterio(cumulative_gain_AGCO2_dst)
    cumulative_gain_AGCO2_dst.update_tags(
        units='megagrams aboveground CO2/ha over entire model period')
    cumulative_gain_AGCO2_dst.update_tags(
        source='annual removal factors and gain year count')
    cumulative_gain_AGCO2_dst.update_tags(
        extent='Full model extent')

    cumulative_gain_BGCO2_dst = rasterio.open(cumulative_gain_BGCO2, 'w', **kwargs)
    uu.add_universal_metadata_rasterio(cumulative_gain_BGCO2_dst)
    cumulative_gain_BGCO2_dst.update_tags(
        units='megagrams belowground CO2/ha over entire model period')
    cumulative_gain_BGCO2_dst.update_tags(
        source='annual removal factors and gain year count')
    cumulative_gain_BGCO2_dst.update_tags(
        extent='Full model extent')

    cumulative_gain_AGCO2_BGCO2_dst = rasterio.open(cumulative_gain_AGCO2_BGCO2, 'w', **kwargs)
    cumulative_gain_AGCO2_BGCO2_dst.update_tags(
        units='megagrams aboveground+belowground CO2/ha over entire model period')
    cumulative_gain_AGCO2_BGCO2_dst.update_tags(
        source='annual removal factors and gain year count')
    cumulative_gain_AGCO2_BGCO2_dst.update_tags(
        extent='Full model extent')

    uu.check_memory()

    # Iterates across the windows (1 pixel strips) of the input tiles
    for idx, window in windows:

        # Creates a processing window for each input raster
        gain_rate_AGC_window = gain_rate_AGC_src.read(1, window=window)
        gain_rate_BGC_window = gain_rate_BGC_src.read(1, window=window)
        gain_year_count_window = gain_year_count_src.read(1, window=window)

        # Converts the annual removal rate into gross removals
        cumulative_gain_AGCO2_window = gain_rate_AGC_window * gain_year_count_window * cn.c_to_co2
        cumulative_gain_BGCO2_window = gain_rate_BGC_window * gain_year_count_window * cn.c_to_co2
        cumulative_gain_AGCO2_BGCO2_window = cumulative_gain_AGCO2_window + cumulative_gain_BGCO2_window

        # Writes the output windows to the output files
        cumulative_gain_AGCO2_dst.write_band(1, cumulative_gain_AGCO2_window, window=window)
        cumulative_gain_BGCO2_dst.write_band(1, cumulative_gain_BGCO2_window, window=window)
        cumulative_gain_AGCO2_BGCO2_dst.write_band(1, cumulative_gain_AGCO2_BGCO2_window, window=window)


    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, output_pattern_list[0])
