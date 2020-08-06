import datetime
import rasterio
from subprocess import Popen, PIPE, STDOUT, check_call
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Calculates cumulative aboveground carbon dioxide gain in mangroves
def gross_removals_all_forest_types(tile_id, output_pattern_list, sensit_type):

    uu.print_log("Calculating cumulative CO2 removals:", tile_id)

    # Start time
    start = datetime.datetime.now()

    # Names of the input tiles, modified according to sensitivity analysis
    gain_rate_AGC = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_annual_gain_AGC_all_types)
    gain_rate_BGC = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_annual_gain_BGC_all_types)
    gain_year_count = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_gain_year_count)

    # Names of the output removal tiles
    cumulative_gain_AGCO2 = '{0}_{1}.tif'.format(tile_id, output_pattern_list[0])
    cumulative_gain_BGCO2 = '{0}_{1}.tif'.format(tile_id, output_pattern_list[1])
    cumulative_gain_AGCO2_BGCO2 = '{0}_{1}.tif'.format(tile_id, output_pattern_list[2])

    # Opens the input tiles
    gain_rate_AGC_src = rasterio.open(gain_rate_AGC)
    gain_rate_BGC_src = rasterio.open(gain_rate_BGC)
    gain_year_count_src = rasterio.open(gain_year_count)

    # Grabs metadata for an input tile
    kwargs = gain_rate_AGC_src.meta

    # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
    windows = gain_rate_AGC_src.block_windows(1)

    # Updates kwargs for the output dataset.
    kwargs.update(
        driver='GTiff',
        count=1,
        compress='lzw',
        nodata=0
    )

    # The output files: aboveground gross removals, belowground gross removals, above+belowground gross removals. Adds metadata tags
    cumulative_gain_AGCO2_dst = rasterio.open(cumulative_gain_AGCO2, 'w', **kwargs)
    uu.add_rasterio_tags(cumulative_gain_AGCO2_dst, sensit_type)
    cumulative_gain_AGCO2_dst.update_tags(
        units='megagrams aboveground CO2/ha over entire model period')
    cumulative_gain_AGCO2_dst.update_tags(
        source='annual removal factors and gain year count')
    cumulative_gain_AGCO2_dst.update_tags(
        extent='Full model extent')

    cumulative_gain_BGCO2_dst = rasterio.open(cumulative_gain_BGCO2, 'w', **kwargs)
    uu.add_rasterio_tags(cumulative_gain_BGCO2_dst, sensit_type)
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
