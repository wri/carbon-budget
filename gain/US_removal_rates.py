import datetime
import numpy as np
import os
import rasterio
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu


# Creates annual AGB and BGB removal rate rasters for US using US-specific removal rates
def US_removal_rate_calc(tile_id, gain_table_group_region_age_dict, gain_table_group_region_dict, output_pattern_list):

    uu.print_log("Assigning US removal rates:", tile_id)

    # Start time
    start = datetime.datetime.now()

    # Names of the input tiles
    gain = '{0}_{1}.tif'.format(cn.pattern_gain, tile_id)
    US_age_cat = '{0}_{1}.tif'.format(tile_id, cn.pattern_age_cat_natrl_forest_US)
    US_forest_group = '{0}_{1}.tif'.format(tile_id, cn.pattern_FIA_forest_group_processed)
    US_region = '{0}_{1}.tif'.format(tile_id, cn.pattern_FIA_regions_processed)

    # Opens standard model gain rate tile
    with rasterio.open(US_age_cat) as US_age_cat_src:

        # Grabs metadata about the tif, like its location/projection/cell size
        kwargs = US_age_cat_src.meta

        # Grabs the windows of the tile (stripes) so we can iterate over the entire tif without running out of memory
        windows = US_age_cat_src.block_windows(1)

        # Opens other necessary tiles
        gain_src = rasterio.open(gain)
        # US_age_cat_src = rasterio.open(US_age_cat)
        US_forest_group_src = rasterio.open(US_forest_group)
        US_region_src = rasterio.open(US_region)

        # Updates kwargs for the output dataset
        kwargs.update(
            driver='GTiff',
            count=1,
            compress='lzw',
            nodata=0,
            dtype='float32'
        )

        # Opens the output tiles (aboveground and belowground), giving them the proporties of the standard model removal rate tiles
        agc_bgc_rate_dst = rasterio.open('{0}_{1}.tif'.format(tile_id, output_pattern_list[0]), 'w', **kwargs)

        # Iterates across the windows (1 pixel strips) of the input tile
        for idx, window in windows:

            # Creates window for each input raster
            gain_window = gain_src.read(1, window=window)
            US_age_cat_window = US_age_cat_src.read(1, window=window).astype('float32')
            US_forest_group_window = US_forest_group_src.read(1, window=window).astype('float32')
            US_region_window = US_region_src.read(1, window=window).astype('float32')


            # Creates empty windows (arrays) that will store gain rates. There are separate arrays for
            # no Hansen gain pixels and for Hansen gain pixels. These are later combined.
            # Pixels without and with Hansen gain are treated separately because gain pixels automatically get the youngest
            # removal rate, regardless of their age category.
            agc_bgc_without_gain_pixel_window = np.zeros((1,40000), dtype='float32')
            agc_bgc_with_gain_pixel_window = np.zeros((1,40000), dtype='float32')

            # Performs the same operation on the three rasters as is done on the values in the table in order to
            # make the codes (dictionary key) match. Then, combines the three rasters. These values now match the key values in the spreadsheet.
            group_region_age_combined_window = (US_age_cat_window * 10000 + US_forest_group_window * 100 + US_region_window)


            # Masks the combined age-group-region raster to the three input tiles (age category, forest group, FIA region).
            # Excludes all Hamsen gain pixels.
            group_region_age_combined_window = np.ma.masked_where(US_age_cat_window == 0, group_region_age_combined_window).filled(0).astype('uint16')
            group_region_age_combined_window = np.ma.masked_where(US_forest_group_window == 0, group_region_age_combined_window).filled(0)
            group_region_age_combined_window = np.ma.masked_where(US_region_window == 0, group_region_age_combined_window).filled(0)
            group_region_age_combined_window = np.ma.masked_where(gain_window != 0, group_region_age_combined_window).filled(0)

            # Applies the dictionary of group-region-age gain rates to the group-region-age numpy array to
            # get annual gain rates (Mg AGC+BGC/ha/yr) for each non-Hansen gain pixel
            for key, value in gain_table_group_region_age_dict.items():
                agc_bgc_without_gain_pixel_window[group_region_age_combined_window == key] = value


            # This is for pixels with Hansen gain, so it assumes the age category is young and therefore only
            # includes region and forest group.
            # Performs the same operation on the two rasters as is done on the values in the table in order to
            # make the codes (dictionary key) match. Then, combines the two rasters. These values now match the key values in the spreadsheet.
            group_region_combined_window = (US_forest_group_window * 100 + US_region_window)

            # Masks the combined age-group-region raster to the three input tiles (age category, forest group, FIA region).
            # It masks to age category simply to limit the output to pixels that had some age category input.
            # The real age category masking comes when the array is masked to only where Hansen gain occurs.
            group_region_combined_window = np.ma.masked_where(US_age_cat_window == 0, group_region_combined_window).filled(0).astype('uint16')
            group_region_combined_window = np.ma.masked_where(US_forest_group_window == 0, group_region_combined_window).filled(0)
            group_region_combined_window = np.ma.masked_where(US_region_window == 0, group_region_combined_window).filled(0)
            group_region_combined_window = np.ma.masked_where(gain_window == 0, group_region_combined_window).filled(0)

            # Applies the dictionary of group-region gain rates to the group-region numpy array to
            # get annual gain rates (Mg AGC+BGC/ha/yr) for each pixel that doesn't have Hansen gain
            for key, value in gain_table_group_region_dict.items():
                agc_bgc_with_gain_pixel_window[group_region_combined_window == key] = value

            agc_bgc_rate_window = agc_bgc_without_gain_pixel_window + agc_bgc_with_gain_pixel_window

            # Writes the output to raster
            agc_bgc_rate_dst.write_band(1, agc_bgc_rate_window, window=window)


    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, output_pattern_list[0])