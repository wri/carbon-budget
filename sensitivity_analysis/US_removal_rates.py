import datetime
import numpy as np
import os
import rasterio
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Creates Hansen tiles out of FIA region shapefile
def prep_FIA_regions(tile_id):

    uu.print_log("Creating Hansen tile for FIA regions")

    # Start time
    start = datetime.datetime.now()

    uu.print_log("Getting extent of", tile_id)
    xmin, ymin, xmax, ymax = uu.coords(tile_id)


    uu.print_log("Rasterizing FIA region shapefile", tile_id)
    blocksizex = 1024
    blocksizey = 1024
    uu.rasterize('{}.shp'.format(cn.name_FIA_regions_raw[:-4]),
                   "{0}_{1}.tif".format(tile_id, cn.pattern_FIA_regions_processed),
                        xmin, ymin, xmax, ymax, blocksizex, blocksizey, '.00025', 'Byte', 'regionCode', '0')

    uu.print_log("Checking if {} contains any data...".format(tile_id))
    no_data = uu.check_for_data("{0}_{1}.tif".format(tile_id, cn.pattern_FIA_regions_processed))

    if no_data:

        uu.print_log("  No data found. Deleting {}.".format(tile_id))
        os.remove("{0}_{1}.tif".format(tile_id, cn.pattern_FIA_regions_processed))

    else:

        uu.print_log("  Data found in {}. Copying tile to s3...".format(tile_id))
        uu.upload_final(cn.FIA_regions_processed_dir, tile_id, cn.pattern_FIA_regions_processed)
        uu.print_log("    Tile copied to s3")

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_FIA_regions_processed)


# Creates annual AGB and BGB removal rate rasters for US using US-specific removal rates
def US_removal_rate_calc(tile_id, gain_table_group_region_age_dict, gain_table_group_region_dict, output_pattern_list, sensit_type):

    uu.print_log("Assigning US removal rates:", tile_id)

    # Start time
    start = datetime.datetime.now()

    # Names of the input tiles
    gain = '{0}_{1}.tif'.format(cn.pattern_gain, tile_id)
    annual_gain_standard = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_AGB_IPCC_defaults)  # Used as the template extent/default for the US
    US_age_cat = '{0}_{1}.tif'.format(tile_id, cn.pattern_US_forest_age_cat_processed)
    US_forest_group = '{0}_{1}.tif'.format(tile_id, cn.pattern_FIA_forest_group_processed)
    US_region = '{0}_{1}.tif'.format(tile_id, cn.pattern_FIA_regions_processed)

    # Opens standard model removals rate tile
    with rasterio.open(annual_gain_standard) as annual_gain_standard_src:

        # Grabs metadata about the tif, like its location/projection/cell size
        kwargs = annual_gain_standard_src.meta

        # Grabs the windows of the tile (stripes) so we can iterate over the entire tif without running out of memory
        windows = annual_gain_standard_src.block_windows(1)

        # Opens other necessary tiles
        gain_src = rasterio.open(gain)
        US_age_cat_src = rasterio.open(US_age_cat)
        US_forest_group_src = rasterio.open(US_forest_group)
        US_region_src = rasterio.open(US_region)

        # Updates kwargs for the output dataset
        kwargs.update(
            driver='GTiff',
            count=1,
            compress='DEFLATE',
            nodata=0
        )

        # Opens the output tiles (aboveground and belowground), giving them the proporties of the standard model removal rate tiles
        agb_dst = rasterio.open('{0}_{1}.tif'.format(tile_id, output_pattern_list[0]), 'w', **kwargs)
        bgb_dst = rasterio.open('{0}_{1}.tif'.format(tile_id, output_pattern_list[1]), 'w', **kwargs)

        # Iterates across the windows (1 pixel strips) of the input tile
        for idx, window in windows:

            # Creates window for each input raster
            annual_gain_standard_window = annual_gain_standard_src.read(1, window=window)
            gain_window = gain_src.read(1, window=window)
            US_age_cat_window = US_age_cat_src.read(1, window=window)
            US_forest_group_window = US_forest_group_src.read(1, window=window)
            US_region_window = US_region_src.read(1, window=window)

            # Masks the three input tiles (age category, forest group, FIA region) to the pixels to the standard removals model extent
            age_cat_masked_window = np.ma.masked_where(annual_gain_standard_window == 0, US_age_cat_window).filled(0).astype('uint16')
            US_forest_group_masked_window = np.ma.masked_where(annual_gain_standard_window == 0, US_forest_group_window).filled(0).astype('uint16')
            US_region_masked_window = np.ma.masked_where(annual_gain_standard_window == 0, US_region_window).filled(0).astype('uint16')

            # Performs the same operation on the three rasters as is done on the values in the table in order to
            # make the codes (dictionary key) match. Then, combines the three rasters. These values now match the key values in the spreadsheet.
            group_region_age_combined_window = (age_cat_masked_window * 10 + US_forest_group_masked_window * 100 + US_region_masked_window).astype('float32')

            # Applies the dictionary of group-region-age removals rates to the group-region-age numpy array to
            # get annual removals rates (metric tons aboveground biomass/yr) for each pixel that has removals in the standard model
            for key, value in gain_table_group_region_age_dict.items():
                annual_gain_standard_window[group_region_age_combined_window == key] = value

            # Replaces all values that have Hansen gain pixels with 0 so that they can be filled with Hansen removals pixel-specific
            # values (rates for youngest forest age category)
            agb_without_gain_pixel_window = np.ma.masked_where(gain_window != 0, annual_gain_standard_window).filled(0)

            # Creates key array for the dictionary that applies to just Hansen gain pixels, then masks the
            # array to just Hansen gain pixels. This is now ready for matching with the dictionary for Hansen gain pixels.
            agb_with_gain_pixel_window = (US_forest_group_masked_window * 100 + US_region_masked_window).astype('float32')
            agb_with_gain_pixel_window = np.ma.masked_where((gain_window == 0) & (annual_gain_standard_window != 0), agb_with_gain_pixel_window).filled(0)

            # Applies the dictionary of region-age-group removals rates to the region-age-group array to
            # get annual removals rates (metric tons aboveground biomass/yr) for each pixel that has removals in the standard model
            for key, value in gain_table_group_region_dict.items():
                agb_with_gain_pixel_window[agb_with_gain_pixel_window == key] = value

            # Combines the array of removal rates that has no rates where there are Hansen gain pixels with the array of
            # removal rates that has has values only where there are Hansen gain pixels
            agb_dst_window = agb_without_gain_pixel_window + agb_with_gain_pixel_window

            # The maximum rate in the US-specific removals rate dictionary
            max_rate = max(gain_table_group_region_age_dict.values())

            # Any pixel that has a rate 1.2x as large as the largest rate in the dictionary gets assigned the standard model's rate
            # for that pixel. This can occur for pixels that don't have a rate for a given region-group-age or region-group
            # and therefore the key value gets stuck in the array rather than being replaced by a non-existent rate
            # from the dictionary.
            agb_dst_corrected_window = np.where(agb_dst_window > (max_rate*1.2), annual_gain_standard_window, agb_dst_window)

            # Calculates BGB removal rate from AGB removal rate
            bgb_dst_window = agb_dst_corrected_window * cn.below_to_above_non_mang

            # Writes the output windows to the outputs
            agb_dst.write_band(1, agb_dst_corrected_window, window=window)
            bgb_dst.write_band(1, bgb_dst_window, window=window)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, output_pattern_list[0])