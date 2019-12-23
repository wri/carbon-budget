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

    print "Creating Hansen tile for FIA regions"

    # Start time
    start = datetime.datetime.now()

    print "Getting extent of", tile_id
    xmin, ymin, xmax, ymax = uu.coords(tile_id)


    print "Rasterizing FIA region shapefile", tile_id
    uu.rasterize('{}.shp'.format(cn.name_FIA_regions_raw[:-4]),
                   "{0}_{1}.tif".format(tile_id, cn.pattern_FIA_regions_processed),
                        xmin, ymin, xmax, ymax, '.00025', 'Byte', 'regionCode', '0')

    print "Checking if {} contains any data...".format(tile_id)
    no_data = uu.check_for_data("{0}_{1}.tif".format(tile_id, cn.pattern_FIA_regions_processed))

    if no_data:

        print "  No data found. Deleting {}.".format(tile_id)
        os.remove("{0}_{1}.tif".format(tile_id, cn.pattern_FIA_regions_processed))

    else:

        print "  Data found in {}. Copying tile to s3...".format(tile_id)
        uu.upload_final(cn.FIA_regions_processed_dir, tile_id, cn.pattern_FIA_regions_processed)
        print "    Tile copied to s3"

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_FIA_regions_processed)


def US_removal_rate_calc(tile_id, gain_table_dict, pattern, sensit_type):

    print "Assigning US removal rates:", tile_id

    # Start time
    start = datetime.datetime.now()

    # Names of the input tiles
    gain = '{0}_{1}.tif'.format(cn.pattern_gain, tile_id)
    annual_gain_standard = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_AGB_natrl_forest)
    US_age_cat = '{0}_{1}.tif'.format(tile_id, cn.pattern_US_forest_age_cat_processed)
    US_forest_group = '{0}_{1}.tif'.format(tile_id, cn.pattern_FIA_forest_group_processed)
    US_region = '{0}_{1}.tif'.format(tile_id, cn.pattern_FIA_regions_processed)

    print "  Reading input files and evaluating conditions"

    # Opens biomass tile
    with rasterio.open(annual_gain_standard) as annual_gain_standard_src:

        # Grabs metadata about the tif, like its location/projection/cellsize
        kwargs = annual_gain_standard_src.meta

        # Grabs the windows of the tile (stripes) so we can iterate over the entire tif without running out of memory
        windows = annual_gain_standard_src.block_windows(1)

        # Opens gain tile
        gain_src = rasterio.open(gain)
        US_age_cat_src = rasterio.open(US_age_cat)
        US_forest_group_src = rasterio.open(US_forest_group)
        US_region_src = rasterio.open(US_region)

        # Opens the output tile, giving it the arguments of the input tiles
        dst = rasterio.open('{0}_{1}.tif'.format(tile_id, pattern), 'w', **kwargs)

        # Iterates across the windows (1 pixel strips) of the input tile
        for idx, window in windows:

            # Creates windows for each input raster
            annual_gain_standard_window = annual_gain_standard_src.read(1, window=window)
            gain_window = gain_src.read(1, window=window)
            US_age_cat_window = US_age_cat_src.read(1, window=window)
            US_forest_group_window = US_forest_group_src.read(1, window=window)
            US_region_window = US_region_src.read(1, window=window)

            # Create a 0s array for the output
            dst_data = np.zeros((window.height, window.width), dtype='float32')
            print dst_data[0][1:50]

            age_cat_masked_window = np.ma.masked_where(annual_gain_standard_window == 0, US_age_cat_window).filled(0)
            US_forest_group_masked_window = np.ma.masked_where(annual_gain_standard_window == 0, US_forest_group_window).filled(0)
            US_region_masked_window = np.ma.masked_where(annual_gain_standard_window == 0, US_region_window).filled(0)

            print age_cat_masked_window[0][1:50]
            print US_forest_group_masked_window[0][1:50]
            print US_region_masked_window[0][1:50]

            group_region_age_combined_window = age_cat_masked_window * 10 + US_forest_group_masked_window * 100 + US_region_masked_window
            print US_forest_group_masked_window * 100
            print group_region_age_combined_window[0][1:50]

            group_region_age_combined_window = group_region_age_combined_window.astype('float32')
            print group_region_age_combined_window[0][1:50]

            # Applies the dictionary of continent-ecozone-age gain rates to the continent-ecozone-age array to
            # get annual gain rates (metric tons aboveground biomass/yr) for each pixel
            for key, value in gain_table_dict.iteritems():
                dst_data[group_region_age_combined_window == key] = value

            print dst_data[0][1:50]

                ###### NEED TO ADD IN MAKING ANY HANSEN GAIN PIXEL HAVE THE YOUNG RATE

            os.quit()




            # Writes the output window to the output
            dst.write_band(1, group_region_age_combined_window, window=window)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, pattern)