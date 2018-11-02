### This script assigns annual above and belowground biomass gain rates (in the units of IPCC Table 4.9 (currently tonnes
### biomass/ha/yr)) to nnon-mangrove natural forest pixels.
### The inputs are continent-ecozone tiles and natural forest age category tiles, as well as IPCC Table 4.9, formatted
### for easy ingestion by pandas.
### Essentially, this does some processing of the IPCC gain rate table, then uses it as a dictionary that it applies
### to every pixel in every tile.
### Each continent-ecozone-forest age category combination gets its own code, which matches the codes in the
### processed IPCC table.
### Belowground biomass gain rate is a constant proportion of aboveground biomass gain rate.

import utilities
import datetime
import numpy as np
import rasterio
import subprocess

# Necessary to suppress a pandas error later on
np.set_printoptions(threshold=np.nan)

def annual_gain_rate(tile_id, gain_table_dict):

    # Converts the forest age category decision tree output values to the three age categories--
    # 10000: primary forest; 20000: secondary forest > 20 years; 30000: secondary forest <= 20 years
    # These are five digits so they can easily be added to the four digits of the continent-ecozone code to make unique codes
    # for each continent-ecozone-age combination.
    # The key in the dictionary is the forest age category decision tree endpoints.
    age_dict = {0: 0, 1: 20000, 2: 20000, 3: 10000, 4: 30000, 5: 20000, 6: 10000, 7: 30000, 8: 30000, 9: 30000, 10: 30000}

    print "Processing:", tile_id

    # Start time
    start = datetime.datetime.now()

    # Names of the forest age category, continent-ecozone, and mangrove biomass tiles
    age_cat = '{0}_{1}.tif'.format(utilities.pattern_age_cat_natrl_forest, tile_id)
    cont_eco = '{0}_{1}.tif'.format(utilities.pattern_cont_eco_processed, tile_id)
    mangrove_biomass = '{0}_{1}.tif'.format(utilities.pattern_mangrove_biomass, tile_id)

    # Name of the output natural forest gain rate tile
    AGB_gain_rate = '{0}_{1}.tif'.format(utilities.pattern_annual_gain_AGB_natrl_forest, tile_id)

    # Removes the nodata values in the mangrove biomass rasters because having nodata values in the mangroves didn't work
    # in gdal_calc. The gdal_calc expression didn't know how to evaluate nodata values, so I had to remove them.
    # Mangrove tiles that have the nodata pixels removed
    mangrove_reclass = '{0}_reclass_{1}.tif'.format(utilities.pattern_mangrove_biomass, tile_id)
    print "Removing nodata values in mangrove biomass {}".format(tile_id)
    cmd = ['gdal_translate', '-a_nodata', 'none', mangrove_biomass, mangrove_reclass]
    subprocess.check_call(cmd)

    print "  Reading input files and creating aboveground biomass gain rate for {}".format(tile_id)

    # Opens continent-ecozone tile
    with rasterio.open(cont_eco) as cont_eco_src:

        # Grabs metadata about the tif, like its location/projection/cellsize
        kwargs = cont_eco_src.meta

        # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
        windows = cont_eco_src.block_windows(1)

        # Opens age category tile
        with rasterio.open(mangrove_reclass) as mangrove_biomass_src:

            # Opens age category tile
            with rasterio.open(age_cat) as age_cat_src:

                # Updates kwargs for the output dataset.
                # Need to update data type to float 32 so that it can handle fractional gain rates
                kwargs.update(
                    driver='GTiff',
                    count=1,
                    compress='lzw',
                    nodata=0,
                    dtype='float32'
                )

                # Opens the output aboveground biomass gain rate tile, giving it the arguments of the input tiles
                with rasterio.open(AGB_gain_rate, 'w', **kwargs) as dst_above:

                    # Iterates across the windows (1 pixel strips) of the input tile
                    for idx, window in windows:

                        # Creates windows for each input raster
                        cont_eco = cont_eco_src.read(1, window=window)
                        mangrove = mangrove_biomass_src.read(1, window=window)
                        age_cat = age_cat_src.read(1, window=window)

                        # print mangrove

                        # Recodes the input forest age category array with 10 different values into the 3 actual age categories
                        age_recode = np.vectorize(age_dict.get)(age_cat)

                        # Adds the age category codes to the continent-ecozone codes to create an array of unique continent-ecozone-age codes
                        cont_eco_age = cont_eco + age_recode

                        # Converts the continent-ecozone-age array to float so that the values can be replaced with fractional gain rates
                        cont_eco_age = cont_eco_age.astype('float32')

                        # Applies the dictionary of continent-ecozone-age gain rates to the continent-ecozone-age array to
                        # get annual gain rates (metric tons aboveground biomass/yr) for each pixel
                        for key, value in gain_table_dict.iteritems():
                            cont_eco_age[cont_eco_age == key] = value

                        dst_above_data = cont_eco_age

                        # print dst_above_data

                        # test = np.ma.masked_where(mangrove != 0, dst_above_data)

                        # print test
                        #
                        # print mangrove.shape
                        # print dst_above_data.shape
                        # print test.shape

                        test = dst_above_data[mangrove == 0]

                        # test = dst_above_data[mangrove != 0]

                        # Writes the output window to the output
                        dst_above.write_band(1, test, window=window)

                        # sys.exit()

    utilities.upload_final(utilities.pattern_annual_gain_AGB_natrl_forest, utilities.annual_gain_AGB_natrl_forest_dir, tile_id)

    # Calculates belowground biomass rate from aboveground biomass rate
    print "  Creating belowground biomass gain rate tile"
    above_to_below_calc = '--calc=(A>0)*A*{}'.format(utilities.above_to_below_natrl_forest)
    below_outfilename = '{0}_{1}.tif'.format(utilities.pattern_annual_gain_BGB_natrl_forest, tile_id)
    below_outfilearg = '--outfile={}'.format(below_outfilename)
    cmd = ['gdal_calc.py', '-A', AGB_gain_rate, above_to_below_calc, below_outfilearg,
           '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW']
    subprocess.check_call(cmd)

    utilities.upload_final(utilities.pattern_annual_gain_BGB_natrl_forest, utilities.annual_gain_BGB_natrl_forest_dir, tile_id)

    end = datetime.datetime.now()
    elapsed_time = end-start

    print "  Processing time for tile", tile_id, ":", elapsed_time
