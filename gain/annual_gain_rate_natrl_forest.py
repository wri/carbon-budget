### This script assigns an annual biomass gain rate (in the units of IPCC Table 4.9 (currently tons aboveground
### biomass/ha/yr)) to natural forest non-mangrove pixels.
### The inputs are continent-ecozone tiles and natural forest age category tiles, as well as IPCC Table 4.9, formatted
### for easy ingestion by pandas.
### Essentially, this does some processing of the IPCC gain rate table, then uses it as a dictionary that it applies
### to every pixel in every tile.
### Each continent-ecozone-forest age category combination gets its own code, which matches the codes in the
### processed IPCC table.

import utilities
import datetime
import numpy as np
import rasterio

# Necessary to suppress a pandas error later on
np.set_printoptions(threshold=np.nan)

def annual_gain_rate(tile_id, gain_table_dict):

    upload_dir = 's3://gfw2-data/climate/carbon_model/annual_gain_rate_natural_forest/20181003/'

    # Converts the forest age category decision tree output values to the three age categories--
    # 10000: primary forest; 20000: secondary forest > 20 years; 30000: secondary forest <= 20 years
    # These are five digits so they can easily be added to the four digits of the continent-ecozone code to make unique codes
    # for each continent-ecozone-age combination.
    # The key in the dictionary is the forest age category decision tree endpoints.
    age_dict = {0: 0, 1: 20000, 2: 20000, 3: 10000, 4: 30000, 5: 20000, 6: 10000, 7: 30000, 8: 30000, 9: 30000, 10: 30000}

    print "Processing:", tile_id

    # Start time
    start = datetime.datetime.now()

    # Names of the forest age category and continent-ecozone tiles
    age_cat = 'forest_age_category_{}.tif'.format(tile_id)
    cont_eco = 'fao_ecozones_continents_processed_{}.tif'.format(tile_id)

    print "  Reading input files and evaluating conditions"

    # Opens continent-ecozone tile
    with rasterio.open(cont_eco) as cont_eco_src:

        # Grabs metadata about the tif, like its location/projection/cellsize
        kwargs = cont_eco_src.meta

        # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
        windows = cont_eco_src.block_windows(1)

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

            # Opens the output tile, giving it the arguments of the input tiles
            with rasterio.open('annual_gain_rate_natural_forest_{}.tif'.format(tile_id), 'w', **kwargs) as dst:

                # Iterates across the windows (1 pixel strips) of the input tile
                for idx, window in windows:

                    # Creates windows for each input raster
                    cont_eco = cont_eco_src.read(1, window=window)
                    age_cat = age_cat_src.read(1, window=window)

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

                    dst_data = cont_eco_age

                    # Writes the output window to the output
                    dst.write_band(1, dst_data, window=window)

    pattern = 'annual_gain_rate_natural_forest'

    utilities.upload_final(pattern, upload_dir, tile_id)

    end = datetime.datetime.now()
    elapsed_time = end-start

    print "  Processing time for tile", tile_id, ":", elapsed_time




