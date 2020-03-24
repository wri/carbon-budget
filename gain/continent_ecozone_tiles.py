### Creates tiles in which each pixel is a combination of the continent and FAO FRA 2000 ecozone.
### The tiles are based on a shapefile which combines the FAO FRA 2000 ecozone shapefile and a continent shapefile.
### The FAO FRA 2000 shapefile is from http://www.fao.org/geonetwork/srv/en/resources.get?id=1255&fname=eco_zone.zip&access=private
### The continent shapefile is from https://www.baruch.cuny.edu/confluence/display/geoportal/ESRI+International+Data
### Various processing steps in ArcMap were used to make sure that the entirety of the ecozone shapefile had
### continents assigned to it. The creation of the continent-ecozone shapefile was done in ArcMap.
### In the resulting ecozone-continent shapefile, the final field has continent and ecozone concatenated.
### That ecozone-continent field can be parsed to get the ecozone and continent for every pixel,
### which are necessary for assigning gain rates to pixels.
### This script also breaks the input tiles into windows that are 1024 pixels on each side and assigns all pixels that
### don't have a continent-ecozone code to the most common code in that window.
### This is done to expand the extent of the continent-ecozone tiles to include pixels that don't have a continent-ecozone
### code because they are just outside the original shapefile.
### It is necessary to expand the continent-ecozone codes into those nearby areas because otherwise some forest age category
### pixels are outside the continent-ecozone pixels and can't have gain rates assigned to them.
### This maneuver provides the necessary continent-ecozone information to assign gain rates.

import rasterio
import numpy as np
import datetime
from scipy import stats
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def create_continent_ecozone_tiles(tile_id):

    print("Processing:", tile_id)

    # Start time
    start = datetime.datetime.now()

    xmin, ymin, xmax, ymax = uu.coords(tile_id)
    print("Extent of", tile_id, "-- ymax:", ymax, "; ymin:", ymin, "; xmax", xmax, "; xmin:", xmin)

    print("Rasterizing ecozone to extent of biomass tile {}".format(tile_id))

    cont_eco_raw = "{0}_{1}".format(tile_id, cn.pattern_cont_eco_raw)

    # This makes rasters that are made of 1024 x 1024 pixel windows instead of 40000 x 1 pixel windows
    # to improve assigning pixels without continent-ecozone codes to a continent-ecozone code.
    # This way, pixels without continent-ecozone are assigned a code based on what's in a window nearby, rather
    # than a window that spans the entire 10x10 degree tile.
    uu.rasterize('fao_ecozones_fra_2000_continents_assigned_dissolved_FINAL_20180906.shp',
                                              cont_eco_raw, xmin, ymin, xmax, ymax, '.00025', 'Int16', 'gainEcoCon', '0')

    # Opens continent-ecozone tile.
    # Everything from here down is used to assign pixels without continent ecozone codes to a continent-ecozone in the 1024x1024 windows.
    with rasterio.open('{}.tif'.format(cont_eco_raw)) as cont_eco_raw_src:

        # Grabs metadata about the tif, like its location/projection/cellsize
        kwargs = cont_eco_raw_src.meta

        # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
        windows = cont_eco_raw_src.block_windows(1)

        # Updates kwargs for the output dataset.
        # Need to update data type to float 32 so that it can handle fractional gain rates
        kwargs.update(
            driver='GTiff',
            count=1,
            compress='lzw',
            nodata=0
        )

        # Opens the output tile, giving it the arguments of the input tiles
        with rasterio.open('{0}_{1}.tif'.format(tile_id, cn.pattern_cont_eco_processed), 'w', **kwargs) as dst:

            # Iterates across the windows (1024 x 1024 pixel boxes) of the input tile.
            for idx, window in windows:

                # Creates windows for each input raster
                cont_eco_raw = cont_eco_raw_src.read(1, window=window)

                # Turns the 2D array into a 1D array that is n x n long.
                # This makes to easier to remove 0s and find the mode of the remaining continent-ecozone codes
                cont_eco_raw_flat = cont_eco_raw.flatten()

                # Removes all zeros from the array, leaving just pixels with continent-ecozone codes
                non_zeros = np.delete(cont_eco_raw_flat, np.where(cont_eco_raw_flat == 0))

                # If there were only pixels without continent-ecozone codes in the array, the mode is assigned 0
                if non_zeros.size < 1:

                    # print "  Window is all 0s"
                    mode = 0

                # If there were pixels with continent-ecozone codes, the mode is the most common code among those in the window
                else:

                    mode = stats.mode(non_zeros)[0]
                    # print "  Window is not all 0s. Mode is", mode

                cont_eco_processed = cont_eco_raw

                # Assigns all pixels without a continent-ecozone code in that window to that most common code
                cont_eco_processed[cont_eco_processed == 0] = mode

                # Writes the output window to the output.
                # Although the windows for the input tiles are 1024 x 1024 pixels,
                # the windows for these output files are 40000 x 1 pixels, like all the other tiles in this model,
                # so they should work fine with all the other tiles.
                dst.write_band(1, cont_eco_processed, window=window)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_annual_gain_AGB_mangrove)





