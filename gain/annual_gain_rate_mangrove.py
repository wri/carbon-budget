### Creates tiles of annual aboveground and belowground biomass gain rates for mangroves using IPCC Wetlands Supplement Table 4.4 rates.
### Its inputs are the continent-ecozone tiles, mangrove biomass tiles (for locations of mangroves), and the IPCC
### gain rate table.

import datetime
import numpy as np
import rasterio
import sys
sys.path.append('../')
import constants_and_names as cn

# Necessary to suppress a pandas error later on
np.set_printoptions(threshold=np.nan)

def annual_gain_rate(tile_id, gain_above_dict, gain_below_dict):

    print gain_above_dict
    print gain_below_dict

    print "Processing:", tile_id

    # Start time
    start = datetime.datetime.now()

    # Name of the mangrove biomass tile
    mangrove_biomass = '{0}_{1}.tif'.format(tile_id, cn.pattern_mangrove_biomass_2000)

    # Name of the continent-ecozone tile
    cont_eco = '{0}_{1}.tif'.format(tile_id, cn.pattern_cont_eco_processed)

    # Names of the output aboveground and belowground mangrove gain rate tiles
    AGB_gain_rate = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_AGB_mangrove)
    BGB_gain_rate = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_BGB_mangrove)

    print "  Reading input files and creating aboveground and belowground biomass gain rates for {}".format(tile_id)

    # Opens continent-ecozone tile
    with rasterio.open(cont_eco) as cont_eco_src:

        # Grabs metadata about the tif, like its location/projection/cellsize
        kwargs = cont_eco_src.meta

        # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
        windows = cont_eco_src.block_windows(1)

        # Opens mangrove biomass tile
        with rasterio.open(mangrove_biomass) as mangrove_AGB_src:

            # Updates kwargs for the output dataset.
            # Need to update data type to float 32 so that it can handle fractional gain rates
            kwargs.update(
                driver='GTiff',
                count=1,
                compress='lzw',
                nodata=0,
                dtype='float32'
            )

            # Opens the aboveground output tile, giving it the arguments of the input tiles
            with rasterio.open(AGB_gain_rate, 'w', **kwargs) as dst_above:

                # Opens the belowground output tile, giving it the arguments of the input tiles
                with rasterio.open(BGB_gain_rate, 'w', **kwargs) as dst_below:

                    # Iterates across the windows (1 pixel strips) of the input tile
                    for idx, window in windows:

                        # Creates windows for each input raster
                        cont_eco = cont_eco_src.read(1, window=window)
                        mangrove_AGB = mangrove_AGB_src.read(1, window=window)

                        # Converts the continent-ecozone array to float so that the values can be replaced with fractional gain rates
                        cont_eco = cont_eco.astype('float32')

                        # Reclassifies mangrove biomass to 1 or 0 to make a mask of mangrove pixels
                        mangrove_AGB[mangrove_AGB > 0] = 1



                        print cont_eco[-1][-1]


                        aboveground = cont_eco

                        print cont_eco[-1][-1]

                        # Applies the dictionary of continent-ecozone gain rates to the continent-ecozone array to
                        # get annual gain rates (metric tons aboveground biomass/yr) for each pixel
                        for key, value in gain_above_dict.iteritems():
                            aboveground[aboveground == key] = value

                        print cont_eco[-1][-1]

                        # Masks out pixels without mangroves, leaving gain rates in only pixels with mangroves
                        dst_above_data = aboveground * mangrove_AGB

                        # Writes the output window to the output
                        dst_above.write_band(1, dst_above_data, window=window)



                        belowground = cont_eco

                        print belowground[-1][-1]
                        print cont_eco[-1][-1]

                        for key, value in gain_below_dict.iteritems():
                            belowground[belowground == key] = value



                        dst_below_data = belowground * mangrove_AGB

                        dst_below.write_band(1, dst_below_data, window=window)

                        sys.quit()

    # # Calculates belowground biomass rate from aboveground biomass rate
    # print "  Creating belowground biomass gain rate for {}".format(tile_id)
    # above_to_below_calc = '--calc=(A>0)*A*{}'.format(cn.below_to_above_mangrove)
    # below_outfilename = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_BGB_mangrove)
    # below_outfilearg = '--outfile={}'.format(below_outfilename)
    # cmd = ['gdal_calc.py', '-A', AGB_gain_rate, above_to_below_calc, below_outfilearg,
    #        '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW']
    # subprocess.check_call(cmd)

    end = datetime.datetime.now()
    elapsed_time = end-start

    print "  Processing time for tile", tile_id, ":", elapsed_time




