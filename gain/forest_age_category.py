import utilities
import datetime
import numpy as np
import rasterio

# np.set_printoptions(threshold=np.nan)

def my_fun2(x, general_const):
    return x[2:], general_const

def forest_age_category(tile_id, gain_table_dict):

    upload_dir = 's3://gfw2-data/climate/carbon_model/forest_age_category/20180920/'
    # upload_dir = r'C:\GIS\Carbon_model\test_forest_age_category'

    print "Processing:", tile_id

    # Gets the bounding coordinates of each tile. Needed to determine if the tile is in the tropics (within 30 deg of the equator)
    ymax, xmin, ymin, xmax = utilities.coords(tile_id)
    print "  ymax:", ymax

    # Default value is that the tile is not in the tropics
    tropics = 0

    # Criteria for assigning a tile to the tropics
    if (ymax > -30) & (ymax <= 30) :

        tropics = 1

    print "  Tile in tropics:", tropics

    # start time
    start = datetime.datetime.now()

    # Names of the loss, gain and tree cover density tiles
    loss = '{}.tif'.format(tile_id)
    gain = 'Hansen_GFC2015_gain_{}.tif'.format(tile_id)
    tcd = 'Hansen_GFC2014_treecover2000_{}.tif'.format(tile_id)
    ifl = '{}_res_ifl_2000.tif'.format(tile_id)
    biomass = '{}_biomass.tif'.format(tile_id)
    cont_eco = 'fao_ecozones_continents_{0}.tif'.format(tile_id)

    print "  Reading input files and evaluating conditions"

    # Opens biomass tile
    with rasterio.open(loss) as loss_src:

        # Grabs metadata about the tif, like its location/projection/cellsize
        kwargs = loss_src.meta

        # Grabs the windows of the tile (stripes) so we can iterate over the entire tif without running out of memory
        windows = loss_src.block_windows(1)

        # Opens gain tile
        with rasterio.open(gain) as gain_src:

            # Opens ifl tile
            with rasterio.open(ifl) as ifl_src:

                # Opens continent-ecozone combinations tile
                with rasterio.open(cont_eco) as cont_eco_src:

                    # Opens biomass 2000 tile
                    with rasterio.open(biomass) as biomass_src:

                        # Opens tree cover density tile
                        with rasterio.open(tcd) as extent_src:

                            # Updates kwargs for the output dataset
                            kwargs.update(
                                driver='GTiff',
                                count=1,
                                compress='lzw',
                                nodata=0
                            )

                            # Opens the output tile, giving it the arguments of the input tiles
                            with rasterio.open('forest_age_category_{}.tif'.format(tile_id), 'w', **kwargs) as dst:

                                # Iterates across the windows (1 pixel strips) of the input tile
                                for idx, window in windows:

                                    # Creates windows for each input raster
                                    loss = loss_src.read(1, window=window)
                                    gain = gain_src.read(1, window=window)
                                    tcd = extent_src.read(1, window=window)
                                    ifl = ifl_src.read(1, window=window)
                                    cont_eco = cont_eco_src.read(1, window=window)
                                    biomass = biomass_src.read(1, window=window)

                                    # Creates a numpy array that has the <=20 year secondary forest growth rate x 20
                                    # based on the continent-ecozone code of each pixel (the dictionary).
                                    # This is used to assign pixels to the correct age category.
                                    gain_20_years = np.vectorize(gain_table_dict.get)(cont_eco)*20

                                    # Create a 0s array for the output
                                    dst_data = np.zeros((window.height, window.width), dtype='uint8')

                                    # Logic tree for assigning age categories begins here
                                    # No change pixels- no loss or gain
                                    if tropics == 0:

                                        dst_data[np.where((tcd > 0) & (gain == 0) & (loss == 0))] = 1

                                    if tropics == 1:

                                        dst_data[np.where((tcd > 0) & (gain == 0) & (loss == 0) & (ifl != 1))] = 2
                                        dst_data[np.where((tcd > 0) & (gain == 0) & (loss == 0) & (ifl == 1))] = 3

                                    # Loss-only pixels
                                    dst_data[np.where((gain == 0) & (loss > 0) & (ifl != 1) & (biomass <= gain_20_years))] = 4
                                    dst_data[np.where((gain == 0) & (loss > 0) & (ifl != 1) & (biomass > gain_20_years))] = 5
                                    dst_data[np.where((gain == 0) & (loss > 0) & (ifl ==1))] = 6

                                    # Gain-only pixels
                                    dst_data[np.where((gain == 1) & (loss == 0))] = 7

                                    # Pixels with loss and gain
                                    dst_data[np.where((gain == 1) & (loss >= 13))] = 8
                                    dst_data[np.where((gain == 1) & (loss > 0) & (loss <= 6))] = 9
                                    dst_data[np.where((gain == 1) & (loss > 6) & (loss < 13))] = 10

                                    # Writes the output window to the output
                                    dst.write_band(1, dst_data, window=window)

    pattern = 'forest_age_category'

    utilities.upload_final(pattern, upload_dir, tile_id)

    end = datetime.datetime.now()
    elapsed_time = end-start

    print "Processing time for tile", tile_id, ":", elapsed_time




