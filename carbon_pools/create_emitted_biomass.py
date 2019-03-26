import datetime
import sys
import numpy as np
import rasterio
import os
import subprocess
import glob
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def create_emitted_biomass(tile_id):

    start = datetime.datetime.now()

    # Names of the input tiles
    natrl_forest_biomass_2000 = '{0}_{1}.tif'.format(tile_id, cn.pattern_WHRC_biomass_2000_unmasked)
    mangrove_biomass_2000 = '{0}_{1}.tif'.format(tile_id, cn.pattern_mangrove_biomass_2000)
    mangrove_gain = '{0}_{1}.tif'.format(tile_id, cn.pattern_cumul_gain_AGC_mangrove)
    planted_forest_gain = '{0}_{1}.tif'.format(tile_id, cn.pattern_cumul_gain_AGC_planted_forest_non_mangrove)
    natrl_forest_gain = '{0}_{1}.tif'.format(tile_id, cn.pattern_cumul_gain_AGC_natrl_forest)
    mangrove_emis_year = '{0}_{1}.tif'.format(tile_id, cn.pattern_cont_eco_processed)
    planted_forest_emis_year = '{0}_{1}.tif'.format(tile_id, cn.pattern_cont_eco_processed)
    natrl_forest_emis_year = '{0}_{1}.tif'.format(tile_id, cn.pattern_natrl_forest_biomass_2000)


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
                            with rasterio.open('{0}{1}.tif'.format(tile_id, cn.pattern_age_cat_natrl_forest), 'w', **kwargs) as dst:

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











    print 'Uploading tiles to s3'
    for type in cn.pool_types:

        uu.upload_final('{0}/{1}/'.format(cn.agc_dir, type), tile_id, '{0}_{1}'.format(cn.pattern_agc, type))
        uu.upload_final('{0}/{1}/'.format(cn.bgc_dir, type), tile_id, '{0}_{1}'.format(cn.pattern_bgc, type))
        uu.upload_final('{0}/{1}/'.format(cn.deadwood_dir, type), tile_id, '{0}_{1}'.format(cn.pattern_deadwood, type))
        uu.upload_final('{0}/{1}/'.format(cn.litter_dir, type), tile_id, '{0}_{1}'.format(cn.pattern_litter, type))
        uu.upload_final('{0}/{1}/'.format(cn.soil_C_pool_dir, type), tile_id, '{0}_{1}'.format(cn.pattern_soil_pool, type))
        uu.upload_final('{0}/{1}/'.format(cn.total_C_dir, type), tile_id, '{0}_{1}'.format(cn.pattern_total_C, type))

    print "Elapsed time: {}".format(datetime.datetime.now() - start)
