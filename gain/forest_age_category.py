import utilities
import subprocess
import datetime
import numpy as np
import rasterio

def forest_age_category(tile_id):

    upload_dir = 's3://gfw2-data/climate/carbon_model/forest_age_category/20180912/'

    print "Processing:", tile_id

    ymax, xmin, ymin, xmax = utilities.coords(tile_id)

    tropics = 0

    if ymax > -30 & ymax <= 30:

        tropics = 1

    # start time
    start = datetime.datetime.now()

    # Names of the loss, gain and tree cover density tiles
    loss = '{}.tif'.format(tile_id)
    gain = 'Hansen_GFC2015_gain_{}.tif'.format(tile_id)
    tcd = 'Hansen_GFC2014_treecover2000_{}.tif'.format(tile_id)
    ifl = '{}_res_ifl_2000tif'.format(tile_id)
    biomass = '{}_biomass.tif'.format(tile_id)
    cont_eco = 'fao_ecozones_{0}.tif'.format(tile_id)

    print "  Reading input files and evaluating condtions"

    # open loss and grab metadata about the tif,
    # like its location / projection / cellsize
    with rasterio.open(loss) as loss_src:
        kwargs = loss_src.meta

        # also grab the windows (stripes) so we can iterate
        # over the entire tif without running out of memory
        windows = loss_src.block_windows(1)

        # open gain
        with rasterio.open(gain) as gain_src:
            # open extent
            with rasterio.open(tcd) as extent_src:
                # update those kwargs for the dataset we'll write out
                kwargs.update(
                    driver='GTiff',
                    count=1,
                    compress='lzw',
                    nodata=0
                )

                with rasterio.open('forest_age_category_{}.tif'.format(tile_id), 'w', **kwargs) as dst:
                    for idx, window in windows:
                        loss = loss_src.read(1, window=window)
                        gain = gain_src.read(1, window=window)
                        tcd = extent_src.read(1, window=window)

                        # create an empty array
                        dst_data = np.zeros((window.height, window.width), dtype='uint8')

                        # where loss & gain, set output to 100, otherwise keep dst_data value
                        dst_data[np.where((tcd > 0) & (loss >= 0) & (gain >= 0))] = 100

                        dst_data[np.where((tcd > 0) & (gain == 0) & (loss == 0) & (tropics == 0))] = 1
                        dst_data[np.where((tcd > 0) & (gain == 0) & (loss == 0) & (tropics == 1) & (ifl == 0))] = 2
                        dst_data[np.where((tcd > 0) & (gain == 0) & (loss == 0) & (tropics == 1) & (ifl == 1))] = 3


                        dst_data[np.where((tcd > 0) & (gain == 0) & (loss > 0) & (ifl == 1))] = 6


                        # # where loss & gain, set output to 100, otherwise keep dst_data value
                        # dst_data[np.where((loss_data > 0) & (gain_data > 0))] = 100
                        #
                        # # etc
                        # dst_data[np.where((loss_data > 10) & (gain_data > 0) & (extent_data > 50))] = 99
                        # dst_data[np.where((loss_data < 10) & (gain_data == 0) & (extent_data < 50))] = 1
                        # dst_data[np.where((loss_data > 10) & (gain_data > 0) & (extent_data >= 90))] = 2

                        dst.write_band(1, dst_data, window=window)


    pattern = 'forest_age_category'

    utilities.upload_final(pattern, upload_dir, tile_id)

    end = datetime.datetime.now()
    elapsed_time = end-start

    print "Processing time for tile", tile_id, ":", elapsed_time




