import utilities
import subprocess
import datetime

def forest_age_category(tile_id):

    print "Processing:", tile_id

    # start time
    start = datetime.datetime.now()

    # Names of the loss, gain and tree cover density tiles
    loss = '{}.tif'.format(tile_id)
    gain = 'Hansen_GFC2015_gain_{}.tif'.format(tile_id)
    tcd = 'Hansen_GFC2014_treecover2000_{}.tif'.format(tile_id)
    ifl = '{}_res_ifl_2000tif'.format(tile)
    biomass = '{}_biomass.tif'.format(biomass)








    utilities.upload_final(upload_dir, tile_id)

    end = datetime.datetime.now()
    elapsed_time = end-start

    print "Processing time for tile", tile_id, ":", elapsed_time




