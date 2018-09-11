import subprocess
import datetime
import os
import sys
import glob

import gain.utilities

def calc_gain(tile_id):

    start = datetime.datetime.now()

    print "\n-------TILE ID: {}".format(tile_id)
    
    if not os.path.exists("outdata/"):
        try:
            os.mkdir("outdata/")
        except:
            pass

    loss = 's3://gfw2-data/forest_change/hansen_2016/{}.tif'
    tcd = 's3://gfw2-data/forest_cover/2000_treecover/Hansen_GFC2014_treecover2000_{}.tif'
    gain = 's3://gfw2-data/forest_change/tree_cover_gain/gaindata_2012/Hansen_GFC2015_gain_{}.tif'
    plantations = 's3://gfw-files/sam/carbon_budget/data_inputs2/gfw_plantations/{}_res_gfw_plantations.tif'
    
    for tree_raster in [{loss:'{}_loss.tif'}, {tcd: '{}_tcd.tif'}, {gain: '{}_gain.tif'}, {plantations: '{}_plantations.tif'}]:
        for source, dest in tree_raster.iteritems():
            source = source.format(tile_id)
            print source
      
            dest = dest.format(tile_id)
            gain.utilities.s3_download(source, dest)

    # download growth
    for growthtype in ['old', 'young']:
        source = 's3://gfw-files/sam/carbon_budget/growth_rasters/{0}_{1}.tif'.format(tile_id, growthtype)
        gain.utilities.s3_download(source, '.')
    
    # run c++
    gain_tiles_cmd = ['./calc_gain.exe', tile_id]
    subprocess.check_call(gain_tiles_cmd)
    
    
    # source = 'outdata/{}_gain.tif'.format(tile_id)
    # cmd = ['aws', 's3', 'cp', source, 's3://gfw-files/sam/carbon_budget/gain/']
    # subprocess.check_call(cmd)   

    # # remove files:
    # tiles = glob.glob('{}*tif'.format(tile_id))
    # for tile in tiles:
        # os.remove(tile)

calc_gain('00N_110E')
