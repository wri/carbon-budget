import subprocess
import datetime
import os
import sys
import glob

import utilities

def calc_gain(tile_id):

    start = datetime.datetime.now()

    print "\n-------TILE ID: {}".format(tile_id)
    
    if not os.path.exists("oudata/"):
        try:
            os.mkdir("outdata/")
        except:
            pass
            
    tcd = '{}_treecover2000.tif'.format(tile_id)
    loss = '{}_loss.tif'.format(tile_id)
    gain = '{}_gain.tif'.format(tile_id)
    
    old_gr = "{}_res_old.tif".format(tile_id)
    young_gr = "{}_res_youn.tif".format(tile_id)
    plantations = '{}_res_gfw_plantations.tif'.format(tile_id)
    
    # download files
    for filetype in ['treecover2000', 'lossyear', 'gain']:
        utilities.wget2015data(tile_id, filetype)
    
    # download plantations
    utilities.download_plant(tile_id)

    # download growth
    utilities.download_growth(tile_id)

    # run c++
    gain_tiles_cmd = ['./calc_gain.exe', tile_id]
    subprocess.check_call(gain_tiles_cmd)
    source = 'outdata/{}_gain.tif'.format(tile_id)
    cmd = ['aws', 's3', 'cp', source, 's3://gfw-files/sam/carbon_budget/gain/']
    subprocess.check_call(cmd)   

    # remove files:
    tiles = glob.glob('{}*tif'.format(tile_id))
    for tile in tiles:
        os.remove(tile)

#calc_gain('00N_140E')
