import subprocess
import datetime
import os
import sys
import pandas as pd
import glob

import utilities
import process_burned_area
import tile_peat_dict

def calc_emissions(tile_id):

    start = datetime.datetime.now()

    print "\n-------TILE ID: {}".format(tile_id)

    if not os.path.exists("oudata/"):
        try:
            os.mkdir("outdata/")
        except:
            pass
    else:
        files = glob.glob("outdata/*")
        print files

        for f in files:
            os.remove(f)

   
    print 'writing emissions tiles'
    
    emissions_tiles_cmd = ['cpp_util/calc_emissions_v3.exe', tile_id]
    subprocess.check_call(emissions_tiles_cmd)

   #upload tiles
    utilities.upload_final(tile_id)    
    
    #delete tiels
    utilities.del_tiles(tile_id)

    print "elapsed time: {}".format(datetime.datetime.now() - start)
    
#calc_emissions('00N_110E')
