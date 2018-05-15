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
    outdata_dir = 'outdata/'

    if not os.path.exists(outdata_dir):
        try:
            os.mkdir(outdata_dir)
        except:
            pass
    else:
        files = glob.glob("{}*".format(outdata_dir))
        print files

        for f in files:
            os.remove(f)

   
    print 'writing emissions tiles'
    
    emissions_tiles_cmd = ['cpp_util/calc_emissions_v3.exe', tile_id]
    subprocess.check_call(emissions_tiles_cmd)

    print 'tile writing completed'

    # #upload tiles
    # print 'uploading tiles to AWS'
    # utilities.upload_final(tile_id)
    
    #delete tiles
#    utilities.del_tiles(tile_id)

    print "elapsed time: {}".format(datetime.datetime.now() - start)
