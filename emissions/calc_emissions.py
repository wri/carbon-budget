import subprocess
import datetime
import os
import sys
import pandas as pd
import glob

import utilities
import process_burned_area
import tile_peat_dict
import multiprocess_emissions

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

    # Upload tiles to s3
    print 'Uploading tiles to s3'
    output_dir = multiprocess_emissions.file_locn
    utilities.upload_final(output_dir, tile_id)
    
    # Delete tiles from spot machine-- not necessary because the files are being moved, not copied, from the spot machine
    print 'Deleting tiles from spot machine'
    utilities.del_tiles(tile_id)

    print "elapsed time: {}".format(datetime.datetime.now() - start)
