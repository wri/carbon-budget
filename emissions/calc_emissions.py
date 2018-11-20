import subprocess
import datetime
import os
import glob

import utilities

# once all the carbon pools are created, run this to calculate emissions


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

    # Removes no data pixels from raster-- I HAVEN'T TESTED THIS YET
    utilities.remove_nodata(tile_id)

    # Upload tiles to s3
    print 'Uploading tiles to s3'
    output_dir = 's3://gfw2-data/climate/carbon_model/output_emissions/20181117'
    utilities.upload_final(output_dir, tile_id)
    
    # Delete input tiles from spot machine. I don't think this is actually deleting the tiles at this point.
    print 'Deleting tiles from spot machine'
    utilities.del_tiles(tile_id)

    print "elapsed time: {}".format(datetime.datetime.now() - start)
