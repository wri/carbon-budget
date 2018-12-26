import datetime
import sys
import os
import subprocess
import glob
sys.path.append('../')
import constants_and_names
import universal_util

def create_carbon_pools(tile_id):

    start = datetime.datetime.now()

    print 'Writing aboveground carbon, belowground carbon, deadwood, litter, soil, and total carbon tiles'
    calc_all_cmd = ['./calc_carbon_pools.exe', tile_id]
    subprocess.check_call(calc_all_cmd)

    print 'Uploading tiles to s3'
    for type in constants_and_names.pool_types:

        universal_util.upload_final('{0}/{1}/'.format(constants_and_names.agc_dir, type), tile_id, '{0}_{1}'.format(constants_and_names.pattern_agc, type))
        universal_util.upload_final('{0}/{1}/'.format(constants_and_names.bgc_dir, type), tile_id, '{0}_{1}'.format(constants_and_names.pattern_bgc, type))
        universal_util.upload_final('{0}/{1}/'.format(constants_and_names.deadwood_dir, type), tile_id, '{0}_{1}'.format(constants_and_names.pattern_deadwood, type))
        universal_util.upload_final('{0}/{1}/'.format(constants_and_names.litter_dir, type), tile_id, '{0}_{1}'.format(constants_and_names.pattern_litter, type))
        universal_util.upload_final('{0}/{1}/'.format(constants_and_names.soil_C_pool_dir, type), tile_id, '{0}_{1}'.format(constants_and_names.pattern_soil_pool, type))
        universal_util.upload_final('{0}/{1}/'.format(constants_and_names.total_C_dir, type), tile_id, '{0}_{1}'.format(constants_and_names.pattern_total_C, type))

    print "Deleting intermediate tiles"
    tiles_to_remove = glob.glob('*{}*'.format(tile_id))
    for tile in tiles_to_remove:
        try:
            os.remove(tile)
        except:
            pass

    print "elapsed time: {}".format(datetime.datetime.now() - start)
