import datetime
import sys
import os
import subprocess
import glob
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def create_carbon_pools(tile_id):

    start = datetime.datetime.now()

    print 'Writing aboveground carbon, belowground carbon, deadwood, litter, soil, and total carbon tiles'
    calc_all_cmd = ['./calc_carbon_pools.exe', tile_id]
    subprocess.check_call(calc_all_cmd)

    print 'Uploading tiles to s3'
    for type in cn.pool_types:

        uu.upload_final('{0}/{1}/'.format(cn.agc_dir, type), tile_id, '{0}_{1}'.format(cn.pattern_agc, type))
        uu.upload_final('{0}/{1}/'.format(cn.bgc_dir, type), tile_id, '{0}_{1}'.format(cn.pattern_bgc, type))
        uu.upload_final('{0}/{1}/'.format(cn.deadwood_dir, type), tile_id, '{0}_{1}'.format(cn.pattern_deadwood, type))
        uu.upload_final('{0}/{1}/'.format(cn.litter_dir, type), tile_id, '{0}_{1}'.format(cn.pattern_litter, type))
        uu.upload_final('{0}/{1}/'.format(cn.soil_C_pool_dir, type), tile_id, '{0}_{1}'.format(cn.pattern_soil_pool, type))
        uu.upload_final('{0}/{1}/'.format(cn.total_C_dir, type), tile_id, '{0}_{1}'.format(cn.pattern_total_C, type))

    print "Deleting intermediate tiles"
    tiles_to_remove = glob.glob('*{}*'.format(tile_id))
    for tile in tiles_to_remove:
        try:
            os.remove(tile)
        except:
            pass

    print "elapsed time: {}".format(datetime.datetime.now() - start)
