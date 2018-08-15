import datetime
import os
import subprocess
import glob

import util
from create_input_files import create_input_files

'''
first create these files:
"_biomass.tif", "_res_fao_ecozones_bor_tem_tro.tif", "_res_srtm.tif", _res_precip.tif", "_soil.tif";

in order to create these files:
_carbon.tif", "_bgc.tif",  "_deadwood.tif", "_litter.tif", _totalc.tif";
'''


def create_carbon_pools(tile_id):
    start = datetime.datetime.now()

    # location where files will be saved on s3
    # carbon_budget_input_data_dir = 's3://gfw-files/sam/carbon_budget/data_inputs3/'       # previously
    carbon_budget_input_data_dir = 's3://gfw2-data/climate/carbon_model/inputs_for_carbon_pools/processed/'

    print "copy down biomass tile"
    biomass_tile = '{}_biomass.tif'.format(tile_id)
    # util.download('s3://WHRC-carbon/global_27m_tiles/final_global_27m_tiles/biomass_10x10deg/{}'.format(biomass_tile))    # previously
    util.download('s3://WHRC-carbon/WHRC_V4/Processed/{}'.format(biomass_tile))

    print "creating input files"
    create_input_files(tile_id, carbon_budget_input_data_dir, biomass_tile)
 
    print 'writing aboveground carbon, belowground carbon, deadwood, litter, total carbon'
    calc_all_cmd = ['./calc_all.exe', tile_id]
    subprocess.check_call(calc_all_cmd)

    print 'uploading tiles to s3'
    tile_types  = ['carbon', 'bgc', 'deadwood', 'litter', 'soil', 'total_carbon']
    for tile in tile_types:
       if tile == 'total_carbon':
           tile_name = "{}_totalc.tif".format(tile_id)
       else:
           tile_name = "{0}_{1}.tif".format(tile_id, tile)

       util.upload(tile_name, 's3://gfw2-data/climate/carbon_model/carbon_pools/20180815/{}/'.format(tile))

    print "deleting intermediate data"
    tiles_to_remove = glob.glob('*{}*'.format(tile_id))
    for tile in tiles_to_remove:
        try:
            os.remove(tile)
        except:
            pass

    print "elapsed time: {}".format(datetime.datetime.now() - start)

# create_carbon_pools('00N_000E')
