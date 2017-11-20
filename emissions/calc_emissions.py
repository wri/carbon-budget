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

    # # download files
    # peat_file = tile_peat_dict.tile_peat_dict(tile_id) # based on tile id, know which peat file to download (hwsd, hist, jukka)

    # files = {'carbon_pool':['bgc', 'carbon', 'deadwood', 'soil', 'litter'], 'data_prep': [peat_file, 'fao_ecozones_bor_tem_tro', 'ifl_2000', 'gfw_plantations', 'Goode_FinalClassification_15_50uncertain_expanded_wgs84', 'climate_zone'], 'burned_area':['burn_loss_year']}
    # utilities.download(files, tile_id)

    # # download hansen tile
    # hansen_tile = utilities.wgetloss(tile_id)
    
    print 'writing emissions tiles'
    emissions_tiles_cmd = ['./calc_emissions_v3.exe', tile_id]
    subprocess.check_call(emissions_tiles_cmd)

   # merge tiles
    #utilities.merge_tiles(tile_id)

   # upload tiles
    #utilities.upload_final(tile_id)    
    
    # delete tiels
    #utilities.del_tiles(tile_id)


    print "elapsed time: {}".format(datetime.datetime.now() - start)

calc_emissions('00N_110E')
