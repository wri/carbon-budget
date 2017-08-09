import subprocess
import datetime
import os
import sys
import pandas as pd

import utilities
import process_burned_area
import tile_peat_dict

def calc_emissions(tile_id):

    start = datetime.datetime.now()

    print "/n-------TILE ID: {}".format(tile_id)

    files = {'carbon_pool':['bgc', 'carbon', 'deadwood', 'soil', 'litter'], 'data_prep': ['fao_ecozones_bor_tem_tro', 'ifl_2000', 'peatland_drainage_proj', 'gfw_plantations', 'hwsd_histosoles', 'forest_model', 'climate_zone', 'cifor_peat_mask'], 'burned_area':['burn_loss_year']}
    
    # download files
    peat_file = tile_peat_dict.tile_peat_dict(tile_id) # based on tile id, know which peat file to download (hwsd, hist, jukka)
    
    files = {'carbon_pool':['bgc', 'carbon', 'deadwood', 'soil', 'litter'], 'data_prep': [peat_file, 'fao_ecozones_bor_tem_tro', 'ifl_2000', 'gfw_plantations', 'forest_model', 'climate_zone'], 'burned_area':['burn_loss_year']}
    utilities.download(files, tile_id)
    
    # download hansen tile
    utilities.wgetloss(tile_id)

    print 'writing emissions tiles'
    emissions_tiles_cmd = ['./calc_emissions_v2.exe', tile_id]
    subprocess.check_call(emissions_tiles_cmd)

    print 'uploading emissions tile to s3'
    #upload_emissions = ['aws', 's3', 'cp', emission_tile, 's3://gfw-files/sam/carbon_budget/emissions/']
    #subprocess.check_call(upload_emissions)

    print "deleting intermediate data"
    tiles_to_remove = ['']

    for tile in tiles_to_remove:
        try:
            print "test"
            #os.remove(tile)
        except:
            pass

    print "elapsed time: {}".format(datetime.datetime.now() - start)


calc_emissions('00N_140E')
