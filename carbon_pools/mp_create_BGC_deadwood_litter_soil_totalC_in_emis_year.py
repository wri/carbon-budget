'''
This script creates carbon in belowground, deadwood, litter, and soil pools at the time of tree cover loss for loss pixels.
It also calculates total carbon for loss pixels.
For belowground carbon (as with aboveground carbon), the pools are carbon 2000 + carbon gain until loss year.
For deadwood, litter, and soil, the pools are based on carbon 2000.
Total carbon is thus a mixture of stocks in 2000 and in the year of tree cover loss.

NOTE: Because there are so many input files, this script needs a machine with extra disk space.
Thus, create a spot machine with extra disk space: spotutil new r4.16xlarge dgibbs_wri --disk_size 1024    (this is the maximum value).
'''

import create_BGC_deadwood_litter_soil_totalC
from multiprocessing.pool import Pool
from functools import partial
import subprocess
import os
import pandas as pd
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Tells the pool creation functions to calculate carbon pools as they were at the year of loss in loss pixels only
extent = "loss"

pd.options.mode.chained_assignment = None

# tile_list = uu.tile_list(cn.AGC_emis_year_dir)
tile_list = ['00N_110E', '30N_080W'] # test tiles
# tile_list = ['80N_020E', '00N_020E', '30N_080W', '00N_110E'] # test tiles
print tile_list
print "There are {} tiles to process".format(str(len(tile_list)))

# For downloading all tiles in the input folders.
input_files = [
    cn.AGC_emis_year_dir,
    cn.WHRC_biomass_2000_unmasked_dir,
    cn.mangrove_biomass_2000_dir,
    cn.cont_eco_dir,
    cn.bor_tem_trop_processed_dir,
    cn.precip_processed_dir,
    cn.soil_C_full_extent_2000_dir,
    cn.elevation_processed_dir
    ]

# for input in input_files:
#     uu.s3_folder_download('{}'.format(input), '.')

# For copying individual tiles to spot machine for testing.
for tile in tile_list:

    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.AGC_emis_year_dir, tile,
                                                            cn.pattern_AGC_emis_year), '.')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.cont_eco_dir, tile,
                                                            cn.pattern_cont_eco_processed), '.')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.bor_tem_trop_processed_dir, tile,
                                                            cn.pattern_bor_tem_trop_processed), '.')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.precip_processed_dir, tile,
                                                            cn.pattern_precip), '.')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.soil_C_full_extent_2000_dir, tile,
                                                            cn.pattern_soil_C_full_extent_2000), '.')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.elevation_processed_dir, tile,
                                                            cn.pattern_elevation), '.')
    try:
        uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.WHRC_biomass_2000_unmasked_dir, tile,
                                                            cn.pattern_WHRC_biomass_2000_unmasked), '.')
    except:
        print "No WHRC biomass in", tile
    try:
        uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.mangrove_biomass_2000_dir, tile, cn.pattern_mangrove_biomass_2000), '.')
    except:
        print "No mangrove biomass in", tile


# Table with IPCC Wetland Supplement Table 4.4 default mangrove gain rates
cmd = ['aws', 's3', 'cp', os.path.join(cn.gain_spreadsheet_dir, cn.gain_spreadsheet), '.']
subprocess.check_call(cmd)

# Imports the table with the ecozone-continent codes and the carbon gain rates
gain_table = pd.read_excel("{}".format(cn.gain_spreadsheet),
                           sheet_name = "mangrove gain, for model")

# Removes rows with duplicate codes (N. and S. America for the same ecozone)
gain_table_simplified = gain_table.drop_duplicates(subset='gainEcoCon', keep='first')

mang_BGB_AGB_ratio = create_BGC_deadwood_litter_soil_totalC.mangrove_pool_ratio_dict(gain_table_simplified,
                                                                           cn.below_to_above_trop_dry_mang,
                                                                           cn.below_to_above_trop_wet_mang,
                                                                           cn.below_to_above_subtrop_mang)

mang_deadwood_AGB_ratio = create_BGC_deadwood_litter_soil_totalC.mangrove_pool_ratio_dict(gain_table_simplified,
                                                                           cn.deadwood_to_above_trop_dry_mang,
                                                                           cn.deadwood_to_above_trop_wet_mang,
                                                                           cn.deadwood_to_above_subtrop_mang)

mang_litter_AGB_ratio = create_BGC_deadwood_litter_soil_totalC.mangrove_pool_ratio_dict(gain_table_simplified,
                                                                           cn.litter_to_above_trop_dry_mang,
                                                                           cn.litter_to_above_trop_wet_mang,
                                                                           cn.litter_to_above_subtrop_mang)

print "Creating carbon pools..."

# 18 processors used between 300 and 400 GB memory, so it was okay on a r4.16xlarge spot machine
num_of_processes = 18
num_of_processes = 2
pool = Pool(num_of_processes)
pool.map(partial(create_BGC_deadwood_litter_soil_totalC.create_BGC, mang_BGB_AGB_ratio=mang_BGB_AGB_ratio), tile_list)
pool.close()
pool.join()

# uu.upload_final_set(cn.BGC_emis_year_dir, cn.pattern_BGC_emis_year)
# # cmd = ['rm *{}*.tif'.format(cn.pattern_BGC_emis_year)]
# # subprocess.check_call(cmd)

num_of_processes = 16
num_of_processes = 2
pool = Pool(num_of_processes)
pool.map(partial(create_BGC_deadwood_litter_soil_totalC.create_deadwood, mang_deadwood_AGB_ratio=mang_deadwood_AGB_ratio), tile_list)
pool.close()
pool.join()

# uu.upload_final_set(cn.deadwood_emis_year_2000_dir, cn.pattern_deadwood_emis_year_2000)
# # cmd = ['rm *{}*.tif'.format(cn.pattern_deadwood_emis_year_2000)]
# # subprocess.check_call(cmd)

num_of_processes = 16
num_of_processes = 2
pool = Pool(num_of_processes)
pool.map(partial(create_BGC_deadwood_litter_soil_totalC.create_litter, mang_litter_AGB_ratio=mang_litter_AGB_ratio), tile_list)
pool.close()
pool.join()

# uu.upload_final_set(cn.litter_emis_year_2000_dir, cn.pattern_litter_emis_year_2000)
# # cmd = ['rm *{}*.tif'.format(cn.pattern_litter_emis_year_2000)]
# # subprocess.check_call(cmd)

num_of_processes = 16
num_of_processes = 2
pool = Pool(num_of_processes)
pool.map(partial(create_BGC_deadwood_litter_soil_totalC.create_soil), tile_list)
pool.close()
pool.join()

# uu.upload_final_set(cn.soil_C_emis_year_2000_dir, cn.pattern_soil_C_emis_year_2000)
# # cmd = ['rm *{}*.tif'.format(cn.pattern_soil_C_emis_year_2000)]
# # subprocess.check_call(cmd)

# I tried several different processor numbers for this. Ended up using 14 processors, which used about 380 GB memory
# at peak. Probably could've handled 16 processors on an r4.16xlarge machine but I didn't feel like taking the time to check.
num_of_processes = 14
num_of_processes = 2
pool = Pool(num_of_processes)
pool.map(partial(create_BGC_deadwood_litter_soil_totalC.create_total_C), tile_list)
pool.close()
pool.join()

# uu.upload_final_set(cn.total_C_emis_year_dir, cn.pattern_total_C_emis_year)
# # cmd = ['rm *{}*.tif'.format(cn.pattern_total_C_emis_year)]
# # subprocess.check_call(cmd)

# # For single processor use
# for tile in tile_list:
#     create_BGC_deadwood_litter_soil_totalC.create_BGC(tile, mang_BGB_AGB_ratio)
#     create_BGC_deadwood_litter_soil_totalC.create_deadwood(tile, mang_deadwood_AGB_ratio)
#     create_BGC_deadwood_litter_soil_totalC.create_litter(tile, mang_litter_AGB_ratio)
#     create_BGC_deadwood_litter_soil_totalC.create_soil(tile)
#     create_BGC_deadwood_litter_soil_totalC.create_total_C(tile)
#
# uu.upload_final_set(cn.BGC_emis_year_dir, cn.pattern_BGC_emis_year)
# uu.upload_final_set(cn.deadwood_emis_year_2000_dir, cn.pattern_deadwood_emis_year_2000)
# uu.upload_final_set(cn.litter_emis_year_2000_dir, cn.pattern_litter_emis_year_2000)
# uu.upload_final_set(cn.soil_C_emis_year_2000_dir, cn.pattern_soil_C_emis_year_2000)
# uu.upload_final_set(cn.total_C_emis_year_dir, cn.pattern_total_C_emis_year)
