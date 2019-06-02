'''
This script creates carbon in belowground, deadwood, and litter pools in the year 2000 for
all WHRC and mangrove biomass 2000 pixels (i.e. forest extent in 2000 using biomass 2000 values).
It does not calculate soil C carbon in 2000 because that is calculated in a separate script (mp_create_soil_C.py).
It also calculates total carbon (AGC, BGC, deadwood, litter, and soil) for the same pixels (2000 biomass extent).
For BGC, deadwood, and litter, the values are simply functions of the WHRC or mangrove biomass in 2000 at that pixel.
Mangrove biomass gets precedence over WHRC biomass where pixels co-occur.

This multiprocessing script uses the same functions for calculating pools as does mp_create_BGC_deadwood_litter_soil_totalC_in_emis_year.py
because they both just apply the same calculations to their respective AGC inputs (2000 extent/values or loss year extent/values).
Since the loss year pools and 2000 pools have different input AGC tiles and output tile names, the extent argument
tells each pool creation function what input AGC name to expect and how to name the output tiles.
The calculations are the same in either case.

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

# Tells the carbon pool creation functions to calculate carbon pools based on biomass 2000 values at full biomass 2000 extent
extent = "full"

pd.options.mode.chained_assignment = None

tile_list = uu.tile_list(cn.AGC_emis_year_dir)
# tile_list = ['30N_080W'] # test tiles
# tile_list = ['80N_020E', '00N_020E', '30N_080W', '00N_110E'] # test tiles
print tile_list
print "There are {} tiles to process".format(str(len(tile_list)))

# For downloading all tiles in the input folders.
input_files = [
    cn.AGC_2000_dir,
    cn.BGC_2000_dir,
    cn.deadwood_2000_dir,
    cn.litter_2000_dir,
    cn.soil_C_full_extent_2000_dir
    # cn.WHRC_biomass_2000_unmasked_dir,
    # cn.mangrove_biomass_2000_dir,
    # cn.cont_eco_dir,
    # cn.bor_tem_trop_processed_dir,
    # cn.precip_processed_dir,
    # cn.soil_C_full_extent_2000_dir,
    # cn.elevation_processed_dir
    ]

for input in input_files:
    uu.s3_folder_download('{}'.format(input), '.')

# # For copying individual tiles to spot machine for testing.
# for tile in tile_list:
#
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.AGC_2000_dir, tile,
#                                                             cn.pattern_AGC_2000), '.')
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.cont_eco_dir, tile,
#                                                             cn.pattern_cont_eco_processed), '.')
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.bor_tem_trop_processed_dir, tile,
#                                                             cn.pattern_bor_tem_trop_processed), '.')
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.precip_processed_dir, tile,
#                                                             cn.pattern_precip), '.')
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.soil_C_full_extent_2000_dir, tile,
#                                                             cn.pattern_soil_C_full_extent_2000), '.')
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.elevation_processed_dir, tile,
#                                                             cn.pattern_elevation), '.')
#     try:
#         uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.WHRC_biomass_2000_unmasked_dir, tile,
#                                                             cn.pattern_WHRC_biomass_2000_unmasked), '.')
#     except:
#         print "No WHRC biomass in", tile
#     try:
#         uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.mangrove_biomass_2000_dir, tile, cn.pattern_mangrove_biomass_2000), '.')
#     except:
#         print "No mangrove biomass in", tile


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
pool = Pool(num_of_processes)
pool.map(partial(create_BGC_deadwood_litter_soil_totalC.create_BGC, mang_BGB_AGB_ratio=mang_BGB_AGB_ratio, extent=extent), tile_list)
pool.close()
pool.join()

uu.upload_final_set(cn.BGC_2000_dir, cn.pattern_BGC_2000)
# cmd = ['rm *{}*.tif'.format(cn.pattern_BGC_2000)]
# subprocess.check_call(cmd)

# 16 processors used between 300 and 400 GB memory, so it was okay on a r4.16xlarge spot machine
num_of_processes = 16
pool = Pool(num_of_processes)
pool.map(partial(create_BGC_deadwood_litter_soil_totalC.create_deadwood, mang_deadwood_AGB_ratio=mang_deadwood_AGB_ratio, extent=extent), tile_list)
pool.close()
pool.join()

uu.upload_final_set(cn.deadwood_2000_dir, cn.pattern_deadwood_2000)
# cmd = ['rm *{}*.tif'.format(cn.pattern_deadwood_2000)]
# subprocess.check_call(cmd)

num_of_processes = 16
pool = Pool(num_of_processes)
pool.map(partial(create_BGC_deadwood_litter_soil_totalC.create_litter, mang_litter_AGB_ratio=mang_litter_AGB_ratio, extent=extent), tile_list)
pool.close()
pool.join()

uu.upload_final_set(cn.litter_2000_dir, cn.pattern_litter_2000)
# cmd = ['rm *{}*.tif'.format(cn.pattern_litter_2000)]
# subprocess.check_call(cmd)

'''
There's no soil C function here because full extent soil C is created in a different function (mp_create_soil_C.py).
'''

# I tried several different processor numbers for this. Ended up using 14 processors, which used about 380 GB memory
# at peak. Probably could've handled 16 processors on an r4.16xlarge machine but I didn't feel like taking the time to check.
num_of_processes = 16
pool = Pool(num_of_processes)
pool.map(partial(create_BGC_deadwood_litter_soil_totalC.create_total_C, extent=extent), tile_list)
pool.close()
pool.join()

uu.upload_final_set(cn.total_C_2000_dir, cn.pattern_total_C_2000)
# cmd = ['rm *{}*.tif'.format(cn.pattern_total_C_2000)]
# subprocess.check_call(cmd)

# For single processor use
# for tile in tile_list:
    # create_BGC_deadwood_litter_soil_totalC_in_2000.create_BGC(tile, mang_BGB_AGB_ratio, extent)
    # create_BGC_deadwood_litter_soil_totalC_in_2000.create_deadwood(tile, mang_deadwood_AGB_ratio, extent)
    # create_BGC_deadwood_litter_soil_totalC_in_2000.create_litter(tile, mang_litter_AGB_ratio, extent)
    # create_BGC_deadwood_litter_soil_totalC.create_total_C(tile, extent)
#
# uu.upload_final_set(cn.BGC_2000_dir, cn.pattern_BGC_2000)
# uu.upload_final_set(cn.deadwood_2000_dir, cn.pattern_deadwood_2000)
# uu.upload_final_set(cn.litter_2000_dir, cn.pattern_litter_2000)
# uu.upload_final_set(cn.total_C_2000_dir, cn.pattern_total_C_2000)
