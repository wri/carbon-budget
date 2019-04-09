import create_carbon_pools_in_emis_year
import multiprocessing
import subprocess
import os
import pandas as pd
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

pd.options.mode.chained_assignment = None

# tile_list = uu.tile_list(cn.AGC_emis_year_dir)
tile_list = ['00N_110E'] # test tiles
# tile_list = ['80N_020E', '00N_020E', '00N_000E', '00N_110E'] # test tiles
print tile_list
print "There are {} tiles to process".format(str(len(tile_list)))

# For downloading all tiles in the input folders.
input_files = [
    cn.AGC_emis_year_dir,
    cn.mangrove_biomass_2000_dir,
    cn.fao_ecozone_processed_dir,
    cn.precip_processed_dir,
    cn.soil_C_2000_dir,
    cn.elevation_processed_dir
    ]

# for input in input_files:
#     uu.s3_folder_download('{}'.format(input), '.')

# # For copying individual tiles to spot machine for testing.
# for tile in tile_list:
#
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.AGC_emis_year_dir, tile,
#                                                             cn.pattern_AGC_emis_year), '.')
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.mangrove_biomass_2000_dir, tile,
#                                                             cn.pattern_mangrove_biomass_2000), '.')
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.cont_eco_dir, tile,
#                                                             cn.pattern_cont_eco_processed), '.')
#     # uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.precip_processed_dir, tile,
#     #                                                         cn.pattern_precip), '.')
#     # uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.soil_C_2000_dir, tile,
#     #                                                         cn.pattern_soil_C_2000), '.')
#     # uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.elevation_processed_dir, tile,
#     #                                                         cn.pattern_elevation), '.')


# Table with IPCC Wetland Supplement Table 4.4 default mangrove gain rates
cmd = ['aws', 's3', 'cp', os.path.join(cn.gain_spreadsheet_dir, cn.gain_spreadsheet), '.']
subprocess.check_call(cmd)

# Imports the table with the ecozone-continent codes and the carbon gain rates
gain_table = pd.read_excel("{}".format(cn.gain_spreadsheet),
                           sheet_name = "mangrove gain, for model")

# Removes rows with duplicate codes (N. and S. America for the same ecozone)
gain_table_simplified = gain_table.drop_duplicates(subset='gainEcoCon', keep='first')

# Creates belowground:aboveground biomass ratio dictionary for the three mangrove types, where the keys correspond to
# the "mangType" field in the gain rate spreadsheet.
# If the assignment of mangTypes to ecozones changes, that column in the spreadsheet may need to change and the
# keys in this dictionary would need to change accordingly.
# Key 4 is water, so there shouldn't be any mangrove values there.
type_ratio_dict = {'1': cn.below_to_above_trop_dry_mang, '2'  :cn.below_to_above_trop_wet_mang, '3': cn.below_to_above_subtrop_mang, '4': '100'}
type_ratio_dict_final = {int(k):float(v) for k,v in type_ratio_dict.items()}

# Applies the belowground:aboveground biomass ratios for the three mangrove types to the annual aboveground gain rates to get
# a column of belowground annual gain rates by mangrove type
gain_table_simplified['BGB_AGB_ratio'] = gain_table_simplified['mangType'].map(type_ratio_dict_final)

# Converts the continent-ecozone codes and corresponding gain rates to dictionaries for aboveground and belowground gain rates
mang_BGB_AGB_ratio = pd.Series(gain_table_simplified.BGB_AGB_ratio.values,index=gain_table_simplified.gainEcoCon).to_dict()

# Adds a dictionary entry for where the ecozone-continent code is 0 (not in a continent)
mang_BGB_AGB_ratio[0] = 0

# # Converts all the keys (continent-ecozone codes) to float type
# mang_BGB_AGB_ratio = {float(key): value for key, value in mang_BGB_AGB_ratio.iteritems()}

print "Creating carbon pools..."

# count = multiprocessing.cpu_count()
# pool = multiprocessing.Pool(processes=count/3)
# pool.map(create_carbon_pools_in_emis_year.create_BGC, tile_list)

# For single processor use
for tile in tile_list:
    create_carbon_pools_in_emis_year.create_BGC(tile, mang_BGB_AGB_ratio)

print "Uploading output to s3..."

# uu.upload_final_set('{0}/{1}/'.format(cn.AGC_dir, type), '{0}_{1}'.format(cn.pattern_AGC, type))
uu.upload_final_set(cn.BGC_emis_year_dir,cn.pattern_BGC_emis_year)
# uu.upload_final_set('{0}/{1}/'.format(cn.deadwood_dir, type), '{0}_{1}'.format(cn.pattern_deadwood, type))
# uu.upload_final_set('{0}/{1}/'.format(cn.litter_dir, type), '{0}_{1}'.format(cn.pattern_litter, type))
# uu.upload_final_set('{0}/{1}/'.format(cn.soil_C_pool_dir, type), '{0}_{1}'.format(cn.pattern_soil_pool, type))
# uu.upload_final_set('{0}/{1}/'.format(cn.total_C_dir, type), '{0}_{1}'.format(cn.pattern_total_C, type))