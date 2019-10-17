### This script assigns annual above and belowground non-mangrove, non-planted forestbiomass gain rates
### (in the units of IPCC Table 4.9 (currently tonnes biomass/ha/yr)) to non-mangrove natural forest pixels.
### It requires IPCC Table 4.9, formatted for easy ingestion by pandas.
### Essentially, this does some processing of the IPCC gain rate table, then uses it as a dictionary that it applies
### to every pixel in every tile.
### Each continent-ecozone-forest age category combination gets its own code, which matches the codes in the
### processed IPCC table.
### Belowground biomass gain rate is a constant proportion of aboveground biomass gain rate, again according to IPCC tables.

import multiprocessing
from functools import partial
import annual_gain_rate_natrl_forest
import pandas as pd
import subprocess
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

pd.options.mode.chained_assignment = None

biomass_tile_list = uu.tile_list(cn.WHRC_biomass_2000_non_mang_non_planted_dir)
# biomass_tile_list = ['80N_020E', '00N_000E', '00N_020E', '00N_110E'] # test tiles: no mangrove or planted forest, mangrove only, planted forest only, mangrove and planted forest
# biomass_tile_list = ['00N_110E']
print biomass_tile_list
print "There are {} tiles to process".format(str(len(biomass_tile_list)))

# For downloading all tiles in the input folders.
# Mangrove biomass and full-extent planted forests are used to mask out mangrove and planted forests from the natural forests.
download_list = [cn.age_cat_natrl_forest_dir, cn.cont_eco_dir, cn.plant_pre_2000_processed_dir]

for input in download_list:
    uu.s3_folder_download(input, '.')

# # For copying individual tiles to spot machine for testing
# for tile in biomass_tile_list:
#
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.age_cat_natrl_forest_dir, tile, cn.pattern_age_cat_natrl_forest), '.')   # forest age category tiles
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.cont_eco_dir, tile, cn.pattern_cont_eco_processed), '.')        # continents and FAO ecozones 2000
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.plant_pre_2000_processed_dir, tile, cn.pattern_plant_pre_2000), '.')

# Table with IPCC Table 4.9 default gain rates
cmd = ['aws', 's3', 'cp', os.path.join(cn.gain_spreadsheet_dir, cn.gain_spreadsheet), '.']
subprocess.check_call(cmd)

# Imports the table with the ecozone-continent codes and the carbon gain rates
gain_table = pd.read_excel("{}".format(cn.gain_spreadsheet),
                           sheet_name = "natrl fores gain, for model")

# Removes rows with duplicate codes (N. and S. America for the same ecozone)
gain_table_simplified = gain_table.drop_duplicates(subset='gainEcoCon', keep='first')

# Converts gain table from wide to long, so each continent-ecozone-age category has its own row
gain_table_cont_eco_age = pd.melt(gain_table_simplified, id_vars = ['gainEcoCon'], value_vars = ['growth_primary', 'growth_secondary_greater_20', 'growth_secondary_less_20'])
gain_table_cont_eco_age = gain_table_cont_eco_age.dropna()

# Creates a table that has just the continent-ecozone combinations for adding to the dictionary.
# These will be used whenever there is just a continent-ecozone pixel without a forest age pixel
gain_table_con_eco_only = gain_table_cont_eco_age
gain_table_con_eco_only = gain_table_con_eco_only.drop_duplicates(subset='gainEcoCon', keep='first')
gain_table_con_eco_only['value'] = 0
gain_table_con_eco_only['cont_eco_age'] = gain_table_con_eco_only['gainEcoCon']

# Creates a code for each age category so that each continent-ecozone-age combo can have its own unique value
age_dict = {'growth_primary': 10000, 'growth_secondary_greater_20': 20000, 'growth_secondary_less_20': 30000}

# Creates a unique value for each continent-ecozone-age category
gain_table_cont_eco_age = gain_table_cont_eco_age.replace({"variable": age_dict})
gain_table_cont_eco_age['cont_eco_age'] = gain_table_cont_eco_age['gainEcoCon'] + gain_table_cont_eco_age['variable']

# Merges the table of just continent-ecozone codes and the table of  continent-ecozone-age codes
gain_table_all_combos = pd.concat([gain_table_con_eco_only, gain_table_cont_eco_age])

# Converts the continent-ecozone-age codes and corresponding gain rates to a dictionary
gain_table_dict = pd.Series(gain_table_all_combos.value.values,index=gain_table_all_combos.cont_eco_age).to_dict()

# Adds a dictionary entry for where the ecozone-continent-age code is 0 (not in a continent)
gain_table_dict[0] = 0

# Adds a dictionary entry for each forest age code for pixels that have forest age but no continent-ecozone
for key, value in age_dict.iteritems():

    gain_table_dict[value] = 0

# Converts all the keys (continent-ecozone-age codes) to float type
gain_table_dict = {float(key): value for key, value in gain_table_dict.iteritems()}


# This configuration of the multiprocessing call is necessary for passing multiple arguments to the main function
# It is based on the example here: http://spencerimp.blogspot.com/2015/12/python-multiprocess-with-multiple.html
# processes=24 peaks at about 440 GB of memory on an r4.16xlarge machine
count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(processes=24)
pool.map(partial(annual_gain_rate_natrl_forest.annual_gain_rate, gain_table_dict=gain_table_dict), biomass_tile_list)
pool.close()
pool.join()

# # For single processor use
# for tile in biomass_tile_list:
#
#     annual_gain_rate_natrl_forest.annual_gain_rate(tile, gain_table_dict)

print "Tiles processed. Uploading to s3 now..."
uu.upload_final_set(cn.annual_gain_AGB_natrl_forest_dir, cn.pattern_annual_gain_AGB_natrl_forest)
uu.upload_final_set(cn.annual_gain_BGB_natrl_forest_dir, cn.pattern_annual_gain_BGB_natrl_forest)
