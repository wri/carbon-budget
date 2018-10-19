### Creates tiles of annual aboveground biomass gain rates for mangroves using IPCC Wetlands Supplement Table 4.4 rates.
### Its inputs are the continent-ecozone tiles, mangrove biomass tiles (for locations of mangroves), and the IPCC
### gain rate table.

from multiprocessing.pool import Pool
from functools import partial
import utilities
import annual_gain_rate_mangrove
import pandas as pd
import subprocess

pd.options.mode.chained_assignment = None

### Need to update and install some packages on spot machine before running
### sudo pip install rasterio --upgrade
### sudo pip install pandas --upgrade
### sudo pip install xlrd


# biomass_tile_list = utilities.tile_list(utilities.biomass_dir)
# biomass_tile_list = ['20S_110E', '30S_110E'] # test tiles
biomass_tile_list = ['30N_160W'] # test tiles
print biomass_tile_list

# For downloading all tiles in the input folders
download_list = [utilities.cont_eco_dir, utilities.mangrove_biomass_dir]

# for input in download_list:
#     utilities.s3_folder_download('{}'.format(input), '.')

# For copying individual tiles to spot machine for testing
for tile in biomass_tile_list:

    utilities.s3_file_download('{0}{1}_{2}.tif'.format(utilities.cont_eco_dir, utilities.pattern_cont_eco_processed, tile), '.')    # continents and FAO ecozones 2000
    utilities.s3_file_download('{0}{1}_{2}.tif'.format(utilities.mangrove_biomass_dir, utilities.pattern_mangrove_biomass, tile), '.')         # continents and FAO ecozones 2000

# Table with IPCC Wetland Supplement Table 4.4 default mangrove gain rates
cmd = ['aws', 's3', 'cp', 's3://gfw2-data/climate/carbon_model/{}'.format(utilities.gain_spreadsheet), '.']
subprocess.check_call(cmd)

# Imports the table with the ecozone-continent codes and the carbon gain rates
gain_table = pd.read_excel("{}".format(utilities.gain_spreadsheet),
                           sheet_name = "mangrove gain, for model")

# Removes rows with duplicate codes (N. and S. America for the same ecozone)
gain_table_simplified = gain_table.drop_duplicates(subset='gainEcoCon', keep='first')

# Converts the continent-ecozone codes and corresponding gain rates to a dictionary
gain_table_dict = pd.Series(gain_table_simplified.gain_tons_yr.values,index=gain_table_simplified.gainEcoCon).to_dict()

# Adds a dictionary entry for where the ecozone-continent code is 0 (not in a continent)
gain_table_dict[0] = 0

# Converts all the keys (continent-ecozone codes) to float type
gain_table_dict = {float(key): value for key, value in gain_table_dict.iteritems()}

# This configuration of the multiprocessing call is necessary for passing multiple arguments to the main function
# It is based on the example here: http://spencerimp.blogspot.com/2015/12/python-multiprocess-with-multiple.html
num_of_processes = 16
pool = Pool(num_of_processes)
pool.map(partial(annual_gain_rate_mangrove.annual_gain_rate, gain_table_dict=gain_table_dict), biomass_tile_list)
pool.close()
pool.join()

# # For single processor use
# for tile in biomass_tile_list:
#
#     annual_gain_rate_mangrove.annual_gain_rate(tile, gain_table_dict)

