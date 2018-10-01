### This

import multiprocessing
import utilities
import cumulative_gain
import subprocess

### Need to update and install some packages on spot machine before running
### sudo pip install rasterio --upgrade
### sudo pip install pandas --upgrade
### sudo pip install xlrd

# Annual gain rate and the number of years with gain are needed
annual_gain = 's3://gfw2-data/climate/carbon_model/annual_gain_rate/20180926/'
gain_year_count = 's3://gfw2-data/climate/carbon_model/gain_year_count/20180912/'

biomass = 's3://gfw2-data/climate/WHRC_biomass/WHRC_V4/Processed/'
# biomass_tile_list = utilities.tile_list(biomass)
# biomass_tile_list = ['20S_110E', '30S_110E'] # test tiles
biomass_tile_list = ['20S_110E'] # test tiles
print biomass_tile_list

# # For downloading all tiles in the input folders
# download_list = [annual_gain, gain_year_count]
#
# for input in download_list:
#     utilities.s3_folder_download('{}'.format(input), '.')

# For copying individual tiles to spot machine for testing
for tile in biomass_tile_list:

    utilities.s3_file_download('{0}annual_gain_rate_{1}.tif'.format(annual_gain, tile), '.')           # annual gain rate tiles
    utilities.s3_file_download('{0}gain_year_count_{1}.tif'.format(gain_year_count, tile), '.')        # number of years with gain tiles

# count = multiprocessing.cpu_count()
# pool = multiprocessing.Pool(count / 4)
# pool.map(cumulative_gain.cumulative_gain, biomass_tile_list)

# For single processor use
for tile in biomass_tile_list:

    cumulative_gain.cumulative_gain(tile)

