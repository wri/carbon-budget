### This script calculates the cumulative above and belowground carbon gain in mangrove forest pixels from 2001-2015.
### It multiplies the annual biomass gain rate by the number of years of gain by the biomass-to-carbon conversion.

import multiprocessing
import cumulative_gain_mangrove
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

mangrove_biomass_tile_list = uu.tile_list(cn.annual_gain_AGB_mangrove_dir)
# biomass_tile_list = ['20S_110E', '30S_110E'] # test tiles
# mangrove_biomass_tile_list = ['10N_080W'] # test tiles
print mangrove_biomass_tile_list
print "There are {} tiles to process".format(str(len(mangrove_biomass_tile_list)))

# For downloading all tiles in the input folders
download_list = [cn.annual_gain_AGB_mangrove_dir, cn.annual_gain_BGB_mangrove_dir, cn.gain_year_count_mangrove_dir]

for input in download_list:
    uu.s3_folder_download(input, '.')

# # For copying individual tiles to spot machine for testing
# for tile in mangrove_biomass_tile_list:
#
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.annual_gain_AGB_mangrove_dir, tile, cn.pattern_annual_gain_AGB_mangrove), '.')      # annual AGB gain rate tiles
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.annual_gain_BGB_mangrove_dir, tile, cn.pattern_annual_gain_BGB_mangrove), '.')      # annual AGB gain rate tiles
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.gain_year_count_mangrove_dir, tile, cn.pattern_gain_year_count_mangrove), '.')      # number of years with gain tiles

count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(count / 3)
# Calculates cumulative aboveground carbon gain in mangroves
# count/3 peaks at about 380 GB, so this is okay on r4.16xlarge
pool.map(cumulative_gain_mangrove.cumulative_gain_AGC, mangrove_biomass_tile_list)

# Calculates cumulative belowground carbon gain in mangroves
pool.map(cumulative_gain_mangrove.cumulative_gain_BGC, mangrove_biomass_tile_list)
pool.close()
pool.join()

# # For single processor use
# for tile in mangrove_biomass_tile_list:
#
#     cumulative_gain_mangrove.cumulative_gain_AGC(tile)
#
# for tile in mangrove_biomass_tile_list:
#
#     cumulative_gain_mangrove.cumulative_gain_BGC(tile)

uu.upload_final_set(cn.cumul_gain_AGC_mangrove_dir, cn.pattern_cumul_gain_AGC_mangrove)
uu.upload_final_set(cn.cumul_gain_BGC_mangrove_dir, cn.pattern_cumul_gain_BGC_mangrove)
