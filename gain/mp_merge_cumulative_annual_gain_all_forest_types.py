### This script combines the annual gain rate tiles from different forest types (non-mangrove natural forests, mangroves,
### plantations) into combined tiles. It does the same for cumulative gain over the study period.

import multiprocessing
import utilities
import merge_cumulative_annual_gain_all_forest_types
import sys
sys.path.append('../')
import constants_and_names as cn

### Need to update and install some packages on spot machine before running
### sudo pip install rasterio --upgrade

biomass_tile_list = utilities.tile_list(cn.natrl_forest_biomass_2000_dir)
# biomass_tile_list = ['20S_110E', '30S_110E'] # test tiles
# biomass_tile_list = ['10N_080W', '40N_120E'] # test tiles
# biomass_tile_list = ['40N_120E'] # test tiles
print biomass_tile_list

# For downloading all tiles in the input folders
download_list = [cn.annual_gain_AGB_natrl_forest_dir, cn.annual_gain_AGB_mangrove_dir,
                 cn.cumul_gain_AGC_natrl_forest_dir, cn.cumul_gain_AGC_mangrove_dir,
                 cn.annual_gain_BGB_natrl_forest_dir, cn.annual_gain_BGB_mangrove_dir,
                 cn.cumul_gain_BGC_natrl_forest_dir, cn.cumul_gain_BGC_mangrove_dir]

for input in download_list:
    utilities.s3_folder_download(input, '.')

# # For copying individual tiles to spot machine for testing
# for tile in biomass_tile_list:
#
#     utilities.s3_file_download('{0}{1}_{2}.tif'.format(cn.annual_gain_AGB_natrl_forest_dir, tile, cn.pattern_annual_gain_AGB_natrl_forest), '.')  # annual aboveground gain rate tiles for natural forests
#     utilities.s3_file_download('{0}{1}_{2}.tif'.format(cn.annual_gain_AGB_mangrove_dir, tile, cn.pattern_annual_gain_AGB_mangrove), '.')  # annual aboveground gain rate tiles for mangroves
#     utilities.s3_file_download('{0}{1}_{2}.tif'.format(cn.cumul_gain_AGC_natrl_forest_dir, tile, cn.pattern_cumul_gain_AGC_natrl_forest), '.')           # cumulative aboveground gain tiles for natural forests
#     utilities.s3_file_download('{0}{1}_{2}.tif'.format(cn.cumul_gain_AGC_mangrove_dir, tile, cn.pattern_cumul_gain_AGC_mangrove), '.')  # cumulative aboveground gain tiles for mangroves
#     utilities.s3_file_download('{0}{1}_{2}.tif'.format(cn.annual_gain_BGB_natrl_forest_dir, tile, cn.pattern_annual_gain_BGB_natrl_forest), '.')  # annual belowground gain rate tiles for natural forests
#     utilities.s3_file_download('{0}{1}_{2}.tif'.format(cn.annual_gain_BGB_mangrove_dir, tile, cn.pattern_annual_gain_BGB_mangrove), '.')  # annual belowground gain rate tiles for mangroves
#     utilities.s3_file_download('{0}{1}_{2}.tif'.format(cn.cumul_gain_BGC_natrl_forest_dir, tile, cn.pattern_cumul_gain_BGC_natrl_forest), '.')           # cumulative belowground gain tiles for natural forests
#     utilities.s3_file_download('{0}{1}_{2}.tif'.format(cn.cumul_gain_BGC_mangrove_dir, tile, cn.pattern_cumul_gain_BGC_mangrove), '.')  # cumulative belowground gain tiles for mangroves


count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(count / 4)
pool.map(merge_cumulative_annual_gain_all_forest_types.gain_merge, biomass_tile_list)

# # For single processor use
# for tile in biomass_tile_list:
#
#     merge_cumulative_annual_gain_all_forest_types.gain_merge(tile)

