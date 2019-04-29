### This script combines the annual gain rate tiles from different forest types (non-mangrove natural forests, mangroves,
### plantations) into combined tiles. It does the same for cumulative gain over the study period.

import multiprocessing
import utilities
import merge_cumulative_annual_gain_all_forest_types
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

tile_list = uu.create_combined_tile_list(cn.WHRC_biomass_2000_non_mang_non_planted_dir,
                                         cn.annual_gain_AGB_mangrove_dir,
                                         set3=cn.annual_gain_AGB_planted_forest_non_mangrove_dir
                                         )
# tile_list = ['00N_110E'] # test tiles
# tile_list = ['80N_020E', '00N_000E', '00N_020E', '00N_110E'] # test tiles: no mangrove or planted forest, mangrove only, planted forest only, mangrove and planted forest
print tile_list
print "There are {} unique tiles to process".format(str(len(tile_list)))

# For downloading all tiles in the input folders
download_list = [cn.annual_gain_AGB_natrl_forest_dir, cn.annual_gain_AGB_mangrove_dir,
                 cn.cumul_gain_AGC_natrl_forest_dir, cn.cumul_gain_AGC_mangrove_dir,
                 cn.annual_gain_BGB_natrl_forest_dir, cn.annual_gain_BGB_mangrove_dir,
                 cn.cumul_gain_BGC_natrl_forest_dir, cn.cumul_gain_BGC_mangrove_dir,
                 cn.annual_gain_AGB_planted_forest_non_mangrove_dir, cn.annual_gain_BGB_planted_forest_non_mangrove_dir,
                 cn.cumul_gain_AGC_planted_forest_non_mangrove_dir, cn.cumul_gain_BGC_planted_forest_non_mangrove_dir
                 ]

for input in download_list:
    uu.s3_folder_download(input, '.')

# # For copying individual tiles to spot machine for testing
# for tile in biomass_tile_list:
#
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.annual_gain_AGB_natrl_forest_dir, tile, cn.pattern_annual_gain_AGB_natrl_forest), '.')  # annual aboveground gain rate tiles for non-mangrove non-planted natural forests
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.annual_gain_AGB_mangrove_dir, tile, cn.pattern_annual_gain_AGB_mangrove), '.')  # annual aboveground gain rate tiles for mangroves
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.cumul_gain_AGC_natrl_forest_dir, tile, cn.pattern_cumul_gain_AGC_natrl_forest), '.')           # cumulative aboveground gain tiles for non-mangrove non-planted natural forests
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.cumul_gain_AGC_mangrove_dir, tile, cn.pattern_cumul_gain_AGC_mangrove), '.')  # cumulative aboveground gain tiles for mangroves
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.annual_gain_BGB_natrl_forest_dir, tile, cn.pattern_annual_gain_BGB_natrl_forest), '.')  # annual belowground gain rate tiles for non-mangrove non-planted natural forests
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.annual_gain_BGB_mangrove_dir, tile, cn.pattern_annual_gain_BGB_mangrove), '.')  # annual belowground gain rate tiles for mangroves
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.cumul_gain_BGC_natrl_forest_dir, tile, cn.pattern_cumul_gain_BGC_natrl_forest), '.')           # cumulative belowground gain tiles for non-mangrove non-planted natural forests
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.cumul_gain_BGC_mangrove_dir, tile, cn.pattern_cumul_gain_BGC_mangrove), '.')  # cumulative belowground gain tiles for mangroves
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.annual_gain_BGB_planted_forest_non_mangrove_dir, tile, cn.pattern_annual_gain_BGB_planted_forest_non_mangrove), '.')  # annual belowground gain rate tiles for non-mangrove planted forests
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.annual_gain_BGB_planted_forest_non_mangrove_dir, tile, cn.pattern_annual_gain_BGB_planted_forest_non_mangrove), '.')  # annual belowground gain rate tiles for non-mangrove planted forests
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.cumul_gain_BGC_planted_forest_non_mangrove_dir, tile, cn.pattern_cumul_gain_BGC_planted_forest_non_mangrove), '.')    # cumulative belowground gain tiles for non-mangrove planted forests
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.cumul_gain_BGC_planted_forest_non_mangrove_dir, tile, cn.pattern_cumul_gain_BGC_planted_forest_non_mangrove), '.')    # cumulative belowground gain tiles for non-mangrove planted forests

# For multiprocessing
# This works on an r4.16xlarge. count/3 does not work.
count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(count / 4)
pool.map(merge_cumulative_annual_gain_all_forest_types.gain_merge, tile_list)

# # For single processor use
# for tile in tile_list:
#
#     merge_cumulative_annual_gain_all_forest_types.gain_merge(tile)


uu.upload_final_set(cn.annual_gain_combo_dir, cn.pattern_annual_gain_combo)
uu.upload_final_set(cn.cumul_gain_combo_dir, cn.pattern_cumul_gain_combo)