'''
This script calculates the gross emissions in tonnes CO2e/ha for every loss pixel.
'''

import multiprocessing
import calculate_gross_emissions
import utilities
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# tile_list = uu.tile_list(cn.AGC_emis_year_dir)
tile_list = ['50N_020E'] # test tiles
# tile_list = ['80N_020E', '30N_080W', '00N_020E', '00N_110E'] # test tiles: no mangrove or planted forest, mangrove only, planted forest only, mangrove and planted forest
print tile_list
print "There are {} tiles to process".format(str(len(tile_list)))

# For downloading all tiles in the folders
download_list = [cn.AGC_emis_year_dir, cn.BGC_emis_year_dir, cn.deadwood_emis_year_2000_dir, cn.litter_emis_year_2000_dir, cn.soil_C_emis_year_2000_dir,
                 cn.peat_mask_dir, cn.ifl_dir, cn.planted_forest_type_unmasked_dir, cn.drivers_processed_dir, cn.climate_zone_processed_dir,
                 cn.bor_tem_trop_processed_dir, cn.burn_year_dir, cn.plant_pre_2000_raw_dir,
                 cn.loss_dir]

# for input in download_list:
#     uu.s3_folder_download(input, '.')

# For copying individual tiles to s3 for testing
for tile in tile_list:

    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.AGC_emis_year_dir, tile, cn.pattern_AGC_emis_year), './cpp_util/')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.BGC_emis_year_dir, tile, cn.pattern_BGC_emis_year), './cpp_util/')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.deadwood_emis_year_2000_dir, tile, cn.pattern_deadwood_emis_year_2000), './cpp_util/')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.litter_emis_year_2000_dir, tile, cn.pattern_litter_emis_year_2000), './cpp_util/')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.soil_C_emis_year_2000_dir, tile, cn.pattern_soil_C_emis_year_2000), './cpp_util/')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.peat_mask_dir, tile, cn.pattern_peat_mask), './cpp_util/')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.ifl_dir, tile, cn.pattern_ifl), './cpp_util/')
    try:
        uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.planted_forest_type_unmasked_dir, tile, cn.pattern_planted_forest_type_unmasked), './cpp_util/')
    except:
        print "No plantations in", tile
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.drivers_processed_dir, tile, cn.pattern_drivers), './cpp_util/')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.climate_zone_processed_dir, tile, cn.pattern_climate_zone), './cpp_util/')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.bor_tem_trop_processed_dir, tile, cn.pattern_bor_tem_trop_processed), './cpp_util/')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.burn_year_dir, tile, cn.pattern_burn_year), './cpp_util/')
    try:
        uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.plant_pre_2000_processed_dir, tile, cn.pattern_plant_pre_2000), './cpp_util/')
    except:
        print "No pre-2000 plantations in", tile
    uu.s3_file_download('{0}{1}.tif'.format(cn.loss_dir, tile), './cpp_util/')


print "Removing loss pixels from plantations that existed in Indonesia and Malaysia before 2000..."
count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(count/2)
pool.map(utilities.mask_loss_pre_2000_plantation, tile_list)

# # For single processor use
# for tile in tile_list:
#
#       utilities.mask_loss_pre_2000_plantation(tile)


# Creates tiles of 0s for any tile without any plantations
for tile in tile_list:

    uu.make_blank_tile(tile, cn.pattern_planted_forest_type_unmasked, 'cpp_util/')


# # 6.68 GB for four tiles simultaenously
# count = multiprocessing.cpu_count()
# pool = multiprocessing.Pool(count)
# pool.map(calculate_gross_emissions.calc_emissions, tile_list)

# For single processor use
for tile in tile_list:

      calculate_gross_emissions.calc_emissions(tile)

uu.upload_final_set(cn.gross_emis_commod_dir, cn.pattern_gross_emis_commod)
uu.upload_final_set(cn.gross_emis_shifting_ag_dir, cn.pattern_gross_emis_shifting_ag)
uu.upload_final_set(cn.gross_emis_forestry_dir, cn.pattern_gross_emis_forestry)
uu.upload_final_set(cn.gross_emis_wildfire_dir, cn.pattern_gross_emis_wildfire)
uu.upload_final_set(cn.gross_emis_urban_dir, cn.pattern_gross_emis_urban)
uu.upload_final_set(cn.gross_emis_no_driver_dir, cn.pattern_gross_emis_no_driver)
uu.upload_final_set(cn.gross_emis_nodes_dir, cn.pattern_gross_emis_nodes)
uu.upload_final_set(cn.gross_emis_all_drivers_dir, cn.pattern_gross_emis_all_drivers)

