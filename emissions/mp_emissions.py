import multiprocessing
import calc_emissions
import utilities
import tile_peat_dict
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

tile_list = uu.tile_list(cn.AGC_emis_year_dir)
# tile_list = ['00N_110E'] # test tiles
# tile_list = ['80N_020E', '30N_080W', '00N_020E', '00N_110E'] # test tiles: no mangrove or planted forest, mangrove only, planted forest only, mangrove and planted forest
print tile_list
print "There are {} unique tiles to process".format(str(len(tile_list)))

# For downloading all tiles in the folders
download_list = [cn.AGC_emis_year_dir, cn.BGC_emis_year_dir, cn.deadwood_emis_year_2000_dir, cn.litter_emis_year_2000_dir, cn.soil_C_emis_year_2000_dir,
                 cn.peat_mask_dir, cn.ifl_dir, cn.planted_forest_type_unmasked_dir, cn.drivers_processed_dir, cn.climate_zone_processed_dir,
                 cn.bor_tem_trop_processed_dir, cn.burn_year_dir, cn.plant_pre_2000_raw_dir,
                 cn.loss_dir]

for input in download_list:
    uu.s3_folder_download(input, '.')

# For copying individual tiles to s3 for testing
for tile in tile_list:


    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.AGC_emis_year_dir, tile, cn.pattern_AGC_emis_year), '.')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.BGC_emis_year_dir, tile, cn.pattern_BGC_emis_year), '.')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.deadwood_emis_year_2000_dir, tile, cn.pattern_deadwood_emis_year_2000), '.')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.litter_emis_year_2000_dir, tile, cn.pattern_litter_emis_year_2000), '.')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.soil_C_emis_year_2000_dir, tile, cn.pattern_soil_C_emis_year_2000), '.')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.peat_mask_dir, tile, cn.pattern_peat_mask), '.')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.planted_forest_type_unmasked_dir, tile, cn.pattern_planted_forest_type_unmasked), '.')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.drivers_processed_dir, tile, cn.pattern_drivers), '.')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.climate_zone_processed_dir, tile, cn.pattern_climate_zone), '.')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.bor_tem_trop_processed_dir, tile, cn.pattern_bor_tem_trop_processed), '.')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.burn_year_dir, tile, cn.pattern_burn_year), '.')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.plant_pre_2000_processed_dir, tile, cn.pattern_plant_pre_2000), '.')
    uu.s3_file_download('{0}{1}.tif'.format(cn.loss_dir, tile), '.')


#if idn plant tile downloaded, mask loss with plantations because we know that idn gfw_plantations
# were established in yr 2000.

IDN_MYS_tile_list = uu.tile_list(cn.plant_pre_2000_processed_dir)

print "Removing loss from plantations that existed in Indonesia and Malaysia before 2000..."
count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(count-10)
pool.map(utilities.mask_loss, IDN_MYS_tile_list)

if tile_id in ['00N_090E', '00N_100E', '00N_110E', '00N_120E', '00N_130E', '00N_140E', '10N_090E', '10N_100E', '10N_110E', '10N_120E', '10N_130E', '10N_140E']:
    print "cutting out plantations in Indonesia, Malaysia"
    utilities.mask_loss(tile_id)


# Used about 200 GB of memory. count-10 worked fine (with memory to spare) on an r4.16xlarge machine.
count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(count-10)
pool.map(calc_emissions.calc_emissions, tile_list)

# # For single processor use
# for tile in tile_list:
#
#       calc_emissions.calc_emissions(tile)

uu.upload_final_set(cn.climate_zone_processed_dir, cn.pattern_climate_zone)
uu.upload_final_set(cn.plant_pre_2000_processed_dir, cn.pattern_plant_pre_2000)
uu.upload_final_set(cn.drivers_processed_dir, cn.pattern_drivers)
