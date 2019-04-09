import create_carbon_pools_in_emis_year
import multiprocessing
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

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

# For copying individual tiles to spot machine for testing.
for tile in tile_list:

    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.AGC_emis_year_dir, tile,
                                                            cn.pattern_AGC_emis_year), '.')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.mangrove_biomass_2000_dir, tile,
                                                            cn.pattern_mangrove_biomass_2000), '.')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.cont_eco_dir, tile,
                                                            cn.pattern_cont_eco_processed), '.')
    # uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.precip_processed_dir, tile,
    #                                                         cn.pattern_precip), '.')
    # uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.soil_C_2000_dir, tile,
    #                                                         cn.pattern_soil_C_2000), '.')
    # uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.elevation_processed_dir, tile,
    #                                                         cn.pattern_elevation), '.')

print "Creating carbon pools..."

# count = multiprocessing.cpu_count()
# pool = multiprocessing.Pool(processes=count/3)
# pool.map(create_carbon_pools_in_emis_year.create_BGC, tile_list)

# For single processor use
for tile in tile_list:
    create_carbon_pools_in_emis_year.create_BGC(tile)

print "Uploading output to s3..."

# uu.upload_final_set('{0}/{1}/'.format(cn.AGC_dir, type), '{0}_{1}'.format(cn.pattern_AGC, type))
uu.upload_final_set(cn.BGC_emis_year_dir,cn.pattern_BGC_emis_year)
# uu.upload_final_set('{0}/{1}/'.format(cn.deadwood_dir, type), '{0}_{1}'.format(cn.pattern_deadwood, type))
# uu.upload_final_set('{0}/{1}/'.format(cn.litter_dir, type), '{0}_{1}'.format(cn.pattern_litter, type))
# uu.upload_final_set('{0}/{1}/'.format(cn.soil_C_pool_dir, type), '{0}_{1}'.format(cn.pattern_soil_pool, type))
# uu.upload_final_set('{0}/{1}/'.format(cn.total_C_dir, type), '{0}_{1}'.format(cn.pattern_total_C, type))