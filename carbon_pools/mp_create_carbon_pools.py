import create_carbon_pools
import multiprocessing
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# tile_list = uu.create_combined_tile_list(cn.mangrove_biomass_2000_dir,
#                                          cn.WHRC_biomass_2000_unmasked_dir)
tile_list = ['00N_000E'] # test tiles
# tile_list = ['80N_020E', '00N_020E', '00N_000E', '00N_110E'] # test tiles
print tile_list

# For downloading all tiles in the input folders.
input_files = [
    cn.WHRC_biomass_2000_unmasked_dir,
    cn.mangrove_biomass_2000_dir,
    cn.fao_ecozone_processed_dir,
    cn.precip_processed_dir,
    cn.soil_C_processed_dir,
    cn.elevation_processed_dir
    ]

# for input in input_files:
#     uu.s3_folder_download('{}'.format(input), '.')

# For copying individual tiles to spot machine for testing.
for tile in tile_list:

    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.natrl_forest_biomass_emitted_dir, tile,
                                                            cn.pattern_natrl_forest_biomass_emitted), '.')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.mangrove_biomass_emitted_dir, tile,
                                                            cn.pattern_mangrove_biomass_emitted), '.')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.fao_ecozone_processed_dir, tile,
                                                            cn.pattern_fao_ecozone_processed), '.')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.precip_processed_dir, tile,
                                                            cn.pattern_precip), '.')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.soil_C_processed_dir, tile,
                                                            cn.pattern_soil_C), '.')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.elevation_processed_dir, tile,
                                                            cn.pattern_elevation), '.')

tile_count = []
for i, input in enumerate(input_files):

    tile_count[i] = uu.count_tiles_s3(input)

print "The number of tiles for each input to the carbon pools is", tile_count

if len(set(tile_count)) > 0 & tile_count[0] == cn.biomass_tile_count:

    print "Input tiles for carbon pool generation do not exist. You must create them. Exiting now."

    exit()

print "Creating carbon pools"

count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(processes=count/3)
pool.map(create_carbon_pools.create_carbon_pools, tile_list)
