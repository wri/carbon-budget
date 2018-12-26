import create_carbon_pools
import multiprocessing
import sys
sys.path.append('../')
import constants_and_names
import universal_util

# tile_list = universal_util.tile_list(constants_and_names.natrl_forest_biomass_2000_dir)
tile_list = ['10N_080W', '40N_120E'] # test tiles
print tile_list

# For downloading all tiles in the input folders.
input_files = [
    constants_and_names.natrl_forest_biomass_2000_dir,
    constants_and_names.mangrove_biomass_2000_dir,
    constants_and_names.fao_ecozone_processed_dir,
    constants_and_names.precip_processed_dir,
    constants_and_names.soil_C_processed_dir,
    constants_and_names.srtm_processed_dir
    ]

# for input in input_files:
#     universal_util.s3_folder_download('{}'.format(input), '.')

# For copying individual tiles to spot machine for testing.
for tile in tile_list:

    universal_util.s3_file_download('{0}{1}_{2}.tif'.format(constants_and_names.natrl_forest_biomass_emitted_dir, tile,
                                                            constants_and_names.pattern_natrl_forest_biomass_emitted), '.')
    universal_util.s3_file_download('{0}{1}_{2}.tif'.format(constants_and_names.mangrove_biomass_emitted_dir, tile,
                                                            constants_and_names.pattern_mangrove_biomass_emitted), '.')
    universal_util.s3_file_download('{0}{1}_{2}.tif'.format(constants_and_names.fao_ecozone_processed_dir, tile,
                                                            constants_and_names.pattern_fao_ecozone_processed), '.')
    universal_util.s3_file_download('{0}{1}_{2}.tif'.format(constants_and_names.precip_processed_dir, tile,
                                                            constants_and_names.pattern_precip), '.')
    universal_util.s3_file_download('{0}{1}_{2}.tif'.format(constants_and_names.soil_C_processed_dir, tile,
                                                            constants_and_names.pattern_soil_C), '.')
    universal_util.s3_file_download('{0}{1}_{2}.tif'.format(constants_and_names.srtm_processed_dir, tile,
                                                            constants_and_names.pattern_srtm), '.')

tile_count = []
for i, input in enumerate(input_files):

    tile_count[i] = universal_util.count_tiles(input)

print "The number of tiles for each input to the carbon pools is", tile_count

if len(set(tile_count)) > 0 & tile_count[0] == constants_and_names.biomass_tile_count:

    print "Input tiles for carbon pool generation do not exist. You must create them. Exiting now."

    exit()

print "Creating carbon pools"

count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(processes=count/3)
pool.map(create_carbon_pools.create_carbon_pools, tile_list)
