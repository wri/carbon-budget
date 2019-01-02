import create_emitted_biomass
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
    constants_and_names.cumul_gain_AGC_mangrove_dir,
    constants_and_names.cumul_gain_AGC_natrl_forest_dir
    ]

# for input in input_files:
#     universal_util.s3_folder_download('{}'.format(input), '.')

# For copying individual tiles to spot machine for testing.
# The cumulative carbon gain tiles are for adding to the biomass 2000 tiles to get AGC at the time of tree cover loss.
for tile in tile_list:

    universal_util.s3_file_download('{0}{1}_{2}.tif'.format(constants_and_names.natrl_forest_biomass_2000_dir, tile,
                                                            constants_and_names.pattern_natrl_forest_biomass_2000), '.')
    universal_util.s3_file_download('{0}{1}_{2}.tif'.format(constants_and_names.mangrove_biomass_raw_dir, tile,
                                                            constants_and_names.pattern_mangrove_biomass_emitted), '.')
    universal_util.s3_file_download('{0}{1}_{2}.tif'.format(constants_and_names.cumul_gain_AGC_mangrove_dir,
                                                            constants_and_names.pattern_cumul_gain_AGC_mangrove, tile), '.')
    universal_util.s3_file_download('{0}{1}_{2}.tif'.format(constants_and_names.cumul_gain_AGC_natrl_forest_dir,
                                                            constants_and_names.pattern_cumul_gain_AGC_natrl_forest, tile), '.')

print "Creating tiles of emitted biomass (biomass 2000 + biomass accumulation)"

count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(processes=count/3)
pool.map(create_emitted_biomass.create_emitted_biomass, tile_list)
