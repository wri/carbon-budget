import create_aboveground_carbon_in_2000
import multiprocessing
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# tile_list = uu.create_combined_tile_list(cn.WHRC_biomass_2000_unmasked_dir,
#                                          cn.annual_gain_AGB_mangrove_dir
#                                          )
tile_list = ['00N_110E'] # test tiles
# tile_list = ['80N_020E', '00N_020E', '30N_080W', '00N_110E'] # test tiles
print tile_list
print "There are {} unique tiles to process".format(str(len(tile_list)))


# For downloading all tiles in the input folders.
input_files = [
    cn.WHRC_biomass_2000_unmasked_dir, # This uses the unmasked HWRC biomass because it needs the biomass in planted forest pixels, not just the non-mangrove non-planted forest pixels
    cn.mangrove_biomass_2000_dir
    ]

# for input in input_files:
#     uu.s3_folder_download('{}'.format(input), '.')

# # For copying individual tiles to spot machine for testing.
# for tile in tile_list:
#
#     try:
#         uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.mangrove_biomass_2000_dir, tile, cn.pattern_mangrove_biomass_2000), '.')
#     except:
#         print "No mangrove biomass in", tile
#
#     try:
#         uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.WHRC_biomass_2000_unmasked_dir, tile, cn.pattern_WHRC_biomass_2000_unmasked), '.')
#     except:
#         print "No WHRC biomass in", tile

print "Creating tiles of aboveground carbon density in the year 2000"

# count = multiprocessing.cpu_count()
# pool = multiprocessing.Pool(processes=count/5)
# pool.map(create_aboveground_carbon_in_2000.create_2000_AGC, tile_list)

# For single processor use
for tile in tile_list:
    create_aboveground_carbon_in_2000.create_2000_AGC(tile)

uu.upload_final_set(cn.AGC_2000_dir, cn.pattern_AGC_2000)
