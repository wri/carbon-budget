import create_aboveground_carbon_in_emis_year
import multiprocessing
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# tile_list = uu.create_combined_tile_list(cn.mangrove_biomass_2000_dir,
#                                          cn.WHRC_biomass_2000_unmasked_dir)
# tile_list = ['00N_110E'] # test tiles
tile_list = ['80N_020E', '00N_020E', '00N_000E', '00N_110E'] # test tiles
print tile_list

# For downloading all tiles in the input folders.
input_files = [
    cn.WHRC_biomass_2000_unmasked_dir, # This uses the unmasked HWRC biomass because it needs the biomass in planted forest pixels, not just the non-mangrove non-planted forest pixels
    cn.mangrove_biomass_2000_dir,
    cn.cumul_gain_AGC_mangrove_dir,
    cn.cumul_gain_AGC_planted_forest_non_mangrove_dir,
    cn.cumul_gain_AGC_natrl_forest_dir,
    cn.loss_dir
    ]

# for input in input_files:
#     uu.s3_folder_download('{}'.format(input), '.')

# For copying individual tiles to spot machine for testing.
for tile in tile_list:

    try:
        uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.mangrove_biomass_2000_dir, tile, cn.pattern_mangrove_biomass_2000), '.')
    except:
        print "No mangrove biomass in", tile

    try:
        uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.WHRC_biomass_2000_unmasked_dir, tile, cn.pattern_WHRC_biomass_2000_unmasked), '.')
    except:
        print "No WHRC biomass in", tile

    try:
        uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.cumul_gain_AGC_mangrove_dir, tile, cn.pattern_cumul_gain_AGC_mangrove), '.')
    except:
        print "No mangrove carbon accumulation in", tile

    try:
        uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.cumul_gain_AGC_planted_forest_non_mangrove_dir, tile, cn.pattern_cumul_gain_AGC_planted_forest_non_mangrove), '.')
    except:
        print "No planted forests in", tile

    try:
        uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.cumul_gain_AGC_natrl_forest_dir, tile, cn.pattern_cumul_gain_AGC_natrl_forest), '.')
    except:
        print "No non-mangrove non-planted forests in", tile

    try:
        uu.s3_file_download('{0}{1}.tif'.format(cn.loss_dir, tile), '.')
    except:
        print "No loss in", tile

print "Creating tiles of emitted aboveground carbon (carbon 2000 + carbon accumulation until loss year)"

count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(processes=count/3)
pool.map(create_aboveground_carbon_in_emis_year.create_emitted_AGC, tile_list)

# # For single processor use
# for tile in tile_list:
#     create_aboveground_carbon_in_emis_year.create_emitted_AGC(tile)

uu.upload_final_set(cn.AGC_emis_year_dir, cn.pattern_AGC_emis_year)
