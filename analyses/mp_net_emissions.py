### Calculates the net emissions over the study period, with units of CO2/ha on a pixel-by-pixel basis

import multiprocessing
import utilities
import net_emissions
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# biomass_tile_list = uu.tile_list(cn.natrl_forest_biomass_2000_dir)
# # biomass_tile_list = ['10N_080W', '40N_120E'] # test tiles
# # biomass_tile_list = ['00N_000E'] # test tiles
# print biomass_tile_list
# print "There are {} tiles to process".format(str(len(biomass_tile_list)))
#
# # For downloading all tiles in the input folders
# download_list = [cn.cumul_gain_combo_dir, cn.gross_emissions_dir]
#
# for input in download_list:
#     utilities.s3_folder_download('{}'.format(input), '.')
#
# # # For copying individual tiles to spot machine for testing
# # for tile in biomass_tile_list:
# #
# #     utilities.s3_file_download('{0}{1}_{2}.tif'.format(cn.cumul_gain_combo_dir, cn.pattern_cumul_gain_combo, tile), '.')  # cumulative aboveand belowground carbon gain for all forest types
# #     utilities.s3_file_download('{0}{1}_{2}.tif'.format(cn.gross_emissions_dir, tile, cn.pattern_gross_emissions), '.')  # emissions from all drivers
#
# count = multiprocessing.cpu_count()
# pool = multiprocessing.Pool(count / 4)
# pool.map(net_emissions.net_calc, biomass_tile_list)

# # For single processor use
# for tile in biomass_tile_list:
#
#     net_emissions.net_calc(tile)

print "Tiles processed. Uploading to s3 now..."

# Uploads all output tiles to s3
uu.upload_final_set(cn.net_flux_dir, cn.pattern_net_flux)