### Calculates the net emissions over the study period, with units of CO2/ha on a pixel-by-pixel basis

import multiprocessing
import net_flux
from functools import partial
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

tile_list = uu.create_combined_tile_list(cn.gross_emis_all_gases_all_drivers_dir, cn.cumul_gain_AGCO2_BGCO2_all_types_dir)
# tile_list = ['00N_110E'] # test tiles
# tile_list = ['00N_110E', '80N_020E', '30N_080W', '00N_020E'] # test tiles: no mangrove or planted forest, mangrove only, planted forest only, mangrove and planted forest
print tile_list
print "There are {} tiles to process".format(str(len(tile_list)))

# For downloading all tiles in the input folders
download_list = [cn.cumul_gain_AGCO2_BGCO2_all_types_dir, cn.gross_emis_all_gases_all_drivers_dir]

for input in download_list:
    uu.s3_folder_download('{}'.format(input), '.')

# # For copying individual tiles to spot machine for testing
# for tile in tile_list:
#
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.cumul_gain_AGCO2_BGCO2_all_types_dir, tile, cn.pattern_cumul_gain_AGCO2_BGCO2_all_types), '.')  # cumulative aboveand belowground carbon gain for all forest types
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.gross_emis_all_gases_all_drivers_dir, tile, cn.pattern_gross_emis_all_gases_all_drivers), '.')  # emissions from all drivers


# Since the input tile lists have different numbers of tiles, at least one input will need to have some blank tiles made
# so that it has all the necessary input tiles
# The inputs that might need to have dummy tiles made in order to match the tile list of the carbon pools
folder = './'
pattern_list = [cn.pattern_cumul_gain_AGCO2_BGCO2_all_types, cn.pattern_gross_emis_all_gases_all_drivers]

for pattern in pattern_list:
    count = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(count-10)
    pool.map(partial(uu.make_blank_tile, pattern=pattern, folder=folder), tile_list)
    pool.close()
    pool.join()

# Count/3 uses about 380 GB on a r4.16xlarge spot machine
# processes/24 maxes out at about 435 GB on an r4.16xlarge spot machine
count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(processes=24)
pool.map(net_flux.net_calc, tile_list)

# # For single processor use
# for tile in tile_list:
#
#     net_flux.net_calc(tile)

print "Tiles processed. Uploading to s3 now..."

# Uploads all output tiles to s3
uu.upload_final_set(cn.net_flux_dir, cn.pattern_net_flux)