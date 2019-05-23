### Calculates the net emissions over the study period, with units of CO2/ha on a pixel-by-pixel basis

import multiprocessing
import net_flux
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

tile_list = uu.create_combined_tile_list(cn.gross_emis_all_drivers_dir, cn.cumul_gain_combo_dir)
# biomass_tile_list = ['10N_080W', '40N_120E'] # test tiles
# biomass_tile_list = ['00N_000E'] # test tiles
print tile_list
print "There are {} tiles to process".format(str(len(tile_list)))

# For downloading all tiles in the input folders
download_list = [cn.cumul_gain_combo_dir, cn.gross_emis_all_drivers_dir]

# for input in download_list:
#     uu.s3_folder_download('{}'.format(input), '.')

# For copying individual tiles to spot machine for testing
for tile in tile_list:

    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.cumul_gain_combo_dir, cn.pattern_cumul_gain_combo, tile), '.')  # cumulative aboveand belowground carbon gain for all forest types
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.gross_emis_all_drivers_dir, tile, cn.pattern_gross_emis_all_drivers), '.')  # emissions from all drivers

# count = multiprocessing.cpu_count()
# pool = multiprocessing.Pool(count / 4)
# pool.map(net_flux.net_calc, tile_list)

# For single processor use
for tile in tile_list:

    net_flux.net_calc(tile)

print "Tiles processed. Uploading to s3 now..."

# Uploads all output tiles to s3
uu.upload_final_set(cn.net_flux_dir, cn.pattern_net_flux)