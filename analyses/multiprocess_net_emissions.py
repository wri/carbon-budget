### Calculates the net emissions over the study period, with units of CO2/ha on a pixel-by-pixel basis

import multiprocessing
import utilities
import net_emissions
import sys
sys.path.append('../')
import constants_and_names

### Need to update and install some packages on spot machine before running
### sudo pip install rasterio --upgrade

biomass_tile_list = utilities.tile_list(constants_and_names.biomass_dir)
# biomass_tile_list = ['10N_080W', '40N_120E'] # test tiles
# biomass_tile_list = ['40N_120E'] # test tiles
print biomass_tile_list

# For downloading all tiles in the input folders
download_list = [constants_and_names.cumul_gain_combo_dir, constants_and_names.emissions_total_dir]

for input in download_list:
    utilities.s3_folder_download('{}'.format(input), '.')

# # For copying individual tiles to spot machine for testing
# for tile in biomass_tile_list:
#
#     utilities.s3_file_download('{0}{1}_{2}.tif'.format(constants_and_names.cumul_gain_combo_dir, constants_and_names.pattern_cumul_gain_combo, tile), '.')  # cumulative aboveand belowground carbon gain for all forest types
#     utilities.s3_file_download('{0}{1}_{2}.tif'.format(constants_and_names.emissions_total_dir, tile, constants_and_names.pattern_emissions_total), '.')  # emissions from all drivers

count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(count / 4)
pool.map(net_emissions.net_calc, biomass_tile_list)

# # For single processor use
# for tile in biomass_tile_list:
#
#     net_emissions.net_calc(tile)