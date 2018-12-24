###

import multiprocessing
import utilities
import tile_statistics
import subprocess
from functools import partial
import sys
sys.path.append('../')
import constants_and_names

# Creates list of tiles to iterate through
# mangrove_biomass_tile_list = utilities.tile_list(constants_and_names.mangrove_biomass_dir)
tile_list = ["00N_000E", "00N_050W"] # test tiles
print tile_list

# # For downloading all tiles in the folders
# download_list = [constants_and_names.mangrove_biomass_dir]
#
# for input in download_list:
#     utilities.s3_folder_download('{}'.format(input), '.')

# For copying individual tiles to spot machine for testing
for tile in tile_list:
    utilities.s3_file_download('{0}{1}_{2}.tif'.format(constants_and_names.mangrove_biomass_dir, constants_and_names.pattern_mangrove_biomass, tile), '.')      # mangrove biomass tiles
    utilities.s3_file_download('{0}{1}_{2}.tif'.format(constants_and_names.biomass_dir, tile, constants_and_names.pattern_biomass), '.')


# The column names for the tile summary statistics.
# If the statistics calculations are changed in tile_statistics.py, the list here needs to be changed, too.
headers = ['tile_id', 'tile_name', 'pixel_count', 'mean', 'median', 'percentile10', 'percentile25',
           'percentile75', 'percentile90', 'min', 'max']
header_no_brackets = ', '.join(headers)

# Creates the output text file with the column names
with open(constants_and_names.tile_stats, 'w+') as f:
    f.write(header_no_brackets  +'\r\n')
f.close()

# For multiprocessor use
count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(processes=count/4)
pool.map(tile_statistics.create_tile_statistics, tile_list)

# # For single processor use
# for tile in mangrove_biomass_tile_list:
#     tile_statistics.create_tile_statistics(tile)

# Copies the text file to the location on s3 that the tiles are from
cmd = ['aws', 's3', 'cp', constants_and_names.tile_stats, constants_and_names.tile_stats_dir]
subprocess.check_call(cmd)