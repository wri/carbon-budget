### Creates Hansen-style tiles for aboveground mangrove biomass (Mg/ha) from Lola Fatoyinbo's country
### mangrove data.
### Output tiles conform to the dimensions, resolution, and other properties of Hansen loss tiles.

import multiprocessing
import utilities
import mangrove_processing
import subprocess
import os

# # Downloads zipped raw mangrove files
# utilities.s3_file_download(os.path.join(utilities.mangrove_raw_dir, utilities.mangrove_raw), '.')
#
# # Unzips mangrove images into a flat structure (all tifs into main folder using -j argument)
# cmd = ['unzip', '-j', utilities.mangrove_raw]
# subprocess.check_call(cmd)
#
# # Creates vrt of all raw mangrove tifs
# utilities.build_vrt(utilities.mangrove_vrt)

# Location of the biomass tiles, used for getting output tile boundaries
biomass_dir = 's3://gfw2-data/climate/WHRC_biomass/WHRC_V4/Processed/'

# Biomass tiles to iterate through
biomass_tile_list = utilities.tile_list(biomass_dir)
# biomass_tile_list = ['20S_110E'] # test tile
# biomass_tile_list = ['20N_080W'] # test tile
print biomass_tile_list

# For multiprocessor use
count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(processes=count/4)
pool.map(mangrove_processing.create_mangrove_tiles, biomass_tile_list)

# # For single processor use, for testing purposes
# for tile in biomass_tile_list:
#
#     mangrove_processing.create_mangrove_tiles(tile)