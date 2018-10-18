###

import multiprocessing
import utilities
import mangrove_processing
import subprocess
import os
import shutil
import string

mangrove_raw_dir = 's3://gfw2-data/climate/carbon_model/mangrove_biomass/raw_from_Lola_Fatoyinbo_20180911/'
mangrove_raw = 'MaskedSRTMCountriesAGB_WRI.zip'



# utilities.s3_file_download(os.path.join(mangrove_raw_dir, mangrove_raw), '.')
#
# # Unzips ecozone shapefile
# cmd = ['unzip', '-j', mangrove_raw]
# subprocess.check_call(cmd)

utilities.build_vrt(utilities.mangrove_vrt)

# Location of the biomass tiles, used for getting tile boundaries
biomass_dir = 's3://gfw2-data/climate/WHRC_biomass/WHRC_V4/Processed/'

# biomass_tile_list = utilities.tile_list(biomass_dir)
# biomass_tile_list = ["00N_000E", "00N_050W", "00N_060W", "00N_010E", "00N_020E", "00N_030E", "00N_040E", "10N_000E", "10N_010E", "10N_010W", "10N_020E", "10N_020W"] # test tiles
# biomass_tile_list = ['20S_110E'] # test tile
biomass_tile_list = ['20N_050W'] # test tile
print biomass_tile_list

# # For multiprocessor use
# count = multiprocessing.cpu_count()
# pool = multiprocessing.Pool(processes=count/4)
# pool.map(mangrove_processing.create_mangrove_tiles, biomass_tile_list)

# For single processor use
for tile in biomass_tile_list:

    mangrove_processing.create_mangrove_tiles(tile)