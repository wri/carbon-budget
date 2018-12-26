### Creates Hansen-style tiles for aboveground mangrove biomass (Mg/ha) from Lola Fatoyinbo's country
### mangrove data.
### Output tiles conform to the dimensions, resolution, and other properties of Hansen loss tiles.

import multiprocessing
import mangrove_processing
import sys
sys.path.append('../')
import constants_and_names
import universal_util

# # Downloads zipped raw mangrove files
# # NOTE: for some reason, during the unzip process, it asks if a few files need to be replaced, so you have to monitor
# # the unzipping and intervene or it'll never finish.
# universal_util.s3_file_download(os.path.join(constants_and_names.mangrove_biomass_raw_dir, constants_and_names.mangrove_biomass_raw_file), '.')
#
# # Unzips mangrove images into a flat structure (all tifs into main folder using -j argument)
# # NOTE: Unzipping the Australia tif takes a very long time, so don't worry if the script appears to freeze on that.
# cmd = ['unzip', '-j', constants_and_names.mangrove_biomass_raw_file]
# subprocess.check_call(cmd)
#
# # Creates vrt of all raw mangrove tifs
# utilities.build_vrt(utilities.mangrove_vrt)

# Biomass tiles to iterate through
biomass_tile_list = universal_util.tile_list(constants_and_names.biomass_dir)
# biomass_tile_list = ['00N_000E', '20S_120W', '00N_120E'] # test tile
# biomass_tile_list = biomass_tile_list[62:]
print biomass_tile_list

# For multiprocessor use
# This script didn't work with count/4; perhaps that was using too many processors.
# It did work with count/5, though.
count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(processes=count/5)
pool.map(mangrove_processing.create_mangrove_tiles, biomass_tile_list)

# # For single processor use, for testing purposes
# for tile in biomass_tile_list:
#
#     mangrove_processing.create_mangrove_tiles(tile)