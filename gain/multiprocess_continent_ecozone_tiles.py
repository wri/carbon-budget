
import multiprocessing
import utilities
import continent_ecozone_tiles
import subprocess


# cont_ecozone_dir = 's3://gfw2-data/climate/carbon_model/fao_ecozones/'
# cont_ecozone = 'fao_ecozones_fra_2000_continents_assigned_dissolved_FINAL_20180906.zip'
#
# utilities.s3_file_download('{0}{1}'.format(cont_ecozone_dir, cont_ecozone), '.', )
#
# cmd = ['unzip', cont_ecozone]
# subprocess.check_call(cmd)


# Location of the carbon pools, used for tile boundaries
biomass_dir = 's3://WHRC-carbon/WHRC_V4/Processed/'

# biomass_tile_list = utilities.tile_list(biomass_dir)
# biomass_tile_list = ["00N_000E", "00N_050W", "00N_060W", "00N_010E", "00N_020E", "00N_030E", "00N_040E", "10N_000E", "10N_010E", "10N_010W", "10N_020E", "10N_020W"] # test tiles
biomass_tile_list = ['00N_050W'] # test tile
print biomass_tile_list

# # For downloading all tiles
# utilities.s3_folder_download('{}'.format(biomass_dir), '.')

# # For copying individual tiles to s3 for testing
# for tile in biomass_tile_list:
#
#     print tile
#     utilities.s3_file_download('{0}{1}_biomass.tif'.format(biomass_dir, tile), '.')

# count = multiprocessing.cpu_count()
# pool = multiprocessing.Pool(processes=count/4)
# pool.map(continent_ecozone_tiles.create_continent_ecozone_tiles, carbon_tile_list)

# For single processor use
for tile in biomass_tile_list:

    continent_ecozone_tiles.create_continent_ecozone_tiles(tile)