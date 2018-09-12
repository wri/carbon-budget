### Creates tiles in which each pixel is the number of years that trees are believed to have been growing there between 2001 and 2015.
### It is based on the annual Hansen loss data and the 2000-2012 Hansen gain data (as well as the 2000 tree cover density data).
### First it calculates rasters of gain years for pixels that had loss only, gain only, neither loss nor gain, and both loss and gain.
### The gain years for each of these conditions are calculated according to rules that are found in the function called by the multiprocessor command.
### Then it combines those four rasters into a single gain year raster for each tile.
### This is one of the inputs for the carbon gain model.
### If different input rasters for loss (e.g., 2001-2017) and gain (e.g., 2000-2018) are used, the constants in create_gain_year_count.py must be changed.



import multiprocessing
import utilities
import continent_ecozone_tiles
import subprocess

# Ecozone shapefile location and file
cont_ecozone_dir = 's3://gfw2-data/climate/carbon_model/fao_ecozones/'
cont_ecozone = 'fao_ecozones_fra_2000_continents_assigned_dissolved_FINAL_20180906.zip'

# Downloads ecozone shapefile
utilities.s3_file_download('{0}{1}'.format(cont_ecozone_dir, cont_ecozone), '.', )

# Unzips ecozone shapefile
cmd = ['unzip', cont_ecozone]
subprocess.check_call(cmd)

# Location of the carbon pools, used for tile boundaries
biomass_dir = 's3://WHRC-carbon/WHRC_V4/Processed/'

biomass_tile_list = utilities.tile_list(biomass_dir)
# biomass_tile_list = ["00N_000E", "00N_050W", "00N_060W", "00N_010E", "00N_020E", "00N_030E", "00N_040E", "10N_000E", "10N_010E", "10N_010W", "10N_020E", "10N_020W"] # test tiles
# biomass_tile_list = ['00N_050W'] # test tile
print biomass_tile_list

# For multiprocessor use
count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(processes=count/3)
pool.map(continent_ecozone_tiles.create_continent_ecozone_tiles, biomass_tile_list)

# # For single processor use
# for tile in biomass_tile_list:
#
#     continent_ecozone_tiles.create_continent_ecozone_tiles(tile)