### Creates tiles in which each mangrove pixel is the number of years that trees are believed to have been growing there between 2001 and 2015.
### It is based on the annual Hansen loss data and the 2000-2012 Hansen gain data (as well as the 2000 tree cover density data).
### First it calculates rasters of gain years for mangrove pixels that had loss only, gain only, neither loss nor gain, and both loss and gain.
### The gain years for each of these conditions are calculated according to rules that are found in the function called by the multiprocessor commands.
### More gdalcalc commands can be run at the same time than gdalmerge so that's why the number of processors being used is higher
### for the first four processing steps (which use gdalcalc).
### At this point, those rules are the same as for non-mangrove natural forests, except that no change pixels don't use tcd as a factor.
### Then it combines those four rasters into a single gain year raster for each tile.
### This is one of the mangrove inputs for the carbon gain model.
### If different input rasters for loss (e.g., 2001-2017) and gain (e.g., 2000-2018) are used, the year count constants in constants_and_names.py must be changed.

import multiprocessing
import gain_year_count_mangrove
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Lists the tiles that have both mangrove biomass and FAO ecozone information because both of these are necessary for
# calculating mangrove gain
mangrove_biomass_tile_list = uu.tile_list(cn.mangrove_biomass_2000_dir)
ecozone_tile_list = uu.tile_list(cn.cont_eco_dir)
mangrove_ecozone_list = list(set(mangrove_biomass_tile_list).intersection(ecozone_tile_list))
# mangrove_ecozone_list = ['20S_110E', '30S_110E'] # test tiles
# mangrove_ecozone_list = ['10N_080W'] # test tiles
print mangrove_ecozone_list
print "There are {} tiles to process".format(str(len(mangrove_ecozone_list)))

# For downloading all tiles in the input folders
download_list = [cn.loss_dir, cn.gain_dir, cn.mangrove_biomass_2000_dir]

for input in download_list:
    uu.s3_folder_download(input, '.')

# # For copying individual tiles to s3 for testing
# for tile in mangrove_ecozone_list:
#
#     uu.s3_file_download('{0}{1}.tif'.format(cn.loss_dir, tile), '.')
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.gain_dir, tile, cn.pattern_gain), '.')
#     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.mangrove_biomass_2000_dir, tile, cn.pattern_mangrove_biomass_2000), '.')

# Creates gain year count tiles using only pixels that had only loss. Worked on a r4.16xlarge machine.
count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(count/3)
pool.map(gain_year_count_mangrove.create_gain_year_count_loss_only, mangrove_ecozone_list)

# Creates gain year count tiles using only pixels that had only gain
pool.map(gain_year_count_mangrove.create_gain_year_count_gain_only, mangrove_ecozone_list)

# Creates gain year count tiles using only pixels that had neither loss nor gain pixels
pool.map(gain_year_count_mangrove.create_gain_year_count_no_change, mangrove_ecozone_list)

# Creates gain year count tiles using only pixels that had both loss and gain pixels
pool.map(gain_year_count_mangrove.create_gain_year_count_loss_and_gain, mangrove_ecozone_list)

# Merges the four above gain year count tiles for each Hansen tile into a single output tile.
# Using a r4.16xlarge machine, calling one sixth of the processors uses just about all the memory without going over
# (e.g., about 450 GB out of 480 GB).
count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(count/6)
pool.map(gain_year_count_mangrove.create_gain_year_count_merge, mangrove_ecozone_list)
pool.close()
pool.join()

# # For single processor use
# for tile in mangrove_ecozone_list:
#     gain_year_count_mangrove.create_gain_year_count_loss_only(tile)
#
# for tile in mangrove_ecozone_list:
#     gain_year_count_mangrove.create_gain_year_count_gain_only(tile)
#
# for tile in mangrove_ecozone_list:
#     gain_year_count_mangrove.create_gain_year_count_no_change(tile)
#
# for tile in mangrove_ecozone_list:
#     gain_year_count_mangrove.create_gain_year_count_loss_and_gain(tile)
#
# for tile in mangrove_ecozone_list:
#     gain_year_count_mangrove.create_gain_year_count_merge(tile)

# Intermediate output tiles for checking outputs
uu.upload_final_set(cn.gain_year_count_mangrove_dir, "growth_years_loss_only")
uu.upload_final_set(cn.gain_year_count_mangrove_dir, "growth_years_gain_only")
uu.upload_final_set(cn.gain_year_count_mangrove_dir, "growth_years_no_change")
uu.upload_final_set(cn.gain_year_count_mangrove_dir, "growth_years_loss_and_gain")

# This is the final output used later in the model
uu.upload_final_set(cn.gain_year_count_mangrove_dir, cn.pattern_gain_year_count_mangrove)