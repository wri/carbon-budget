###

import multiprocessing
import utilities
import forest_age_category

# Location of the carbon pools
carbon_pool_dir = 's3://gfw2-data/climate/carbon_model/carbon_pools/20180815'

# Loss, gain, and tree cover density, intact forest landscape, and biomass tiles
# All of these are needed for the forest age decision tree
loss = 's3://gfw2-data/forest_change/hansen_2015/Loss_tiles/'
gain = 's3://gfw2-data/forest_change/tree_cover_gain/gaindata_2012/'
tcd = 's3://gfw2-data/forest_cover/2000_treecover/'
ifl = 's3://gfw2-data/climate/carbon_model/other_emissions_inputs/ifl_2000/'
biomass = 's3://WHRC-carbon/WHRC_V4/Processed/'

carbon_tile_list = utilities.tile_list('{}/carbon/'.format(carbon_pool_dir))
# carbon_tile_list = ["00N_000E", "00N_050W", "00N_060W", "00N_010E", "00N_020E", "00N_030E", "00N_040E", "10N_000E", "10N_010E", "10N_010W", "10N_020E", "10N_020W"] # test tiles
carbon_tile_list = ['00N_050W'] # test tile
print carbon_tile_list

# download_list = [loss, gain, tcd, ifl, biomass]
#
# # For downloading all tiles in the folders
# for input in download_list:
#     utilities.s3_folder_download('{}'.format(input), '.')

# For copying individual tiles to s3 for testing
for tile in carbon_tile_list:

    utilities.s3_file_download('{0}{1}.tif'.format(loss, tile), '.')
    utilities.s3_file_download('{0}Hansen_GFC2015_gain_{1}.tif'.format(gain, tile), '.')
    utilities.s3_file_download('{0}Hansen_GFC2014_treecover2000_{1}.tif'.format(tcd, tile), '.')
    utilities.s3_file_download('{0}{1}_res_ifl_2000tif'.format(ifl, tile), '.')
    utilities.s3_file_download('{0}{1}_biomass.tif'.format(biomass, tile), '.')

# count = multiprocessing.cpu_count()
# pool = multiprocessing.Pool(processes=6)
# pool.map(forest_age_category.forest_age_category, carbon_tile_list)

# For single processor use
for tile in carbon_tile_list:

    forest_age_category.forest_age_category(tile)

