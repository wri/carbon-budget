import multiprocessing
import utilities
import create_gain_year_count

# Location of the carbon pools
carbon_pool_dir = 's3://gfw2-data/climate/carbon_model/carbon_pools/20180815'

# Loss, gain, and tree cover density tiles
loss = 's3://gfw2-data/forest_change/hansen_2015/Loss_tiles/'
gain = 's3://gfw2-data/forest_change/tree_cover_gain/gaindata_2012/'
tcd = 's3://gfw2-data/forest_cover/2000_treecover/'

carbon_tile_list = utilities.tile_list('{}/carbon/'.format(carbon_pool_dir))
# carbon_tile_list = ['00N_050W'] # test tile
print carbon_tile_list

# utilities.s3_download('{}'.format(loss), '.')
# utilities.s3_download('{}'.format(gain), '.')
# utilities.s3_download('{}'.format(tcd), '.')

# utilities.s3_download('{}00N_050W.tif'.format(loss), '.')
# utilities.s3_download('{}Hansen_GFC2015_gain_00N_050W.tif'.format(gain), '.')
# utilities.s3_download('{}Hansen_GFC2014_treecover2000_00N_050W.tif'.format(tcd), '.')

if __name__ == '__main__':

    count = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=count / 8)
    pool.map(create_gain_year_count.create_gain_year_count, carbon_tile_list)

for tile in carbon_tile_list:

    create_gain_year_count.create_gain_year_count(tile)