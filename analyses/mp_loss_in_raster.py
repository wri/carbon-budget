### Creates rasters of loss in some other raster of interest. This can then be put through the tile statistics script to
### get the pixel count and total loss area in the raster of interest in each tile.
### This script has two arguments: the directory for the raster of interest (required), and the latitude above which
### tiles will be processed (optional).

from multiprocessing.pool import Pool
from functools import partial
import argparse
import os
import loss_in_raster
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

tile_list = uu.tile_list(cn.loss_dir)
# tile_list = ['00N_110E'] # test tiles
tile_list = ['00N_110E', '70N_100W', '70N_140E'] # test tiles: no mangrove or planted forest, mangrove only, planted forest only, mangrove and planted forest
print tile_list
print "There are {} tiles to process".format(str(len(tile_list)))

parser = argparse.ArgumentParser(description='Create rasters of loss masked by some other raster')
parser.add_argument('--raster-of-interest', '-r', required=True,
                    help='one raster in the s3 directory of the raster that loss will be masked by.')
parser.add_argument('--latitude-constraint', '-l', required=False,
                    help='Enter a latitude in the format of 20, 0, -30, etc. Only tiles north of that will be evaluated. For example, entering 20 means that the tiles with a southern edge of 20 will be processed.')
args = parser.parse_args()

# Gets the path, full name, and type of the raster that loss is being considered in.
args_index = os.path.split(args.raster_of_interest)
raster_path = args_index[0]
raster_name = args_index[1]
raster_type = raster_name[8:-4]

print args_index
print raster_path
print raster_type

# The name of the output rasters-- a combination of loss and the type of the raster of interest
output_name = 'loss_in_{}'.format(raster_type)

# The latitude above which loss will be analyzed
lat = args.latitude_constraint

# For downloading all tiles in the input folders
download_list = [cn.loss_dir, '{}/'.format(raster_path)]

# for input in download_list:
#     uu.s3_folder_download('{}'.format(input), '.')

# For copying individual tiles to spot machine for testing
for tile in tile_list:

    uu.s3_file_download('{0}{1}.tif'.format(cn.loss_dir, tile), '.')  # loss tiles
    uu.s3_file_download('{0}{1}_{2}.tif'.format(raster_path, tile, raster_type), '.')  # raster of interest

num_of_processes = 14
pool = Pool(num_of_processes)
pool.map(partial(loss_in_raster.loss_in_raster, raster_type=raster_type, output_name=output_name, lat=lat), tile_list)
pool.close()
pool.join()

# # For single processor use
# for tile in tile_list:
#
#     loss_in_raster.loss_in_raster(tile, raster_type)

print "Tiles processed. Uploading to s3 now..."

# Uploads all output tiles to s3
uu.upload_final_set('s3://gfw2-data/climate/carbon_model/loss_in_peat/20190917/', output_name)