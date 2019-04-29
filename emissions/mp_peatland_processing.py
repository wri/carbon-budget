
import multiprocessing
import peatland_processing
import sys
import os
import subprocess
import utilities
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Iterates through all tiles with aboveground carbon pool emissions (not just WHRC biomass tiles)
tile_list = uu.tile_list(cn.AGC_emis_year_dir)
# tile_list = ['60N_020E', '70N_070E'] # test tiles
# tile_list = ['60N_020E', '00N_020E', '30N_080W', '00N_110E'] # test tiles
print tile_list
print "There are {} tiles to process".format(str(len(tile_list)))

# Downloads peat layers
uu.s3_file_download(os.path.join(cn.peat_unprocessed_dir, cn.cifor_peat_file), '.')
uu.s3_file_download(os.path.join(cn.peat_unprocessed_dir, cn.jukka_peat_zip), '.')
uu.s3_file_download(os.path.join(cn.peat_unprocessed_dir, cn.soilgrids250_peat_file), '.')

# Unzips the Jukka peat shapefile
cmd = ['unzip', '-j', cn.jukka_peat_zip]
subprocess.check_call(cmd)

jukka_tif = 'jukka_peat.tif'

cmd= ['gdal_rasterize', '-burn', '1', '-co', 'COMPRESS=LZW', '-tr', '{}'.format(cn.Hansen_res), '{}'.format(cn.Hansen_res),
      '-tap', '-ot', 'Byte', '-a_nodata', '0', cn.jukka_peat_shp, jukka_tif]

subprocess.check_call(cmd)

# For multiprocessor use
# This script worked with count/4 on an r4.16xlarge machine.
count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(processes=count/2)
pool.map(peatland_processing.create_peat_mask_tiles, tile_list)

# # For single processor use, for testing purposes
# for tile in tile_list:
#
#     peatland_processing.create_peat_mask_tiles(tile)

print "Uploading output files"
uu.upload_final_set(cn.peat_mask_dir, '{}'.format(cn.pattern_peat_mask))