
import multiprocessing
import peatland_processing
import sys
import os
import subprocess
import utilities
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Iterates through all possible tiles (not just WHRC biomass tiles) to create mangrove biomass tiles that don't have analogous WHRC tiles
# tile_list = uu.create_combined_tile_list(cn.WHRC_biomass_2000_unmasked_dir, cn.mangrove_biomass_2000_dir)
tile_list = ['00N_110E'] # test tiles
# tile_list = ['80N_020E', '00N_020E', '30N_080W', '00N_110E'] # test tiles
print tile_list

# # Downloads peat layers
# uu.s3_file_download(os.path.join(cn.peat_unprocessed_dir, cn.cifor_peat_file), '.')
# uu.s3_file_download(os.path.join(cn.peat_unprocessed_dir, cn.jukka_peat_zip), '.')
# uu.s3_file_download(os.path.join(cn.peat_unprocessed_dir, cn.soilgrids250_peat_file), '.')
#
# # Unzips the Jukka peat shapefile
# cmd = ['unzip', '-j', cn.jukka_peat_zip]
# subprocess.check_call(cmd)

jukka_tif = 'jukka_peat.tif'

# cmd= ['gdal_rasterize', '-burn', '1', '-co', 'COMPRESS=LZW', '-tr', '{}'.format(cn.Hansen_res), '{}'.format(cn.Hansen_res),
#       '-tap', '-ot', 'Byte', '-a_nodata', '0', cn.jukka_peat_shp, jukka_tif]
#
# subprocess.check_call(cmd)

os.system('gdalbuildvrt -r maximum tropic_peat.vrt {0} {1}'.format(jukka_tif, cn.cifor_peat_file))

# # For multiprocessor use
# # This script worked with count/4 on an r3.16xlarge machine.
# count = multiprocessing.cpu_count()
# pool = multiprocessing.Pool(processes=count/4)
# pool.map(peatland_processing.create_peat_mask_tiles, tile_list)

# For single processor use, for testing purposes
for tile in tile_list:

    peatland_processing.create_peat_mask_tiles(tile)


print "Uploading output files"
uu.upload_final_set(cn.peat_mask_dir, cn.pattern_peat_mask)