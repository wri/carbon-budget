'''
This script processes the inputs for the emissions script that haven't been processed by another script.
At this point, that is: climate zone, Indonesia/Malaysia plantations before 2000, tree cover loss drivers (TSC drivers),
and combining IFL2000 (extratropics) and primary forests (tropics) into a single layer.
'''

import multiprocessing
import subprocess
import sys
import os
import prep_other_inputs

sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

tile_list = uu.create_combined_tile_list(cn.WHRC_biomass_2000_unmasked_dir,
                                         cn.mangrove_biomass_2000_dir,
                                         set3=cn.annual_gain_AGC_BGC_planted_forest_unmasked_dir
                                         )
tile_list = ['00N_060W'] # test tiles
# tile_list = ['80N_020E', '30N_080W', '00N_020E', '00N_110E'] # test tiles: no mangrove or planted forest, mangrove only, planted forest only, mangrove and planted forest
print tile_list
print "There are {} unique tiles to process".format(str(len(tile_list)))

# # Files to process: climate zone, IDN/MYS plantations before 2000, tree cover loss drivers, combine IFL and primary forest
# uu.s3_file_download(os.path.join(cn.climate_zone_raw_dir, cn.climate_zone_raw), '.')
# uu.s3_file_download(os.path.join(cn.plant_pre_2000_raw_dir, '{}.zip'.format(cn.pattern_plant_pre_2000_raw)), '.')
# uu.s3_file_download(os.path.join(cn.drivers_raw_dir, '{}.zip'.format(cn.pattern_drivers_raw)), '.')
# uu.s3_folder_download(cn.primary_raw_dir, '.')
# uu.s3_folder_download(cn.ifl_dir, '.')
#
# cmd = ['unzip', '-j', '{}.zip'.format(cn.pattern_plant_pre_2000_raw)]
# subprocess.check_call(cmd)
#
# cmd = ['unzip', '-j', '{}.zip'.format(cn.pattern_drivers_raw)]
# subprocess.check_call(cmd)
#
# # Converts the IDN/MYS pre-2000 plantation shp to a raster
# cmd= ['gdal_rasterize', '-burn', '1', '-co', 'COMPRESS=LZW', '-tr', '{}'.format(cn.Hansen_res), '{}'.format(cn.Hansen_res),
#       '-tap', '-ot', 'Byte', '-a_nodata', '0',
#       '{}.shp'.format(cn.pattern_plant_pre_2000_raw), '{}.tif'.format(cn.pattern_plant_pre_2000_raw)]
# subprocess.check_call(cmd)
#
# # Used about 250 GB of memory. count-7 worked fine (with memory to spare) on an r4.16xlarge machine.
# count = multiprocessing.cpu_count()
# pool = multiprocessing.Pool(count-7)
# pool.map(prep_other_inputs.data_prep, tile_list)

# # For single processor use
# for tile in tile_list:
#
#       prep_other_inputs.data_prep(tile)

primary_vrt = 'primary_2001.vrt'
os.system('gdalbuildvrt {} *2001_primary.tif'.format(primary_vrt))

ifl_vrt = 'ifl_2000.vrt'
os.system('gdalbuildvrt {} *res_ifl_2000.tif'.format(ifl_vrt))

combined_vrt = 'ifl_primary.vrt'
cmd = ['gdal_merge.py', '-init', '0', '-co', 'COMPRESS=LZW', '-ot', 'Byte', '-v',
       '-o', combined_vrt, ifl_vrt, primary_vrt]
subprocess.check_call(cmd)

# # Used about 250 GB of memory. count-7 worked fine (with memory to spare) on an r4.16xlarge machine.
# count = multiprocessing.cpu_count()
# pool = multiprocessing.Pool(count-7)
# pool.map(prep_other_inputs.merge_ifl_primary, tile_list)

# For single processor use
for tile in tile_list:

      prep_other_inputs.merge_ifl_primary(tile)

# uu.upload_final_set(cn.climate_zone_processed_dir, cn.pattern_climate_zone)
# uu.upload_final_set(cn.plant_pre_2000_processed_dir, cn.pattern_plant_pre_2000)
# uu.upload_final_set(cn.drivers_processed_dir, cn.pattern_drivers)
uu.upload_final_set(cn.ifl_primary_processed_dir, cn.pattern_ifl_primary)