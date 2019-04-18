import subprocess
import create_inputs_for_C_pools
import multiprocessing
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

tile_list = uu.create_combined_tile_list(cn.WHRC_biomass_2000_non_mang_non_planted_dir,
                                         cn.annual_gain_AGB_mangrove_dir,
                                         set3=cn.annual_gain_AGB_planted_forest_non_mangrove_dir
                                         )
# tile_list = ['30N_080W'] # test tiles
# tile_list = ['80N_020E', '00N_020E', '30N_080W', '00N_110E'] # test tiles
print tile_list

# Downloads two of the raw input files for creating carbon pools
input_files = [
    cn.fao_ecozone_raw_dir,
    cn.precip_raw_dir
    ]

for input in input_files:
    uu.s3_file_download('{}'.format(input), '.')

print "Unzipping FAO ecozones"
unzip_zones = ['unzip', '{}'.format(cn.pattern_fao_ecozone_raw), '-d', '.']
subprocess.check_call(unzip_zones)

print "Copying elevation (srtm) files"
uu.s3_folder_download(cn.srtm_raw_dir, './srtm')

print "Making elevation (srtm) vrt"
subprocess.check_call('gdalbuildvrt srtm.vrt srtm/*.tif', shell=True)

count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(processes=count / 3)
pool.map(create_inputs_for_C_pools.create_input_files, tile_list)

# # For single processor use
# for tile in tile_list:
#
#     create_inputs_for_C_pools.create_input_files(tile)

print "Done creating inputs for carbon pool tile generation"

print "Uploading output files"
uu.upload_final_set(cn.bor_tem_trop_processed_dir, cn.pattern_bor_tem_trop_processed)
uu.upload_final_set(cn.elevation_processed_dir, cn.pattern_elevation)
uu.upload_final_set(cn.precip_processed_dir, cn.pattern_precip)