import subprocess
import create_inputs_for_C_pools
import multiprocessing
import sys
sys.path.append('../')
import constants_and_names
import universal_util

# tile_list = util.tile_list(constants_and_names.biomass_dir)
tile_list = ['10N_080W', '40N_120E'] # test tiles
print tile_list

# For downloading all tiles in the input folders
input_files = [
    constants_and_names.fao_ecozone_processed_dir,
    constants_and_names.precip_processed_dir,
    constants_and_names.soil_C_dir
    ]

for input in input_files:
    universal_util.s3_folder_download('{}'.format(input), '.')

print "Unzipping FAO ecozones"
unzip_zones = ['unzip', '{}'.format(constants_and_names.pattern_fao_ecozone_raw), '-d', '.']
subprocess.check_call(unzip_zones)

print "Copying srtm files"
copy_srtm = ['aws', 's3', 'sync', constants_and_names.srtm_raw_dir, './srtm']
subprocess.check_call(copy_srtm)

print "Making srtm vrt"
subprocess.check_call('gdalbuildvrt srtm.vrt srtm/*.tif', shell=True)

count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(processes=count / 3)
pool.map(create_inputs_for_C_pools.create_input_files, tile_list)

print " Done creating inputs for carbon tile generation"