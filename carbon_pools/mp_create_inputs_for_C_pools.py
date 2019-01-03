import subprocess
import create_inputs_for_C_pools
import multiprocessing
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# tile_list = util.tile_list(cn.natrl_forest_biomass_2000_dir)
tile_list = ['10N_080W', '40N_120E'] # test tiles
print tile_list

print "Downloading raw ecozone and precipitation files"
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

print "Copying srtm files"
uu.s3_folder_download(cn.srtm_raw_dir, './srtm')

print "Making srtm vrt"
subprocess.check_call('gdalbuildvrt srtm.vrt srtm/*.tif', shell=True)

# # Soil tiles are already processed, so there's no need to include them here.
# # Leaving this in case I ever add in soil processing again.
# print "Copying soil tiles"
# uu.s3_folder_download(cn.soil_C_processed_dir)

count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(processes=count / 3)
pool.map(create_inputs_for_C_pools.create_input_files, tile_list)

print " Done creating inputs for carbon tile generation"