import subprocess
import create_carbon_pools
import multiprocessing
import util
import os
import sys
sys.path.append('../')
import constants_and_names


# Run this if creating carbon pool files and the data inputs don't exist
# this copies down raw data and makes a vrt of SRTM then calls the other code which will
# create the the data inputs and use those to write carbon pool files

input_files = [
    constants_and_names.fao_ecozone_processed_dir,
    constants_and_names.precip_processed_dir,
    constants_and_names.soil_C_dir
    ]

tile_count = []
for i, input in enumerate(input_files):

    tile_count[i] = util.count_tiles(input)

print "The number of tiles for each input to the carbon pools is", tile_count

if len(set(tile_count)) > 0 & tile_count[0] == constants_and_names.biomass_tile_count:

    print "Input tiles for carbon pool generation do not exist. Creating them now..."

    for file in input_files:
        cmd = ['aws', 's3', 'cp', file, '.']
        subprocess.check_call(cmd)

    print "unzip eco zones"
    unzip_zones = ['unzip', '{}'.format(constants_and_names.pattern_fao_ecozone_raw), '-d', '.']
    subprocess.check_call(unzip_zones)

    print "copy srtm files"
    copy_srtm = ['aws', 's3', 'sync', constants_and_names.srtm_raw_dir, './srtm']
    subprocess.check_call(copy_srtm)

    print "make srtm vrt"
    subprocess.check_call('gdalbuildvrt srtm.vrt srtm/*.tif', shell=True)

    print " Done creating inputs for carbon tile generation"

biomass_tile_list = util.tile_list(constants_and_names.biomass_dir)

print biomass_tile_list

count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(processes=count/3)
pool.map(create_carbon_pools.create_carbon_pools, biomass_tile_list)
