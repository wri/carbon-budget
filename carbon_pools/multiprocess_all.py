import subprocess
import create_carbon_pools
import multiprocessing
import util

# Run this if creating carbon pool files and the data inputs don't exist
# this copies down raw data and makes a vrt of SRTM then calls the other code which will
# create the the data inputs and use those to write carbon pool files

# Raw input files
ecozones = 'fao_ecozones_bor_tem_tro_20180619.zip'
precip = 'add_30s_precip.tif'
soil = 'hwsd_oc_final.tif'

# files to copy down
files_to_copy = [
                's3://gfw2-data/climate/carbon_model/inputs_for_carbon_pools/raw/{0}'.format(ecozones),
                's3://gfw2-data/climate/carbon_model/inputs_for_carbon_pools/raw/{0}'.format(precip),
                's3://gfw2-data/climate/carbon_model/inputs_for_carbon_pools/raw/{0}'.format(soil)
                 ]

for file in files_to_copy:
    cmd = ['aws', 's3', 'cp', file, '.']
    subprocess.check_call(cmd)

print "unzip eco zones"
unzip_zones = ['unzip', '{}'.format(ecozones), '-d', '.']
subprocess.check_call(unzip_zones)

print "copy srtm files"
copy_srtm = ['aws', 's3', 'sync', 's3://gfw2-data/analyses/srtm/', './srtm']
subprocess.check_call(copy_srtm)

print "make srtm vrt"
subprocess.check_call('gdalbuildvrt srtm.vrt srtm/*.tif', shell=True)

biomass_tile_list = util.tile_list('s3://WHRC-carbon/WHRC_V4/Processed/')

print biomass_tile_list

# if __name__ == '__main__':
count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(processes=count/3)
pool.map(create_carbon_pools.create_carbon_pools, biomass_tile_list)
