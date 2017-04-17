import multiprocessing
import subprocess
import calc_deadwood

tile_list = ['00N_010E', '00N_020E', '00N_030E', '00N_040E']

print "copy down eco zone shapefile"
copy_ecozone = ['aws', 's3', 'cp', 's3://gfw-files/sam/carbon_budget/fao_ecozones_reclass.zip', '.']
subprocess.check_call(copy_ecozone)

print "unzip eco zones"
unzip_zones = ['unzip', 'fao_ecozones_reclass.zip', '-d', '.']
subprocess.check_call(unzip_zones)

#print "copy down srtm files"
copy_srtm = ['aws', 's3', 'sync', 's3://gfw2-data/analyses/srtm/', './srtm']
#subprocess.check_call(copy_srtm)

print "copy down precip file"
download_precip = ['aws', 's3', 'cp', 's3://gfw-files/sam/carbon_budget/add_30s_precip.tif', '.']
#print "make srtm vrt"
#subprocess.check_call('gdalbuildvrt srtm.vrt srtm/*.tif', shell=True)

if __name__ == '__main__':
     count = multiprocessing.cpu_count()
     pool = multiprocessing.Pool(processes=1)
     pool.map(calc_deadwood.calc_deadwood, tile_list)
