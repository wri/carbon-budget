import multiprocessing
import subprocess
import calc_all

print "copy down eco zone shapefile"
copy_ecozone = ['aws', 's3', 'cp', 's3://gfw-files/sam/carbon_budget/fao_ecozones_bor_tem_tro.zip', '.']
subprocess.check_call(copy_ecozone)

print "unzip eco zones"
unzip_zones = ['unzip', 'fao_ecozones_bor_tem_tro.zip', '-d', '.']
subprocess.check_call(unzip_zones)

print "copy down srtm files"
copy_srtm = ['aws', 's3', 'sync', 's3://gfw2-data/analyses/srtm/', './srtm']
subprocess.check_call(copy_srtm)

print "copy down precip file"
download_precip = ['aws', 's3', 'cp', 's3://gfw-files/sam/carbon_budget/add_30s_precip.tif', '.']
subprocess.check_call(download_precip)

print "copy down soil file"
download_soil = ['aws', 's3', 'cp', 's3://gfw-files/sam/carbon_budget/hwsd_oc_final.tif', '.']
subprocess.check_call(download_soil)

print "make srtm vrt"
subprocess.check_call('gdalbuildvrt srtm.vrt srtm/*.tif', shell=True)

biomass_tile_list = ['00N_010W', '00N_020W', '00N_030W', '00N_050E', '00N_060E', '00N_080E', '00N_110W', '00N_120W', '00N_130W', '00N_140W', '00N_150W', '00N_160W', '00N_170E', '00N_170W', '00N_180W', '10N_030W', '10N_040W', '10N_050W', '10N_060E', '10N_060W', '10N_070W', '10N_080W', '10N_090W', '10N_100W', '10N_110W', '10N_120W', '10N_130W', '10N_140W', '10N_150W', '10N_160W', '10N_170E', '10N_170W', '10N_180W', '10S_000E', '10S_010W', '10S_020W', '10S_030W', '10S_040W', '10S_050W', '10S_060E', '10S_060W', '10S_070E', '10S_070W', '10S_080E', '10S_080W', '10S_090W', '10S_100W', '10S_110W', '10S_120W', '10S_130W', '10S_140W', '10S_150W', '10S_160W', '10S_170W', '10S_180W', '20N_030W', '20N_040W', '20N_050W', '20N_060E', '20N_060W', '20N_070W', '20N_080W', '20N_090W', '20N_120W', '20N_130E', '20N_130W', '20N_140W', '20N_150E', '20N_150W', '20N_160E', '20N_160W', '20N_170E', '20N_170W', '20N_180W', '20S_000E', '20S_010W', '20S_020W', '20S_030W', '20S_040W', '20S_050W', '20S_060E', '20S_060W', '20S_070E', '20S_070W', '20S_080E', '20S_080W', '20S_090E', '20S_090W', '20S_100E', '20S_100W', '20S_110W', '20S_120W', '20S_130W', '20S_140W', '20S_150W', '20S_160W', '20S_170E', '20S_170W', '20S_180W', '30N_030W', '30N_040W', '30N_050W', '30N_060W', '30N_070W', '30N_080W', '30N_130E', '30N_130W', '30N_140W', '30N_150W', '30N_160E', '30N_160W', '30N_170E', '30N_170W', '30N_180W', '30S_000E', '30S_010W', '30S_020W', '30S_030W', '30S_040E', '30S_040W', '30S_050E', '30S_050W', '30S_060E', '30S_060W', '30S_070E', '30S_070W', '30S_080E', '30S_080W', '30S_090E', '30S_090W', '30S_100E', '30S_100W', '30S_110W', '30S_120W', '30S_130W', '30S_140W', '30S_150W', '30S_160E', '30S_160W', '30S_170W', '30S_180W', '40N_030W', '40N_040W', '40N_050W', '40N_060W', '40N_140W', '40N_150E', '40N_150W', '40N_160E', '40N_160W', '40N_170E', '40N_170W', '40N_180W', '40S_000E', '40S_010E', '40S_010W', '40S_020E', '40S_020W', '40S_030E', '40S_030W', '40S_040E', '40S_040W', '40S_050E', '40S_050W', '40S_060E', '40S_060W', '40S_070E', '40S_070W', '40S_080E', '40S_080W', '40S_090E', '40S_090W', '40S_100E', '40S_100W', '40S_110E', '40S_110W', '40S_120E', '40S_120W', '40S_130E', '40S_130W', '40S_140W', '40S_150E', '40S_150W', '40S_160W', '40S_170W', '40S_180W', '50N_020W', '50N_030W', '50N_040W', '50N_050W', '50N_140W', '50N_150W', '50N_160E', '50N_160W', '50N_170E', '50N_170W', '50N_180W', '50S_000E', '50S_010E', '50S_010W', '50S_020E', '50S_020W', '50S_030E', '50S_030W', '50S_040E', '50S_040W', '50S_050E', '50S_050W', '50S_060E', '50S_060W', '50S_070E', '50S_070W', '50S_080E', '50S_080W', '50S_090E', '50S_090W', '50S_100E', '50S_100W', '50S_110E', '50S_110W', '50S_120E', '50S_120W', '50S_130E', '50S_130W', '50S_140E', '50S_140W', '50S_150E', '50S_150W', '50S_160E', '50S_160W', '50S_170E', '50S_170W', '50S_180W', '60N_030W', '60N_040W', '60N_050W', '70N_010W', '70N_040W', '70N_050W', '80N_000E', '80N_010W', '80N_020W', '80N_030W', '80N_040E', '80N_040W', '80N_050W']
biomass_tile_list = ['30N_020W', '40N_010W']

if __name__ == '__main__':
     count = multiprocessing.cpu_count()
     pool = multiprocessing.Pool(processes=2)
     pool.map(calc_all.calc_all, biomass_tile_list)

