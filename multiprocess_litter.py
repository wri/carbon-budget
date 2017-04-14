import multiprocessing
import subprocess
import calc_litter

tile_list = ['00N_010E', '00N_020E', '00N_030E', '00N_040E']

print "copy down climate zone"
copy_climate = ['aws', 's3', 'cp', 's3://gfw-files/sam/carbon_budget/CLIMATE_ZONE.tif', '.']
subprocess.check_call(copy_climate)

print "copy down landcover tiles"
copy_landcover = ['aws', 's3', 'sync', 's3://gfw-files/sam/carbon_budget/umd_landcover/', 'umd_landcover/']

print "make landcover vrt"
subprocess.check_call('gdalbuildvrt landcover.vrt umd_landcover/*.tif', shell=True)

if __name__ == '__main__':
     count = multiprocessing.cpu_count()
     pool = multiprocessing.Pool(processes=1)
     pool.map(calc_litter.calc_litter, tile_list)
