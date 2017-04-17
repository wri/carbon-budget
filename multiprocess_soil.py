import multiprocessing
import subprocess
import calc_soil

tile_list = ['00N_010E', '00N_020E', '00N_030E', '00N_040E']

print "copy down soil file"
copy_soil = ['aws', 's3', 'cp', 'gfw2-data/climate/hwsd_carbon/hwsd_oc_final.tif', '.']
subprocess.check_call(copy_soil)
 
if __name__ == '__main__':
     count = multiprocessing.cpu_count()
     pool = multiprocessing.Pool(processes=1)
     pool.map(calc_soil.calc_soil, tile_list)
