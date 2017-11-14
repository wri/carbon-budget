import multiprocessing
import subprocess
import calc_emissions
import utilities

biomass_tile_list = ['00N_090E', '00N_100E', '00N_110E', '00N_120E', '00N_130E', '00N_140E', '00N_150E', '10N_090E', '10N_100E', '10N_110E', '10N_120E', '10N_130E', '10S_110E', '10S_120E', '10S_140E', '10S_150E']
#biomass_tile_list = ['00N_140E']
   
    
if __name__ == '__main__':
     count = multiprocessing.cpu_count()
     pool = multiprocessing.Pool(processes=13)
     pool.map(calc_emissions.calc_emissions, biomass_tile_list)
     

    
