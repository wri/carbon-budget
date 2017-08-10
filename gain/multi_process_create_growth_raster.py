import multiprocessing

import create_growth_raster

tile_id_list = ["00N_090E", "00N_100E", "00N_110E", "00N_120E", "00N_130E", "00N_140E", "00N_150E", "00N_160E", "10N_090E", "10N_100E", "10N_110E", "10N_120E", "10N_130E", "10S_110E", "10S_120E", "10S_130E", "10S_140E", "10S_150E", "10S_160E", "20N_090E", "20N_100E", "20N_110E", "20N_120E", "20S_160E"]

tile_age_list = []

for tile_id in tile_id_list:
    tile_age_list.append([tile_id, 'old'])
    tile_age_list.append([tile_id, 'young'])
    
if __name__ == '__main__':
    count = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=10)
    pool.map(create_growth_raster.create_growth_raster, tile_age_list)
