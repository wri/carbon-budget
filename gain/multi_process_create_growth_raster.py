import multiprocessing

import create_growth_raster

tile_id_list = ['00N_030E', '00N_040E']

tile_age_list = []

for tile_id in tile_id_list:
    tile_age_list.append([tile_id, 'old'])
    tile_age_list.append([tile_id, 'young'])
    
if __name__ == '__main__':
    count = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=50)
    pool.map(create_growth_raster.create_growth_raster, tile_age_list)