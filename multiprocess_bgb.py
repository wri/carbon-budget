import multiprocessing

import calc_bgb

tile_list = ['00N_010E', '00N_020E', '00N_030E', '00N_040E']

if __name__ == '__main__':
    count = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=1)
    pool.map(calc_bgb.calc_bgb, tile_list)
