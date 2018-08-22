import multiprocessing

import hansen_burnyear
import utilities

tile_list = utilities.list_tiles('s3://gfw2-data/forest_change/hansen_2017/')
tile_list = tile_list[1:]
print tile_list

count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(processes=7)
pool.map(hansen_burnyear.hansen_burnyear, tile_list)