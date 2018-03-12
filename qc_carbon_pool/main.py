from utilities import util
import multiprocessing

# get list of tile from s3
tiles_on_s3 = util.check_output_exists('deadwood')

if __name__ == '__main__':
    count = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=8)
    pool.map(util.qc_minmax_vals, tiles_on_s3)

# download tile

# iterate over and check min/max values are within 1-1000

# if not, write to log file, or make txt file
