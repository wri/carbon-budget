from utilities import util
# get list of tile from s3
tiles_on_s3 = util.check_output_exists('deadwood')

# iterate over and check min/max values are within 1-1000

# if not, write to log file, or make txt file
