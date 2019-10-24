### This script calculates the cumulative above and belowground CO2 gain in non-mangrove planted forest pixels from 2001-2015.
### It multiplies the annual biomass gain rate by the number of years of gain by the biomass-to-carbon conversion and C to CO2 conversion.

import multiprocessing
import cumulative_gain_planted_forest
import argparse
from functools import partial
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def main ():

    # List of output directories and output file name patterns
    output_dir_list = [cn.cumul_gain_AGCO2_planted_forest_non_mangrove_dir, cn.pattern_cumul_gain_AGCO2_planted_forest_non_mangrove]
    output_pattern_list = [cn.cumul_gain_BGCO2_planted_forest_non_mangrove_dir, cn.pattern_cumul_gain_BGCO2_planted_forest_non_mangrove]

    tile_list = uu.tile_list(cn.annual_gain_AGB_planted_forest_non_mangrove_dir)
    # tile_list = ['20S_110E', '30S_110E'] # test tiles
    tile_list = ['00N_110E'] # test tiles
    print tile_list
    print "There are {} tiles to process".format(str(len(tile_list)))

    # The argument for what kind of model run is being done: standard conditions or a sensitivity analysis run
    parser = argparse.ArgumentParser(description='Create tiles of the number of years of carbon gain for mangrove forests')
    parser.add_argument('--model-type', '-t', required=True,
                        help='{}'.format(cn.model_type_arg_help))
    args = parser.parse_args()
    sensit_type = args.model_type

    # Checks whether the sensitivity analysis argument is valid
    uu.check_sensit_type(sensit_type)

    # For downloading all tiles in the input folders
    download_list = [cn.annual_gain_AGB_planted_forest_non_mangrove_dir,
                     cn.annual_gain_BGB_planted_forest_non_mangrove_dir,
                     cn.gain_year_count_planted_forest_non_mangrove_dir]

    # If the model run isn't the standard one, the output directory and file names are changed
    if sensit_type != 'std':

        print "Changing output directory and file name pattern based on sensitivity analysis"

        output_dir_list = uu.alter_dirs(sensit_type, output_dir_list)
        output_pattern_list = uu.alter_patterns(sensit_type, output_pattern_list)
        download_list = uu.alter_dirs(sensit_type, download_list)

    for input in download_list:
        uu.s3_folder_download(input, '.')

    # For copying individual tiles to spot machine for testing
    for tile in tile_list:

        uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.annual_gain_AGB_planted_forest_non_mangrove_dir, tile, cn.pattern_annual_gain_AGB_planted_forest_non_mangrove), '.', sensit_type, 'false')      # annual AGB gain rate tiles
        uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.annual_gain_BGB_planted_forest_non_mangrove_dir, tile, cn.pattern_annual_gain_BGB_planted_forest_non_mangrove), '.', sensit_type, 'false')      # annual AGB gain rate tiles
        uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.gain_year_count_planted_forest_non_mangrove_dir, tile, cn.pattern_gain_year_count_planted_forest_non_mangrove), '.', sensit_type, 'true')      # number of years with gain tiles

    # Creates a single filename pattern to pass to the multiprocessor call
    pattern = output_pattern_list[0]

    count = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(count / 3)
    # Calculates cumulative aboveground carbon gain in non-mangrove planted forests
    # count/3 maxes out at 360 GB on an r4.16xlarge
    pool.map(partial(cumulative_gain_planted_forest.cumulative_gain_AGC, pattern=pattern, sensit_type=sensit_type), tile_list)

    # Creates a single filename pattern to pass to the multiprocessor call
    pattern = output_pattern_list[1]

    # Calculates cumulative belowground carbon gain in non-mangrove planted forests
    # count/3 maxes out at 370 GB on an r4.16xlarge
    pool.map(partial(cumulative_gain_planted_forest.cumulative_gain_BGC, pattern=pattern, sensit_type=sensit_type), tile_list)
    pool.close()
    pool.join()

    # For single processor use
    for tile in tile_list:
        cumulative_gain_planted_forest.cumulative_gain_AGC(tile, output_pattern_list[0], sensit_type)

    for tile in tile_list:
        cumulative_gain_planted_forest.cumulative_gain_BGC(tile, output_pattern_list[0], sensit_type)

    uu.upload_final_set(output_dir_list[0], output_pattern_list[0])
    uu.upload_final_set(output_dir_list[1], output_pattern_list[1])


if __name__ == '__main__':
    main()