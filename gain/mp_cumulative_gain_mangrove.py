### This script calculates the cumulative above and belowground carbon dioxide gain in mangrove forest pixels from 2001-2015.
### It multiplies the annual biomass gain rate by the number of years of gain by the biomass-to-carbon conversion and
### by to the C to CO2 conversion.

import multiprocessing
import cumulative_gain_mangrove
import argparse
from functools import partial
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def main ():

    # List of output directories and output file name patterns
    output_dir_list = [cn.cumul_gain_AGCO2_mangrove_dir, cn.cumul_gain_BGCO2_mangrove_dir]
    output_pattern_list = [cn.pattern_cumul_gain_AGCO2_mangrove, cn.pattern_cumul_gain_BGCO2_mangrove]

    tile_id_list = uu.tile_list(cn.annual_gain_AGB_mangrove_dir)
    # tile_id_list = ['20S_110E', '30S_110E'] # test tiles
    # tile_id_list = ['00N_110E'] # test tiles
    print tile_id_list
    print "There are {} tiles to process".format(str(len(tile_id_list))) + "\n"

    # The argument for what kind of model run is being done: standard conditions or a sensitivity analysis run
    parser = argparse.ArgumentParser(description='Create tiles of the number of years of carbon gain for mangrove forests')
    parser.add_argument('--model-type', '-t', required=True,
                        help='{}'.format(cn.model_type_arg_help))
    args = parser.parse_args()
    sensit_type = args.model_type

    # Checks whether the sensitivity analysis argument is valid
    uu.check_sensit_type(sensit_type)

    # # For downloading all tiles in the input folders
    # download_list = [cn.annual_gain_AGB_mangrove_dir, cn.annual_gain_BGB_mangrove_dir, cn.gain_year_count_mangrove_dir]
    #
    # # If the model run isn't the standard one, the output directory and file names are changed
    # if sensit_type != 'std':
    #
    #     print "Changing output directory and file name pattern based on sensitivity analysis"
    #
    #     download_list = uu.alter_dirs(sensit_type, download_list)
    #     output_dir_list = uu.alter_dirs(sensit_type, output_dir_list)
    #     output_pattern_list = uu.alter_patterns(sensit_type, output_pattern_list)

    # for input in download_list:
    #     uu.s3_folder_download(input, '.')

    download_dict = {
        cn.annual_gain_AGB_mangrove_dir: [cn.pattern_annual_gain_AGB_mangrove, 'false'],
        cn.annual_gain_BGB_mangrove_dir: [cn.pattern_annual_gain_BGB_mangrove, 'false'],
        cn.gain_year_count_mangrove_dir: [cn.pattern_gain_year_count_mangrove, 'true'],
        cn.loss_dir: ['', 'false']      # Don't need for this script-- just included for testing
    }

    for key, values in download_dict.iteritems():
        dir = key
        pattern = values[0]
        sensit_use = values[1]
        uu.s3_flexible_download(dir, pattern, '.', sensit_type, sensit_use, tile_id_list)

    sys.quit()





    # For copying individual tiles to spot machine for testing
    for tile in tile_id_list:

        uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.annual_gain_AGB_mangrove_dir, tile, cn.pattern_annual_gain_AGB_mangrove), '.', sensit_type, 'false')      # annual AGB gain rate tiles
        uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.annual_gain_BGB_mangrove_dir, tile, cn.pattern_annual_gain_BGB_mangrove), '.', sensit_type, 'false')      # annual AGB gain rate tiles
        uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.gain_year_count_mangrove_dir, tile, cn.pattern_gain_year_count_mangrove), '.', sensit_type, 'true')      # number of years with gain tiles

    # Creates a single filename pattern to pass to the multiprocessor call
    pattern = output_pattern_list[0]

    count = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(count / 3)
    # Calculates cumulative aboveground carbon gain in mangroves
    # count/3 peaks at about 380 GB, so this is okay on r4.16xlarge
    pool.map(partial(cumulative_gain_mangrove.cumulative_gain_AGCO2, pattern=pattern, sensit_type=sensit_type), tile_id_list)

    # Creates a single filename pattern to pass to the multiprocessor call
    pattern = output_pattern_list[1]

    # Calculates cumulative belowground carbon gain in mangroves
    pool.map(partial(cumulative_gain_mangrove.cumulative_gain_BGCO2, pattern=pattern, sensit_type=sensit_type), tile_id_list)
    pool.close()
    pool.join()

    # For single processor use
    for tile in tile_id_list:
        cumulative_gain_mangrove.cumulative_gain_AGCO2(tile, output_pattern_list[0], sensit_type)

    for tile in tile_id_list:
        cumulative_gain_mangrove.cumulative_gain_BGCO2(tile, output_pattern_list[1], sensit_type)

    for i in range(0, len(output_dir_list)):
        uu.upload_final_set(output_dir_list[i], output_pattern_list[i])


if __name__ == '__main__':
    main()