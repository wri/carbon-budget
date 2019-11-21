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

    # Files to download for this script. 'true'/'false' says whether the input directory and pattern should be
    # changed for a sensitivity analysis. This does not need to change based on what run is being done;
    # this assignment should be true for all sensitivity analyses and the standard model.
    download_dict = {
        cn.annual_gain_AGB_mangrove_dir: [cn.pattern_annual_gain_AGB_mangrove, 'false'],
        cn.annual_gain_BGB_mangrove_dir: [cn.pattern_annual_gain_BGB_mangrove, 'false'],
        cn.gain_year_count_mangrove_dir: [cn.pattern_gain_year_count_mangrove, 'true']
    }


    # List of tiles to run in the model
    tile_id_list = uu.tile_list_s3(cn.annual_gain_AGB_mangrove_dir)
    # tile_id_list = ['20S_110E', '30S_110E'] # test tiles
    # tile_id_list = ['00N_110E'] # test tiles
    print tile_id_list
    print "There are {} tiles to process".format(str(len(tile_id_list))) + "\n"


    # List of output directories and output file name patterns
    output_dir_list = [cn.cumul_gain_AGCO2_mangrove_dir, cn.cumul_gain_BGCO2_mangrove_dir]
    output_pattern_list = [cn.pattern_cumul_gain_AGCO2_mangrove, cn.pattern_cumul_gain_BGCO2_mangrove]


    # The argument for what kind of model run is being done: standard conditions or a sensitivity analysis run
    parser = argparse.ArgumentParser(description='Create tiles of the number of years of carbon gain for mangrove forests')
    parser.add_argument('--model-type', '-t', required=True,
                        help='{}'.format(cn.model_type_arg_help))
    args = parser.parse_args()
    sensit_type = args.model_type
    # Checks whether the sensitivity analysis argument is valid
    uu.check_sensit_type(sensit_type)


    # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
    for key, values in download_dict.iteritems():
        dir = key
        pattern = values[0]
        sensit_use = values[1]
        uu.s3_flexible_download(dir, pattern, '.', sensit_type, sensit_use, tile_id_list)


    # If the model run isn't the standard one, the output directory and file names are changed
    if sensit_type != 'std':
        print "Changing output directory and file name pattern based on sensitivity analysis"
        output_dir_list = uu.alter_dirs(sensit_type, output_dir_list)
        output_pattern_list = uu.alter_patterns(sensit_type, output_pattern_list)


    # Creates a single filename pattern to pass to the multiprocessor call
    pattern = output_pattern_list[0]

    # Calculates cumulative aboveground carbon gain in mangroves
    # count/3 peaks at about 380 GB, so this is okay on r4.16xlarge
    # count/2 peaks above 480 GB
    # processes=26 peaks at about 400 GB
    count = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=26)
    pool.map(partial(cumulative_gain_mangrove.cumulative_gain_AGCO2, pattern=pattern, sensit_type=sensit_type), tile_id_list)

    # Creates a single filename pattern to pass to the multiprocessor call
    pattern = output_pattern_list[1]

    # Calculates cumulative belowground carbon gain in mangroves
    # count/3 maxes out at about 320 GB
    pool = multiprocessing.Pool(processes=26)
    pool.map(partial(cumulative_gain_mangrove.cumulative_gain_BGCO2, pattern=pattern, sensit_type=sensit_type), tile_id_list)
    pool.close()
    pool.join()

    # # For single processor use
    # for tile_id in tile_id_list:
    #     cumulative_gain_mangrove.cumulative_gain_AGCO2(tile_id, output_pattern_list[0], sensit_type)
    #
    # for tile_id in tile_id_list:
    #     cumulative_gain_mangrove.cumulative_gain_BGCO2(tile_id, output_pattern_list[1], sensit_type)


    # Uploads output tiles to s3
    for i in range(0, len(output_dir_list)):
        uu.upload_final_set(output_dir_list[i], output_pattern_list[i])


if __name__ == '__main__':
    main()