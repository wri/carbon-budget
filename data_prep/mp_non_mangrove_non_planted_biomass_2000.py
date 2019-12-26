### Masks out mangrove and planted forest pixels from WHRC biomass 2000 raster so that
### only non-mangrove, non-planted forest pixels are left of the WHRC biomass 2000 raster

import multiprocessing
from functools import partial
import non_mangrove_non_planted_biomass_2000
import argparse
import pandas as pd
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu


def main ():

    # Files to download for this script.
    # Mangrove biomass and full-extent planted forests are used to mask out mangrove and planted forests from the natural forests
    download_dict = {
        cn.mangrove_biomass_2000_dir: [cn.pattern_mangrove_biomass_2000],
        cn.annual_gain_AGC_BGC_planted_forest_unmasked_dir: [cn.pattern_annual_gain_AGC_BGC_planted_forest_unmasked],
        cn.JPL_processed_dir: [cn.pattern_JPL_unmasked_processed]
    }

    # tile_id_list = uu.tile_list_s3(cn.WHRC_biomass_2000_unmasked_dir)
    # tile_id_list = ['80N_020E', '00N_000E', '00N_020E', '00N_110E'] # test tiles: no mangrove or planted forest, mangrove only, planted forest only, mangrove and planted forest
    tile_id_list = ['00N_110E']
    print tile_id_list
    print "There are {} tiles to process".format(str(len(tile_id_list))) + "\n"


    # List of output directories and output file name patterns
    output_dir_list = [cn.WHRC_biomass_2000_non_mang_non_planted_dir]
    output_pattern_list = [cn.pattern_WHRC_biomass_2000_non_mang_non_planted]


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
        uu.s3_flexible_download(dir, pattern, '.', sensit_type, tile_id_list)


    # If the model run isn't the standard one, the output directory and file names are changed
    if sensit_type != 'std':
        print "Changing output directory and file name pattern based on sensitivity analysis"
        output_dir_list = uu.alter_dirs(sensit_type, output_dir_list)
        output_pattern_list = uu.alter_patterns(sensit_type, output_pattern_list)


    # Creates a single filename pattern to pass to the multiprocessor call
    pattern = output_pattern_list[0]

    # # For multiprocessing. count/3 works on an r4.16xlarge machine
    # count = multiprocessing.cpu_count()
    # pool = multiprocessing.Pool(count/3)
    # pool.map(partial(non_mangrove_non_planted_biomass_2000.mask_biomass, pattern=pattern, sensit_type=sensit_type), tile_id_list)
    # pool.close()
    # pool.join()

    # For single processor use
    for tile_id in tile_id_list:
        non_mangrove_non_planted_biomass_2000.mask_biomass(tile_id, pattern, sensit_type)

    # Uploads output tiles to s3
    uu.upload_final_set(output_dir_list[0], output_pattern_list[0])


if __name__ == '__main__':
    main()