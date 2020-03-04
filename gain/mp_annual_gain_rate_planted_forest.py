### This script assigns annual above and belowground biomass gain rates (in the units of IPCC Table 4.9 (currently tonnes
### biomass/ha/yr)) to non-mangrove planted forest pixels. It masks mangrove pixels from the planted forest carbon gain
### rate tiles so that different forest types are non-overlapping.
### To calculate the aboveground and belowground biomass gain rates from above+belowground carbon gain rate, the
### script uses the IPCC default natural forest values. Although these values don't actually apply to planted forests,
### they are the best we have for parsing planted forests into the component values.
### We want to separate the above+below rate into above and below and convert to biomass so that we can make global
### maps of annual above and below biomass gain rates separately; the natural forests and mangroves already use
### separate above and below annual biomass gain rate files, so this brings planted forests into line with them.


import multiprocessing
import annual_gain_rate_planted_forest
import pandas as pd
from functools import partial
import argparse
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def mp_annual_gain_rate_planted_forest(sensit_type, tile_id_list, run_date = None):

    pd.options.mode.chained_assignment = None


    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # List of tiles to run in the model
        tile_id_list = uu.tile_list_s3(cn.annual_gain_AGC_BGC_planted_forest_unmasked_dir, sensit_type)

    print tile_id_list
    print "There are {} tiles to process".format(str(len(tile_id_list))) + "\n"


    # Files to download for this script.
    download_dict = {
        cn.annual_gain_AGC_BGC_planted_forest_unmasked_dir: [cn.pattern_annual_gain_AGC_BGC_planted_forest_unmasked],
        cn.mangrove_biomass_2000_dir: [cn.pattern_mangrove_biomass_2000],
        cn.plant_pre_2000_processed_dir: [cn.pattern_plant_pre_2000]
    }


    # List of output directories and output file name patterns
    output_dir_list = [cn.annual_gain_AGB_planted_forest_non_mangrove_dir, cn.annual_gain_BGB_planted_forest_non_mangrove_dir]
    output_pattern_list = [cn.pattern_annual_gain_AGB_planted_forest_non_mangrove, cn.pattern_annual_gain_BGB_planted_forest_non_mangrove]

    # If the script is called from the full model run script, a date is provided.
    # This replaces the date in constants_and_names.
    if run_date is not None:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, run_date)


    # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
    for key, values in download_dict.iteritems():
        dir = key
        pattern = values[0]
        uu.s3_flexible_download(dir, pattern, '.', sensit_type, tile_id_list)



    # For multiprocessing
    count = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(count/3)
    # # Masks mangroves out of planted forests where they overlap and pre-2000 plantation pixels
    # # count/3 maxes out at about 370 GB on an r4.16xlarge. Could use more processors.
    # pool.map(partial(annual_gain_rate_planted_forest.mask_mangroves_and_pre_2000_plant, sensit_type=sensit_type),
    #          tile_id_list)
    #
    # # Converts annual above+belowground carbon gain rates into aboveground biomass gain rates
    # # count/3 maxes out at about 260 GB on an r4.16xlarge. Could use more processors.
    # pool.map(partial(annual_gain_rate_planted_forest.create_AGB_rate, output_pattern_list=output_pattern_list),
    #          tile_id_list)
    #
    # # Calculates belowground biomass gain rates from aboveground biomass gain rates
    # # count/3 maxes out at about 260 GB on an r4.16xlarge. Could use more processors.
    # pool.map(partial(annual_gain_rate_planted_forest.create_BGB_rate, output_pattern_list=output_pattern_list),
    #          tile_id_list)

    # Deletes any planted forest annual gain rate tiles that have no planted forest in them after being masked by mangroves.
    # This keep them from unnecessarily being stored on s3.
    pool.map(partial(annual_gain_rate_planted_forest.check_for_planted_forest, output_pattern_list=output_pattern_list),
             tile_id_list)
    pool.close()
    pool.join()


    # # For single processor use
    # for tile_id in tile_id_list:
    #     annual_gain_rate_planted_forest.mask_mangroves_and_pre_2000_plant(tile_id, sensit_type)
    #
    # for tile_id in tile_id_list:
    #     annual_gain_rate_planted_forest.create_AGB_rate(tile_id, output_pattern_list)
    #
    # for tile_id in tile_id_list:
    #     annual_gain_rate_planted_forest.create_BGB_rate(tile_id, output_pattern_list)
    #
    # for tile_id in tile_id_list:
    #     annual_gain_rate_planted_forest.check_for_planted_forest(tile_id, output_pattern_list)


    for i in range(0, len(output_dir_list)):
        uu.upload_final_set(output_dir_list[i], output_pattern_list[i])


if __name__ == '__main__':

    # The argument for what kind of model run is being done: standard conditions or a sensitivity analysis run
    parser = argparse.ArgumentParser(
        description='Create tiles of the number of years of carbon gain for mangrove forests')
    parser.add_argument('--model-type', '-t', required=True,
                        help='{}'.format(cn.model_type_arg_help))
    parser.add_argument('--tile_id_list', '-l', required=True,
                        help='List of tile ids to use in the model. Should be of form 00N_110E or all.')
    args = parser.parse_args()
    sensit_type = args.model_type
    tile_id_list = args.tile_id_list

    # Checks whether the sensitivity analysis and tile_id_list arguments are valid
    uu.check_sensit_type(sensit_type)
    tile_id_list = uu.tile_id_list_check(tile_id_list)

    mp_annual_gain_rate_planted_forest(sensit_type=sensit_type, tile_id_list=tile_id_list)