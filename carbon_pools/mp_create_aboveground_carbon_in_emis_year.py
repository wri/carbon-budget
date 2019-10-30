'''
This script create tiles of the aboveground carbon density in the year in which tree cover loss occurred
using mangrove and non-mangrove (WHRC) aboveground biomass density in 2000 and carbon gain from 2000 until the loss year.
Unlike the AGC in 2000, it outputs values only where there is loss, and the values are carbon in 2000 + gain until loss.
Thus, loss pixels that don't also have gain pixels have all of their carbon accumulation from after 2000 emitted because
all of the carbon accumuluation is assumed to come before the loss happens.
However, pixels that have both loss and gain only emit the portion of the carbon accumulation that occurs before loss.
Therefore, loss+gain pixels only have part of their gross carbon accumulation added to AGC 2000 for all forest types.
This is used for the gross emissions model.
'''

import create_aboveground_carbon_in_emis_year
import multiprocessing
import argparse
from functools import partial
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def main ():

    output_dir_list = [cn.AGC_emis_year_dir]
    output_pattern_list = [cn.pattern_AGC_emis_year]

    tile_list = uu.create_combined_tile_list(cn.WHRC_biomass_2000_unmasked_dir,
                                             cn.annual_gain_AGB_mangrove_dir
                                             )

    # tile_list = ['30N_080W'] # test tiles
    # tile_list = ['00N_110E'] # test tiles
    print tile_list
    print "There are {} unique tiles to process".format(str(len(tile_list))) + "\n"

    # The argument for what kind of model run is being done: standard conditions or a sensitivity analysis run
    parser = argparse.ArgumentParser(description='Create tiles of the number of years of carbon gain for mangrove forests')
    parser.add_argument('--model-type', '-t', required=True,
                        help='{}'.format(cn.model_type_arg_help))
    args = parser.parse_args()
    sensit_type = args.model_type

    # Checks whether the sensitivity analysis argument is valid
    uu.check_sensit_type(sensit_type)

    # For downloading all tiles in the input folders.
    download_list = [
        cn.WHRC_biomass_2000_unmasked_dir, # This uses the unmasked HWRC biomass because it needs the biomass in planted forest pixels, not just the non-mangrove non-planted forest pixels
        cn.mangrove_biomass_2000_dir,
        cn.cumul_gain_AGCO2_mangrove_dir,
        cn.cumul_gain_AGCO2_planted_forest_non_mangrove_dir,
        cn.cumul_gain_AGCO2_natrl_forest_dir,
        cn.annual_gain_AGB_mangrove_dir,
        cn.annual_gain_AGB_planted_forest_non_mangrove_dir,
        cn.annual_gain_AGB_natrl_forest_dir,
        cn.loss_dir, cn.gain_dir
        ]

    # For downloading all tiles in the input folders.
    download_dict = {
        cn.WHRC_biomass_2000_unmasked_dir: [cn.pattern_mangrove_biomass_2000, 'false'],
        cn.mangrove_biomass_2000_dir: [cn.pattern_WHRC_biomass_2000_unmasked, 'false'],
        cn.cumul_gain_AGCO2_mangrove_dir: [cn.pattern_cumul_gain_AGCO2_mangrove, 'true'],
        cn.cumul_gain_AGCO2_planted_forest_non_mangrove_dir: [cn.pattern_cumul_gain_AGCO2_planted_forest_non_mangrove, 'true'],
        cn.cumul_gain_AGCO2_natrl_forest_dir: [cn.pattern_cumul_gain_AGCO2_natrl_forest, 'true'],
        cn.annual_gain_AGB_mangrove_dir: [cn.pattern_annual_gain_AGB_mangrove, 'true'],
        cn.annual_gain_AGB_planted_forest_non_mangrove_dir: [cn.pattern_annual_gain_AGB_planted_forest_non_mangrove, 'true'],
        cn.annual_gain_AGB_natrl_forest_dir: [cn.pattern_annual_gain_AGB_natrl_forest, 'true'],
        cn.loss_dir: ['', 'false'],
        cn.gain_dir: [cn.pattern_gain, 'false']
    }

    # If the model run isn't the standard one, the output directory and file names are changed
    if sensit_type != 'std':

        print "Changing output directory and file name pattern based on sensitivity analysis"

        download_list = uu.alter_dirs(sensit_type, download_list)
        output_dir_list = uu.alter_dirs(sensit_type, output_dir_list)
        output_pattern_list = uu.alter_patterns(sensit_type, output_pattern_list)

    # for input in download_list:
    #     uu.s3_folder_download('{}'.format(input), '.')

    for key, values in download_dict:
        dir = key
        pattern = values[0]
        sensit_use = values[1]
        uu.s3_folder_download_dict(dir, pattern, '.', sensit_type, sensit_use, tile_list)

    # # For copying individual tiles to spot machine for testing.
    # for tile in tile_list:
    #
    #     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.mangrove_biomass_2000_dir, tile, cn.pattern_mangrove_biomass_2000), '.', sensit_type, 'false')
    #     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.WHRC_biomass_2000_unmasked_dir, tile, cn.pattern_WHRC_biomass_2000_unmasked), '.', sensit_type, 'false')
    #     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.cumul_gain_AGCO2_mangrove_dir, tile, cn.pattern_cumul_gain_AGCO2_mangrove), '.', sensit_type, 'true')
    #     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.cumul_gain_AGCO2_planted_forest_non_mangrove_dir, tile, cn.pattern_cumul_gain_AGCO2_planted_forest_non_mangrove), '.', sensit_type, 'true')
    #     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.cumul_gain_AGCO2_natrl_forest_dir, tile, cn.pattern_cumul_gain_AGCO2_natrl_forest), '.', sensit_type, 'true')
    #     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.annual_gain_AGB_mangrove_dir, tile, cn.pattern_annual_gain_AGB_mangrove), '.', sensit_type, 'true')
    #     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.annual_gain_AGB_planted_forest_non_mangrove_dir, tile, cn.pattern_annual_gain_AGB_planted_forest_non_mangrove), '.', sensit_type, 'true')
    #     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.annual_gain_AGB_natrl_forest_dir, tile, cn.pattern_annual_gain_AGB_natrl_forest), '.', sensit_type, 'true')
    #     uu.s3_file_download('{0}{1}.tif'.format(cn.loss_dir, tile), '.', sensit_type, 'false')
    #     uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.gain_dir, cn.pattern_gain, tile), '.', sensit_type, 'false')

    print "Creating tiles of emitted aboveground carbon (carbon 2000 + carbon accumulation until loss year)"

    # Creates a single filename pattern to pass to the multiprocessor call
    pattern = output_pattern_list[0]

    # 16 processors seems to use more than 460 GB-- I don't know exactly how much it uses because I stopped it at 460
    # 14 processors maxes out at 415 GB
    count = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=14)
    pool.map(partial(create_aboveground_carbon_in_emis_year.create_emitted_AGC, pattern=pattern, sensit_type=sensit_type), tile_list)

    # # For single processor use
    # for tile in tile_list:
    #     create_aboveground_carbon_in_emis_year.create_emitted_AGC(tile, output_pattern_list[0], sensit_type)

    uu.upload_final_set(output_dir_list[0], output_pattern_list[0])


if __name__ == '__main__':
    main()