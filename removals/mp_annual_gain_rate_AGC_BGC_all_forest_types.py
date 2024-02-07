"""
Creates tiles of annual aboveground and belowground removal rates for the entire model extent (all forest types).
Also, creates tiles that show what the source of the removal factor is each for each pixel. This can correspond to
particular forest types (mangrove, planted, natural) or data sources (US, Europe, young natural forests from Cook-Patton et al.,
older natural forests from IPCC defaults).
The current hierarchy where pixels overlap is: mangrove > Europe > planted forests > US forests > Cook-Patton et al.
rates for young secondary forests > IPCC defaults for old secondary and primary forests.
This hierarchy is reflected in the removal rates and the forest type rasters.
The different removal rate inputs are in different units but all are standardized to AGC/ha/yr and BGC/ha/yr.

python -m removals.mp_annual_gain_rate_AGC_BGC_all_forest_types -t std -l 00N_000E -nu
python -m removals.mp_annual_gain_rate_AGC_BGC_all_forest_types -t std -l all
"""


import argparse
from functools import partial
import multiprocessing
import os
import sys

import constants_and_names as cn
import universal_util as uu
from . import annual_gain_rate_AGC_BGC_all_forest_types

def mp_annual_gain_rate_AGC_BGC_all_forest_types(tile_id_list):
    """
    :param tile_id_list: list of tile ids to process
    :return: 5 sets of tiles with annual removal factors combined from all removal factor sources:
        removal forest type, aboveground rate, belowground rate, aboveground+belowground rate,
        standard deviation for aboveground rate.
        Units: Mg carbon/ha/yr (including for standard deviation tiles)
    """

    os.chdir(cn.docker_tile_dir)

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # List of tiles to run in the model
        tile_id_list = uu.tile_list_s3(cn.model_extent_dir, cn.SENSIT_TYPE)

    uu.print_log(tile_id_list)
    uu.print_log(f'There are {str(len(tile_id_list))} tiles to process', "\n")


    # Files to download for this script.
    download_dict = {
        cn.model_extent_dir: [cn.pattern_model_extent],
        cn.annual_gain_AGB_mangrove_dir: [cn.pattern_annual_gain_AGB_mangrove],
        cn.annual_gain_BGB_mangrove_dir: [cn.pattern_annual_gain_BGB_mangrove],
        cn.annual_gain_AGC_BGC_natrl_forest_Europe_dir: [cn.pattern_annual_gain_AGC_BGC_natrl_forest_Europe],
        cn.annual_gain_AGC_BGC_planted_forest_dir: [cn.pattern_annual_gain_AGC_BGC_planted_forest],
        cn.annual_gain_AGC_BGC_natrl_forest_US_dir: [cn.pattern_annual_gain_AGC_BGC_natrl_forest_US],
        cn.annual_gain_AGC_natrl_forest_young_dir: [cn.pattern_annual_gain_AGC_natrl_forest_young],
        cn.age_cat_IPCC_dir: [cn.pattern_age_cat_IPCC],
        cn.annual_gain_AGB_IPCC_defaults_dir: [cn.pattern_annual_gain_AGB_IPCC_defaults],
        cn.BGB_AGB_ratio_dir: [cn.pattern_BGB_AGB_ratio],

        cn.stdev_annual_gain_AGB_mangrove_dir: [cn.pattern_stdev_annual_gain_AGB_mangrove],
        cn.stdev_annual_gain_AGC_BGC_natrl_forest_Europe_dir: [cn.pattern_stdev_annual_gain_AGC_BGC_natrl_forest_Europe],
        cn.stdev_annual_gain_AGC_BGC_planted_forest_dir: [cn.pattern_stdev_annual_gain_AGC_BGC_planted_forest],
        cn.stdev_annual_gain_AGC_BGC_natrl_forest_US_dir: [cn.pattern_stdev_annual_gain_AGC_BGC_natrl_forest_US],
        cn.stdev_annual_gain_AGC_natrl_forest_young_dir: [cn.pattern_stdev_annual_gain_AGC_natrl_forest_young],
        cn.stdev_annual_gain_AGB_IPCC_defaults_dir: [cn.pattern_stdev_annual_gain_AGB_IPCC_defaults]
    }


    # List of output directories and output file name patterns
    output_dir_list = [cn.removal_forest_type_dir,
                       cn.annual_gain_AGC_all_types_dir, cn.annual_gain_BGC_all_types_dir,
                       cn.annual_gain_AGC_BGC_all_types_dir, cn.stdev_annual_gain_AGC_all_types_dir]
    output_pattern_list = [cn.pattern_removal_forest_type,
                           cn.pattern_annual_gain_AGC_all_types, cn.pattern_annual_gain_BGC_all_types,
                           cn.pattern_annual_gain_AGC_BGC_all_types, cn.pattern_stdev_annual_gain_AGC_all_types]


    # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
    for key, values in download_dict.items():
        directory = key
        pattern = values[0]
        uu.s3_flexible_download(directory, pattern, cn.docker_tile_dir, cn.SENSIT_TYPE, tile_id_list)


    # If the model run isn't the standard one, the output directory and file names are changed
    if cn.SENSIT_TYPE != 'std':
        uu.print_log('Changing output directory and file name pattern based on sensitivity analysis')
        output_dir_list = uu.alter_dirs(cn.SENSIT_TYPE, output_dir_list)
        output_pattern_list = uu.alter_patterns(cn.SENSIT_TYPE, output_pattern_list)

    # A date can optionally be provided by the full model script or a run of this script.
    # This replaces the date in constants_and_names.
    # Only done if output upload is enabled.
    if cn.RUN_DATE is not None and cn.NO_UPLOAD is not None:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, cn.RUN_DATE)


    if cn.SINGLE_PROCESSOR:
        for tile_id in tile_id_list:
            annual_gain_rate_AGC_BGC_all_forest_types.annual_gain_rate_AGC_BGC_all_forest_types(tile_id, output_pattern_list)

    else:
        # This configuration of the multiprocessing call is necessary for passing multiple arguments to the main function
        # It is based on the example here: http://spencerimp.blogspot.com/2015/12/python-multiprocess-with-multiple.html
        if cn.count == 96:
            if cn.SENSIT_TYPE == 'biomass_swap':
                processes = 13
            else:
                processes = 17  # 30 processors > 740 GB peak; 18 = >740 GB peak; 16 = 660 GB peak; 17 = >680 GB peak
        else:
            processes = 2
        uu.print_log(f'Removal factor processors={processes}')
        with multiprocessing.Pool(processes) as pool:
            pool.map(partial(annual_gain_rate_AGC_BGC_all_forest_types.annual_gain_rate_AGC_BGC_all_forest_types,
                             output_pattern_list=output_pattern_list),
                     tile_id_list)
            pool.close()
            pool.join()


    # No single-processor versions of these check-if-empty functions
    # Checks the gross removals outputs for tiles with no data
    for output_pattern in output_pattern_list:
        if cn.count <= 12:  # For local tests
            processes = 1
            uu.print_log(f'Checking for empty tiles of {output_pattern} pattern with {processes} processors using light function...')
            with multiprocessing.Pool(processes) as pool:
                pool.map(partial(uu.check_and_delete_if_empty_light, output_pattern=output_pattern), tile_id_list)
                pool.close()
                pool.join()
        else:
            processes = 55  # 55 processors = XXX GB peak
            uu.print_log(f'Checking for empty tiles of {output_pattern} pattern with {processes} processors...')
            with multiprocessing.Pool(processes) as pool:
                pool.map(partial(uu.check_and_delete_if_empty, output_pattern=output_pattern), tile_id_list)
                pool.close()
                pool.join()


    # If cn.NO_UPLOAD flag is not activated (by choice or by lack of AWS credentials), output is uploaded
    if not cn.NO_UPLOAD:
        for output_dir, output_pattern in zip(output_dir_list, output_pattern_list):
            uu.upload_final_set(output_dir, output_pattern)


if __name__ == '__main__':

    # The arguments for what kind of model run is being run (standard conditions or a sensitivity analysis) and
    # the tiles to include
    parser = argparse.ArgumentParser(
        description='Create tiles of removal factors for all forest types')
    parser.add_argument('--model-type', '-t', required=True,
                        help=f'{cn.model_type_arg_help}')
    parser.add_argument('--tile_id_list', '-l', required=True,
                        help='List of tile ids to use in the model. Should be of form 00N_110E or 00N_110E,00N_120E or all.')
    parser.add_argument('--run-date', '-d', required=False,
                        help='Date of run. Must be format YYYYMMDD.')
    parser.add_argument('--no-upload', '-nu', action='store_true',
                       help='Disables uploading of outputs to s3')
    parser.add_argument('--single-processor', '-sp', action='store_true',
                       help='Uses single processing rather than multiprocessing')
    args = parser.parse_args()


    # Sets global variables to the command line arguments
    cn.SENSIT_TYPE = args.model_type
    cn.RUN_DATE = args.run_date
    cn.NO_UPLOAD = args.no_upload
    cn.SINGLE_PROCESSOR = args.single_processor

    tile_id_list = args.tile_id_list

    # Disables upload to s3 if no AWS credentials are found in environment
    if not uu.check_aws_creds():
        cn.NO_UPLOAD = True

    # Create the output log
    uu.initiate_log(tile_id_list)

    # Checks whether the sensitivity analysis and tile_id_list arguments are valid
    uu.check_sensit_type(cn.SENSIT_TYPE)
    tile_id_list = uu.tile_id_list_check(tile_id_list)

    mp_annual_gain_rate_AGC_BGC_all_forest_types(tile_id_list)
