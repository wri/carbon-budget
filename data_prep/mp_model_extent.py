"""
This script creates a binary raster of the model extent at the pixel level.
The model extent is ((TCD2000>0 AND WHRC AGB2000>0) OR Hansen gain=1 OR mangrove AGB2000>0).
The rest of the model uses this to mask its extent.
For biomass_swap sensitivity analysis, NASA JPL AGB 2000 replaces WHRC 2000.
For legal_Amazon_loss sensitivity analysis, PRODES 2000 forest extent replaces Hansen tree cover 2000 and Hansen gain
pixels and mangrove pixels outside of (PRODES extent AND WHRC AGB) are not included.

python -m data_prep.mp_model_extent -t std -l 00N_000E -nu
python -m data_prep.mp_model_extent -t std -l all
"""

import argparse
from functools import partial
import multiprocessing
import os
import sys

import constants_and_names as cn
import universal_util as uu
from . import model_extent


def mp_model_extent(tile_id_list):
    """
    :param tile_id_list: list of tile ids to process
    :return: 1 set of tiles where pixels = 1 are included in the model and pixels = 0 are not included in the model
    """

    os.chdir(cn.docker_tile_dir)

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # List of tiles to run in the model. Which biomass tiles to use depends on sensitivity analysis
        if cn.SENSIT_TYPE == 'biomass_swap':
            tile_id_list = uu.tile_list_s3(cn.JPL_processed_dir, cn.SENSIT_TYPE)
        elif cn.SENSIT_TYPE == 'legal_Amazon_loss':
            tile_id_list = uu.tile_list_s3(cn.Brazil_forest_extent_2000_processed_dir, cn.SENSIT_TYPE)
        else:
            tile_id_list = uu.create_combined_tile_list(
                [cn.WHRC_biomass_2000_unmasked_dir, cn.mangrove_biomass_2000_dir, cn.gain_dir, cn.tcd_dir,
                 cn.annual_gain_AGC_BGC_planted_forest_dir],
                sensit_type=cn.SENSIT_TYPE)

    uu.print_log(tile_id_list)
    uu.print_log(f'There are {str(len(tile_id_list))} tiles to process', "\n")


    # Files to download for this script.
    download_dict = {
                    cn.mangrove_biomass_2000_dir: [cn.pattern_mangrove_biomass_2000],
                    cn.gain_dir: [cn.pattern_data_lake]
    }

    if cn.SENSIT_TYPE == 'legal_Amazon_loss':
        download_dict[cn.Brazil_forest_extent_2000_processed_dir] = [cn.pattern_Brazil_forest_extent_2000_processed]
    else:
        download_dict[cn.tcd_dir] = [cn.pattern_tcd]

    if cn.SENSIT_TYPE == 'biomass_swap':
        download_dict[cn.JPL_processed_dir] = [cn.pattern_JPL_unmasked_processed]
    else:
        download_dict[cn.WHRC_biomass_2000_unmasked_dir] = [cn.pattern_WHRC_biomass_2000_unmasked]

    # List of output directories and output file name patterns
    output_dir_list = [cn.model_extent_dir]
    output_pattern_list = [cn.pattern_model_extent]


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
    if cn.RUN_DATE is not None and cn.NO_UPLOAD is False:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, cn.RUN_DATE)

    # Creates a single filename pattern to pass to the multiprocessor call
    pattern = output_pattern_list[0]

    if cn.SINGLE_PROCESSOR:
        for tile_id in tile_id_list:
            model_extent.model_extent(tile_id, pattern)
    else:
        # This configuration of the multiprocessing call is necessary for passing multiple arguments to the main function
        # It is based on the example here: http://spencerimp.blogspot.com/2015/12/python-multiprocess-with-multiple.html
        if cn.count == 96:
            if cn.SENSIT_TYPE == 'biomass_swap':
                processes = 38
            else:
                processes = 45 # 30 processors = 480 GB peak (sporadic decreases followed by sustained increases);
                # 36 = 550 GB peak; 40 = 590 GB peak; 42 = 631 GB peak; 43 = 690 GB peak; 45 = too high
        else:
            processes = 3
        uu.print_log('Model extent processors=', processes)
        with multiprocessing.Pool(processes) as pool:
            pool.map(partial(model_extent.model_extent, pattern=pattern), tile_id_list)
            pool.close()
            pool.join()


    # No single-processor versions of these check-if-empty functions
    output_pattern = output_pattern_list[0]
    if cn.count <= 2:  # For local tests
        processes = 1
        uu.print_log(f'Checking for empty tiles of {output_pattern} pattern with {output_pattern} processors using light function...')
        with multiprocessing.Pool(processes) as pool:
            pool.map(partial(uu.check_and_delete_if_empty_light, output_pattern=output_pattern), tile_id_list)
            pool.close()
            pool.join()
    else:
        processes = 58  # 50 processors = 620 GB peak; 55 = 640 GB; 58 = 650 GB (continues to increase very slowly several hundred tiles in)
        uu.print_log(f'Checking for empty tiles of {output_pattern} pattern with {output_pattern} processors...')
        with multiprocessing.Pool(processes) as pool:
            pool.map(partial(uu.check_and_delete_if_empty, output_pattern=output_pattern), tile_id_list)
            pool.close()
            pool.join()


    # If no_upload flag is not activated (by choice or by lack of AWS credentials), output is uploaded
    if not cn.NO_UPLOAD:
        uu.upload_final_set(output_dir_list[0], output_pattern_list[0])


if __name__ == '__main__':

    # The arguments for what kind of model run is being run (standard conditions or a sensitivity analysis) and
    # the tiles to include
    parser = argparse.ArgumentParser(
        description='Create tiles of the pixels included in the model (model extent)')
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

    mp_model_extent(tile_id_list=tile_id_list)
