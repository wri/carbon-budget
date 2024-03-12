"""
Creates tiles of the number of years in which carbon removals occur during the model duration (2001 to 2020 currently).
It is based on the annual Hansen loss data and the 2000-2012 Hansen gain data.
First it separately calculates rasters of gain years for model pixels that had loss-only,
gain-only, neither loss nor gain, and both loss-and-gain.
The gain years for each of these conditions are calculated according to rules that are found in the function called by the multiprocessor commands.
The same gain year count rules are applied to all types of forest (mangrove, planted, etc.).
Then it combines those four rasters into a single gain year raster for each tile using rasterio because
summing the arrays using rasterio is faster and uses less memory than combining them with gdalmerge.
If different input rasters for loss (e.g., 2001-2017) and gain (e.g., 2000-2018) are used, the year count constants in constants_and_names.py must be changed.

python -m removals.mp_gain_year_count_all_forest_types -t std -l 00N_000E -nu
python -m removals.mp_gain_year_count_all_forest_types -t std -l all
"""

import argparse
from functools import partial
import multiprocessing
import os
import sys

import constants_and_names as cn
import universal_util as uu
from . import gain_year_count_all_forest_types

def mp_gain_year_count_all_forest_types(tile_id_list):
    """
    :param tile_id_list: list of tile ids to process
    :return: 5 sets of tiles that show the estimated years of carbon accumulation.
        The only one used later in the model is the combined one. The other four are for QC.
        Units: years.
    """

    os.chdir(cn.docker_tile_dir)

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # No point in making gain year count tiles for tiles that don't have annual removals
        tile_id_list = uu.tile_list_s3(cn.annual_gain_AGC_all_types_dir, cn.SENSIT_TYPE)

    uu.print_log(tile_id_list)
    uu.print_log(f'There are {str(len(tile_id_list))} tiles to process', "\n")

    # Files to download for this script. 'true'/'false' says whether the input directory and pattern should be
    # changed for a sensitivity analysis. This does not need to change based on what run is being done;
    # this assignment should be true for all sensitivity analyses and the standard model.
    download_dict = {
        cn.gain_dir: [cn.pattern_data_lake],
        cn.model_extent_dir: [cn.pattern_model_extent]
    }

    # Adds the correct loss tile to the download dictionary depending on the model run
    if cn.SENSIT_TYPE == 'legal_Amazon_loss':
        download_dict[cn.Brazil_annual_loss_processed_dir] = [cn.pattern_Brazil_annual_loss_processed]
    elif cn.SENSIT_TYPE == 'Mekong_loss':
        download_dict[cn.Mekong_loss_processed_dir] = [cn.pattern_Mekong_loss_processed]
    else:
        download_dict[cn.loss_dir] = [cn.pattern_loss]


    output_dir_list = [cn.gain_year_count_dir]
    output_pattern_list = [cn.pattern_gain_year_count]


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

    # Creates a single filename pattern to pass to the multiprocessor call
    pattern = output_pattern_list[0]


    if cn.SINGLE_PROCESSOR:

        for tile_id in tile_id_list:
            gain_year_count_all_forest_types.create_gain_year_count_loss_only(tile_id)

        for tile_id in tile_id_list:
            if cn.SENSIT_TYPE == 'maxgain':
                gain_year_count_all_forest_types.create_gain_year_count_gain_only_maxgain(tile_id)
            else:
                gain_year_count_all_forest_types.create_gain_year_count_gain_only_standard(tile_id)

        for tile_id in tile_id_list:
            gain_year_count_all_forest_types.create_gain_year_count_no_change_standard(tile_id)

        for tile_id in tile_id_list:
            if cn.SENSIT_TYPE == 'maxgain':
                gain_year_count_all_forest_types.create_gain_year_count_loss_and_gain_maxgain(tile_id)
            else:
                gain_year_count_all_forest_types.create_gain_year_count_loss_and_gain_standard(tile_id)

        for tile_id in tile_id_list:
            gain_year_count_all_forest_types.create_gain_year_count_merge(tile_id, pattern)

    else:

        # Creates gain year count tiles using only pixels that had only loss
        if cn.count == 96:
            processes = 70   # 90>=740 GB peak; 70=610 GB peak
        else:
            processes = int(cn.count/2)
        uu.print_log(f'Gain year count loss-only pixels max processors={processes}')
        with multiprocessing.Pool(processes) as pool:
            pool.map(partial(gain_year_count_all_forest_types.create_gain_year_count_loss_only),
                     tile_id_list)
            pool.close()
            pool.join()

        # Creates gain year count tiles using only pixels that had only gain
        if cn.count == 96:
            processes = 90   # 66 = 330 GB peak; 75 = 380 GB peak; 90 = 530 GB peak
        else:
            processes = int(cn.count/2)
        uu.print_log(f'Gain year count gain-only pixels max processors={processes}')
        with multiprocessing.Pool(processes) as pool:
            if cn.SENSIT_TYPE == 'maxgain':
                pool.map(partial(gain_year_count_all_forest_types.create_gain_year_count_gain_only_maxgain),
                         tile_id_list)
            elif cn.SENSIT_TYPE == 'legal_Amazon_loss':
                uu.print_log('Gain-only pixels do not apply to legal_Amazon_loss sensitivity analysis. Skipping this step.')
            else:
                pool.map(partial(gain_year_count_all_forest_types.create_gain_year_count_gain_only_standard),
                         tile_id_list)
            pool.close()
            pool.join()

        # Creates gain year count tiles using only pixels that had neither loss nor gain pixels
        if cn.count == 96:
            processes = 50   # 66 = 360 GB peak; 88 = 430 GB peak; 90 = 510 GB peak
        else:
            processes = int(cn.count/2)
        uu.print_log(f'Gain year count no-change pixels max processors={processes}')
        with multiprocessing.Pool(processes) as pool:
            if cn.SENSIT_TYPE == 'legal_Amazon_loss':
                pool.map(partial(gain_year_count_all_forest_types.create_gain_year_count_no_change_legal_Amazon_loss),
                         tile_id_list)
            else:
                pool.map(partial(gain_year_count_all_forest_types.create_gain_year_count_no_change_standard),
                         tile_id_list)
            pool.close()
            pool.join()

        # Creates gain year count tiles using only pixels that had both loss and gain
        if cn.count == 96:
            processes = 50   # 66 = 370 GB peak; 88 = 430 GB peak; 90 = 550 GB peak
        else:
            processes = int(cn.count/2)
        uu.print_log(f'Gain year count loss & gain pixels max processors={processes}')
        with multiprocessing.Pool(processes) as pool:
            if cn.SENSIT_TYPE == 'maxgain':
                pool.map(partial(gain_year_count_all_forest_types.create_gain_year_count_loss_and_gain_maxgain),
                         tile_id_list)
            else:
                pool.map(partial(gain_year_count_all_forest_types.create_gain_year_count_loss_and_gain_standard),
                         tile_id_list)
            pool.close()
            pool.join()

        # Combines the four above gain year count tiles for each Hansen tile into a single output tile
        if cn.count == 96:
            processes = 50   # 28 processors = 220 GB peak; 62 = 470 GB peak; 78 = 600 GB peak; 80 = 620 GB peak; 84 = 630 GB peak
        elif cn.count < 4:
            processes = 1
        else:
            processes = int(cn.count/4)
        uu.print_log(f'Gain year count gain merge all combos max processors={processes}')
        with multiprocessing.Pool(processes) as pool:
            pool.map(partial(gain_year_count_all_forest_types.create_gain_year_count_merge, pattern=pattern),
                     tile_id_list)
            pool.close()
            pool.join()


    # If cn.NO_UPLOAD flag is not activated (by choice or by lack of AWS credentials), output is uploaded
    if not cn.NO_UPLOAD:

        # Intermediate output tiles for checking outputs
        uu.upload_final_set(output_dir_list[0], "gain_year_count_loss_only")
        uu.upload_final_set(output_dir_list[0], "gain_year_count_gain_only")
        uu.upload_final_set(output_dir_list[0], "gain_year_count_no_change")
        uu.upload_final_set(output_dir_list[0], "gain_year_count_loss_and_gain")

        # This is the final output used later in the model
        uu.upload_final_set(output_dir_list[0], output_pattern_list[0])


if __name__ == '__main__':

    # The arguments for what kind of model run is being run (standard conditions or a sensitivity analysis) and
    # the tiles to include
    parser = argparse.ArgumentParser(
        description='Create tiles of number of years in which removals occurred during the model period')
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

    mp_gain_year_count_all_forest_types(tile_id_list)
