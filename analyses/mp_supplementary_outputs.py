"""

"""

import multiprocessing
from functools import partial
import datetime
import argparse
import os
import glob
import sys

import constants_and_names as cn
import universal_util as uu

from . import supplementary_outputs

def mp_supplementary_outputs(tile_id_list_outer):
    """
    :param tile_id_list:
    :param thresh:
    :param std_net_flux:
    :return:
    """

    os.chdir(cn.docker_base_dir)

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list_outer == 'all':
        # List of tiles to run in the model
        tile_id_list_outer = uu.tile_list_s3(cn.net_flux_dir, cn.SENSIT_TYPE)

    uu.print_log(tile_id_list_outer)
    uu.print_log(f'There are {str(len(tile_id_list_outer))} tiles to process', "\n")

    # Files to be processed for this script
    download_dict = {
        # cn.cumul_gain_AGCO2_BGCO2_all_types_dir: [cn.pattern_cumul_gain_AGCO2_BGCO2_all_types],
        cn.gross_emis_all_gases_all_drivers_biomass_soil_dir: [cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil]
        # cn.net_flux_dir: [cn.pattern_net_flux]
        }

    uu.print_log(f'Model outputs to process are: {download_dict}')

    # List of output directories and output file name patterns.
    # Outputs must be in the same order as the download dictionary above, and then follow the same order for all outputs.
    # Currently, it's: per pixel full extent, per hectare forest extent, per pixel forest extent.
    # Aggregated output comes at the end and has no corresponding pattern.
    output_dir_list = [
                        cn.cumul_gain_AGCO2_BGCO2_all_types_per_pixel_full_extent_dir,
                        cn.cumul_gain_AGCO2_BGCO2_all_types_forest_extent_dir,
                        cn.cumul_gain_AGCO2_BGCO2_all_types_per_pixel_forest_extent_dir,
                        cn.gross_emis_all_gases_all_drivers_biomass_soil_per_pixel_full_extent_dir,
                        cn.gross_emis_all_gases_all_drivers_biomass_soil_forest_extent_dir,
                        cn.gross_emis_all_gases_all_drivers_biomass_soil_per_pixel_forest_extent_dir,
                        cn.net_flux_per_pixel_full_extent_dir,
                        cn.net_flux_forest_extent_dir,
                        cn.net_flux_per_pixel_forest_extent_dir,
                        cn.output_aggreg_dir]
    output_pattern_list = [
                            cn.pattern_cumul_gain_AGCO2_BGCO2_all_types_per_pixel_full_extent,
                            cn.pattern_cumul_gain_AGCO2_BGCO2_all_types_forest_extent,
                            cn.pattern_cumul_gain_AGCO2_BGCO2_all_types_per_pixel_forest_extent,
                            cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil_per_pixel_full_extent,
                            cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil_forest_extent,
                            cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil_per_pixel_forest_extent,
                            cn.pattern_net_flux_per_pixel_full_extent,
                            cn.pattern_net_flux_forest_extent,
                            cn.pattern_net_flux_per_pixel_forest_extent]

    # If the model run isn't the standard one, the output directory is changed
    if cn.SENSIT_TYPE != 'std':
        uu.print_log('Changing output directory and file name pattern based on sensitivity analysis')
        output_dir_list = uu.alter_dirs(cn.SENSIT_TYPE, output_dir_list)

    # A date can optionally be provided by the full model script or a run of this script.
    # This replaces the date in constants_and_names.
    # Only done if output upload is enabled.
    if cn.RUN_DATE is not None and cn.NO_UPLOAD is not None:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, cn.RUN_DATE)

    # Pixel area tiles-- necessary for calculating sum of pixels for any set of tiles
    uu.s3_flexible_download(cn.pixel_area_rewindow_dir, cn.pattern_pixel_area_rewindow, cn.docker_base_dir, cn.SENSIT_TYPE, tile_id_list)
    # Tree cover density, Hansen gain, and mangrove biomass tiles-- necessary for filtering sums to model extent
    uu.s3_flexible_download(cn.tcd_rewindow_dir, cn.pattern_tcd_rewindow, cn.docker_base_dir, cn.SENSIT_TYPE, tile_id_list)
    uu.s3_flexible_download(cn.gain_rewindow_dir, cn.pattern_gain_rewindow, cn.docker_base_dir, cn.SENSIT_TYPE, tile_id_list)
    uu.s3_flexible_download(cn.mangrove_biomass_2000_rewindow_dir, cn.pattern_mangrove_biomass_2000_rewindow, cn.docker_base_dir, cn.SENSIT_TYPE, tile_id_list)

    # Pixel area tiles-- necessary for calculating per pixel values
    uu.s3_flexible_download(cn.pixel_area_dir, cn.pattern_pixel_area, cn.docker_base_dir, cn.SENSIT_TYPE, tile_id_list_outer)
    # Tree cover density, Hansen gain, and mangrove biomass tiles-- necessary for masking to forest extent
    uu.s3_flexible_download(cn.tcd_dir, cn.pattern_tcd, cn.docker_base_dir, cn.SENSIT_TYPE, tile_id_list_outer)
    uu.s3_flexible_download(cn.gain_dir, cn.pattern_gain, cn.docker_base_dir, cn.SENSIT_TYPE, tile_id_list_outer)
    uu.s3_flexible_download(cn.mangrove_biomass_2000_dir, cn.pattern_mangrove_biomass_2000, cn.docker_base_dir, cn.SENSIT_TYPE, tile_id_list_outer)

    # Iterates through the types of tiles to be processed
    for input_dir, download_pattern_name in download_dict.items():

        input_pattern = download_pattern_name[0]

        # If a full model run is specified, the correct set of tiles for the particular script is listed.
        # A new list is named so that tile_id_list stays as the command line argument.
        if tile_id_list == 'all':
            # List of tiles to run in the model
            tile_id_list_inner = uu.tile_list_s3(input_dir, cn.SENSIT_TYPE)
        else:
            tile_id_list_inner = tile_id_list_outer

        uu.print_log(tile_id_list_inner)
        uu.print_log(f'There are {str(len(tile_id_list_inner))} tiles to process for pattern {input_pattern}', "\n")
        uu.print_log(f'Processing: {input_dir}; {input_pattern}')

        # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
        uu.print_log(f'Downloading tiles from {input_dir}')
        uu.s3_flexible_download(input_dir, input_pattern, cn.docker_base_dir, cn.SENSIT_TYPE, tile_id_list_inner)

        # Blank list of output patterns, populated below
        output_patterns = []

        # Matches the output patterns with the input pattern.
        # This requires that the output patterns be grouped by input pattern and be in the order described in
        # the comment above.
        if "gross_removals" in input_pattern:
            output_patterns = output_pattern_list[0:3]
        elif "gross_emis" in input_pattern:
            output_patterns = output_pattern_list[3:6]
        elif "net_flux" in input_pattern:
            output_patterns = output_pattern_list[6:9]
        else:
            uu.exception_log('No output patterns found for input pattern. Please check.')

        uu.print_log(f'Input pattern: {input_pattern}')
        uu.print_log(f'Output patterns: {output_patterns}')


        # # Creates the per-pixel and forest extent supplementary outputs
        # if cn.SINGLE_PROCESSOR:
        #     for tile_id in tile_id_list_inner:
        #         supplementary_outputs.supplementary_outputs(tile_id, input_pattern, output_patterns)
        #
        # else:
        #     # Gross removals: 20 processors = >740 GB peak; 15 = 570 GB peak; 17 = 660 GB peak; 18 = 670 GB peak
        #     # Gross emissions: 17 processors = 660 GB peak; 18 = 710 GB peak
        #     if cn.count == 96:
        #         processes = 18
        #     else:
        #         processes = 2
        #     uu.print_log(f'Creating derivative outputs for {input_pattern} with {processes} processors...')
        #     pool = multiprocessing.Pool(processes)
        #     pool.map(partial(supplementary_outputs.supplementary_outputs, input_pattern=input_pattern,
        #                      output_patterns=output_patterns),
        #              tile_id_list_inner)
        #     pool.close()
        #     pool.join()



        # # Converts the 10x10 degree Hansen tiles that are in windows of 40000x1 pixels to windows of 160x160 pixels,
        # # which is the resolution of the output tiles. This will allow the 30x30 m pixels in each window to be summed.
        download_pattern_name = output_patterns[2]
        #
        # if cn.SINGLE_PROCESSOR:
        #     for tile_id in tile_id_list_inner:
        #         uu.rewindow(tile_id, download_pattern_name)
        #
        # else:
        #     if cn.count == 96:
        #         if cn.SENSIT_TYPE == 'biomass_swap':
        #             processes = 12  # 12 processors = XXX GB peak
        #         else:
        #             processes = 16  # 16 processors = XXX GB peak
        #     else:
        #         processes = 8
        #     uu.print_log(f'Rewindow max processors= {processes}')
        #     pool = multiprocessing.Pool(processes)
        #     pool.map(partial(uu.rewindow, download_pattern_name=download_pattern_name),
        #              tile_id_list_inner)
        #     pool.close()
        #     pool.join()



        # Converts the existing (per ha) values to per pixel values (e.g., emissions/ha to emissions/pixel)
        # and sums those values in each 160x160 pixel window.
        # The sum for each 160x160 pixel window is stored in a 2D array, which is then converted back into a raster at
        # 0.04x0.04 degree resolution (approximately 10m in the tropics).
        # Each pixel in that raster is the sum of the 30m pixels converted to value/pixel (instead of value/ha).
        # The 0.04x0.04 degree tile is output.
        # For multiprocessor use. This used about 450 GB of memory with count/2, it's okay on an r4.16xlarge
        if cn.SINGLE_PROCESSOR:
            for tile_id in tile_id_list_inner:
                supplementary_outputs.aggregate(tile_id, download_pattern_name)

        else:
            if cn.count == 96:
                if cn.SENSIT_TYPE == 'biomass_swap':
                    processes = 10  # 10 processors = XXX GB peak
                else:
                    processes = 12  # 16 processors = 180 GB peak; 16 = XXX GB peak; 20 = >750 GB (maxed out)
            else:
                processes = 8
            uu.print_log(f'Conversion to per pixel and aggregate max processors={processes}')
            pool = multiprocessing.Pool(processes)
            pool.map(partial(supplementary_outputs.aggregate, download_pattern_name=download_pattern_name),
                     tile_id_list_inner)
            pool.close()
            pool.join()








if __name__ == '__main__':

    # The argument for what kind of model run is being done: standard conditions or a sensitivity analysis run
    parser = argparse.ArgumentParser(
        description='Create supplementary outputs: aggregated maps, per-pixel at original resolution, forest-only at original resolution')
    parser.add_argument('--model-type', '-t', required=True,
                        help=f'{cn.model_type_arg_help}')
    parser.add_argument('--tile_id_list', '-l', required=True,
                        help='List of tile ids to use in the model. Should be of form 00N_110E or 00N_110E,00N_120E or all.')
    parser.add_argument('--run-date', '-d', required=False,
                        help='Date of run. Must be format YYYYMMDD.')
    parser.add_argument('--std-net-flux-aggreg', '-sagg', required=False,
                        help='The s3 standard model net flux aggregated tif, for comparison with the sensitivity analysis map')
    parser.add_argument('--no-upload', '-nu', action='store_true',
                       help='Disables uploading of outputs to s3')
    parser.add_argument('--single-processor', '-sp', action='store_true',
                       help='Uses single processing rather than multiprocessing')
    args = parser.parse_args()

    # Sets global variables to the command line arguments
    cn.SENSIT_TYPE = args.model_type
    cn.RUN_DATE = args.run_date
    cn.NO_UPLOAD = args.no_upload
    cn.STD_NET_FLUX = args.std_net_flux_aggreg
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

    mp_supplementary_outputs(tile_id_list)