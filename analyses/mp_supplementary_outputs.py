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

    # Checks whether the canopy cover argument is valid
    if cn.THRESH < 0 or cn.THRESH > 99:
        uu.exception_log('Invalid tcd. Please provide an integer between 0 and 99.')

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list_outer == 'all':
        # List of tiles to run in the model
        tile_id_list_outer = uu.tile_list_s3(cn.net_flux_dir, cn.SENSIT_TYPE)

    uu.print_log(tile_id_list_outer)
    uu.print_log(f'There are {str(len(tile_id_list_outer))} tiles to process', "\n")

    # Files to download for this script
    download_dict = {
        cn.annual_gain_AGC_all_types_dir: [cn.pattern_annual_gain_AGC_all_types],
        cn.cumul_gain_AGCO2_BGCO2_all_types_dir: [cn.pattern_cumul_gain_AGCO2_BGCO2_all_types],
        cn.gross_emis_all_gases_all_drivers_biomass_soil_dir: [cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil],
        cn.net_flux_dir: [cn.pattern_net_flux]
        # cn.pixel_area_rewindow_dir: [cn.pattern_pixel_area_rewindow],
        # cn.tcd_rewindow_dir: [cn.pattern_tcd_rewindow],
        # cn.gain_rewindow_dir: [cn.pattern_gain_rewindow],
        # cn.mangrove_biomass_2000_rewindow_dir: [cn.pattern_mangrove_biomass_2000_rewindow],
        # cn.pixel_area_dir: [cn.pattern_pixel_area],
        # cn.tcd_dir: [cn.pattern_tcd],
        # cn.gain_dir: [cn.pattern_gain],
        # cn.mangrove_biomass_2000_dir: [cn.pattern_mangrove_biomass_2000]
        }

    uu.print_log(f'Model outputs to process are: {download_dict}')

    # List of output directories and output file name patterns.
    # Outputs must be in the same order as the download dictionary above, and then follow the same order for all outputs.
    # Currently, it's: per pixel full extent, per hectare forest extent, per pixel forest extent, aggregated output (has no pattern).
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
    for input_dir, download_pattern in download_dict.items():

        download_pattern_name = download_pattern[0]

        # If a full model run is specified, the correct set of tiles for the particular script is listed.
        # A new list is named so that tile_id_list stays as the command line argument.
        if tile_id_list == 'all':
            # List of tiles to run in the model
            tile_id_list_input = uu.tile_list_s3(input_dir, cn.SENSIT_TYPE)
        else:
            tile_id_list_input = tile_id_list_outer

        uu.print_log(tile_id_list_input)
        uu.print_log(f'There are {str(len(tile_id_list_input))} tiles to process', "\n")

        # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
        uu.print_log(f'Downloading tiles from {input_dir}')
        uu.s3_flexible_download(input_dir, download_pattern_name, cn.docker_base_dir, cn.SENSIT_TYPE, tile_id_list_input)






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
    parser.add_argument('--tcd-threshold', '-tcd', required=False, default=cn.canopy_threshold,
                        help='Tree cover density threshold above which pixels will be included in the aggregation. Default is 30.')
    parser.add_argument('--std-net-flux-aggreg', '-sagg', required=False,
                        help='The s3 standard model net flux aggregated tif, for comparison with the sensitivity analysis map')
    parser.add_argument('--no-upload', '-nu', action='store_true',
                       help='Disables uploading of outputs to s3')
    args = parser.parse_args()

    # Sets global variables to the command line arguments
    cn.SENSIT_TYPE = args.model_type
    cn.RUN_DATE = args.run_date
    cn.NO_UPLOAD = args.no_upload
    cn.STD_NET_FLUX = args.std_net_flux_aggreg
    cn.THRESH = int(args.tcd_threshold)

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