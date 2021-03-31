'''
Script to create three supplementary tiled outputs for each main model output (gross emissions, gross removals, net flux),
which are already in per hectare values for full model extent:
1. per pixel values for full model extent (all pixels included in model extent)
2. per hectare values for forest extent (within the model extent, pixels that have TCD>30 OR Hansen gain OR mangrove biomass)
3. per pixel values for forest extent
The forest extent outputs are for sharing with partners because they limit the model to just the relevant pixels
(those within forests).
Forest extent is defined in the methods section of Harris et al. 2021 Nature Climate Change.
It is roughly implemented in mp_model_extent.py but using TCD>0 rather thant TCD>30. Here, the TCD>30 requirement
is implemented instead as a subset of the full model extent pixels.
Forest extent is: ((TCD2000>30 AND WHRC AGB2000>0) OR Hansen gain=1 OR mangrove AGB2000>0) NOT IN pre-2000 plantations.
The WHRC AGB2000 and pre-2000 plantations conditions were set in mp_model_extent.py, so they don't show up here.
'''


import multiprocessing
from subprocess import Popen, PIPE, STDOUT, check_call
from functools import partial
import datetime
import argparse
import os
import glob
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu
sys.path.append(os.path.join(cn.docker_app,'analyses'))
import create_supplementary_outputs

def mp_create_supplementary_outputs(sensit_type, tile_id_list, run_date = None, no_upload = None):

    os.chdir(cn.docker_base_dir)

    tile_id_list_outer = tile_id_list

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list_outer == 'all':
        # List of tiles to run in the model
        tile_id_list_outer = uu.tile_list_s3(cn.net_flux_dir, sensit_type)

    uu.print_log(tile_id_list_outer)
    uu.print_log("There are {} tiles to process".format(str(len(tile_id_list_outer))) + "\n")


    # Files to download for this script
    download_dict = {
        cn.cumul_gain_AGCO2_BGCO2_all_types_dir: [cn.pattern_cumul_gain_AGCO2_BGCO2_all_types],
        cn.gross_emis_all_gases_all_drivers_biomass_soil_dir: [cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil],
        cn.net_flux_dir: [cn.pattern_net_flux]
    }

    # List of output directories and output file name patterns.
    # Outputs must be in the same order as the download dictionary above, and then follow the same order for all outputs.
    # Currently, it's: per pixel full extent, per hectare forest extent, per pixel forest extent.
    output_dir_list = [
                        cn.cumul_gain_AGCO2_BGCO2_all_types_per_pixel_full_extent_dir,
                        cn.cumul_gain_AGCO2_BGCO2_all_types_forest_extent_dir,
                        cn.cumul_gain_AGCO2_BGCO2_all_types_per_pixel_forest_extent_dir,
                        cn.gross_emis_all_gases_all_drivers_biomass_soil_per_pixel_full_extent_dir,
                        cn.gross_emis_all_gases_all_drivers_biomass_soil_forest_extent_dir,
                        cn.gross_emis_all_gases_all_drivers_biomass_soil_per_pixel_forest_extent_dir,
                        cn.net_flux_per_pixel_full_extent_dir,
                        cn.net_flux_forest_extent_dir,
                        cn.net_flux_per_pixel_forest_extent_dir]
    output_pattern_list = [
                            cn.pattern_cumul_gain_AGCO2_BGCO2_all_types_per_pixel_full_extent,
                            cn.pattern_cumul_gain_AGCO2_BGCO2_all_types_forest_extent,
                            cn.pattern_cumul_gain_AGCO2_BGCO2_all_types_per_pixel_forest_extent,
                            cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil_per_pixel_full_extent,
                            cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil_forest_extent,
                            cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil_per_pixel_forest_extent,
                            cn.pattern_net_flux_per_pixel_full_extent,
                            cn.pattern_net_flux_forest_extent,
                            cn.pattern_net_flux_per_pixel_forest_extent
    ]


    # Pixel area tiles-- necessary for calculating per pixel values
    uu.s3_flexible_download(cn.pixel_area_dir, cn.pattern_pixel_area, cn.docker_base_dir, sensit_type, tile_id_list_outer)
    # Tree cover density, Hansen gain, and mangrove biomass tiles-- necessary for masking to forest extent
    uu.s3_flexible_download(cn.tcd_dir, cn.pattern_tcd, cn.docker_base_dir, sensit_type, tile_id_list_outer)
    uu.s3_flexible_download(cn.gain_dir, cn.pattern_gain, cn.docker_base_dir, sensit_type, tile_id_list_outer)
    uu.s3_flexible_download(cn.mangrove_biomass_2000_dir, cn.pattern_mangrove_biomass_2000, cn.docker_base_dir, sensit_type, tile_id_list_outer)

    uu.print_log("Model outputs to process are:", download_dict)

    # If the model run isn't the standard one, the output directory is changed
    if sensit_type != 'std':
        uu.print_log("Changing output directory and file name pattern based on sensitivity analysis")
        output_dir_list = uu.alter_dirs(sensit_type, output_dir_list)

    # A date can optionally be provided by the full model script or a run of this script.
    # This replaces the date in constants_and_names.
    if run_date is not None:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, run_date)


    # Iterates through input tile sets
    for key, values in download_dict.items():

        # Sets the directory and pattern for the input being processed
        input_dir = key
        input_pattern = values[0]

        # If a full model run is specified, the correct set of tiles for the particular script is listed.
        # A new list is named so that tile_id_list stays as the command line argument.
        if tile_id_list == 'all':
            # List of tiles to run in the model
            tile_id_list_input = uu.tile_list_s3(input_dir, sensit_type)
        else:
            tile_id_list_input = tile_id_list_outer

        uu.print_log(tile_id_list_input)
        uu.print_log("There are {} tiles to process".format(str(len(tile_id_list_input))) + "\n")

        uu.print_log("Downloading tiles from", input_dir)
        uu.s3_flexible_download(input_dir, input_pattern, cn.docker_base_dir, sensit_type, tile_id_list_input)

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
            uu.exception_log(no_upload, "No output patterns found for input pattern. Please check.")

        uu.print_log("Input pattern:", input_pattern)
        uu.print_log("Output patterns:", output_patterns)

        # Gross removals: 20 processors = >740 GB peak; 15 = 570 GB peak; 17 = 660 GB peak; 18 = 670 GB peak
        # Gross emissions: 17 processors = 660 GB peak; 18 = 710 GB peak
        if cn.count == 96:
            processes = 18
        else:
            processes = 2
        uu.print_log("Creating derivative outputs for {0} with {1} processors...".format(input_pattern, processes))
        pool = multiprocessing.Pool(processes)
        pool.map(partial(create_supplementary_outputs.create_supplementary_outputs, input_pattern=input_pattern,
                         output_patterns=output_patterns, sensit_type=sensit_type, no_upload=no_upload), tile_id_list_input)
        pool.close()
        pool.join()

        # # For single processor use
        # for tile_id in tile_id_list_input:
        #     create_supplementary_outputs.create_supplementary_outputs(tile_id, input_pattern, output_patterns, sensit_type, no_upload)

        # Checks the two forest extent output tiles created from each input tile for whether there is data in them.
        # Because the extent is restricted in the forest extent pixels, some tiles with pixels in the full extent
        # version may not have pixels in the forest extent version.
        for output_pattern in output_patterns[1:3]:
            if cn.count <= 2:  # For local tests
                processes = 1
                uu.print_log("Checking for empty tiles of {0} pattern with {1} processors using light function...".format(output_pattern, processes))
                pool = multiprocessing.Pool(processes)
                pool.map(partial(uu.check_and_delete_if_empty_light, output_pattern=output_pattern), tile_id_list_input)
                pool.close()
                pool.join()
            else:
                processes = 55  # 50 processors = 560 GB peak for gross removals; 55 = XXX GB peak
                uu.print_log("Checking for empty tiles of {0} pattern with {1} processors...".format(output_pattern, processes))
                pool = multiprocessing.Pool(processes)
                pool.map(partial(uu.check_and_delete_if_empty, output_pattern=output_pattern), tile_id_list_input)
                pool.close()
                pool.join()


    # If no_upload flag is not activated, output is uploaded
    if not no_upload:

        for i in range(0, len(output_dir_list)):
            uu.upload_final_set(output_dir_list[i], output_pattern_list[i])


if __name__ == '__main__':

    # The argument for what kind of model run is being done: standard conditions or a sensitivity analysis run
    parser = argparse.ArgumentParser(
        description='Create tiles of the number of years of carbon gain for mangrove forests')
    parser.add_argument('--model-type', '-t', required=True,
                        help='{}'.format(cn.model_type_arg_help))
    parser.add_argument('--tile_id_list', '-l', required=True,
                        help='List of tile ids to use in the model. Should be of form 00N_110E or 00N_110E,00N_120E or all.')
    parser.add_argument('--run-date', '-d', required=False,
                        help='Date of run. Must be format YYYYMMDD.')
    parser.add_argument('--no-upload', '-nu', action='store_true',
                       help='Disables uploading of outputs to s3')
    args = parser.parse_args()
    sensit_type = args.model_type
    tile_id_list = args.tile_id_list
    run_date = args.run_date
    no_upload = args.no_upload

    # Create the output log
    uu.initiate_log(tile_id_list=tile_id_list, sensit_type=sensit_type, run_date=run_date, no_upload=no_upload)

    # Checks whether the sensitivity analysis and tile_id_list arguments are valid
    uu.check_sensit_type(sensit_type)
    tile_id_list = uu.tile_id_list_check(tile_id_list)

    mp_create_supplementary_outputs(sensit_type=sensit_type, tile_id_list=tile_id_list,
                                    run_date=run_date, no_upload=no_upload)