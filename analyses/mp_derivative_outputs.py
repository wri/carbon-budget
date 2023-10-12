"""
Final step of the flux model. This creates various derivative outputs which are used on the GFW platform and for
supplemental analyses. Derivative outputs for gross emissions, gross removals, and net flux at 0.00025x0.000025 deg
resolution for full model extent (all pixels included in mp_model_extent.py):
1. Full extent flux Mg per pixel at 0.00025x0.00025 deg (all pixels included in mp_model_extent.py)
2. Forest extent flux Mg per hectare at 0.00025x0.00025 deg (forest extent defined below)
3. Forest extent flux Mg per pixel at 0.00025x0.00025 deg (forest extent defined below)
4. Forest extent flux Mt at 0.04x0.04 deg (aggregated output, ~ 4x4 km at equator)
For sensitivity analyses only:
5. Percent difference between standard model and sensitivity analysis for aggregated map
6. Pixels with sign changes between standard model and sensitivity analysis for aggregated map

The forest extent outputs are for sharing with partners because they limit the model to just the relevant pixels
(those within forests, as defined below).
Forest extent is defined in the methods section of Harris et al. 2021 Nature Climate Change:
within the model extent, pixels that have TCD>30 OR Hansen gain OR mangrove biomass.
More formally, forest extent is:
((TCD2000>30 AND WHRC AGB2000>0) OR Hansen gain=1 OR mangrove AGB2000>0) NOT IN pre-2000 plantations.
The WHRC AGB2000 condition was set in mp_model_extent.py, so it doesn't show up here.

python -m analyses.mp_derivative_outputs -t std -l 00N_000E -nu
python -m analyses.mp_derivative_outputs -t std -l all
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

from . import derivative_outputs

def mp_derivative_outputs(tile_id_list):
    """
    :param tile_id_list: list of tile ids to process
    :return: derivative outputs at native and aggregated resolution for emissions, removals, and net flux
    """

    os.chdir(cn.docker_tile_dir)

    # Keeps tile_id_list as its own variable for referencing in the tile set for loop
    tile_id_list_outer = tile_id_list

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list_outer == 'all':
        # List of tiles to run in the model
        tile_id_list_outer = uu.tile_list_s3(cn.net_flux_dir, cn.SENSIT_TYPE)

    uu.print_log(tile_id_list_outer)
    uu.print_log(f'There are {str(len(tile_id_list_outer))} tiles to process', "\n")

    # Tile sets to be processed for this script. The three main outputs from the model.
    download_dict = {
        cn.cumul_gain_AGCO2_BGCO2_all_types_dir: [cn.pattern_cumul_gain_AGCO2_BGCO2_all_types],
        cn.gross_emis_all_gases_all_drivers_biomass_soil_dir: [cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil],
        cn.net_flux_dir: [cn.pattern_net_flux]
        }

    uu.print_log(f'Model outputs to process are: {download_dict}')

    # List of output directories and output file name patterns.
    # Outputs must be in the same order as the download dictionary above, and then follow the following order for all outputs:
    # per pixel full extent, per hectare forest extent, per pixel forest extent.
    # Aggregated output comes at the end.
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
                            cn.pattern_net_flux_per_pixel_forest_extent,
                            f'tcd{cn.canopy_threshold}_{cn.pattern_aggreg}']

    # If the model run isn't the standard one, the output directory is changed
    if cn.SENSIT_TYPE != 'std':
        uu.print_log('Changing output directory and file name pattern based on sensitivity analysis')
        output_dir_list = uu.alter_dirs(cn.SENSIT_TYPE, output_dir_list)

    # A date can optionally be provided by the full model script or a run of this script.
    # This replaces the date in constants_and_names.
    # Only done if output upload is enabled.
    if cn.RUN_DATE is not None and cn.NO_UPLOAD is not None:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, cn.RUN_DATE)

    # Pixel area tiles-- necessary for calculating per pixel values
    uu.s3_flexible_download(cn.pixel_area_dir, cn.pattern_pixel_area, cn.docker_tile_dir, cn.SENSIT_TYPE, tile_id_list_outer)
    # Tree cover density, Hansen gain, and mangrove biomass tiles-- necessary for masking to forest extent
    uu.s3_flexible_download(cn.tcd_dir, cn.pattern_tcd, cn.docker_tile_dir, cn.SENSIT_TYPE, tile_id_list_outer)
    uu.s3_flexible_download(cn.gain_dir, cn.pattern_data_lake, cn.docker_tile_dir, cn.SENSIT_TYPE, tile_id_list_outer)
    uu.s3_flexible_download(cn.mangrove_biomass_2000_dir, cn.pattern_mangrove_biomass_2000, cn.docker_tile_dir, cn.SENSIT_TYPE, tile_id_list_outer)
    uu.s3_flexible_download(cn.plant_pre_2000_processed_dir, cn.pattern_plant_pre_2000, cn.docker_tile_dir, cn.SENSIT_TYPE, tile_id_list_outer)

    # Iterates through the types of tiles to be processed
    for input_dir, download_pattern_name in download_dict.items():

        # Pattern for tile set being processed
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
        uu.s3_flexible_download(input_dir, input_pattern, cn.docker_tile_dir, cn.SENSIT_TYPE, tile_id_list_inner)

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


        ### STEP 1: Creates the full extent per-pixel, forest extent per hectare
        ### and forest extent per pixel 0.00025x0.00025 deg derivative outputs
        uu.print_log("STEP 1: Creating derivative per-pixel and forest extent outputs")
        uu.print_log(f'Input pattern: {input_pattern}')
        uu.print_log(f'Output patterns: {output_patterns}')

        if cn.SINGLE_PROCESSOR:
            for tile_id in tile_id_list_inner:
                derivative_outputs.forest_extent_per_pixel_outputs(tile_id, input_pattern, output_patterns)
        else:
            # 18 = >740 GB peak; 15=XXX GB peak
            if cn.count == 96:
                processes = 15
            else:
                processes = 2
            uu.print_log(f'Creating derivative outputs for {input_pattern} with {processes} processors...')
            pool = multiprocessing.Pool(processes)
            pool.map(partial(derivative_outputs.forest_extent_per_pixel_outputs, input_pattern=input_pattern,
                             output_patterns=output_patterns),
                     tile_id_list_inner)
            pool.close()
            pool.join()


        ### STEP 2: Converts the forest extent 10x10 degree Hansen tiles that
        ### are in windows of 40000x1 pixels to windows of 160x160 pixels.
        ### This will allow the 0.00025x0.00025 deg pixels in each window to be summed into the aggregated pixels
        ### in the next step.
        uu.print_log("STEP 2: Rewindow tiles")

        # The forest extent per-pixel pattern for that model output. This derivative output is used for aggregation
        # because aggregation is just for forest extent and sums the per-pixel values within each aggregated pixel.
        download_pattern_name = output_patterns[2]

        if cn.SINGLE_PROCESSOR:
            for tile_id in tile_id_list_inner:
                uu.rewindow(tile_id, download_pattern_name)
        else:
            if cn.count == 96:
                if cn.SENSIT_TYPE == 'biomass_swap':
                    processes = 12  # 12 processors = XXX GB peak
                else:
                    processes = 14  # 14 processors = XXX GB peak
            else:
                processes = 8
            uu.print_log(f'Rewindow max processors= {processes}')
            pool = multiprocessing.Pool(processes)
            pool.map(partial(uu.rewindow, download_pattern_name=download_pattern_name),
                     tile_id_list_inner)
            pool.close()
            pool.join()


        ### STEP 3: Aggregates the rewindowed per-pixel values in each 160x160 window.
        ### The sum for each 160x160 pixel window is stored in a 2D array, which is then converted back into a raster at
        ### 0.04x0.04 degree resolution .
        ### Each aggregated pixel in this raster is the sum of the forest extent 0.00025x0.00025 deg per-pixel maps.
        ### 10x10 deg tiles at 0.04x0.04 deg resolution are output.
        uu.print_log("STEP 3: Aggregate pixels within tiles")

        if cn.SINGLE_PROCESSOR:
            for tile_id in tile_id_list_inner:
                derivative_outputs.aggregate_within_tile(tile_id, download_pattern_name)
        else:
            if cn.count == 96:
                if cn.SENSIT_TYPE == 'biomass_swap':
                    processes = 10  # 10 processors = XXX GB peak
                else:
                    processes = 11  # 16 processors = 180 GB peak; 16 = XXX GB peak; 20 = >750 GB (maxed out)
            else:
                processes = 8
            uu.print_log(f'Aggregate max processors={processes}')
            pool = multiprocessing.Pool(processes)
            pool.map(partial(derivative_outputs.aggregate_within_tile, download_pattern_name=download_pattern_name),
                     tile_id_list_inner)
            pool.close()
            pool.join()


        ### STEP 4: Combines 10x10 deg aggregated tiles into a global aggregated map
        uu.print_log("STEP 4: Combine tiles into global raster")
        derivative_outputs.aggregate_tiles(input_pattern, download_pattern_name)


        ### STEP 5: Clean up folder
        uu.print_log("STEP 5: Clean up folder")
        vrt_list = glob.glob('*vrt')
        for vrt in vrt_list:
            os.remove(vrt)

        rewindow_list = glob.glob(f'*rewindow.tif')
        for rewindow in rewindow_list:
            os.remove(rewindow)

        aggreg_list = glob.glob(f'*_0_04deg.tif')
        for aggreg in aggreg_list:
            os.remove(aggreg)


        ### STEP 6: Checks the two forest extent output tiles created from each input tile for whether there is data in them.
        ### Because the extent is restricted in the forest extent pixels, some tiles with pixels in the full extent
        ### version may not have pixels in the forest extent version.
        uu.print_log("STEP 6: Checking forest extent outputs for data")
        for output_pattern in output_patterns[1:3]:
            if cn.SINGLE_PROCESSOR or cn.count < 4:
                for tile_id in tile_id_list_inner:
                    uu.check_and_delete_if_empty_light(tile_id, output_pattern)
            else:
                processes = 55  # 50 processors = 560 GB peak for gross removals; 55 = XXX GB peak
                uu.print_log(f'Checking for empty tiles of {output_pattern} pattern with {processes} processors...')
                pool = multiprocessing.Pool(processes)
                pool.map(partial(uu.check_and_delete_if_empty, output_pattern=output_pattern), tile_id_list_inner)
                pool.close()
                pool.join()


    ### OPTIONAL STEP 7: Upload 0.00025x0.00025 deg and aggregated outputs to s3
    # If cn.NO_UPLOAD flag is not activated (by choice or by lack of AWS credentials), output is uploaded
    if not cn.NO_UPLOAD:
        uu.print_log("STEP 7: Uploading outputs to s3")
        for output_dir, output_pattern in zip(output_dir_list, output_pattern_list):
            uu.upload_final_set(output_dir, output_pattern)


    # ### OPTIONAL STEP 8: Compares sensitivity analysis aggregated net flux map to standard model aggregated net flux map in two ways.
    # ### This does not work for comparing the raw outputs of the biomass_swap and US_removals sensitivity models because their
    # ### extents are different from the standard model's extent (tropics and US tiles vs. global).
    # ### Thus, in order to do this comparison, you need to clip the standard model net flux and US_removals net flux to
    # ### the outline of the US and clip the standard model net flux to the extent of JPL AGB2000.
    # ### Then, manually upload the clipped US_removals and biomass_swap net flux rasters to the spot machine and the
    # ### code below should work.
    # ### WARNING: THIS HAS NOT BEEN TESTED SINCE MODEL V1.2.0 AND IS NOT LIKELY TO WORK WITHOUT SIGNIFICANT REVISIONS
    # ### AND REFACTORING. THUS, IT IS COMMENTED OUT.
    # if cn.SENSIT_TYPE not in ['std', 'biomass_swap', 'US_removals', 'legal_Amazon_loss']:
    #
    #     if std_net_flux:
    #
    #         uu.print_log('Standard aggregated flux results provided. Creating comparison maps.')
    #
    #         # Copies the standard model aggregation outputs to s3. Only net flux is used, though.
    #         uu.s3_file_download(std_net_flux, cn.docker_base_dir, cn.SENSIT_TYPE)
    #
    #         # Identifies the standard model net flux map
    #         std_aggreg_flux = os.path.split(std_net_flux)[1]
    #
    #         try:
    #             # Identifies the sensitivity model net flux map
    #             sensit_aggreg_flux = glob.glob('net_flux_Mt_CO2e_*{}*'.format(cn.SENSIT_TYPE))[0]
    #
    #             uu.print_log(f'Standard model net flux: {std_aggreg_flux}')
    #             uu.print_log(f'Sensitivity model net flux: {sensit_aggreg_flux}')
    #
    #         except:
    #             uu.print_log(
    #                 'Cannot do comparison. One of the input flux tiles is not valid. Verify that both net flux rasters are on the spot machine.')
    #
    #         uu.print_log(f'Creating map of percent difference between standard and {cn.SENSIT_TYPE} net flux')
    #         aggregate_results_to_4_km.percent_diff(std_aggreg_flux, sensit_aggreg_flux)
    #
    #         uu.print_log(
    #             f'Creating map of which pixels change sign and which stay the same between standard and {cn.SENSIT_TYPE}')
    #         aggregate_results_to_4_km.sign_change(std_aggreg_flux, sensit_aggreg_flux)
    #
    #         # If cn.NO_UPLOAD flag is not activated (by choice or by lack of AWS credentials), output is uploaded
    #         if not cn.NO_UPLOAD:
    #             uu.upload_final_set(output_dir_list[0], cn.pattern_aggreg_sensit_perc_diff)
    #             uu.upload_final_set(output_dir_list[0], cn.pattern_aggreg_sensit_sign_change)
    #
    #     else:
    #
    #         uu.print_log('No standard aggregated flux results provided. Not creating comparison maps.')


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

    mp_derivative_outputs(tile_id_list)