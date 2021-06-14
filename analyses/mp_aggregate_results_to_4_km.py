'''
This script creates maps of model outputs at roughly 5km resolution (0.05x0.05 degrees), where each output pixel
represents the total value in the pixel (not the density) (hence, the aggregated results).
This is currently set up for annual removal rate, gross removals, gross emissions, and net flux.
It iterates through all the model outputs that are supplied.
The rewindowed pixel area tiles, tcd, Hansen gain, and mangrove biomass tiles must already be created and in s3
(created using mp_rewindow_tiles.py).
First, this script rewindows the model output into 200x200 (0.05x0.05 degree) windows, instead of the native
40000x1 pixel windows.
Then it calculates the per pixel value for each model output pixel and sums those values within each 0.05x0.05 degree
aggregated pixel.
It converts emissions, removals, and net flux from totals over the model period to annual values.
For sensitivity analysis runs, it only processes outputs which actually have a sensitivity analysis version.
The user has to supply a tcd threshold for which forest pixels to include in the results. Defaults to cn.canopy_threshold.
For sensitivity analysis, the s3 folder with the aggregations for the standard model must be specified.
sample command: python mp_aggregate_results_to_4_km.py -tcd 30 -t no_shifting_ag -sagg s3://gfw2-data/climate/carbon_model/0_4deg_output_aggregation/biomass_soil/standard/20200901/net_flux_Mt_CO2e_biomass_soil_per_year_tcd30_0_4deg_modelv1_2_0_std_20200901.tif
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
import aggregate_results_to_4_km

def mp_aggregate_results_to_4_km(sensit_type, thresh, tile_id_list, std_net_flux = None, run_date = None, no_upload = None):

    os.chdir(cn.docker_base_dir)

    # Files to download for this script
    download_dict = {
             cn.annual_gain_AGC_all_types_dir: [cn.pattern_annual_gain_AGC_all_types],
             cn.cumul_gain_AGCO2_BGCO2_all_types_dir: [cn.pattern_cumul_gain_AGCO2_BGCO2_all_types],
             cn.gross_emis_all_gases_all_drivers_biomass_soil_dir: [cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil],
             cn.net_flux_dir: [cn.pattern_net_flux]
             }

    # Checks whether the canopy cover argument is valid
    if thresh < 0 or thresh > 99:
        uu.exception_log(no_upload, 'Invalid tcd. Please provide an integer between 0 and 99.')


    if uu.check_aws_creds():

        # Pixel area tiles-- necessary for calculating sum of pixels for any set of tiles
        uu.s3_flexible_download(cn.pixel_area_rewindow_dir, cn.pattern_pixel_area_rewindow, cn.docker_base_dir, sensit_type, tile_id_list)
        # Tree cover density, Hansen gain, and mangrove biomass tiles-- necessary for filtering sums to model extent
        uu.s3_flexible_download(cn.tcd_rewindow_dir, cn.pattern_tcd_rewindow, cn.docker_base_dir, sensit_type, tile_id_list)
        uu.s3_flexible_download(cn.gain_rewindow_dir, cn.pattern_gain_rewindow, cn.docker_base_dir, sensit_type, tile_id_list)
        uu.s3_flexible_download(cn.mangrove_biomass_2000_rewindow_dir, cn.pattern_mangrove_biomass_2000_rewindow, cn.docker_base_dir, sensit_type, tile_id_list)

    uu.print_log("Model outputs to process are:", download_dict)

    # List of output directories. Modified later for sensitivity analysis.
    # Output pattern is determined later.
    output_dir_list = [cn.output_aggreg_dir]

    # If the model run isn't the standard one, the output directory is changed
    if sensit_type != 'std':
        uu.print_log("Changing output directory and file name pattern based on sensitivity analysis")
        output_dir_list = uu.alter_dirs(sensit_type, output_dir_list)

    # A date can optionally be provided by the full model script or a run of this script.
    # This replaces the date in constants_and_names.
    # Only done if output upload is enabled.
    if run_date is not None and no_upload is not None:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, run_date)


    # Iterates through the types of tiles to be processed
    for dir, download_pattern in list(download_dict.items()):

        download_pattern_name = download_pattern[0]

        # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list, if AWS credentials are found
        if uu.check_aws_creds():

            uu.s3_flexible_download(dir, download_pattern_name, cn.docker_base_dir, sensit_type, tile_id_list)

        if tile_id_list == 'all':
            # List of tiles to run in the model
            tile_id_list = uu.tile_list_s3(dir, sensit_type)

        # Gets an actual tile id to use as a dummy in creating the actual tile pattern
        local_tile_list = uu.tile_list_spot_machine(cn.docker_base_dir, download_pattern_name)
        sample_tile_id = uu.get_tile_id(local_tile_list[0])

        # Renames the tiles according to the sensitivity analysis before creating dummy tiles.
        # The renaming function requires a whole tile name, so this passes a dummy time name that is then stripped a few
        # lines later.
        tile_id = sample_tile_id    # a dummy tile id (but it has to be a real tile id). It is removed later.
        output_pattern = uu.sensit_tile_rename(sensit_type, tile_id, download_pattern_name)
        pattern = output_pattern[9:-4]

        # For sensitivity analysis runs, only aggregates the tiles if they were created as part of the sensitivity analysis
        if (sensit_type != 'std') & (sensit_type not in pattern):
            uu.print_log("{} not a sensitivity analysis output. Skipping aggregation...".format(pattern))
            uu.print_log("")

            continue

        # Lists the tiles of the particular type that is being iterated through.
        # Excludes all intermediate files
        tile_list = uu.tile_list_spot_machine(".", "{}.tif".format(pattern))
        # from https://stackoverflow.com/questions/12666897/removing-an-item-from-list-matching-a-substring
        tile_list = [i for i in tile_list if not ('hanson_2013' in i)]
        tile_list = [i for i in tile_list if not ('rewindow' in i)]
        tile_list = [i for i in tile_list if not ('0_4deg' in i)]
        tile_list = [i for i in tile_list if not ('.ovr' in i)]

        # tile_list = ['00N_070W_cumul_gain_AGCO2_BGCO2_t_ha_all_forest_types_2001_15_biomass_swap.tif']  # test tiles

        uu.print_log("There are {0} tiles to process for pattern {1}".format(str(len(tile_list)), download_pattern_name) + "\n")
        uu.print_log("Processing:", dir, "; ", pattern)

        # Converts the 10x10 degree Hansen tiles that are in windows of 40000x1 pixels to windows of 200x200 pixels,
        # which is the resolution of the output tiles. This will allow the 30x30 m pixels in each window to be summed.
        if cn.count == 96:
            if sensit_type == 'biomass_swap':
                processes = 12  # 12 processors = XXX GB peak
            else:
                processes = 16  # 16 processors = XXX GB peak
        else:
            processes = 8
        uu.print_log('Rewindow max processors=', processes)
        pool = multiprocessing.Pool(processes)
        pool.map(partial(uu.rewindow, download_pattern_name=download_pattern_name, no_upload=no_upload), tile_id_list)
        # Added these in response to error12: Cannot allocate memory error.
        # This fix was mentioned here: of https://stackoverflow.com/questions/26717120/python-cannot-allocate-memory-using-multiprocessing-pool
        # Could also try this: https://stackoverflow.com/questions/42584525/python-multiprocessing-debugging-oserror-errno-12-cannot-allocate-memory
        pool.close()
        pool.join()

        # # For single processor use
        # for tile_id in tile_id_list:
        #
        #     uu.rewindow(tile_id, download_pattern_name,no_upload)


        # Converts the existing (per ha) values to per pixel values (e.g., emissions/ha to emissions/pixel)
        # and sums those values in each 200x200 pixel window.
        # The sum for each 200x200 pixel window is stored in a 2D array, which is then converted back into a raster at
        # 0.05x0.05 degree resolution (approximately 10m in the tropics).
        # Each pixel in that raster is the sum of the 30m pixels converted to value/pixel (instead of value/ha).
        # The 0.05x0.05 degree tile is output.
        # For multiprocessor use. This used about 450 GB of memory with count/2, it's okay on an r4.16xlarge
        if cn.count == 96:
            if sensit_type == 'biomass_swap':
                processes = 10  # 10 processors = XXX GB peak
            else:
                processes = 12  # 16 processors = 180 GB peak; 16 = XXX GB peak; 20 = >750 GB (maxed out)
        else:
            processes = 8
        uu.print_log('Conversion to per pixel and aggregate max processors=', processes)
        pool = multiprocessing.Pool(processes)
        pool.map(partial(aggregate_results_to_4_km.aggregate, thresh=thresh, sensit_type=sensit_type,
                         no_upload=no_upload), tile_list)
        pool.close()
        pool.join()

        # # For single processor use
        # for tile in tile_list:
        #
        #     aggregate_results_to_4_km.aggregate(tile, thresh, sensit_type, no_upload)

        # Makes a vrt of all the output 10x10 tiles (10 km resolution)
        out_vrt = "{}_0_4deg.vrt".format(pattern)
        os.system('gdalbuildvrt -tr 0.04 0.04 {0} *{1}_0_4deg*.tif'.format(out_vrt, pattern))

        # Creates the output name for the 10km map
        out_pattern = uu.name_aggregated_output(download_pattern_name, thresh, sensit_type)
        uu.print_log(out_pattern)

        # Produces a single raster of all the 10x10 tiles (0.4 degree resolution)
        cmd = ['gdalwarp', '-t_srs', "EPSG:4326", '-overwrite', '-dstnodata', '0', '-co', 'COMPRESS=LZW',
               '-tr', '0.04', '0.04',
               out_vrt, '{}.tif'.format(out_pattern)]
        uu.log_subprocess_output_full(cmd)


        # Adds metadata tags to output rasters
        uu.add_universal_metadata_tags('{0}.tif'.format(out_pattern), sensit_type)

        # Units are different for annual removal factor, so metadata has to reflect that
        if 'annual_removal_factor' in out_pattern:
            cmd = ['gdal_edit.py',
                   '-mo', 'units=Mg aboveground carbon/yr/pixel, where pixels are 0.04x0.04 degrees',
                   '-mo', 'source=per hectare version of the same model output, aggregated from 0.00025x0.00025 degree pixels',
                   '-mo', 'extent=Global',
                   '-mo', 'scale=negative values are removals',
                   '-mo', 'treecover_density_threshold={0} (only model pixels with canopy cover > {0} are included in aggregation'.format(thresh),
                   '{0}.tif'.format(out_pattern)]
            uu.log_subprocess_output_full(cmd)

        else:
            cmd = ['gdal_edit.py',
                   '-mo', 'units=Mg CO2e/yr/pixel, where pixels are 0.04x0.04 degrees',
                   '-mo', 'source=per hectare version of the same model output, aggregated from 0.00025x0.00025 degree pixels',
                   '-mo', 'extent=Global',
                   '-mo', 'treecover_density_threshold={0} (only model pixels with canopy cover > {0} are included in aggregation'.format(thresh),
                   '{0}.tif'.format(out_pattern)]
            uu.log_subprocess_output_full(cmd)


        # If no_upload flag is not activated, output is uploaded
        if not no_upload:

            uu.print_log("Tiles processed. Uploading to s3 now...")
            uu.upload_final_set(output_dir_list[0], out_pattern)

        # Cleans up the folder before starting on the next raster type
        vrtList = glob.glob('*vrt')
        for vrt in vrtList:
            os.remove(vrt)

        # for tile_name in tile_list:
        #     tile_id = uu.get_tile_id(tile_name)
        #     # os.remove('{0}_{1}.tif'.format(tile_id, pattern))
        #     os.remove('{0}_{1}_rewindow.tif'.format(tile_id, pattern))
        #     os.remove('{0}_{1}_0_4deg.tif'.format(tile_id, pattern))

        os.quit()


    # Compares the net flux from the standard model and the sensitivity analysis in two ways.
    # This does not work for compariing the raw outputs of the biomass_swap and US_removals sensitivity models because their
    # extents are different from the standard model's extent (tropics and US tiles vs. global).
    # Thus, in order to do this comparison, you need to clip the standard model net flux and US_removals net flux to
    # the outline of the US and clip the standard model net flux to the extent of JPL AGB2000.
    # Then, manually upload the clipped US_removals and biomass_swap net flux rasters to the spot machine and the
    # code below should work.
    if sensit_type not in ['std', 'biomass_swap', 'US_removals', 'legal_Amazon_loss']:

        if std_net_flux:

            uu.print_log("Standard aggregated flux results provided. Creating comparison maps.")

            # Copies the standard model aggregation outputs to s3. Only net flux is used, though.
            uu.s3_file_download(std_net_flux, cn.docker_base_dir, sensit_type)

            # Identifies the standard model net flux map
            std_aggreg_flux = os.path.split(std_net_flux)[1]

            try:
                # Identifies the sensitivity model net flux map
                sensit_aggreg_flux = glob.glob('net_flux_Mt_CO2e_*{}*'.format(sensit_type))[0]

                uu.print_log("Standard model net flux:", std_aggreg_flux)
                uu.print_log("Sensitivity model net flux:", sensit_aggreg_flux)

            except:
                uu.print_log('Cannot do comparison. One of the input flux tiles is not valid. Verify that both net flux rasters are on the spot machine.')

            uu.print_log("Creating map of percent difference between standard and {} net flux".format(sensit_type))
            aggregate_results_to_4_km.percent_diff(std_aggreg_flux, sensit_aggreg_flux, sensit_type, no_upload)

            uu.print_log("Creating map of which pixels change sign and which stay the same between standard and {}".format(sensit_type))
            aggregate_results_to_4_km.sign_change(std_aggreg_flux, sensit_aggreg_flux, sensit_type, no_upload)

            # If no_upload flag is not activated, output is uploaded
            if not no_upload:

                uu.upload_final_set(output_dir_list[0], cn.pattern_aggreg_sensit_perc_diff)
                uu.upload_final_set(output_dir_list[0], cn.pattern_aggreg_sensit_sign_change)

        else:

            uu.print_log("No standard aggregated flux results provided. Not creating comparison maps.")


if __name__ == '__main__':

    # The argument for what kind of model run is being done: standard conditions or a sensitivity analysis run
    parser = argparse.ArgumentParser(
        description='Create maps of model outputs at aggregated/coarser resolution')
    parser.add_argument('--model-type', '-t', required=True,
                        help='{}'.format(cn.model_type_arg_help))
    parser.add_argument('--tile_id_list', '-l', required=True,
                        help='List of tile ids to use in the model. Should be of form 00N_110E or 00N_110E,00N_120E or all.')
    parser.add_argument('--tcd-threshold', '-tcd', required=False, default=cn.canopy_threshold,
                        help='Tree cover density threshold above which pixels will be included in the aggregation. Default is 30.')
    parser.add_argument('--std-net-flux-aggreg', '-sagg', required=False,
                        help='The s3 standard model net flux aggregated tif, for comparison with the sensitivity analysis map')
    parser.add_argument('--no-upload', '-nu', action='store_true',
                       help='Disables uploading of outputs to s3')
    args = parser.parse_args()
    sensit_type = args.model_type
    tile_id_list = args.tile_id_list
    std_net_flux = args.std_net_flux_aggreg
    thresh = args.tcd_threshold
    thresh = int(thresh)
    no_upload = args.no_upload

    # Disables upload to s3 if no AWS credentials are found in environment
    if not uu.check_aws_creds():
        no_upload = True
        uu.print_log("s3 credentials not found. Uploading to s3 disabled.")

    # Create the output log
    uu.initiate_log(tile_id_list=tile_id_list, sensit_type=sensit_type, thresh=thresh, std_net_flux=std_net_flux,
                    no_upload=no_upload)

    # Checks whether the sensitivity analysis and tile_id_list arguments are valid
    uu.check_sensit_type(sensit_type)
    tile_id_list = uu.tile_id_list_check(tile_id_list)

    mp_aggregate_results_to_4_km(sensit_type=sensit_type, tile_id_list=tile_id_list, thresh=thresh,
                                 std_net_flux=std_net_flux, no_upload=no_upload)