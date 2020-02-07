'''
This script creates maps of model outputs at roughly 10km resolution (0.1x0.1 degrees), where each output pixel
represents the total value in the pixel (not the density) (hence, the aggregated results).
This is currently only set up for gross emissions from biomass+soil and net flux from biomass+soil.
It iterates through all the model outputs that are supplied.
First, it rewindows the model output, pixel area tile, and tcd tile into 400x400 (0.1x0.1 degree) windows, instead of the native
40000x1 pixel windows.
Then it calculates the per pixel value for each model output pixel and sums those values within each 0.1x0.1 degree
aggregated pixel.
It converts cumulative carbon gain to CO2 gain per year, converts cumulative CO2 flux to CO2 flux per year, and
converts cumulative gross CO2 emissions to gross CO2 emissions per year.
The user has to supply a tcd threshold for which forest pixels to include in the results.
sample command: python mp_aggregate_results_to_10_km.py -tcd 30 -t no_shifting_ag -sagg s3://gfw2-data/climate/carbon_model/10km_output_aggregation/biomass_soil/standard/20191107/net_flux_t_CO2e_per_year_biomass_soil_10km_tcd30_modelv1_1_2_std_2019_11_07.tif
'''


import multiprocessing
import aggregate_results_to_10_km
import subprocess
from functools import partial
import argparse
import os
import glob
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def mp_aggregate_results_to_10_km(sensit_type, thresh, std_net_flux):

    # Files to download for this script. 'true'/'false' says whether the input directory and pattern should be
    # changed for a sensitivity analysis. This does not need to change based on what run is being done;
    # this assignment should be true for all sensitivity analyses and the standard model.
    download_dict = {
             # cn.gross_emis_all_gases_all_drivers_biomass_soil_dir: [cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil],
             # cn.cumul_gain_AGCO2_BGCO2_all_types_dir: [cn.pattern_cumul_gain_AGCO2_BGCO2_all_types],
             cn.net_flux_dir: [cn.pattern_net_flux]
             }

    # Checks whether the canopy cover argument is valid
    if thresh < 0 or thresh > 99:
        raise Exception('Invalid tcd. Please provide an integer between 0 and 99.')

    # Checks whether the sensitivity analysis argument is valid
    uu.check_sensit_type(sensit_type)

    tile_id_list = ['40N_090W', '00N_110E'] # test tiles
    # tile_id_list = 'all'

    # Pixel area tiles-- necessary for calculating sum of pixels for any set of tiles
    uu.s3_flexible_download(cn.pixel_area_dir, cn.pattern_pixel_area, '.', sensit_type, tile_id_list)
    # tree cover density tiles-- necessary for filtering sums by tcd
    uu.s3_flexible_download(cn.tcd_dir, cn.pattern_tcd, '.', sensit_type, tile_id_list)

    print "Model outputs to process are:", download_dict 

    # List of output directories. Modified later for sensitivity analysis.
    # Output pattern is determined later.
    output_dir_list = [cn.output_aggreg_dir]

    # If the model run isn't the standard one, the output directory is changed
    if sensit_type != 'std':
        print "Changing output directory and file name pattern based on sensitivity analysis"
        output_dir_list = uu.alter_dirs(sensit_type, output_dir_list)


    # Iterates through the types of tiles to be processed
    for dir, download_pattern in download_dict.items():

        download_pattern_name = download_pattern[0]

        # Downloads the model output tiles to be processed
        uu.s3_flexible_download(dir, download_pattern_name, '.', sensit_type, tile_id_list)

        # Gets an actual tile id to use as a dummy in creating the actual tile pattern
        local_tile_list = uu.tile_list_spot_machine('.', download_pattern_name)
        sample_tile_id = uu.get_tile_id(local_tile_list[0])

        # Renames the tiles according to the sensitivity analysis before creating dummy tiles.
        # The renaming function requires a whole tile name, so this passes a dummy time name that is then stripped a few
        # lines later.
        tile_id = sample_tile_id    # a dummy tile id (but it has to be a real tile id). It is removed later.
        output_pattern = uu.sensit_tile_rename(sensit_type, tile_id, download_pattern_name)
        pattern = output_pattern[9:-4]

        # Lists the tiles of the particular type that is being iterates through.
        # Excludes all intermediate files
        tile_list = uu.tile_list_spot_machine(".", "{}.tif".format(pattern))
        # from https://stackoverflow.com/questions/12666897/removing-an-item-from-list-matching-a-substring
        tile_list = [i for i in tile_list if not ('hanson_2013' in i)]
        tile_list = [i for i in tile_list if not ('rewindow' in i)]
        tile_list = [i for i in tile_list if not ('0_4deg' in i)]

        # tile_list = ['00N_070W_cumul_gain_AGCO2_BGCO2_t_ha_all_forest_types_2001_15_biomass_swap.tif']  # test tiles

        print tile_list
        print "There are {} tiles to process".format(str(len(tile_list))) + "\n"
        print "Processing:", dir, "; ", pattern

        # Converts the 10x10 degree Hansen tiles that are in windows of 40000x1 pixels to windows of 400x400 pixels,
        # which is the resolution of the output tiles. This will allow the 30x30 m pixels in each window to be summed.
        # For multiprocessor use. count/2 used about 400 GB of memory on an r4.16xlarge machine, so that was okay.
        count = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(count/4)
        pool.map(aggregate_results_to_10_km.rewindow, tile_list)
        # Added these in response to error12: Cannot allocate memory error.
        # This fix was mentioned here: of https://stackoverflow.com/questions/26717120/python-cannot-allocate-memory-using-multiprocessing-pool
        # Could also try this: https://stackoverflow.com/questions/42584525/python-multiprocessing-debugging-oserror-errno-12-cannot-allocate-memory
        pool.close()
        pool.join()

        # # For single processor use
        # for tile in tile_list:
        #
        #     aggregate_results_to_10_km.rewindow(tile)

        # Converts the existing (per ha) values to per pixel values (e.g., emissions/ha to emissions/pixel)
        # and sums those values in each 400x400 pixel window.
        # The sum for each 400x400 pixel window is stored in a 2D array, which is then converted back into a raster at
        # 0.1x0.1 degree resolution (approximately 10m in the tropics).
        # Each pixel in that raster is the sum of the 30m pixels converted to value/pixel (instead of value/ha).
        # The 0.1x0.1 degree tile is output.
        # For multiprocessor use. This used about 450 GB of memory with count/2, it's okay on an r4.16xlarge
        count = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(count/4)
        pool.map(partial(aggregate_results_to_10_km.aggregate, thresh=thresh), tile_list)
        # Added these in response to error12: Cannot allocate memory error.
        # This fix was mentioned here: of https://stackoverflow.com/questions/26717120/python-cannot-allocate-memory-using-multiprocessing-pool
        # Could also try this: https://stackoverflow.com/questions/42584525/python-multiprocessing-debugging-oserror-errno-12-cannot-allocate-memory
        pool.close()
        pool.join()

        # # For single processor use
        # for tile in tile_list:
        #
        #     aggregate_results_to_10_km.aggregate(tile, thresh)

        # Makes a vrt of all the output 10x10 tiles (10 km resolution)
        out_vrt = "{}_10km.vrt".format(pattern)
        os.system('gdalbuildvrt -tr 0.04 0.04 {0} *{1}_10km*.tif'.format(out_vrt, pattern))

        # Creates the output name for the 10km map
        out_pattern = uu.name_aggregated_output(download_pattern_name, thresh, sensit_type)
        print out_pattern

        # Produces a single raster of all the 10x10 tiles (10 km resolution)
        cmd = ['gdalwarp', '-t_srs', "EPSG:4326", '-overwrite', '-dstnodata', '0', '-co', 'COMPRESS=LZW',
               '-tr', '0.04', '0.04',
               out_vrt, '{}.tif'.format(out_pattern)]
        subprocess.check_call(cmd)

        print "Tiles processed. Uploading to s3 now..."

        # Uploads all output tiles to s3
        uu.upload_final_set(output_dir_list[0], out_pattern)

        # # Cleans up the folder before starting on the next raster type
        # vrtList = glob.glob('*vrt')
        # for vrt in vrtList:
        #     os.remove(vrt)
        #
        # for tile_id in tile_list:
        #     os.remove('{0}_{1}.tif'.format(tile_id, pattern))
        #     os.remove('{0}_{1}_rewindow.tif'.format(tile_id, pattern))
        #     os.remove('{0}_{1}_10km.tif'.format(tile_id, pattern))


    # Compares the net flux from the standard model and the sensitivity analysis in two ways.
    # This does not work for compariing the raw outputs of the biomass_swap and US_removals sensitivity models because their
    # extents are different from the standard model's extent (tropics and US tiles vs. global).
    # Thus, in order to do this comparison, you need to clip the standard model net flux and US_removals net flux to
    # the outline of the US and clip the standard model net flux to the extent of JPL AGB2000.
    # Then, manually upload the clipped US_removals and biomass_swap net flux rasters to the spot machine and the
    # code below should work.
    if sensit_type != 'std':

        if std_net_flux:

            print "Standard aggregated flux results provided. Creating comparison maps."

            # Copies the standard model aggregation outputs to s3. Only net flux is used, though.
            uu.s3_file_download(std_net_flux, '.', sensit_type)

            # Identifies the standard model net flux map
            std_aggreg_flux = os.path.split(std_net_flux)[1]

            try:
                # Identifies the sensitivity model net flux map
                sensit_aggreg_flux = glob.glob('net_flux_Mt_CO2e_*{}*'.format(sensit_type))[0]

                print "Standard model net flux:", std_aggreg_flux
                print "Sensitivity model net flux:", sensit_aggreg_flux

            except:
                print 'Cannot do comparison. One of the input flux tiles is not valid. Verify that both net flux rasters are on the spot machine.'

            print "Creating map of percent difference between standard and {} net flux".format(sensit_type)
            aggregate_results_to_10_km.percent_diff(std_aggreg_flux, sensit_aggreg_flux, sensit_type)
            uu.upload_final_set(output_dir_list[0], cn.pattern_aggreg_sensit_perc_diff)

            print "Creating map of which pixels change sign and which stay the same between standard and {}".format(sensit_type)
            aggregate_results_to_10_km.sign_change(std_aggreg_flux, sensit_aggreg_flux, sensit_type)
            uu.upload_final_set(output_dir_list[0], cn.pattern_aggreg_sensit_sign_change)

        else:

            print "No standard aggregated flux results provided. Not creating comparison maps."


if __name__ == '__main__':

    # The argument for what kind of model run is being done: standard conditions or a sensitivity analysis run
    parser = argparse.ArgumentParser(
        description='Create tiles of the number of years of carbon gain for mangrove forests')
    parser.add_argument('--model-type', '-t', required=True,
                        help='{}'.format(cn.model_type_arg_help))
    parser.add_argument('--tcd-threshold', '-tcd', required=True,
                        help='Tree cover density threshold above which pixels will be included in the aggregation.')
    parser.add_argument('--std-net-flux-aggreg', '-sagg', required=False,
                        help='The s3 standard model net flux aggregated tif, for comparison with the sensitivity analysis map')
    args = parser.parse_args()
    thresh = args.tcd_threshold
    thresh = int(thresh)
    sensit_type = args.model_type
    std_net_flux = args.std_net_flux_aggreg

    # Checks whether the sensitivity analysis and tile_id_list arguments are valid
    uu.check_sensit_type(sensit_type)

    mp_aggregate_results_to_10_km(sensit_type=sensit_type, thresh=thresh, std_net_flux=std_net_flux)