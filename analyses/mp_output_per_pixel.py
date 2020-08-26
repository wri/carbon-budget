### Calculates the net emissions over the study period, with units of Mg CO2e/ha on a pixel-by-pixel basis.
### This only uses gross emissions from biomass+soil (doesn't run with gross emissions from soil_only).

import multiprocessing
import argparse
import os
import datetime
from functools import partial
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu
sys.path.append(os.path.join(cn.docker_app,'analyses'))
import output_per_pixel

def mp_output_per_pixel(sensit_type, tile_id_list, run_date = None):

    os.chdir(cn.docker_base_dir)


    # Pixel area tiles-- necessary for calculating values per pixel
    uu.s3_flexible_download(cn.pixel_area_dir, cn.pattern_pixel_area, cn.docker_base_dir, 'std', tile_id_list)

    # Files to download for this script. Unusually, this script needs the output pattern in the dictionary as well!
    download_dict = {
        cn.cumul_gain_AGCO2_BGCO2_all_types_dir:
            [cn.pattern_cumul_gain_AGCO2_BGCO2_all_types, cn.pattern_cumul_gain_AGCO2_BGCO2_all_types_per_pixel],
        cn.gross_emis_all_gases_all_drivers_biomass_soil_dir:
            [cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil, cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil_per_pixel],
        cn.net_flux_dir:
            [cn.pattern_net_flux, cn.pattern_net_flux_per_pixel]
    }


    # List of output directories and output file name patterns
    output_dir_list = [
                       cn.cumul_gain_AGCO2_BGCO2_all_types_per_pixel_dir,
                       cn.gross_emis_all_gases_all_drivers_biomass_soil_per_pixel_dir,
                       cn.net_flux_per_pixel_dir]
    output_pattern_list = [
                           cn.pattern_cumul_gain_AGCO2_BGCO2_all_types_per_pixel,
                           cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil_per_pixel,
                           cn.pattern_net_flux_per_pixel]


    # If the model run isn't the standard one, the output directory and file names are changed
    if sensit_type != 'std':
        uu.print_log("Changing output directory and file name pattern based on sensitivity analysis")
        output_dir_list = uu.alter_dirs(sensit_type, output_dir_list)
        output_pattern_list = uu.alter_patterns(sensit_type, output_pattern_list)

    # A date can optionally be provided by the full model script or a run of this script.
    # This replaces the date in constants_and_names.
    if run_date is not None:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, run_date)


    # Iterates through input tile sets
    for key, values in download_dict.items():

        # Sets the directory and pattern for the input being processed
        input_dir = key
        input_pattern = values[0]

        # If a full model run is specified, the correct set of tiles for the particular script is listed
        if tile_id_list == 'all':
            # List of tiles to run in the model
            tile_id_list = uu.tile_list_s3(input_dir, sensit_type)

        uu.print_log(tile_id_list)
        uu.print_log("There are {} tiles to process".format(str(len(tile_id_list))) + "\n")

        uu.print_log("Downloading tiles from", input_dir)
        uu.s3_flexible_download(input_dir, input_pattern, cn.docker_base_dir, sensit_type, tile_id_list)

        # The pattern of the output files
        output_pattern = values[1]

        # 20 processors = 430 GB peak for cumul gain; 30 = 640 GB peak for cumul gain;
        # 32 = 680 GB peak for cumul gain; 33 = 710 GB peak for cumul gain, gross emis, net flux
        if cn.count == 96:
            processes = 20
        else:
            processes = 2
        uu.print_log("Creating {0} with {1} processors...".format(output_pattern, processes))
        pool = multiprocessing.Pool(processes)
        pool.map(partial(output_per_pixel.output_per_pixel, input_pattern=input_pattern,
                         output_pattern=output_pattern, sensit_type=sensit_type), tile_id_list)
        pool.close()
        pool.join()

        # # For single processor use
        # for tile_id in tile_id_list:
        #     output_per_pixel.output_per_pixel(tile_id, input_pattern, output_pattern, sensit_type)


        metadata_list = ['units=Mg CO2e/pixel over model duration (2001-20{})'.format(cn.loss_years),
                                    'extent=Model extent',
                                    'pixel_areas=Pixel areas depend on the latitude at which the pixel is found',
                                    'scale=If this is for net flux, negative values are net sinks and positive values are net sources']
        if cn.count == 96:
            processes = 45  # 45 processors = XXX GB peak
        else:
            processes = 9
        uu.print_log('Adding metadata tags max processors=', processes)
        pool = multiprocessing.Pool(processes)
        pool.map(partial(uu.add_metadata_tags, output_pattern=output_pattern, sensit_type=sensit_type, metadata_list=metadata_list),
                 tile_id_list)
        pool.close()
        pool.join()

        # for tile_id in tile_id_list:
        #     uu.add_metadata_tags(tile_id, output_pattern, sensit_type, metadata_list)

    # Uploads output tiles to s3
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
    args = parser.parse_args()
    sensit_type = args.model_type
    tile_id_list = args.tile_id_list
    run_date = args.run_date

    # Create the output log
    uu.initiate_log(tile_id_list=tile_id_list, sensit_type=sensit_type, run_date=run_date)

    # Checks whether the sensitivity analysis and tile_id_list arguments are valid
    uu.check_sensit_type(sensit_type)
    tile_id_list = uu.tile_id_list_check(tile_id_list)

    mp_output_per_pixel(sensit_type=sensit_type, tile_id_list=tile_id_list, run_date=run_date)