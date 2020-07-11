### Calculates the net emissions over the study period, with units of Mg CO2e/ha on a pixel-by-pixel basis.
### This only uses gross emissions from biomass+soil (doesn't run with gross emissions from soil_only).

import multiprocessing
import argparse
import os
import datetime
from functools import partial
import sys
sys.path.append('/usr/local/app/analyses/')
import net_flux
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def mp_net_flux(sensit_type, tile_id_list, run_date = None):

    os.chdir(cn.docker_base_dir)

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # List of tiles to run in the model
        tile_id_list = uu.create_combined_tile_list(cn.gross_emis_all_gases_all_drivers_biomass_soil_dir,
                                                    cn.cumul_gain_AGCO2_BGCO2_all_types_dir,
                                                    sensit_type=sensit_type)

    uu.print_log(tile_id_list)
    uu.print_log("There are {} tiles to process".format(str(len(tile_id_list))) + "\n")


    # Files to download for this script
    download_dict = {
        cn.cumul_gain_AGCO2_BGCO2_all_types_dir: [cn.pattern_cumul_gain_AGCO2_BGCO2_all_types],
        cn.gross_emis_all_gases_all_drivers_biomass_soil_dir: [cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil]
    }


    # List of output directories and output file name patterns
    output_dir_list = [cn.net_flux_dir]
    output_pattern_list = [cn.pattern_net_flux]


    # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
    for key, values in download_dict.items():
        dir = key
        pattern = values[0]
        uu.s3_flexible_download(dir, pattern, cn.docker_base_dir, sensit_type, tile_id_list)


    # If the model run isn't the standard one, the output directory and file names are changed
    if sensit_type != 'std':
        uu.print_log("Changing output directory and file name pattern based on sensitivity analysis")
        output_dir_list = uu.alter_dirs(sensit_type, output_dir_list)
        output_pattern_list = uu.alter_patterns(sensit_type, output_pattern_list)

    # A date can optionally be provided by the full model script or a run of this script.
    # This replaces the date in constants_and_names.
    if run_date is not None:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, run_date)


    # Since the input tile lists have different numbers of tiles, at least one input will need to have some blank tiles made
    # so that it has all the necessary input tiles
    # The inputs that might need to have dummy tiles made in order to match the tile list of the carbon pools
    folder = os.getcwd()
    for download_dir, download_pattern in download_dict.items():

        # Renames the tiles according to the sensitivity analysis before creating dummy tiles.
        # The renaming function requires a whole tile name, so this passes a dummy time name that is then stripped a few
        # lines later.
        pattern = download_pattern[0]

        processes=54
        uu.print_log('Blank tile creation max processors=', processes)
        pool = multiprocessing.Pool(processes)
        pool.map(partial(uu.make_blank_tile, pattern=pattern, folder=folder,
                                             sensit_type=sensit_type), tile_id_list)
        pool.close()
        pool.join()


    # # For single processor use
    # folder = './'
    # for download_dir, download_pattern in download_dict.iteritems():
    #
    #     for tile_id in tile_id_list:
    #         uu.make_blank_tile(tile_id, download_pattern[0], folder, sensit_type)


    # Creates a single filename pattern to pass to the multiprocessor call
    pattern = output_pattern_list[0]

    # Count/3 uses about 380 GB on a r4.16xlarge spot machine
    # processes/24 maxes out at about 435 GB on an r4.16xlarge spot machine
    pool = multiprocessing.Pool(processes=24)
    pool.map(partial(net_flux.net_calc, pattern=pattern, sensit_type=sensit_type), tile_id_list)

    # # For single processor use
    # for tile_id in tile_id_list:
    #     net_flux.net_calc(tile_id, output_pattern_list[0], sensit_type)

    # Print the list of blank created tiles, delete the tiles, and delete their text file
    uu.list_and_delete_blank_tiles()

    # Uploads output tiles to s3
    uu.upload_final_set(output_dir_list[0], output_pattern_list[0])


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

    mp_net_flux(sensit_type=sensit_type, tile_id_list=tile_id_list, run_date=run_date)