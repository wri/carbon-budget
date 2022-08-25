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
import net_flux

def mp_net_flux(tile_id_list):

    os.chdir(cn.docker_base_dir)

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # List of tiles to run in the model
        tile_id_list = uu.create_combined_tile_list(cn.gross_emis_all_gases_all_drivers_biomass_soil_dir,
                                                    cn.cumul_gain_AGCO2_BGCO2_all_types_dir,
                                                    sensit_type=cn.SENSIT_TYPE)

    uu.print_log(tile_id_list)
    uu.print_log(f'There are {str(len(tile_id_list))} tiles to process', '\n')


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
        uu.s3_flexible_download(dir, pattern, cn.docker_base_dir, cn.SENSIT_TYPE, tile_id_list)


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
    if cn.count == 96:
        if cn.SENSIT_TYPE == 'biomass_swap':
            processes = 32 # 32 processors = XXX GB peak
        else:
            processes = 40   # 38 = 690 GB peak; 40 = 715 GB peak
    else:
        processes = 9
    uu.print_log(f'Net flux max processors={processes}')
    pool = multiprocessing.Pool(processes)
    pool.map(partial(net_flux.net_calc, pattern=pattern),
             tile_id_list)
    pool.close()
    pool.join()

    # # For single processor use
    # for tile_id in tile_id_list:
    #     net_flux.net_calc(tile_id, output_pattern_list[0])


    # If cn.NO_UPLOAD flag is not activated (by choice or by lack of AWS credentials), output is uploaded
    if not cn.NO_UPLOAD:
        uu.upload_final_set(output_dir_list[0], output_pattern_list[0])


if __name__ == '__main__':

    # The argument for what kind of model run is being done: standard conditions or a sensitivity analysis run
    parser = argparse.ArgumentParser(
        description='Creates tiles of net GHG flux over model period')
    parser.add_argument('--model-type', '-t', required=True,
                        help='{}'.format(cn.model_type_arg_help))
    parser.add_argument('--tile_id_list', '-l', required=True,
                        help='List of tile ids to use in the model. Should be of form 00N_110E or 00N_110E,00N_120E or all.')
    parser.add_argument('--run-date', '-d', required=False,
                        help='Date of run. Must be format YYYYMMDD.')
    parser.add_argument('--no-upload', '-nu', action='store_true',
                       help='Disables uploading of outputs to s3')
    args = parser.parse_args()

    # Sets global variables to the command line arguments
    cn.SENSIT_TYPE = args.model_type
    cn.RUN_DATE = args.run_date
    cn.NO_UPLOAD = args.no_upload

    tile_id_list = args.tile_id_list

    # Disables upload to s3 if no AWS credentials are found in environment
    if not uu.check_aws_creds():
        cn.NO_UPLOAD = True

    # Create the output log
    uu.initiate_log(tile_id_list)

    # Checks whether the sensitivity analysis and tile_id_list arguments are valid
    uu.check_sensit_type(cn.SENSIT_TYPE)
    tile_id_list = uu.tile_id_list_check(tile_id_list)

    mp_net_flux(tile_id_list)