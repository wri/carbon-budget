'''
This script creates the three inputs used for creating the carbon pools besides aboveground carbon.
It takes several hours to run.
'''

from subprocess import Popen, PIPE, STDOUT, check_call
import os
import argparse
import datetime
import create_inputs_for_C_pools
import multiprocessing
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def mp_create_inputs_for_C_pools(tile_id_list, run_date = None):

    os.chdir(cn.docker_base_dir)
    sensit_type = 'std'

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # List of tiles to run in the model
        tile_id_list = uu.tile_list_s3(cn.model_extent_dir, sensit_type)


    # List of output directories and output file name patterns
    output_dir_list = [cn.bor_tem_trop_processed_dir, cn.elevation_processed_dir, cn.precip_processed_dir]
    output_pattern_list = [cn.pattern_bor_tem_trop_processed, cn.pattern_elevation, cn.pattern_precip]


    # A date can optionally be provided by the full model script or a run of this script.
    # This replaces the date in constants_and_names.
    if run_date is not None:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, run_date)


    # Downloads two of the raw input files for creating carbon pools
    input_files = [cn.fao_ecozone_raw_dir, cn.precip_raw_dir]

    for input in input_files:
        uu.s3_file_download('{}'.format(input), cn.docker_base_dir, sensit_type)

    uu.print_log("Unzipping boreal/temperate/tropical file (from FAO ecozones)")
    cmd = ['unzip', '{}'.format(cn.pattern_fao_ecozone_raw), '-d', cn.docker_base_dir]

    # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        uu.log_subprocess_output(process.stdout)

    uu.print_log("Copying elevation (srtm) files")
    uu.s3_folder_download(cn.srtm_raw_dir, './srtm', sensit_type)

    uu.print_log("Making elevation (srtm) vrt")
    check_call('gdalbuildvrt srtm.vrt srtm/*.tif', shell=True)  # I don't know how to convert this to output to the pipe, so just leaving as is

    # Worked with count/3 on an r4.16xlarge (140 out of 480 GB used). I think it should be fine with count/2 but didn't try it.
    processes = int(cn.count/2)
    uu.print_log('Inputs for C pools max processors=', processes)
    pool = multiprocessing.Pool(processes)
    pool.map(create_inputs_for_C_pools.create_input_files, tile_id_list)

    # # For single processor use
    # for tile_id in tile_id_list:
    #
    #     create_inputs_for_C_pools.create_input_files(tile_id)

    uu.print_log("Uploading output files")
    for i in range(0, len(output_dir_list)):
        uu.upload_final_set(output_dir_list[i], output_pattern_list[i])


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Create tiles of the annual AGB and BGB gain rates for mangrove forests')
    parser.add_argument('--tile_id_list', '-l', required=True,
                        help='List of tile ids to use in the model. Should be of form 00N_110E or 00N_110E,00N_120E or all.')
    parser.add_argument('--run-date', '-d', required=False,
                        help='Date of run. Must be format YYYYMMDD.')
    args = parser.parse_args()
    tile_id_list = args.tile_id_list
    run_date = args.run_date

    # Create the output log
    uu.initiate_log(tile_id_list, run_date=run_date)

    mp_create_inputs_for_C_pools(tile_id_list, run_date=run_date)