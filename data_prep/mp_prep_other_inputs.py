'''
This script processes the inputs for the emissions script that haven't been processed by another script.
At this point, that is: climate zone, Indonesia/Malaysia plantations before 2000, tree cover loss drivers (TSC drivers),
and combining IFL2000 (extratropics) and primary forests (tropics) into a single layer.
'''

from subprocess import Popen, PIPE, STDOUT, check_call
import argparse
import multiprocessing
import datetime
from functools import partial
import sys
import os
import prep_other_inputs

sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def mp_prep_other_inputs(tile_id_list, run_date):

    os.chdir(cn.docker_base_dir)
    sensit_type='std'

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # List of tiles to run in the model
        tile_id_list = uu.create_combined_tile_list(cn.WHRC_biomass_2000_unmasked_dir,
                                             cn.mangrove_biomass_2000_dir,
                                             set3=cn.annual_gain_AGC_BGC_planted_forest_unmasked_dir
                                             )

    uu.print_log(tile_id_list)
    uu.print_log("There are {} tiles to process".format(str(len(tile_id_list))) + "\n")


    # List of output directories and output file name patterns
    output_dir_list = [cn.climate_zone_processed_dir, cn.plant_pre_2000_processed_dir,
                       cn.drivers_processed_dir, cn.ifl_primary_processed_dir]
    output_pattern_list = [cn.pattern_climate_zone, cn.pattern_plant_pre_2000, cn.pattern_drivers, cn.pattern_ifl_primary]


    # If the model run isn't the standard one, the output directory and file names are changed
    if sensit_type != 'std':

        uu.print_log("Changing output directory and file name pattern based on sensitivity analysis")
        output_dir_list = uu.alter_dirs(sensit_type, output_dir_list)
        output_pattern_list = uu.alter_patterns(sensit_type, output_pattern_list)


    # A date can optionally be provided by the full model script or a run of this script.
    # This replaces the date in constants_and_names.
    if run_date is not None:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, run_date)


    # Files to process: climate zone, IDN/MYS plantations before 2000, tree cover loss drivers, combine IFL and primary forest
    uu.s3_file_download(os.path.join(cn.climate_zone_raw_dir, cn.climate_zone_raw), cn.docker_base_dir, sensit_type)
    uu.s3_file_download(os.path.join(cn.plant_pre_2000_raw_dir, '{}.zip'.format(cn.pattern_plant_pre_2000_raw)), cn.docker_base_dir, sensit_type)
    uu.s3_file_download(os.path.join(cn.drivers_raw_dir, '{}.zip'.format(cn.pattern_drivers_raw)), cn.docker_base_dir, sensit_type)
    uu.s3_folder_download(cn.primary_raw_dir, cn.docker_base_dir, sensit_type)
    uu.s3_folder_download(cn.ifl_dir, cn.docker_base_dir, sensit_type)

    cmd = ['unzip', '-j', '{}.zip'.format(cn.pattern_plant_pre_2000_raw)]
    # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        uu.log_subprocess_output(process.stdout)

    cmd = ['unzip', '-j', '{}.zip'.format(cn.pattern_drivers_raw)]
    # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        uu.log_subprocess_output(process.stdout)

    # Converts the IDN/MYS pre-2000 plantation shp to a raster
    cmd= ['gdal_rasterize', '-burn', '1', '-co', 'COMPRESS=LZW', '-tr', '{}'.format(cn.Hansen_res), '{}'.format(cn.Hansen_res),
          '-tap', '-ot', 'Byte', '-a_nodata', '0',
          '{}.shp'.format(cn.pattern_plant_pre_2000_raw), '{}.tif'.format(cn.pattern_plant_pre_2000_raw)]
    # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        uu.log_subprocess_output(process.stdout)

    # Used about 250 GB of memory. count-7 worked fine (with memory to spare) on an r4.16xlarge machine.
    pool = multiprocessing.Pool(cn.count-7)
    pool.map(prep_other_inputs.data_prep, tile_id_list)

    # # For single processor use
    # for tile_id in tile_id_list:
    #
    #       prep_other_inputs.data_prep(tile_id)

    # Creates a vrt of the primary forests with nodata=0
    primary_vrt = 'primary_2001.vrt'
    os.system('gdalbuildvrt -srcnodata 0 {} *2001_primary.tif'.format(primary_vrt))

    # count/3 uses about 300GB, so there's room for more processors on an r4.16xlarge
    uu.print_log("Creating primary forest tiles...")
    pool = multiprocessing.Pool(int(cn.count/3))
    pool.map(partial(prep_other_inputs.create_primary_tile, primary_vrt=primary_vrt), tile_id_list)

    # # For single processor use
    # for tile_id in tile_id_list:
    #
    #       prep_other_inputs.create_primary_tile(tile_id, primary_vrt)

    # Uses very little memory since it's just file renaming
    uu.print_log("Assigning each tile to ifl2000 or primary forest...")
    pool = multiprocessing.Pool(cn.count-5)
    pool.map(prep_other_inputs.create_combined_ifl_primary, tile_id_list)

    # # For single processor use
    # for tile_id in tile_id_list:
    #
    #       prep_other_inputs.create_combined_ifl_primary(tile_id)


    # Uploads output tiles to s3
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
    uu.initiate_log(tile_id_list=tile_id_list, run_date=run_date)

    mp_prep_other_inputs(tile_id_list=tile_id_list, run_date=run_date)