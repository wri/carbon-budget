'''
This script makes mask tiles of where peat pixels are. Peat is represented by 1s; non-peat is no-data.
Between 40N and 60S, CIFOR peat and Jukka peat (IDN and MYS) are combined to map peat.
Outside that band (>40N, since there are no tiles at >60S), SoilGrids250m is used to mask peat.
Any pixel that is marked as most likely being a histosol subgroup is classified as peat.
Between 40N and 60S, SoilGrids250m is not used.
'''


import multiprocessing
import peatland_processing
import argparse
import datetime
import sys
import os
from subprocess import Popen, PIPE, STDOUT, check_call
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def mp_peatland_processing(tile_id_list, run_date = None):

    os.chdir(cn.docker_base_dir)
    sensit_type = 'std'

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # List of tiles to run in the model
        tile_id_list = uu.tile_list_s3(cn.pixel_area_dir)

    uu.print_log(tile_id_list)
    uu.print_log("There are {} tiles to process".format(str(len(tile_id_list))) + "\n")


    # List of output directories and output file name patterns
    output_dir_list = [cn.peat_mask_dir]
    output_pattern_list = [cn.pattern_peat_mask]


    # A date can optionally be provided by the full model script or a run of this script.
    # This replaces the date in constants_and_names.
    if run_date is not None:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, run_date)


    # # # Download SoilGrids250 most probable soil class rasters.
    # # # There are 459 tiles and it takes about 20 minutes to download them
    # # cmd = ['wget', '--recursive', '--no-parent', '-nH', '--cut-dirs=7',
    # #                '--accept', '*.geotiff', '{}'.format(cn.soilgrids250_peat_url)]
    # # uu.log_subprocess_output_full(cmd)
    #
    # uu.print_log("Making SoilGrids250 most likely soil class vrt...")
    # check_call('gdalbuildvrt most_likely_soil_class.vrt *'.format(cn.pattern_soilgrids_most_likely_class), shell=True)
    # uu.print_log("Done making SoilGrids250 most likely soil class vrt")
    #
    #
    # # Downloads peat layers
    # uu.s3_file_download(os.path.join(cn.peat_unprocessed_dir, cn.cifor_peat_file), cn.docker_base_dir, sensit_type)
    # uu.s3_file_download(os.path.join(cn.peat_unprocessed_dir, cn.jukka_peat_zip), cn.docker_base_dir, sensit_type)
    #
    #
    # # Unzips the Jukka peat shapefile (IDN and MYS)
    # cmd = ['unzip', '-o', '-j', cn.jukka_peat_zip]
    # uu.log_subprocess_output_full(cmd)
    #
    # jukka_tif = 'jukka_peat.tif'
    #
    # # Converts the Jukka peat shapefile to a raster
    # uu.print_log('Rasterizing jukka peat...')
    # cmd= ['gdal_rasterize', '-burn', '1', '-co', 'COMPRESS=LZW', '-tr', '{}'.format(cn.Hansen_res), '{}'.format(cn.Hansen_res),
    #       '-tap', '-ot', 'Byte', '-a_nodata', '0', cn.jukka_peat_shp, jukka_tif]
    # uu.log_subprocess_output_full(cmd)
    # uu.print_log('   Jukka peat rasterized')

    # For multiprocessor use
    # count-10 maxes out at about 100 GB on an r5d.16xlarge
    processes=cn.count-10
    uu.print_log('Peatland preprocessing max processors=', processes)
    pool = multiprocessing.Pool(processes)
    pool.map(peatland_processing.create_peat_mask_tiles, tile_id_list)
    pool.close()
    pool.join()

    # # For single processor use, for testing purposes
    # for tile_id in tile_id_list:
    #
    #     peatland_processing.create_peat_mask_tiles(tile_id)

    output_pattern = output_pattern_list[0]
    processes = 50  # 50 processors = XXX GB peak
    uu.print_log("Checking for empty tiles of {0} pattern with {1} processors...".format(output_pattern, processes))
    pool = multiprocessing.Pool(processes)
    pool.map(partial(uu.check_and_delete_if_empty, output_pattern=output_pattern), tile_id_list)
    pool.close()
    pool.join()

    uu.print_log("Uploading output files")
    uu.upload_final_set(output_dir_list[0], output_pattern_list[0])


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

    mp_peatland_processing(tile_id_list=tile_id_list, run_date=run_date)