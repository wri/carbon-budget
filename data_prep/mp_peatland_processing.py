'''
This script makes mask tiles of where peat pixels are. Peat is represented by 1s; non-peat is no-data.
Between 40N and 60S, Gumbricht et al. 2017 (CIFOR) peat is used.
Miettinen et al. 2016 and Dargie et al. 2017 supplement it in IDN/MYS and the Congo basin, respectively.
Outside that band (>40N, since there are no tiles at >60S), Xu et al. 2018 is used to mask peat.
Between 40N and 60S, Xu et al. 2018 is not used.

python -m data_prep.mp_peatland_processing -l 00N_000E -nu
python -m data_prep.mp_peatland_processing -l all
'''


import argparse
from functools import partial
import multiprocessing
import os
import sys

import constants_and_names as cn
import universal_util as uu
from . import peatland_processing


def mp_peatland_processing(tile_id_list):

    os.chdir(cn.docker_base_dir)

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # List of tiles to run in the model
        tile_id_list = uu.tile_list_s3(cn.pixel_area_dir)

    uu.print_log(tile_id_list)
    uu.print_log(f'There are {str(len(tile_id_list))} tiles to process', "\n")


    # List of output directories and output file name patterns
    output_dir_list = [cn.peat_mask_dir]
    output_pattern_list = [cn.pattern_peat_mask]


    # A date can optionally be provided by the full model script or a run of this script.
    # This replaces the date in constants_and_names.
    # Only done if output upload is enabled.
    if cn.RUN_DATE is not None and cn.NO_UPLOAD is False:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, cn.RUN_DATE)

    # NOTE: Locally merged in ArcMap all the Xu et al. 2018 peat shapefiles that are above 40N into a single shapefile:
    # Xu_et_al_north_of_40N__20230228.shp. Only merged the Xu et al. shapefiles that were north of 40N because
    # below that latitude, the model uses Gumbricht (CIFOR) 2017.

    # Downloads peat layers
    uu.s3_file_download(os.path.join(cn.peat_unprocessed_dir, cn.Gumbricht_peat_name), cn.docker_base_dir, cn.SENSIT_TYPE)
    uu.s3_file_download(os.path.join(cn.peat_unprocessed_dir, cn.Miettinen_peat_zip), cn.docker_base_dir, cn.SENSIT_TYPE)
    uu.s3_file_download(os.path.join(cn.peat_unprocessed_dir, cn.Xu_peat_zip), cn.docker_base_dir, cn.SENSIT_TYPE)
    uu.s3_file_download(os.path.join(cn.peat_unprocessed_dir, cn.Dargie_name), cn.docker_base_dir, cn.SENSIT_TYPE)

    # Unzips the Miettinen et al. peat shapefile (IDN and MYS)
    cmd = ['unzip', '-o', '-j', cn.Miettinen_peat_zip]
    uu.log_subprocess_output_full(cmd)

    # Unzips the Dargie et al. peat shapefile (Congo basin)
    cmd = ['unzip', '-o', '-j', cn.Xu_peat_zip]
    uu.log_subprocess_output_full(cmd)

    # Converts the Miettinen IDN/MYS peat shapefile to a raster
    uu.print_log('Rasterizing Miettinen map...')
    cmd= ['gdal_rasterize', '-burn', '1', '-co', 'COMPRESS=DEFLATE', '-tr', '{}'.format(cn.Hansen_res), '{}'.format(cn.Hansen_res),
          '-tap', '-ot', 'Byte', '-a_nodata', '0', cn.Miettinen_peat_shp, cn.Miettinen_peat_tif]
    uu.log_subprocess_output_full(cmd)
    uu.print_log('   Miettinen IDN/MYS peat rasterized')

    # Masks the Dargie raster to just the peat class (code 4).
    uu.print_log('Masking Dargie map to just peat class...')
    Dargie_calc = f'--calc=(A==4)'
    Dargie_outfilearg = f'--outfile={cn.Dargie_peat_name}'
    cmd = ['gdal_calc.py', '-A', cn.Dargie_name, Dargie_calc, Dargie_outfilearg,
           '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=DEFLATE', '--type', 'Byte']
    uu.log_subprocess_output_full(cmd)

    if cn.SINGLE_PROCESSOR:
        for tile_id in tile_id_list:
            peatland_processing.create_peat_mask_tiles(tile_id)
    else:
        processes = 60 #30=160 GB peak; 60=XXX GB peak
        uu.print_log('Peat map processors=', processes)
        with multiprocessing.Pool(processes) as pool:
            pool.map(peatland_processing.create_peat_mask_tiles, tile_id_list)
            pool.close()
            pool.join()


    # No single-processor versions of these check-if-empty functions
    output_pattern = output_pattern_list[0]
    if cn.count <= 2:  # For local tests
        processes = 1
        uu.print_log(f'Checking for empty tiles of {output_pattern} pattern with {output_pattern} processors using light function...')
        with multiprocessing.Pool(processes) as pool:
            pool.map(partial(uu.check_and_delete_if_empty_light, output_pattern=output_pattern), tile_id_list)
            pool.close()
            pool.join()
    else:
        processes = 75  # 58 processors = 220 GB peak; 75=XXX GB peak
        uu.print_log(f'Checking for empty tiles of {output_pattern} pattern with {output_pattern} processors...')
        with multiprocessing.Pool(processes) as pool:
            pool.map(partial(uu.check_and_delete_if_empty, output_pattern=output_pattern), tile_id_list)
            pool.close()
            pool.join()


    # If no_upload flag is not activated (by choice or by lack of AWS credentials), output is uploaded
    if not cn.NO_UPLOAD:
        uu.upload_final_set(output_dir_list[0], output_pattern_list[0])


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Creates tiles of the extent of peatlands')
    parser.add_argument('--tile_id_list', '-l', required=True,
                        help='List of tile ids to use in the model. Should be of form 00N_110E or 00N_110E,00N_120E or all.')
    parser.add_argument('--run-date', '-d', required=False,
                        help='Date of run. Must be format YYYYMMDD.')
    parser.add_argument('--no-upload', '-nu', action='store_true',
                       help='Disables uploading of outputs to s3')
    parser.add_argument('--single-processor', '-sp', action='store_true',
                       help='Uses single processing rather than multiprocessing')
    args = parser.parse_args()

    # Sets global variables to the command line arguments
    cn.SENSIT_TYPE = 'std'
    cn.RUN_DATE = args.run_date
    cn.NO_UPLOAD = args.no_upload
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

    mp_peatland_processing(tile_id_list=tile_id_list)