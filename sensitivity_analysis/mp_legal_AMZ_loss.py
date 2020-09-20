'''
Downloaded PRODES loss from http://www.dpi.inpe.br/prodesdigital/dadosn/mosaicos/
'''


import multiprocessing
from functools import partial
import glob
import datetime
import argparse
from osgeo import gdal
import legal_AMZ_loss
import pandas as pd
from subprocess import Popen, PIPE, STDOUT, check_call
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu
sys.path.append('../gain')
import annual_gain_rate_natrl_forest
import cumulative_gain_natrl_forest
import merge_cumulative_annual_gain_all_forest_types
sys.path.append('../carbon_pools')
import create_carbon_pools

def main ():

    # Create the output log
    uu.initiate_log()

    os.chdir(cn.docker_base_dir)

    Brazil_stages = ['all', 'create_forest_extent', 'create_loss']


    # The argument for what kind of model run is being done: standard conditions or a sensitivity analysis run
    parser = argparse.ArgumentParser(description='Create tiles of forest extent in legal Amazon in 2000 and annual loss according to PRODES')
    parser.add_argument('--stages', '-s', required=True,
                        help='Stages of creating Brazil legal Amazon-specific gross cumulative removals. Options are {}'.format(Brazil_stages))
    parser.add_argument('--run_through', '-r', required=True,
                        help='Options: true or false. true: run named stage and following stages. false: run only named stage.')
    args = parser.parse_args()
    stage_input = args.stages
    run_through = args.run_through


    # Checks the validity of the two arguments. If either one is invalid, the script ends.
    if (stage_input not in Brazil_stages):
        uu.exception_log('Invalid stage selection. Please provide a stage from', Brazil_stages)
    else:
        pass
    if (run_through not in ['true', 'false']):
        uu.exception_log('Invalid run through option. Please enter true or false.')
    else:
        pass

    actual_stages = uu.analysis_stages(Brazil_stages, stage_input, run_through)
    uu.print_log(actual_stages)


    # By definition, this script is for US-specific removals
    sensit_type = 'legal_Amazon_loss'

    # List of output directories and output file name patterns
    master_output_dir_list = [cn.Brazil_forest_extent_2000_processed_dir, cn.Brazil_annual_loss_processed_dir]

    master_output_pattern_list = [cn.pattern_Brazil_forest_extent_2000_processed, cn.pattern_Brazil_annual_loss_processed]


    # Creates forest extent 2000 raster from multiple PRODES forest extent rasters
    ###NOTE: Didn't redo this for model v1.2.0, so I don't know if it still works.
    if 'create_forest_extent' in actual_stages:

        uu.print_log('Creating forest extent tiles')

        # List of tiles that could be run. This list is only used to create the FIA region tiles if they don't already exist.
        tile_id_list = uu.tile_list_s3(cn.WHRC_biomass_2000_unmasked_dir)
        # tile_id_list = ["00N_000E", "00N_050W", "00N_060W", "00N_010E", "00N_020E", "00N_030E", "00N_040E", "10N_000E", "10N_010E", "10N_010W", "10N_020E", "10N_020W"] # test tiles
        # tile_id_list = ['50N_130W'] # test tiles
        uu.print_log(tile_id_list)
        uu.print_log("There are {} tiles to process".format(str(len(tile_id_list))) + "\n")

        # Downloads input rasters and lists them
        uu.s3_folder_download(cn.Brazil_forest_extent_2000_raw_dir, cn.docker_base_dir, sensit_type)
        raw_forest_extent_inputs = glob.glob('*_AMZ_warped_*tif')   # The list of tiles to merge

        # Gets the resolution of a more recent PRODES raster, which has a higher resolution. The merged output matches that.
        raw_forest_extent_input_2019 = glob.glob('*2019_AMZ_warped_*tif')
        prodes_2019 = gdal.Open(raw_forest_extent_input_2019[0])
        transform_2019 = prodes_2019.GetGeoTransform()
        pixelSizeX = transform_2019[1]
        pixelSizeY = -transform_2019[5]
        uu.print_log(pixelSizeX)
        uu.print_log(pixelSizeY)

        # This merges all six rasters together, so it takes a lot of memory and time. It seems to repeatedly max out
        # at about 300 GB as it progresses abot 15% each time; then the memory drops back to 0 and slowly increases.
        cmd = ['gdal_merge.py', '-o', '{}.tif'.format(cn.pattern_Brazil_forest_extent_2000_merged),
               '-co', 'COMPRESS=LZW', '-a_nodata', '0', '-n', '0', '-ot', 'Byte', '-ps', '{}'.format(pixelSizeX), '{}'.format(pixelSizeY),
               raw_forest_extent_inputs[0], raw_forest_extent_inputs[1], raw_forest_extent_inputs[2],
               raw_forest_extent_inputs[3], raw_forest_extent_inputs[4], raw_forest_extent_inputs[5]]
        uu.log_subprocess_output_full(cmd)

        # Uploads the merged forest extent raster to s3 for future reference
        uu.upload_final_set(cn.Brazil_forest_extent_2000_merged_dir, cn.pattern_Brazil_forest_extent_2000_merged)

        # Creates legal Amazon extent 2000 tiles
        source_raster = '{}.tif'.format(cn.pattern_Brazil_forest_extent_2000_merged)
        out_pattern = cn.pattern_Brazil_forest_extent_2000_processed
        dt = 'Byte'
        pool = multiprocessing.Pool(int(cn.count/2))
        pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt), tile_id_list)

        # Checks if each tile has data in it. Only tiles with data are uploaded.
        upload_dir = master_output_dir_list[0]
        pattern = master_output_pattern_list[0]
        pool = multiprocessing.Pool(cn.count - 5)
        pool.map(partial(uu.check_and_upload, upload_dir=upload_dir, pattern=pattern), tile_id_list)


    # Creates annual loss raster for 2001-2019 from multiples PRODES rasters
    if 'create_loss' in actual_stages:

        uu.print_log('Creating annual PRODES loss tiles')

        tile_id_list = uu.tile_list_s3(cn.Brazil_forest_extent_2000_processed_dir)
        uu.print_log(tile_id_list)
        uu.print_log("There are {} tiles to process".format(str(len(tile_id_list))) + "\n")

        # Downloads input rasters and lists them
        uu.s3_folder_download(cn.Brazil_annual_loss_raw_dir, cn.docker_base_dir, sensit_type)

        # Gets the resolution of the more recent PRODES raster, which has a higher resolution. The merged output matches that.
        raw_forest_extent_input_2019 = glob.glob('Prodes2019_*tif')
        prodes_2019 = gdal.Open(raw_forest_extent_input_2019[0])
        transform_2019 = prodes_2019.GetGeoTransform()
        pixelSizeX = transform_2019[1]
        pixelSizeY = -transform_2019[5]

        # This merges both loss rasters together, so it takes a lot of memory and time. It seems to max out
        # at about 150 GB. Loss from PRODES2014 needs to go second so that its loss years get priority over PRODES2017,
        # which seems to have a preponderance of 2007 loss that appears to often be earlier loss years.
        cmd = ['gdal_merge.py', '-o', '{}.tif'.format(cn.pattern_Brazil_annual_loss_merged),
               '-co', 'COMPRESS=LZW', '-a_nodata', '0', '-n', '0', '-ot', 'Byte', '-ps', '{}'.format(pixelSizeX), '{}'.format(pixelSizeY),
               'Prodes2019_annual_loss_2008_2019.tif', 'Prodes2014_annual_loss_2001_2007.tif']
        uu.log_subprocess_output_full(cmd)

        # Uploads the merged loss raster to s3 for future reference
        uu.upload_final_set(cn.Brazil_annual_loss_merged_dir, cn.pattern_Brazil_annual_loss_merged)

        # Creates annual loss 2001-2015 tiles
        source_raster = '{}.tif'.format(cn.pattern_Brazil_annual_loss_merged)
        out_pattern = cn.pattern_Brazil_annual_loss_processed
        dt = 'Byte'
        pool = multiprocessing.Pool(int(cn.count/2))
        pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt), tile_id_list)

        # Checks if each tile has data in it. Only tiles with data are uploaded.
        # In practice, every Amazon tile has loss in it but I figured I'd do this just to be thorough.
        upload_dir = master_output_dir_list[1]
        pattern = master_output_pattern_list[1]
        pool = multiprocessing.Pool(cn.count - 5)
        pool.map(partial(uu.check_and_upload, upload_dir=upload_dir, pattern=pattern), tile_id_list)


if __name__ == '__main__':
    main()