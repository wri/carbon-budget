'''
This script creates tiles of soil carbon density, one of the carbon emitted_pools.
At this time, mineral soil carbon is for the top 30 cm of soil.
Mangrove soil carbon gets precedence over mineral soil carbon where there is mangrove biomass.
Mangrove soil C is limited to where mangrove AGB is.
Where there is no mangrove biomass, mineral soil C is used.
Peatland carbon is not recognized or involved in any way.
This is a convoluted way of doing this processing. Originally, I tried making mangrove soil tiles masked to
mangrove AGB tiles, then making a vrt of all those mangrove soil C tiles and the mineral soil raster, and then
using gdal_warp on that to get combined tiles.
However, for reasons I couldn't figure out, the gdalbuildvrt step in which I combined the mangrove 10x10 tiles
and the mineral soil raster never actually combined the mangrove tiles with the mineral soil raster; I just kept
getting mineral soil C values out.
So, I switched to this somewhat more convoluted method that uses both gdal and rasterio/numpy.
'''

from subprocess import Popen, PIPE, STDOUT, check_call
import create_soil_C
from functools import partial
import multiprocessing
import datetime
import glob
import argparse
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def mp_create_soil_C(tile_id_list, no_upload=None):

    os.chdir(cn.docker_base_dir)
    sensit_type = 'std'

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # List of tiles to run in the model
        tile_id_list = uu.create_combined_tile_list(cn.WHRC_biomass_2000_unmasked_dir,
                                             cn.mangrove_biomass_2000_dir,
                                             set3=cn.gain_dir
                                             )

    uu.print_log(tile_id_list)
    uu.print_log("There are {} tiles to process".format(str(len(tile_id_list))) + "\n")


    # List of output directories and output file name patterns
    output_dir_list = [cn.soil_C_full_extent_2000_non_mang_dir, cn.soil_C_full_extent_2000_dir,
                       cn.stdev_soil_C_full_extent_2000_dir]
    output_pattern_list = [cn.pattern_soil_C_full_extent_2000_non_mang, cn.pattern_soil_C_full_extent_2000,
                           cn.pattern_stdev_soil_C_full_extent]


    ### Soil carbon density

    uu.print_log("Downloading mangrove soil C rasters")
    uu.s3_file_download(os.path.join(cn.mangrove_soil_C_dir, cn.name_mangrove_soil_C), cn.docker_base_dir, sensit_type)

    # For downloading all tiles in the input folders.
    input_files = [cn.mangrove_biomass_2000_dir]

    for input in input_files:
        uu.s3_folder_download(input, cn.docker_base_dir, sensit_type)

    # Download raw mineral soil C density tiles.
    # First tries to download index.html.tmp from every folder, then goes back and downloads all the tifs in each folder
    # Based on https://stackoverflow.com/questions/273743/using-wget-to-recursively-fetch-a-directory-with-arbitrary-files-in-it
    # There are 12951 tiles and it takes about 3 hours to download them!
    cmd = ['wget', '--recursive', '-nH', '--cut-dirs=6', '--no-parent', '--reject', 'index.html*',
                   '--accept', '*.tif', '{}'.format(cn.mineral_soil_C_url)]
    uu.log_subprocess_output_full(cmd)

    uu.print_log("Unzipping mangrove soil C rasters...")
    cmd = ['unzip', '-j', cn.name_mangrove_soil_C, '-d', cn.docker_base_dir]
    uu.log_subprocess_output_full(cmd)

    # Mangrove soil receives precedence over mineral soil
    uu.print_log("Making mangrove soil C vrt...")
    check_call('gdalbuildvrt mangrove_soil_C.vrt *{}*.tif'.format(cn.pattern_mangrove_soil_C_raw), shell=True)
    uu.print_log("Done making mangrove soil C vrt")

    uu.print_log("Making mangrove soil C tiles...")

    if cn.count == 96:
        processes = 32   # 32 processors = 570 GB peak
    else:
        processes = int(cn.count/3)
    uu.print_log('Mangrove soil C max processors=', processes)
    pool = multiprocessing.Pool(processes)
    pool.map(partial(create_soil_C.create_mangrove_soil_C, no_upload=no_upload), tile_id_list)
    pool.close()
    pool.join()

    # # For single processor use
    # for tile_id in tile_id_list:
    #
    #     create_soil_C.create_mangrove_soil_C(tile_id, no_Upload)

    uu.print_log('Done making mangrove soil C tiles', '\n')

    uu.print_log("Making mineral soil C vrt...")
    check_call('gdalbuildvrt mineral_soil_C.vrt *{}*'.format(cn.pattern_mineral_soil_C_raw), shell=True)
    uu.print_log("Done making mineral soil C vrt")

    # Creates mineral soil C density tiles
    source_raster = 'mineral_soil_C.vrt'
    out_pattern = cn.pattern_soil_C_full_extent_2000_non_mang
    dt = 'Int16'
    if cn.count == 96:
        processes = 80  # 32 processors = 100 GB peak; 50 = 160 GB peak; 80 = XXX GB peak
    else:
        processes = int(cn.count/2)
    uu.print_log("Creating mineral soil C density tiles with {} processors...".format(processes))
    pool = multiprocessing.Pool(processes)
    pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt,
                     no_upload=no_upload), tile_id_list)
    pool.close()
    pool.join()

    # # For single processor use
    # for tile_id in tile_id_list:
    #
    #     create_soil_C.create_mineral_soil_C(tile_id)

    uu.print_log("Done making non-mangrove soil C tiles", "\n")

    output_pattern = cn.pattern_soil_C_full_extent_2000_non_mang
    processes = 60 # 50 processors = ~450 GB peak; 60 = XXX GB peak
    uu.print_log("Checking for empty tiles of {0} pattern with {1} processors...".format(output_pattern, processes))
    pool = multiprocessing.Pool(processes)
    pool.map(partial(uu.check_and_delete_if_empty, output_pattern=output_pattern), tile_id_list)
    pool.close()
    pool.join()

    # If no_upload flag is not activated (by choice or by lack of AWS credentials), output is uploaded to s3
    if not no_upload:

        uu.print_log("Uploading non-mangrove soil C density tiles")
        uu.upload_final_set(output_dir_list[0], output_pattern_list[0])


    uu.print_log("Making combined (mangrove & non-mangrove) soil C tiles...")

    if cn.count == 96:
        processes = 45   # 45 processors = XXX GB peak
    else:
        processes = int(cn.count/2)
    uu.print_log('Combined soil C max processors=', processes)
    pool = multiprocessing.Pool(processes)
    pool.map(partial(create_soil_C.create_combined_soil_C, no_upload=no_upload), tile_id_list)
    pool.close()
    pool.join()

    # # For single processor use
    # for tile in tile_list:
    #
    #     create_soil_C.create_combined_soil_C(tile_id, no_upload)

    uu.print_log("Done making combined soil C tiles")

    # If no_upload flag is not activated (by choice or by lack of AWS credentials), output is uploaded
    if not no_upload:

        uu.print_log("Uploading combined soil C density tiles")
        uu.upload_final_set(output_dir_list[1], output_pattern_list[1])


    # Need to delete soil c density rasters because they have the same pattern as the standard deviation rasters
    uu.print_log("Deleting raw soil C density rasters")
    c_stocks = glob.glob('*{}*'.format(cn.pattern_soil_C_full_extent_2000))
    for c_stock in c_stocks:
        os.remove(c_stock)


    ### Soil carbon density uncertainty

    # Separate directories for the 5% CI and 95% CI
    dir_CI05 = '{0}{1}'.format(cn.docker_base_dir, 'CI05/')
    dir_CI95 = '{0}{1}'.format(cn.docker_base_dir, 'CI95/')
    vrt_CI05 = 'mineral_soil_C_CI05.vrt'
    vrt_CI95 = 'mineral_soil_C_CI95.vrt'
    soil_C_stdev_global = 'soil_C_stdev.tif'

    # Download raw mineral soil C density 5% CI tiles
    # First tries to download index.html.tmp from every folder, then goes back and downloads all the tifs in each folder
    # Based on https://stackoverflow.com/questions/273743/using-wget-to-recursively-fetch-a-directory-with-arbitrary-files-in-it
    # Like soil C density rasters, there are 12951 tifs and they take about 3 hours to download.
    os.mkdir(dir_CI05)

    cmd = ['wget', '--recursive', '-nH', '--cut-dirs=6', '--no-parent', '--reject', 'index.html*',
                   '--directory-prefix={}'.format(dir_CI05),
                   '--accept', '*.tif', '{}'.format(cn.CI5_mineral_soil_C_url)]
    uu.log_subprocess_output_full(cmd)

    uu.print_log("Making mineral soil C 5% CI vrt...")

    check_call('gdalbuildvrt {0} {1}*{2}*'.format(vrt_CI05, dir_CI05, cn.pattern_uncert_mineral_soil_C_raw), shell=True)
    uu.print_log("Done making mineral soil C CI05 vrt")

    # Download raw mineral soil C density 5% CI tiles
    # Like soil C density rasters, there are 12951 tifs and they take about 3 hours to download.
    os.mkdir(dir_CI95)

    cmd = ['wget', '--recursive', '-nH', '--cut-dirs=6', '--no-parent', '--reject', 'index.html*',
                   '--directory-prefix={}'.format(dir_CI95),
                   '--accept', '*.tif', '{}'.format(cn.CI95_mineral_soil_C_url)]
    uu.log_subprocess_output_full(cmd)

    uu.print_log("Making mineral soil C 95% CI vrt...")

    check_call('gdalbuildvrt {0} {1}*{2}*'.format(vrt_CI95, dir_CI95, cn.pattern_uncert_mineral_soil_C_raw), shell=True)
    uu.print_log("Done making mineral soil C CI95 vrt")


    uu.print_log("Creating raster of standard deviations in soil C at native SoilGrids250 resolution. This may take a while...")
    # global tif with approximation of the soil C stanard deviation (based on the 5% and 95% CIs)

    # This takes about 20 minutes. It doesn't show any progress until the last moment, when it quickly counts
    # up to 100.
    calc = '--calc=(A-B)/3'
    out_filearg = '--outfile={}'.format(soil_C_stdev_global)
    cmd = ['gdal_calc.py', '-A', vrt_CI95, '-B', vrt_CI05, calc, out_filearg,
           '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW', '--type=Float32']
    uu.log_subprocess_output_full(cmd)

    uu.print_log("{} created.".format(soil_C_stdev_global))


    # Creates soil carbon 2000 density standard deviation tiles
    out_pattern = cn.pattern_stdev_soil_C_full_extent
    dt = 'Float32'
    source_raster = soil_C_stdev_global
    if cn.count == 96:
        processes = 56  # 32 processors = 290 GB peak; 56 = XXX GB peal
    else:
        processes = 2
    uu.print_log("Creating mineral soil C stock stdev tiles with {} processors...".format(processes))
    pool = multiprocessing.Pool(processes)
    pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt,
                     no_upload=no_upload), tile_id_list)
    pool.close()
    pool.join()


    output_pattern = cn.pattern_stdev_soil_C_full_extent
    processes = 50 # 50 processors = 550 GB peak
    uu.print_log("Checking for empty tiles of {0} pattern with {1} processors...".format(output_pattern, processes))
    pool = multiprocessing.Pool(processes)
    pool.map(partial(uu.check_and_delete_if_empty, output_pattern=output_pattern), tile_id_list)
    pool.close()
    pool.join()


    # Checks the gross removals outputs for tiles with no data
    for output_pattern in output_pattern_list:
        if cn.count <= 2:  # For local tests
            processes = 1
            uu.print_log("Checking for empty tiles of {0} pattern with {1} processors using light function...".format(
                output_pattern, processes))
            pool = multiprocessing.Pool(processes)
            pool.map(partial(uu.check_and_delete_if_empty_light, output_pattern=output_pattern), tile_id_list)
            pool.close()
            pool.join()
        else:
            processes = 55  # 50 processors = XXX GB peak
            uu.print_log(
                "Checking for empty tiles of {0} pattern with {1} processors...".format(output_pattern, processes))
            pool = multiprocessing.Pool(processes)
            pool.map(partial(uu.check_and_delete_if_empty, output_pattern=output_pattern), tile_id_list)
            pool.close()
            pool.join()


    # If no_upload flag is not activated (by choice or by lack of AWS credentials), output is uploaded
    if not no_upload:

        uu.print_log("Uploading soil C density standard deviation tiles")
        uu.upload_final_set(output_dir_list[2], output_pattern_list[2])


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Creates tiles of soil carbon density in 2000')
    parser.add_argument('--tile_id_list', '-l', required=True,
                        help='List of tile ids to use in the model. Should be of form 00N_110E or 00N_110E,00N_120E or all.')
    parser.add_argument('--run-date', '-d', required=False,
                        help='Date of run. Must be format YYYYMMDD.')
    parser.add_argument('--no-upload', '-nu', action='store_true',
                       help='Disables uploading of outputs to s3')
    args = parser.parse_args()
    tile_id_list = args.tile_id_list
    run_date = args.run_date
    no_upload = args.no_upload

    # Disables upload to s3 if no AWS credentials are found in environment
    if not uu.check_aws_creds():
        no_upload = True

    # Create the output log
    uu.initiate_log(tile_id_list, run_date=run_date)
    tile_id_list = uu.tile_id_list_check(tile_id_list)

    mp_create_soil_C(tile_id_list=tile_id_list, no_upload=no_upload)