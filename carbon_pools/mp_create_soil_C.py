'''
This script creates tiles of soil carbon density, one of the carbon pools.
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
import multiprocessing
import datetime
import glob
import argparse
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def mp_create_soil_C(tile_id_list):

    os.chdir(cn.docker_base_dir)
    sensit_type = 'std'

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # List of tiles to run in the model
        tile_id_list = uu.create_combined_tile_list(cn.WHRC_biomass_2000_unmasked_dir,
                                             cn.annual_gain_AGB_mangrove_dir
                                             )

    uu.print_log(tile_id_list)
    uu.print_log("There are {} tiles to process".format(str(len(tile_id_list))) + "\n")


    # List of output directories and output file name patterns
    output_dir_list = [cn.soil_C_full_extent_2000_dir, cn.stdev_soil_C_full_extent_2000_dir]
    output_pattern_list = [cn.pattern_soil_C_full_extent_2000, cn.pattern_stdev_soil_C_full_extent]


    # uu.print_log("Downloading mangrove soil C rasters")
    # uu.s3_file_download(os.path.join(cn.mangrove_soil_C_dir, cn.name_mangrove_soil_C), cn.docker_base_dir, sensit_type)

    # For downloading all tiles in the input folders.
    input_files = [cn.mangrove_biomass_2000_dir]

    for input in input_files:
        uu.s3_folder_download(input, cn.docker_base_dir, sensit_type)

    # # Download raw mineral soil C density tiles.
    # # First tries to download index.html.tmp from every folder, then goes back and downloads all the tifs in each folder
    # # Based on https://stackoverflow.com/questions/273743/using-wget-to-recursively-fetch-a-directory-with-arbitrary-files-in-it
    # # There are 12951 tiles!
    # cmd = ['wget', '--recursive', '-nH', '--cut-dirs=6', '--no-parent', '--reject', 'index.html*',
    #                '--accept', '*.tif', '{}'.format(cn.mineral_soil_C_url)]
    # process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    # with process.stdout:
    #     uu.log_subprocess_output(process.stdout)


    uu.print_log("Unzipping mangrove soil C rasters...")
    cmd = ['unzip', '-j', cn.name_mangrove_soil_C, '-d', cn.docker_base_dir]
    # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        uu.log_subprocess_output(process.stdout)

    # Mangrove soil receives precedence over mineral soil
    uu.print_log("Making mangrove soil C vrt...")
    check_call('gdalbuildvrt mangrove_soil_C.vrt *{}*.tif'.format(cn.pattern_mangrove_soil_C_raw), shell=True)
    uu.print_log("Done making mangrove soil C vrt")

    uu.print_log("Making mangrove soil C tiles...")

    if cn.count == 96:
        processes = 32   # 32 processors = XXX GB peak
    else:
        processes = int(cn.count/3)
    uu.print_log('Mangrove soil C max processors=', processes)
    pool = multiprocessing.Pool(processes)
    pool.map(create_soil_C.create_mangrove_soil_C, tile_id_list)
    pool.close()
    pool.join()

    # # For single processor use
    # for tile_id in tile_id_list:
    #
    #     create_soil_C.create_mangrove_soil_C(tile_id)

    uu.print_log("Done making mangrove soil C tiles")

    # Mangrove soil receives precedence over mineral soil
    uu.print_log("Making mineral soil C vrt...")
    check_call('gdalbuildvrt mineral_soil_C.vrt *{}*'.format(cn.pattern_mineral_soil_C_raw), shell=True)
    uu.print_log("Done making mineral soil C vrt")

    # Creates European natural forest removal rate tiles
    source_raster = 'mineral_soil_C.vrt'
    out_pattern = 'mineral_soil'
    dt = 'Int16'
    if cn.count == 96:
        processes = 32  # 32 processors = XXX GB peak
    else:
        processes = int(cn.count/2)
    uu.print_log("Creating mineral soil C density tiles with {} processors...".format(processes))
    pool = multiprocessing.Pool(processes)
    pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt), tile_id_list)
    pool.close()
    pool.join()

    # # For single processor use
    # for tile_id in tile_id_list:
    #
    #     create_soil_C.create_mineral_soil_C(tile_id)

    uu.print_log("Done making mineral soil C tiles")


    uu.print_log("Making combined (mangrove & non-mangrove) soil C tiles...")

    # With count/2 on an r4.16xlarge machine, this was overpowered (used about 240 GB). Could increase the pool.
    if cn.count == 96:
        processes = 45   # 45 processors = XXX GB peak
    else:
        processes = int(cn.count/2)
    uu.print_log('Combined soil C max processors=', processes)
    pool = multiprocessing.Pool(processes)
    pool.map(create_soil_C.create_combined_soil_C, tile_id_list)

    # # For single processor use
    # for tile in tile_list:
    #
    #     create_soil_C.create_combined_soil_C(tile_id)

    uu.print_log("Done making combined soil C tiles")

    uu.print_log("Uploading soil C density tiles")
    uu.upload_final_set(output_dir_list[0], output_pattern_list[0])

    # Need to delete soil c density rasters because they have the same pattern as the standard deviation rasters
    uu.print_log("Deleting raw soil C density rasters")
    c_stocks = glob.glob('*{}*'.format(cn.pattern_soil_C_full_extent_2000))
    for c_stock in c_stocks:
        os.remove(c_stock)


    # # Download raw mineral soil C density standard deviation tiles.
    # # First tries to download index.html.tmp from every folder, then goes back and downloads all the tifs in each folder
    # # Based on https://stackoverflow.com/questions/273743/using-wget-to-recursively-fetch-a-directory-with-arbitrary-files-in-it
    # cmd = ['wget', '--recursive', '-nH', '--cut-dirs=6', '--no-parent', '--reject', 'index.html*',
    #                '--accept', '*.tif', '{}'.format(cn.stdev_mineral_soil_C_url)]
    # process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    # with process.stdout:
    #     uu.log_subprocess_output(process.stdout)
    #
    #
    # # Makes a vrt of mineral soil C standard deviation
    # uu.print_log("Making mineral soil C vrt...")
    # check_call('gdalbuildvrt mineral_soil_C_stdev.vrt *{}*'.format(cn.pattern_mineral_soil_C_raw), shell=True)
    # uu.print_log("Done making mineral soil C stdev vrt")
    #
    # # Creates European natural forest removal rate tiles
    # source_raster = 'mineral_soil_C_stdev.vrt'
    # out_pattern = cn.pattern_stdev_soil_C_full_extent
    # dt = 'Int16'
    # if cn.count == 96:
    #     processes = 32  # 32 processors = XXX GB peak
    # else:
    #     processes = int(cn.count/2)
    # uu.print_log("Creating mineral soil C stock stdev tiles with {} processors...".format(processes))
    # pool = multiprocessing.Pool(processes)
    # pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt), tile_id_list)
    # pool.close()
    # pool.join()
    #
    # uu.print_log("Uploading soil C density standard deviation tiles")
    # uu.upload_final_set(output_dir_list[1], output_pattern_list[1])


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

    mp_create_soil_C(tile_id_list=tile_id_list)