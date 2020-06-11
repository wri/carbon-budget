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
and the mineral soil raster never actually combined the mangrove tiles with the mineral soil raste; I just kept
getting mineral soil C values out.
So, I switched to this somewhat more convoluted method that uses both gdal and rasterio/numpy.
'''

import subprocess
import create_soil_C
import multiprocessing
import argparse
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def mp_create_soil_C(tile_id_list, run_date = None):

    os.chdir(cn.docker_base_dir)
    sensit_type = 'std'

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # List of tiles to run in the model
        tile_id_list = uu.create_combined_tile_list(cn.WHRC_biomass_2000_non_mang_non_planted_dir,
                                             cn.annual_gain_AGB_mangrove_dir
                                             )

    uu.print_log(tile_id_list)
    uu.print_log("There are {} tiles to process".format(str(len(tile_id_list))) + "\n")


    # List of output directories and output file name patterns
    output_dir_list = [cn.soil_C_full_extent_2000_dir]
    output_pattern_list = [cn.pattern_soil_C_full_extent_2000]


    uu.print_log("Downloading mangrove soil C rasters")
    uu.s3_file_download(os.path.join(cn.mangrove_soil_C_dir, cn.pattern_mangrove_soil_C), cn.docker_base_dir, sensit_type)

    uu.print_log("Downloading mineral soil C raster")
    uu.s3_file_download(os.path.join(cn.mineral_soil_C_dir, cn.pattern_mineral_soil_C), cn.docker_base_dir, sensit_type)

    # For downloading all tiles in the input folders.
    input_files = [cn.mangrove_biomass_2000_dir]

    for input in input_files:
        uu.s3_folder_download(input, cn.docker_base_dir, sensit_type)

    # # For downloading files directly from the internet. NOTE: for some reason, unzip doesn't work on the mangrove
    # # zip file if it is downloaded using wget but it does work if it comes from s3.
    # print "Downloading soil grids 250 raster"
    # cmd = ['wget', 'https://dataverse.harvard.edu/file.xhtml?persistentId=doi:10.7910/DVN/OCYUIT/BY6SFR&version=4.0', '-O', cn.mineral_soil_C_name]
    # subprocess.check_call(cmd)
    #
    # print "Downloading mangrove soil C raster"
    # cmd = ['wget', 'https://files.isric.org/soilgrids/data/recent/OCSTHA_M_30cm_250m_ll.tif', '-O', cn.mineral_soil_C_name]
    # subprocess.check_call(cmd)


    uu.print_log("Unzipping mangrove soil C images...")
    unzip_zones = ['unzip', '-j', cn.pattern_mangrove_soil_C, '-d', cn.docker_base_dir]
    subprocess.check_call(unzip_zones)

    # Mangrove soil receives precedence over mineral soil
    uu.print_log("Making mangrove soil C vrt...")
    subprocess.check_call('gdalbuildvrt mangrove_soil_C.vrt *dSOCS_0_100cm*.tif', shell=True)
    uu.print_log("Done making mangrove soil C vrt")

    uu.print_log("Making mangrove soil C tiles...")

    # count/3 worked on a r4.16xlarge machine. Memory usage maxed out around 350 GB during the gdal_calc step.
    pool = multiprocessing.Pool(processes=int(cn.count/3))
    pool.map(create_soil_C.create_mangrove_soil_C, tile_id_list)

    # # For single processor use
    # for tile_id in tile_id_list:
    #
    #     create_soil_C.create_mangrove_soil_C(tile_id)

    uu.print_log("Done making mangrove soil C tiles")
    uu.print_log("Uploading mangrove output soil")

    # Mangrove soil receives precedence over mineral soil
    uu.print_log("Making mineral soil C vrt...")
    subprocess.check_call('gdalbuildvrt mineral_soil_C.vrt {}'.format(cn.pattern_mineral_soil_C), shell=True)
    uu.print_log("Done making mineral soil C vrt")

    uu.print_log("Making mineral soil C tiles...")

    pool = multiprocessing.Pool(processes=int(cn.count/2))
    pool.map(create_soil_C.create_mineral_soil_C, tile_id_list)

    # # For single processor use
    # for tile_id in tile_id_list:
    #
    #     create_soil_C.create_mineral_soil_C(tile_id)

    uu.print_log("Done making mineral soil C tiles")

    uu.print_log("Making combined soil C tiles...")

    # With count/2 on an r4.16xlarge machine, this was overpowered (used about 240 GB). Could increase the pool.
    pool = multiprocessing.Pool(processes=int(cn.count/2))
    pool.map(create_soil_C.create_combined_soil_C, tile_id_list)

    # # For single processor use
    # for tile in tile_list:
    #
    #     create_soil_C.create_combined_soil_C(tile_id)

    uu.print_log("Done making combined soil C tiles")

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

    mp_create_soil_C(tile_id_list=tile_id_list, run_date=run_date)