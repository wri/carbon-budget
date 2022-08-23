'''
Creates tiles of when tree cover loss coincides with burning or preceded burning by one year.
There are four steps to this: 1) acquire raw hdfs from MODIS burned area sftp; 2) make tifs of burned area for
each year in each MODIS h-v tile; 3) make annual Hansen-style (extent, res, etc.) tiles of burned area;
4) make tiles of where TCL and burning coincided (same year or with 1 year lag).
To update this, steps 1-3 can be run on only the latest year of MODIS burned area product. Only step 4 needs to be run
on the entire time series. That is, steps 1-3 operate on burned area products separately for each year, so adding
another year of data won't change steps 1-3 for preceding years.

NOTE: The step in which hdf files are opened and converted to tifs (step 2) requires
osgeo/gdal:ubuntu-full-X.X.X Docker image (change in Dockerfile).
The "small' Docker image doesn't have an hdf driver in gdal, so it can't read
the hdf files on the ftp site. The rest of the burned area analysis can be done with a 'small' version of the Docker image
(though that would require terminating the Docker container and restarting it, which would only make sense if the
analysis was being continued later).

Step 4 takes many hours to run, mostly because it only uses five processors since each one requires so much memory.
The other steps might take an hour or two to run.

This is still basically as Sam Gibbes wrote it in early 2018, with file name changes and other input/output changes
by David Gibbs. The real processing code is still all by Sam's parts.
'''

import multiprocessing
from functools import partial
import pandas as pd
import datetime
import glob
import shutil
import argparse
from subprocess import Popen, PIPE, STDOUT, check_call
import os
import sys
import utilities
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu
sys.path.append(os.path.join(cn.docker_app,'burn_date'))
import stack_ba_hv
import clip_year_tiles
import hansen_burnyear_final


def mp_burn_year(tile_id_list, run_date = None, no_upload = None):

    os.chdir(cn.docker_base_dir)

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # List of tiles to run in the model
        tile_id_list = uu.tile_list_s3(cn.pixel_area_dir)

    uu.print_log(tile_id_list)
    uu.print_log("There are {} tiles to process".format(str(len(tile_id_list))) + "\n")

    # List of output directories and output file name patterns
    output_dir_list = [cn.burn_year_dir]
    output_pattern_list = [cn.pattern_burn_year]

    # A date can optionally be provided by the full model script or a run of this script.
    # This replaces the date in constants_and_names.
    # Only done if output upload is enabled.
    if run_date is not None and no_upload is not None:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, run_date)

    global_grid_hv = ["h00v08", "h00v09", "h00v10", "h01v07", "h01v08", "h01v09", "h01v10", "h01v11", "h02v06",
                      "h02v08", "h02v09", "h02v10", "h02v11", "h03v06", "h03v07", "h03v09", "h03v10", "h03v11",
                      "h04v09", "h04v10", "h04v11", "h05v10", "h05v11", "h05v13", "h06v03", "h06v11", "h07v03",
                      "h07v05", "h07v06", "h07v07", "h08v03", "h08v04", "h08v05", "h08v06", "h08v07", "h08v08",
                      "h08v09", "h08v11", "h09v02", "h09v03", "h09v04", "h09v05", "h09v06", "h09v07", "h09v08",
                      "h09v09", "h10v02", "h10v03", "h10v04", "h10v05", "h10v06", "h10v07", "h10v08", "h10v09",
                      "h10v10", "h10v11", "h11v02", "h11v03", "h11v04", "h11v05", "h11v06", "h11v07", "h11v08",
                      "h11v09", "h11v10", "h11v11", "h11v12", "h12v02", "h12v03", "h12v04", "h12v05", "h12v07",
                      "h12v08", "h12v09", "h12v10", "h12v11", "h12v12", "h12v13", "h13v02", "h13v03", "h13v04",
                      "h13v08", "h13v09", "h13v10", "h13v11", "h13v12", "h13v13", "h13v14", "h14v02", "h14v03",
                      "h14v04", "h14v09", "h14v10", "h14v11", "h14v14", "h15v02", "h15v03", "h15v05", "h15v07",
                      "h15v11", "h16v02", "h16v05", "h16v06", "h16v07", "h16v08", "h16v09", "h17v02", "h17v03",
                      "h17v04", "h17v05", "h17v06", "h17v07", "h17v08", "h17v10", "h17v12", "h17v13", "h18v02",
                      "h18v03", "h18v04", "h18v05", "h18v06", "h18v07", "h18v08", "h18v09", "h19v02", "h19v03",
                      "h19v04", "h19v05", "h19v06", "h19v07", "h19v08", "h19v09", "h19v10", "h19v11", "h19v12",
                      "h20v02", "h20v03", "h20v04", "h20v05", "h20v06", "h20v07", "h20v08", "h20v09", "h20v10",
                      "h20v11", "h20v12", "h20v13", "h21v02", "h21v03", "h21v04", "h21v05", "h21v06", "h21v07",
                      "h21v08", "h21v09", "h21v10", "h21v11", "h21v13", "h22v02", "h22v03", "h22v04", "h22v05",
                      "h22v06", "h22v07", "h22v08", "h22v09", "h22v10", "h22v11", "h22v13", "h23v02", "h23v03",
                      "h23v04", "h23v05", "h23v06", "h23v07", "h23v08", "h23v09", "h23v10", "h23v11", "h24v02",
                      "h24v03", "h24v04", "h24v05", "h24v06", "h24v07", "h24v12", "h25v02", "h25v03", "h25v04",
                      "h25v05", "h25v06", "h25v07", "h25v08", "h25v09", "h26v02", "h26v03", "h26v04", "h26v05",
                      "h26v06", "h26v07", "h26v08", "h27v03", "h27v04", "h27v05", "h27v06", "h27v07", "h27v08",
                      "h27v09", "h27v10", "h27v11", "h27v12", "h28v03", "h28v04", "h28v05", "h28v06", "h28v07",
                      "h28v08", "h28v09", "h28v10", "h28v11", "h28v12", "h28v13", "h29v03", "h29v05", "h29v06",
                      "h29v07", "h29v08", "h29v09", "h29v10", "h29v11", "h29v12", "h29v13", "h30v06", "h30v07",
                      "h30v08", "h30v09", "h30v10", "h30v11", "h30v12", "h30v13", "h31v06", "h31v07", "h31v08",
                      "h31v09", "h31v10", "h31v11", "h31v12", "h31v13", "h32v07", "h32v08", "h32v09", "h32v10",
                      "h32v11", "h32v12", "h33v07", "h33v08", "h33v09", "h33v10", "h33v11", "h34v07", "h34v08",
                      "h34v09", "h34v10", "h35v08", "h35v09", "h35v10"]


    # Step 1: download hdf files for relevant year(s) from sftp site.
    # This only needs to be done for the most recent year of data.

    '''
    Downloading the hdf files from the sftp burned area site is done outside the script in the sftp shell on the command line.
    This will download all the 2021 hdfs to the spot machine. There will be a pause of a few minutes before the first
    hdf is downloaded but then it should go quickly (5 minutes for 2021 data).
    Change 2021 to other year for future years of downloads. 
    https://modis-fire.umd.edu/files/MODIS_C6_BA_User_Guide_1.3.pdf, page 24, section 4.1.3

    Change directory to /app/burn_date/ and download hdfs into burn_date folder:

    sftp fire@fuoco.geog.umd.edu
    [For password] burnt
    cd data/MODIS/C6/MCD64A1/HDF
    ls [to check that it's the folder with all the h-v tile folders]
    get h??v??/MCD64A1.A2021*
    bye    //exits the stfp shell
    
    Before moving to the next step, confirm that all months of burned area data were downloaded. 
    The last month will have the format MCD64A1.A20**336.h... or so.
    '''


    # # Uploads the latest year of raw burn area hdfs to s3.
    # # All hdfs go in this folder
    # cmd = ['aws', 's3', 'cp', '{0}/burn_date/'.format(cn.docker_app), cn.burn_year_hdf_raw_dir, '--recursive', '--exclude', '*', '--include', '*hdf']
    # uu.log_subprocess_output_full(cmd)
    #
    #
    # # Step 2:
    # # Makes burned area rasters for each year for each MODIS horizontal-vertical tile.
    # # This only needs to be done for the most recent year of data (set in stach_ba_hv).
    # uu.print_log("Stacking hdf into MODIS burned area tifs by year and MODIS hv tile...")
    #
    # count = multiprocessing.cpu_count()
    # pool = multiprocessing.Pool(processes=count - 10)
    # pool.map(stack_ba_hv.stack_ba_hv, global_grid_hv)
    # pool.close()
    # pool.join()
    #
    # # # For single processor use
    # # for hv_tile in global_grid_hv:
    # #     stack_ba_hv.stack_ba_hv(hv_tile)
    #
    #
    # # Step 3:
    # # Creates a 10x10 degree wgs 84 tile of .00025 res burned year.
    # # Downloads all MODIS hv tiles from s3,
    # # makes a mosaic for each year, and warps to Hansen extent.
    # # Range is inclusive at lower end and exclusive at upper end (e.g., 2001, 2022 goes from 2001 to 2021).
    # # This only needs to be done for the most recent year of data.
    # # NOTE: The first time I ran this for the 2020 TCL update, I got an error about uploading the log to s3
    # # after most of the tiles were processed. I didn't know why it happened, so I reran the step and it went fine.
    #
    # start_year = 2000 + cn.loss_years
    # end_year = 2000 + cn.loss_years + 1
    #
    # # Assumes that only the last year of fires are being processed
    # for year in range(start_year, end_year):
    #
    #     uu.print_log("Processing", year)
    #
    #     # Downloads all hv tifs for this year
    #     include = '{0}_*.tif'.format(year)
    #     year_tifs_folder = "{}_year_tifs".format(year)
    #     utilities.makedir(year_tifs_folder)
    #
    #     uu.print_log("Downloading MODIS burn date files from s3...")
    #
    #     cmd = ['aws', 's3', 'cp', cn.burn_year_stacked_hv_tif_dir, year_tifs_folder]
    #     cmd += ['--recursive', '--exclude', "*", '--include', include]
    #     uu.log_subprocess_output_full(cmd)
    #
    #     uu.print_log("Creating vrt of MODIS files...")
    #
    #     vrt_name = "global_vrt_{}.vrt".format(year)
    #
    #     # Builds list of vrt files
    #     with open('vrt_files.txt', 'w') as vrt_files:
    #         vrt_tifs = glob.glob(year_tifs_folder + "/*.tif")
    #         for tif in vrt_tifs:
    #             vrt_files.write(tif + "\n")
    #
    #     # Creates vrt with wgs84 MODIS tiles.
    #     cmd = ['gdalbuildvrt', '-input_file_list', 'vrt_files.txt', vrt_name]
    #     uu.log_subprocess_output_full(cmd)
    #
    #     uu.print_log("Reprojecting vrt...")
    #
    #     # Builds new vrt and virtually project it
    #     # This reprojection could be done as part of the clip_year_tiles function but Sam had it out here like this and
    #     # so I'm leaving it like that.
    #     vrt_wgs84 = 'global_vrt_{}_wgs84.vrt'.format(year)
    #     cmd = ['gdalwarp', '-of', 'VRT', '-t_srs', "EPSG:4326", '-tap', '-tr', str(cn.Hansen_res), str(cn.Hansen_res),
    #            '-overwrite', vrt_name, vrt_wgs84]
    #     uu.log_subprocess_output_full(cmd)
    #
    #     # Creates a list of lists, with year and tile id to send to multi processor
    #     tile_year_list = []
    #     for tile_id in tile_id_list:
    #         tile_year_list.append([tile_id, year])
    #
    #     # Given a list of tiles and years ['00N_000E', 2017] and a VRT of burn data,
    #     # the global vrt has pixels representing burned or not. This process clips the global VRT
    #     # and changes the pixel value to represent the year the pixel was burned. Each tile has value of
    #     # year burned and NoData.
    #     count = multiprocessing.cpu_count()
    #     pool = multiprocessing.Pool(processes=count-5)
    #     pool.map(partial(clip_year_tiles.clip_year_tiles, no_upload=no_upload), tile_year_list)
    #     pool.close()
    #     pool.join()
    #
    #     # # For single processor use
    #     # for tile_year in tile_year_list:
    #     #     clip_year_tiles.clip_year_tiles(tile_year, no_upload)
    #
    #     uu.print_log("Processing for {} done. Moving to next year.".format(year))


    # Step 4:
    # Creates a single Hansen tile covering all years that represents where burning coincided with tree cover loss
    # or preceded TCL by one year.
    # This needs to be done on all years each time burned area is updated.

    # Downloads the loss tiles. The step 3 burn year tiles are downloaded within hansen_burnyear
    uu.s3_folder_download(cn.loss_dir, '.', 'std', cn.pattern_loss)

    uu.print_log("Extracting burn year data that coincides with tree cover loss...")

    # Downloads the 10x10 deg burn year tiles (1 for each year in which there was burned area), stack and evaluate
    # to return burn year values on hansen loss pixels within 1 year of loss date
    if cn.count == 96:
        processes = 5
        # 6 processors = >750 GB peak (1 processor can use up to 130 GB of memory)
    else:
        processes = 1
    pool = multiprocessing.Pool(processes)
    pool.map(partial(hansen_burnyear_final.hansen_burnyear, no_upload=no_upload), tile_id_list)
    pool.close()
    pool.join()

    # # For single processor use
    # for tile_id in tile_id_list:
    #     hansen_burnyear_final.hansen_burnyear(tile_id, no_upload)


    # If no_upload flag is not activated (by choice or by lack of AWS credentials), output is uploaded
    if not no_upload:

        uu.upload_final_set(output_dir_list[0], output_pattern_list[0])


if __name__ == '__main__':

    # The arguments for what kind of model run is being run (standard conditions or a sensitivity analysis) and
    # the tiles to include
    parser = argparse.ArgumentParser(
        description='Creates tiles of the year in which pixels were burned')
    parser.add_argument('--tile_id_list', '-l', required=True,
                        help='List of tile ids to use in the model. Should be of form 00N_110E or 00N_110E,00N_120E or all.')
    parser.add_argument('--run-date', '-d', required=False,
                        help='Date of run. Must be format YYYYMMDD.')
    parser.add_argument('--no-upload', '-nu', action='store_true',
                       help='Disables uploading of outputs to s3')
    args = parser.parse_args()
    tile_id_list = args.tile_id_list
    run_date = args.run_date
    no_upload = args.NO_UPLOAD

    # Create the output log
    uu.initiate_log(tile_id_list=tile_id_list, sensit_type='std', run_date=run_date, no_upload=no_upload)

    # Checks whether the tile_id_list argument is valid
    tile_id_list = uu.tile_id_list_check(tile_id_list)

    mp_burn_year(tile_id_list=tile_id_list, run_date=run_date, no_upload=no_upload)