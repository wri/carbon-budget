### Creates tiles in which each pixel is a combination of the continent and FAO FRA 2000 ecozone.
### The tiles are based on a shapefile which combines the FAO FRA 2000 ecozone shapefile and a continent shapefile.
### The FAO FRA 2000 shapefile is from http://www.fao.org/geonetwork/srv/en/resources.get?id=1255&fname=eco_zone.zip&access=private
### The continent shapefile is from https://www.baruch.cuny.edu/confluence/display/geoportal/ESRI+International+Data
### Various processing steps in ArcMap were used to make sure that the entirety of the ecozone shapefile had
### continents assigned to it. The creation of the continent-ecozone shapefile was done in ArcMap.
### In the resulting ecozone-continent shapefile, the final field has continent and ecozone concatenated.
### That ecozone-continent field can be parsed to get the ecozone and continent for every pixel,
### which are necessary for assigning removals rates to pixels.
### This script also breaks the input tiles into windows that are 1024 pixels on each side and assigns all pixels that
### don't have a continent-ecozone code to the most common code in that window.
### This is done to expand the extent of the continent-ecozone tiles to include pixels that don't have a continent-ecozone
### code because they are just outside the original shapefile.
### It is necessary to expand the continent-ecozone codes into those nearby areas because otherwise some forest age category
### pixels are outside the continent-ecozone pixels and can't have removals rates assigned to them.
### This maneuver provides the necessary continent-ecozone information to assign removals rates.


import multiprocessing
import continent_ecozone_tiles
from subprocess import Popen, PIPE, STDOUT, check_call
import datetime
import argparse
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def mp_continent_ecozone_tiles(tile_id_list, run_date = None):

    os.chdir(cn.docker_base_dir)

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # List of tiles to run in the model
        tile_id_list = uu.create_combined_tile_list(cn.pattern_WHRC_biomass_2000_non_mang_non_planted, cn.mangrove_biomass_2000_dir)

    uu.print_log(tile_id_list)
    uu.print_log("There are {} tiles to process".format(str(len(tile_id_list))) + "\n")


    # if the continent-ecozone shapefile hasn't already been downloaded, it will be downloaded and unzipped
    uu.s3_file_download(cn.cont_eco_s3_zip, cn.docker_base_dir, 'std')

    # Unzips ecozone shapefile
    cmd = ['unzip', cn.cont_eco_zip]
    uu.log_subprocess_output_full(cmd)


    # List of output directories and output file name patterns
    output_dir_list = [cn.cont_eco_raw_dir, cn.cont_eco_dir]
    output_pattern_list = [cn.pattern_cont_eco_raw, cn.pattern_cont_eco_processed]


    # A date can optionally be provided by the full model script or a run of this script.
    # This replaces the date in constants_and_names.
    if run_date is not None:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, run_date)


    # For multiprocessor use
    processes = int(cn.count/4)
    uu.print_log('Continent-ecozone tile creation max processors=', processes)
    pool.map(continent_ecozone_tiles.create_continent_ecozone_tiles, tile_id_list)


    # Uploads the continent-ecozone tile to s3 before the codes are expanded to pixels in 1024x1024 windows that don't have codes.
    # These are not used for the model. They are for reference and completeness.
    for i in range(0, len(output_dir_list)):
        uu.upload_final_set(output_dir_list[i], output_pattern_list[i])


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Create tiles of continent-ecozone combination')
    parser.add_argument('--tile_id_list', '-l', required=True,
                        help='List of tile ids to use in the model. Should be of form 00N_110E or 00N_110E,00N_120E or all.')
    parser.add_argument('--run-date', '-d', required=False,
                        help='Date of run. Must be format YYYYMMDD.')
    args = parser.parse_args()
    tile_id_list = args.tile_id_list
    run_date = args.run_date

    # Disables upload to s3 if no AWS credentials are found in environment
    if not uu.check_aws_creds():
        no_upload = True

    # Create the output log
    uu.initiate_log(tile_id_list=tile_id_list, run_date=run_date)

    mp_continent_ecozone_tiles(tile_id_list=tile_id_list, run_date=run_date)