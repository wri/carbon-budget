import multiprocessing
import aggregate_results_to_10_km
import subprocess
from functools import partial
import argparse
import os
import glob
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def main():

    parser = argparse.ArgumentParser(description='Create maps of model output at approximately 10 km resolution')
    parser.add_argument('--tcd-threshold', '-tcd', required=True,
                        help='Tree cover density threshold above which pixels will be included in the aggregation.')
    args = parser.parse_args()

    thresh = args.tcd_threshold
    print thresh

    if thresh < 0 or thresh > 99:
        raise Exception('Invalid tcd. Please provide an integer between 0 and 99.')

    # tile_id_list = ['00N_100E', '00N_110E', '00N_120E'] # test tiles
    # tile_id_list = ['00N_110E'] # test tiles
    tile_id_list = ['30N_110W']  # test tiles
    # tile_id_list = ['00N_110E', '80N_020E', '30N_080W', '00N_020E'] # test tiles: no mangrove or planted forest, mangrove only, planted forest only, mangrove and planted forest
    # print tile_id_list
    # print "There are {} tiles to process".format(str(len(tile_id_list)))

    # For copying individual tiles to spot machine for testing
    for tile_id in tile_id_list:
        # uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.gross_emis_all_drivers_dir, tile_id, cn.pattern_gross_emis_all_drivers), '.')
        # uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.annual_gain_combo_dir, tile_id, cn.pattern_annual_gain_combo), '.')
        # uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.cumul_gain_combo_dir, tile_id, cn.pattern_cumul_gain_combo), '.')
        uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.net_flux_dir, tile_id, cn.pattern_net_flux), '.')
        uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.pixel_area_dir, cn.pattern_pixel_area, tile_id), '.')
        # uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.tcd_dir, cn.pattern_tcd, tile_id), '.')

    # # Pixel area tiles-- necessary for calculating sum of pixels for any set of tiles
    # uu.s3_folder_download(cn.pixel_area_dir, '.')
    # # tree cover density tiles-- necessary for filtering sums by tcd
    # uu.s3_folder_download(cn.tcd_dir, '.')

    input_dict = {
             # cn.annual_gain_combo_dir: cn.pattern_annual_gain_combo,
             # cn.cumul_gain_combo_dir: cn.pattern_cumul_gain_combo,
             # cn.gross_emis_all_drivers_dir: cn.pattern_gross_emis_all_drivers,
             cn.net_flux_dir: cn.pattern_net_flux
             }

    print "Model outputs to process are:", input_dict

    for dir, pattern in input_dict.items():

        tile_id_list = uu.tile_list(dir)
        print tile_id_list
        print "There are {} tiles to process".format(str(len(tile_id_list)))
        print "Processing:", dir, "; ", pattern

        # uu.s3_folder_download(dir, '.')

        # Lists the tiles of the particular type that is being iterates through.
        # Excludes all intermediate files
        tile_list = uu.tile_list_spot_machine(".", "{}.tif".format(pattern))
        # from https://stackoverflow.com/questions/12666897/removing-an-item-from-list-matching-a-substring
        tile_list = [i for i in tile_list if not ('hanson_2013' in i)]
        tile_list = [i for i in tile_list if not ('rewindow' in i)]
        tile_list = [i for i in tile_list if not ('10km' in i)]
        print "Tiles to process:", tile_list

        # For multiprocessor use
        count = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(count/3)
        pool.map(aggregate_results_to_10_km.rewindow, tile_list)
        # Added these in response to error12: Cannot allocate memory error.
        # This fix was mentioned here: of https://stackoverflow.com/questions/26717120/python-cannot-allocate-memory-using-multiprocessing-pool
        # Could also try this: https://stackoverflow.com/questions/42584525/python-multiprocessing-debugging-oserror-errno-12-cannot-allocate-memory
        pool.close()
        pool.join()

        # For multiprocessor use. This used about 275 GB of memory with count/3, so count/2 should work on an r4.16xlarge

        count = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(count/2)
        pool.map(partial(aggregate_results_to_10_km.aggregate, thresh=thresh), tile_list)
        # Added these in response to error12: Cannot allocate memory error.
        # This fix was mentioned here: of https://stackoverflow.com/questions/26717120/python-cannot-allocate-memory-using-multiprocessing-pool
        # Could also try this: https://stackoverflow.com/questions/42584525/python-multiprocessing-debugging-oserror-errno-12-cannot-allocate-memory
        pool.close()
        pool.join()

        # Makes a vrt of all the output 10x10 tiles (10 km resolution)
        out_vrt = "{}_10km.vrt".format(pattern)
        os.system('gdalbuildvrt -tr 0.1 0.1 {0} *{1}_10km*.tif'.format(out_vrt, pattern))

        # Produces a single raster of all the 10x10 tiles (10 km resolution)
        cmd = ['gdalwarp', '-t_srs', "EPSG:4326", '-overwrite', '-dstnodata', '0', '-co', 'COMPRESS=LZW',
               '-tr', '0.1', '0.1',
               out_vrt, '{}_10km.tif'.format(pattern)]
        subprocess.check_call(cmd)

        print "Tiles processed. Uploading to s3 now..."

        vrtList = glob.glob('*vrt')
        for vrt in vrtList:
            os.remove(vrt)

        for tile_id in tile_id_list:

            os.remove('{0}_{1}.tif'.format(tile_id, pattern))
            os.remove('{0}_{1}_rewindow.tif'.format(tile_id, pattern))
            os.remove('{0}_{1}_10km.tif'.format(tile_id, pattern))

        print '{}_10km.tif'.format(pattern)

        # Uploads all output tiles to s3
        uu.upload_final_set(cn.output_aggreg_dir, '{}_10km'.format(pattern))


if __name__ == '__main__':
    main()