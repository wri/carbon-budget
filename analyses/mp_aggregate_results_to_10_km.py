import multiprocessing
import aggregate_results_to_10_km
import subprocess
import os
import glob
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# tile_list = uu.tile_list(cn.net_flux_dir)
# tile_id_list = ['00N_100E', '00N_110E', '00N_120E'] # test tiles
tile_id_list = ['00N_110E'] # test tiles
# tile_id_list = ['00N_110E', '80N_020E', '30N_080W', '00N_020E'] # test tiles: no mangrove or planted forest, mangrove only, planted forest only, mangrove and planted forest
print tile_id_list
print "There are {} tiles to process".format(str(len(tile_id_list)))

# For copying individual tiles to spot machine for testing
for tile_id in tile_id_list:
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.gross_emis_all_drivers_dir, tile_id, cn.pattern_gross_emis_all_drivers), '.')
    # uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.annual_gain_combo_dir, tile_id, cn.pattern_annual_gain_combo), '.')
    # uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.cumul_gain_combo_dir, tile_id, cn.pattern_cumul_gain_combo), '.')
    # uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.net_flux_dir, tile_id, cn.pattern_net_flux), '.')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.pixel_area_dir, cn.pattern_pixel_area, tile_id), '.')

tile_list = uu.tile_list_spot_machine(".", ".tif")
# from https://stackoverflow.com/questions/12666897/removing-an-item-from-list-matching-a-substring
tile_list = [i for i in tile_list if not ('hanson_2013' in i)]
tile_list = [i for i in tile_list if not ('per_pixel' in i)]
tile_list = [i for i in tile_list if not ('average' in i)]
print "Tiles to process:", tile_list

pixel_count_dict = dict.fromkeys(tile_list, 0)
print pixel_count_dict

for tile in tile_list:
    aggregate_results_to_10_km.rewindow(tile)

# tile_list = glob.glob("*retile.tif")
print tile_list
# For single processor use
for tile in tile_list:
    aggregate_results_to_10_km.convert_to_per_pixel(tile, pixel_count_dict)

# # tile_list = glob.glob("*per_pixel.tif")
# print tile_list
# # For single processor use
# for tile in tile_list:
#     aggregate_results_to_10_km.average_10km(tile)



# out_vrt = "value_per_pixel.vrt"
# os.system('gdalbuildvrt {} *per_pixel.tif'.format(out_vrt))
#
# avg_10km = "average_10_km.tif"
# cmd = ['gdalwarp', '-co', 'COMPRESS=LZW', '-tr', '0.096342599', '0.096342599', '-overwrite', '-r', 'average',
#        '-tap', out_vrt, avg_10km]
#
# subprocess.check_call(cmd)
print "Tiles processed. Uploading to s3 now..."

# Uploads all output tiles to s3
uu.upload_final_set(cn.gross_emis_all_drivers_aggreg_dir, cn.pattern_gross_emis_all_drivers_aggreg)



# # Pixel area tiles-- necessary for calculating sum of pixels for any set of tiles
# uu.s3_folder_download(cn.pixel_area_dir, '.')
#
# # For downloading all tiles in selected folders
# download_list = [cn.gross_emis_all_drivers_dir, cn.cumul_gain_combo_dir, cn.net_flux_dir, cn.pixel_area_dir]
#
# # Iterates through each set of tiles and gets statistics of it
# for input in download_list:
#
#     # uu.s3_folder_download(input, '.')
#
#     # List of all the tiles on the spot machine to be summarized (excludes pixel area tiles and tiles created by gdal_calc
#     # (in case this script was already run on this spot machine and created output from gdal_calc)
#     tile_list = uu.tile_list_spot_machine(".", ".tif")
#     # from https://stackoverflow.com/questions/12666897/removing-an-item-from-list-matching-a-substring
#     tile_list = [i for i in tile_list if not ('value_per_pixel' in i)]
#     # tile_list = ['00N_000E_biomass.tif']
#     # tile_list = download_tile_list
#     print tile_list
#
#     tile_pattern = tile_list[0][9:-4]
#
#     # # For multiprocessor use
#     # count = multiprocessing.cpu_count()
#     # pool = multiprocessing.Pool(count/4)
#     # pool.map(aggregate_results_to_10_km.aggregate_results, tile_list)
#     # # Added these in response to error12: Cannot allocate memory error.
#     # # This fix was mentioned here: of https://stackoverflow.com/questions/26717120/python-cannot-allocate-memory-using-multiprocessing-pool
#     # # Could also try this: https://stackoverflow.com/questions/42584525/python-multiprocessing-debugging-oserror-errno-12-cannot-allocate-memory
#     # pool.close()
#     # pool.join()
#
#     # For single processor use
#     for tile in tile_list:
#         aggregate_results_to_10_km.aggregate_results(tile)
#
#     print "Tiles processed. Uploading to s3 now..."
#
#     # Uploads all output tiles to s3
#     uu.upload_final_set(cn.gross_emis_all_drivers_aggreg_dir, cn.pattern_gross_emis_all_drivers_aggreg)