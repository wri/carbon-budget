import os
import psycopg2
import sys
import rasterio

import constants_and_names as cn
import universal_util as uu

# Creates 1x1 degree tiles of planted forest properties
def create_1x1_plantation_from_1x1_gadm(tile_1x1):

    # Gets the bounding coordinates for the 1x1 degree tile
    coords = tile_1x1.split("_")
    uu.print_log(coords)
    ymax_1x1 = coords[0]
    ymin_1x1 = float(ymax_1x1) - 1
    xmin_1x1 = coords[1]
    xmax_1x1 = float(xmin_1x1) + 1

    uu.print_log("For", tile_1x1, "-- xmin_1x1:", xmin_1x1, "; xmax_1x1:", xmax_1x1,
                 "; ymin_1x1", ymin_1x1, "; ymax_1x1:", ymax_1x1)

    RF_1x1 = f'{ymax_1x1}_{xmin_1x1}_{cn.pattern_annual_gain_AGC_planted_forest}.tif'

    cmd = ['gdal_rasterize', '-tr', str(cn.Hansen_res), str(cn.Hansen_res), '-co', 'COMPRESS=DEFLATE',
           'PG:dbname=ubuntu', '-l', cn.planted_forest_postgis_db, RF_1x1,
           '-te', str(xmin_1x1), str(ymin_1x1), str(xmax_1x1), str(ymax_1x1),
           '-a', 'growth', '-a_nodata', '0', '-ot', 'Float32']
    uu.log_subprocess_output_full(cmd)

    uu.print_log(f'Checking if {RF_1x1} contains any data...')
    no_data = uu.check_for_data(RF_1x1)
    print(no_data)

    if not no_data:

        uu.print_log(f'  Data found in {RF_1x1}. Rasterizing other SDPT outputs...')

        uu.print_log('Rasterizing planted forest removal factor standard deviation...')
        cmd = ['gdal_rasterize', '-tr', str(cn.Hansen_res), str(cn.Hansen_res), '-co', 'COMPRESS=DEFLATE',
               'PG:dbname=ubuntu', '-l', cn.planted_forest_postgis_db,
               f'{ymax_1x1}_{xmin_1x1}_{cn.pattern_stdev_annual_gain_AGC_planted_forest}.tif',
               '-te', str(xmin_1x1), str(ymin_1x1), str(xmax_1x1), str(ymax_1x1),
               '-a', 'growSDError', '-a_nodata', '0', '-ot', 'Float32']
        uu.log_subprocess_output_full(cmd)

        uu.print_log('Rasterizing planted forest type...')
        cmd = ['gdal_rasterize', '-tr', str(cn.Hansen_res), str(cn.Hansen_res), '-co', 'COMPRESS=DEFLATE',
               'PG:dbname=ubuntu', '-l', cn.planted_forest_postgis_db,
               f'{ymax_1x1}_{xmin_1x1}_{cn.pattern_planted_forest_type}.tif',
               '-te', str(xmin_1x1), str(ymin_1x1), str(xmax_1x1), str(ymax_1x1),
               '-a', 'type_reclass', '-a_nodata', '0', '-ot', 'Byte']
        uu.log_subprocess_output_full(cmd)

        # TODO: add the plantation year rasterization

        # uu.print_log('Rasterizing planted forest establishment year...')
        # cmd = ['gdal_rasterize', '-tr', str(cn.Hansen_res), str(cn.Hansen_res), '-co', 'COMPRESS=DEFLATE',
        #        'PG:dbname=ubuntu', '-l', cn.planted_forest_postgis_db, f'{ymax_1x1}_{xmin_1x1}_{cn.pattern_planted_forest_estab_year}.tif',
        #        '-te', str(xmin_1x1), str(ymin_1x1), str(xmax_1x1), str(ymax_1x1),
        #        '-a', 'plant_year', '-a_nodata', '0', '-ot', 'UInt8']
        # uu.log_subprocess_output_full(cmd)

    else:

        uu.print_log('No SDPT in 1x1 cell. Deleting raster.')
        os.remove(RF_1x1)



# Combines the 1x1 planted forest output tiles into 10x10 planted forest output tiles
def create_10x10_plantation_tiles(tile_id, plant_gain_1x1_vrt, plant_stdev_1x1_vrt,
                                  plant_type_1x1_vrt, plant_estab_year_1x1_vrt):

    uu.print_log("Getting bounding coordinates for tile", tile_id)
    xmin, ymin, xmax, ymax = uu.coords(tile_id)
    uu.print_log("  xmin:", xmin, "; xmax:", xmax, "; ymin", ymin, "; ymax:", ymax)

    RF_10x10 = f'{tile_id}_{cn.pattern_annual_gain_AGC_planted_forest}.tif'
    uu.print_log("Rasterizing", RF_10x10)
    cmd = ['gdalwarp', '-tr', str(cn.Hansen_res), str(cn.Hansen_res),
           '-co', 'COMPRESS=DEFLATE', '-tap', '-te', str(xmin), str(ymin), str(xmax), str(ymax),
           '-dstnodata', '0', '-t_srs', 'EPSG:4326', '-overwrite', '-ot', 'Float32', plant_gain_1x1_vrt, RF_10x10]
    uu.log_subprocess_output_full(cmd)


    uu.print_log(f'Checking if {RF_10x10} contains any data...')
    no_data = uu.check_for_data(RF_1x1)

    if not no_data:

        uu.print_log(f'  Data found in {RF_10x10}. Rasterizing other SDPT outputs...')

        RF_stdev_10x10 = f'{tile_id}_{cn.pattern_stdev_annual_gain_AGC_planted_forest}.tif'
        print("Rasterizing", RF_stdev_10x10)
        cmd = ['gdalwarp', '-tr', str(cn.Hansen_res), str(cn.Hansen_res),
               '-co', 'COMPRESS=DEFLATE', '-tap', '-te', str(xmin), str(ymin), str(xmax), str(ymax),
               '-dstnodata', '0', '-t_srs', 'EPSG:4326', '-overwrite', '-ot', 'Float32', plant_stdev_1x1_vrt,
               RF_stdev_10x10]
        subprocess.check_call(cmd)

        type_10x10 = f'{tile_id}_{cn.pattern_planted_forest_type}.tif'
        uu.print_log("Rasterizing", type_10x10)
        cmd = ['gdalwarp', '-tr', str(cn.Hansen_res), str(cn.Hansen_res),
               '-co', 'COMPRESS=DEFLATE', '-tap', '-te', str(xmin), str(ymin), str(xmax), str(ymax),
               '-dstnodata', '0', '-t_srs', 'EPSG:4326', '-overwrite', '-ot', 'Byte', plant_type_1x1_vrt,
               type_10x10]
        uu.log_subprocess_output_full(cmd)

        estab_year_10x10 = f'{tile_id}_{cn.pattern_planted_forest_estab_year}.tif'
        uu.print_log("Rasterizing", estab_year_10x10)
        cmd = ['gdalwarp', '-tr', str(cn.Hansen_res), str(cn.Hansen_res),
               '-co', 'COMPRESS=DEFLATE', '-tap', '-te', str(xmin), str(ymin), str(xmax), str(ymax),
               '-dstnodata', '0', '-t_srs', 'EPSG:4326', '-overwrite', '-ot', 'UInt8', plant_estab_year_1x1_vrt,
               estab_year_10x10]
        uu.log_subprocess_output_full(cmd)

    else:

        uu.print_log(f'  No data found in {RF_10x10}. Deleting and not rasterizing other SDPT outputs...')
        os.remove(RF_10x10)