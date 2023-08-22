import os
import psycopg2
import sys
import rasterio

import constants_and_names as cn
import universal_util as uu

# Creates 1x1 degree tiles for the entire extent of planted forest using the supplied growth rates
# (defining coordinate in the northwest corner of the tile).
# Because this iterates through all 1x1 tiles in countries with planted forests, it first checks
# whether each 1x1 tile intersects planted forests before creating a 1x1 planted forest tile for that
# 1x1 country extent tile.
def create_1x1_plantation_from_1x1_gadm(tile_1x1):

    # Gets the bounding coordinates for the 1x1 degree tile
    coords = tile_1x1.split("_")
    uu.print_log(coords)
    ymax_1x1 = coords[0]
    ymin_1x1 = float(ymax_1x1) - 1
    xmin_1x1 = coords[1]
    xmax_1x1 = float(xmin_1x1) + 1

    uu.print_log("For", tile_1x1, "-- xmin_1x1:", xmin_1x1, "; xmax_1x1:", xmax_1x1, "; ymin_1x1", ymin_1x1, "; ymax_1x1:", ymax_1x1)

    cmd = ['gdal_rasterize', '-tr', '{}'.format(cn.Hansen_res), '{}'.format(cn.Hansen_res), '-co', 'COMPRESS=DEFLATE',
           'PG:dbname=ubuntu', '-l', 'all_plant', 'plant_gain_{0}_{1}.tif'.format(ymax_1x1, xmin_1x1), '-te',
           str(xmin_1x1), str(ymin_1x1), str(xmax_1x1), str(ymax_1x1), '-a', 'growth', '-a_nodata', '0']
    uu.log_subprocess_output_full(cmd)


    with rasterio.open('plant_gain_{0}_{1}.tif'.format(ymax_1x1, xmin_1x1)) as src:
        data = src.read(1)  # Read the pixel values from the first band
        if data.min() == data.max():
            uu.print_log("No SDPT in 1x1 cell. Deleting raster.")
            os.remove('plant_gain_{0}_{1}.tif'.format(ymax_1x1, xmin_1x1))
        else:
            uu.print_log("SDPT in 1x1 cell. Rasterizing other parameters...")

            cmd = ['gdal_rasterize', '-tr', '{}'.format(cn.Hansen_res), '{}'.format(cn.Hansen_res), '-co',
                   'COMPRESS=DEFLATE',
                   'PG:dbname=ubuntu', '-l', 'all_plant', 'plant_gain_SD_{0}_{1}.tif'.format(ymax_1x1, xmin_1x1), '-te',
                   str(xmin_1x1), str(ymin_1x1), str(xmax_1x1), str(ymax_1x1), '-a', 'growSDError', '-a_nodata', '0']
            uu.log_subprocess_output_full(cmd)

            cmd = ['gdal_rasterize', '-tr', '{}'.format(cn.Hansen_res), '{}'.format(cn.Hansen_res), '-co',
                   'COMPRESS=DEFLATE',
                   'PG:dbname=ubuntu', '-l', 'all_plant', 'plant_class_{0}_{1}.tif'.format(ymax_1x1, xmin_1x1), '-te',
                   str(xmin_1x1), str(ymin_1x1), str(xmax_1x1), str(ymax_1x1), '-a', 'type_reclass', '-a_nodata', '0']
            uu.log_subprocess_output_full(cmd)

            #TODO: add the plantation year rasterization

            # cmd = ['gdal_rasterize', '-tr', '{}'.format(cn.Hansen_res), '{}'.format(cn.Hansen_res), '-co',
            #        'COMPRESS=DEFLATE',
            #        'PG:dbname=ubuntu', '-l', 'all_plant', 'plant_gain_{0}_{1}.tif'.format(ymax_1x1, xmin_1x1), '-te',
            #        str(xmin_1x1), str(ymin_1x1), str(xmax_1x1), str(ymax_1x1), '-a', 'plant_year', '-a_nodata', '0']
            # uu.log_subprocess_output_full(cmd)




# Combines the 1x1 plantation tiles into 10x10 plantation carbon gain rate tiles, the final output of this process
def create_10x10_plantation_tile(tile_id, plant_gain_1x1_vrt):

    uu.print_log("Getting bounding coordinates for tile", tile_id)
    xmin, ymin, xmax, ymax = uu.coords(tile_id)
    uu.print_log("  xmin:", xmin, "; xmax:", xmax, "; ymin", ymin, "; ymax:", ymax)

    tile_10x10 = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_AGC_BGC_planted_forest_unmasked)
    uu.print_log("Rasterizing", tile_10x10)
    cmd = ['gdalwarp', '-tr', '{}'.format(str(cn.Hansen_res)), '{}'.format(str(cn.Hansen_res)),
           '-co', 'COMPRESS=DEFLATE', '-tap', '-te', str(xmin), str(ymin), str(xmax), str(ymax),
           '-dstnodata', '0', '-t_srs', 'EPSG:4326', '-overwrite', '-ot', 'Float32', plant_gain_1x1_vrt, tile_10x10]
    uu.log_subprocess_output_full(cmd)

    tile_10x10 = '{0}_{1}.tif'.format(tile_id, cn.pattern_planted_forest_type_unmasked)
    uu.print_log("Rasterizing", tile_10x10)
    cmd = ['gdalwarp', '-tr', '{}'.format(str(cn.Hansen_res)), '{}'.format(str(cn.Hansen_res)),
           '-co', 'COMPRESS=DEFLATE', '-tap', '-te', str(xmin), str(ymin), str(xmax), str(ymax),
           '-dstnodata', '0', '-t_srs', 'EPSG:4326', '-overwrite', '-ot', 'Byte', plant_type_1x1_vrt, tile_10x10]
    uu.log_subprocess_output_full(cmd)

    tile_10x10 = '{0}_{1}.tif'.format(tile_id, cn.pattern_planted_forest_stdev_unmasked)
    print("Rasterizing", tile_10x10)
    cmd = ['gdalwarp', '-tr', '{}'.format(str(cn.Hansen_res)), '{}'.format(str(cn.Hansen_res)),
           '-co', 'COMPRESS=DEFLATE', '-tap', '-te', str(xmin), str(ymin), str(xmax), str(ymax),
           '-dstnodata', '0', '-t_srs', 'EPSG:4326', '-overwrite', '-ot', 'Float32', plant_stdev_1x1_vrt, tile_10x10]
    subprocess.check_call(cmd)

    uu.print_log("Checking if {} contains any data...".format(tile_id))
    stats = uu.check_for_data(tile_10x10)

    if stats[0] > 0:

        uu.print_log("  Data found in {}. Copying tile to s3...".format(tile_id))
        uu.upload_final(cn.annual_gain_AGC_BGC_planted_forest_unmasked_dir, tile_id, cn.pattern_annual_gain_AGC_BGC_planted_forest_unmasked)
        uu.print_log("    Tile converted and copied to s3")

    else:

        uu.print_log("  No data found. Not copying {}.".format(tile_id))

