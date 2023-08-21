
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

    # Intersects the plantations PostGIS table with the 1x1 tile, then saves any growth rates in that tile as a 1x1 tile
    # https://gis.stackexchange.com/questions/30267/how-to-create-a-valid-global-polygon-grid-in-postgis
    # https://stackoverflow.com/questions/48978616/best-way-to-run-st-intersects-on-features-inside-one-table
    # https://postgis.net/docs/ST_Intersects.html
    uu.print_log(f'Checking if {tile_1x1} has plantations in it')

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

    os.quit()

    # # If any features in the PostGIS table were intersected with the 1x1 GADM tile, then the features in this 1x1 tile
    # # are converted to a planted forest gain rate tile and a plantation type tile
    # if len(features) > 0:
    #
    #     uu.print_log("There are plantations in {}. Converting to gain rate and plantation type rasters...".format(tile_1x1))
    #
    #     # https://gis.stackexchange.com/questions/187224/how-to-use-gdal-rasterize-with-postgis-vector
    #     # For plantation gain rate
    #     cmd = ['gdal_rasterize', '-tr', '{}'.format(cn.Hansen_res), '{}'.format(cn.Hansen_res), '-co', 'COMPRESS=DEFLATE', 'PG:dbname=ubuntu', '-l', 'all_plant', 'plant_gain_{0}_{1}.tif'.format(ymax_1x1, xmin_1x1), '-te', str(xmin_1x1), str(ymin_1x1), str(xmax_1x1), str(ymax_1x1), '-a', 'growth', '-a_nodata', '0']
    #     uu.log_subprocess_output_full(cmd)
    #
    #     # https://gis.stackexchange.com/questions/187224/how-to-use-gdal-rasterize-with-postgis-vector
    #     # For plantation type
    #     cmd = ['gdal_rasterize', '-tr', '{}'.format(cn.Hansen_res), '{}'.format(cn.Hansen_res), '-co', 'COMPRESS=DEFLATE', 'PG:dbname=ubuntu', '-l', 'all_plant', 'plant_type_{0}_{1}.tif'.format(ymax_1x1, xmin_1x1), '-te', str(xmin_1x1), str(ymin_1x1), str(xmax_1x1), str(ymax_1x1), '-a', 'type_reclass', '-a_nodata', '0']
    #     uu.log_subprocess_output_full(cmd)
    #
    # # If no features in the PostGIS table were intersected with the 1x1 GADM tile, nothing happens.
    # else:
    #     uu.print_log("There are no plantations in {}. Not converting to raster.".format(tile_1x1))


# Creates 1x1 degree tiles for the entire extent of planted forest using the supplied growth rates
# (defining coordinate in the northwest corner of the tile).
# Because this iterates through only 1x1 tiles that are known to have planted forests (from a previous run
# of this script), it does not need to check whether there are planted forests in this tile. It goes directly
# to intersecting the planted forest table with the 1x1 tile.
def create_1x1_plantation_growth_from_1x1_planted(tile_1x1):

    # Gets the bounding coordinates for the 1x1 degree tile
    coords = tile_1x1.split("_")
    xmin_1x1 = str(coords[3])[:-4]
    xmax_1x1 = int(xmin_1x1) + 1
    ymax_1x1 = int(coords[2])
    ymin_1x1 = ymax_1x1 - 1

    uu.print_log("For", tile_1x1, "-- xmin_1x1:", xmin_1x1, "; xmax_1x1:", xmax_1x1, "; ymin_1x1", ymin_1x1, "; ymax_1x1:", ymax_1x1)

    uu.print_log("There are plantations in {}. Converting to raster...".format(tile_1x1))

    # https://gis.stackexchange.com/questions/187224/how-to-use-gdal-rasterize-with-postgis-vector
    cmd = ['gdal_rasterize', '-tr', '{}'.format(cn.Hansen_res), '{}'.format(cn.Hansen_res), '-co', 'COMPRESS=DEFLATE',
           'PG:dbname=ubuntu', '-l', 'all_plant', 'plant_gain_{0}_{1}.tif'.format(ymax_1x1, xmin_1x1), '-te',
           str(xmin_1x1), str(ymin_1x1), str(xmax_1x1), str(ymax_1x1), '-a', 'growth', '-a_nodata', '0']
    uu.log_subprocess_output_full(cmd)


# Creates 1x1 degree tiles for the entire extent of planted forest using the supplied forest types
# (defining coordinate in the northwest corner of the tile).
# Because this iterates through only 1x1 tiles that are known to have planted forests (from a previous run
# of this script), it does not need to check whether there are planted forests in this tile. It goes directly
# to intersecting the planted forest table with the 1x1 tile.
def create_1x1_plantation_type_from_1x1_planted(tile_1x1):

    # Gets the bounding coordinates for the 1x1 degree tile
    coords = tile_1x1.split("_")
    xmin_1x1 = str(coords[3])[:-4]
    xmax_1x1 = int(xmin_1x1) + 1
    ymax_1x1 = int(coords[2])
    ymin_1x1 = ymax_1x1 - 1

    uu.print_log("For", tile_1x1, "-- xmin_1x1:", xmin_1x1, "; xmax_1x1:", xmax_1x1, "; ymin_1x1", ymin_1x1, "; ymax_1x1:", ymax_1x1)

    uu.print_log("There are plantations in {}. Converting to raster...".format(tile_1x1))

    # https://gis.stackexchange.com/questions/187224/how-to-use-gdal-rasterize-with-postgis-vector
    cmd = ['gdal_rasterize', '-tr', '{}'.format(cn.Hansen_res), '{}'.format(cn.Hansen_res), '-co', 'COMPRESS=DEFLATE', 'PG:dbname=ubuntu',
           '-l', 'all_plant', 'plant_type_{0}_{1}.tif'.format(ymax_1x1, xmin_1x1),
           '-te', str(xmin_1x1), str(ymin_1x1), str(xmax_1x1), str(ymax_1x1),
           '-a', 'type_reclass', '-a_nodata', '0', '-ot', 'Byte']
    uu.log_subprocess_output_full(cmd)



# Makes 1x1 rasters of the removal rate standard deviation
def create_1x1_plantation_stdev_from_1x1_planted(tile_1x1):

    # Gets the bounding coordinates for the 1x1 degree tile
    coords = tile_1x1.split("_")
    xmin_1x1 = str(coords[3])[:-4]
    xmax_1x1 = int(xmin_1x1) + 1
    ymax_1x1 = int(coords[2])
    ymin_1x1 = ymax_1x1 - 1

    print("For", tile_1x1, "-- xmin_1x1:", xmin_1x1, "; xmax_1x1:", xmax_1x1, "; ymin_1x1", ymin_1x1, "; ymax_1x1:", ymax_1x1)

    print("There are plantations in {}. Converting to stdev raster...".format(tile_1x1))

    # https://gis.stackexchange.com/questions/187224/how-to-use-gdal-rasterize-with-postgis-vector
    cmd = ['gdal_rasterize', '-tr', '{}'.format(cn.Hansen_res), '{}'.format(cn.Hansen_res), '-co', 'COMPRESS=DEFLATE', 'PG:dbname=ubuntu',
           '-l', 'all_plant', 'plant_stdev_{0}_{1}.tif'.format(ymax_1x1, xmin_1x1),
           '-te', str(xmin_1x1), str(ymin_1x1), str(xmax_1x1), str(ymax_1x1),
           '-a', 'SD_error', '-a_nodata', '0']
    subprocess.check_call(cmd)



# Combines the 1x1 plantation tiles into 10x10 plantation carbon gain rate tiles, the final output of this process
def create_10x10_plantation_gain(tile_id, plant_gain_1x1_vrt):

    uu.print_log("Getting bounding coordinates for tile", tile_id)
    xmin, ymin, xmax, ymax = uu.coords(tile_id)
    uu.print_log("  xmin:", xmin, "; xmax:", xmax, "; ymin", ymin, "; ymax:", ymax)

    tile_10x10 = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_AGC_BGC_planted_forest_unmasked)
    uu.print_log("Rasterizing", tile_10x10)
    cmd = ['gdalwarp', '-tr', '{}'.format(str(cn.Hansen_res)), '{}'.format(str(cn.Hansen_res)),
           '-co', 'COMPRESS=DEFLATE', '-tap', '-te', str(xmin), str(ymin), str(xmax), str(ymax),
           '-dstnodata', '0', '-t_srs', 'EPSG:4326', '-overwrite', '-ot', 'Float32', plant_gain_1x1_vrt, tile_10x10]
    uu.log_subprocess_output_full(cmd)

    uu.print_log("Checking if {} contains any data...".format(tile_id))
    stats = uu.check_for_data(tile_10x10)

    if stats[0] > 0:

        uu.print_log("  Data found in {}. Copying tile to s3...".format(tile_id))
        uu.upload_final(cn.annual_gain_AGC_BGC_planted_forest_unmasked_dir, tile_id, cn.pattern_annual_gain_AGC_BGC_planted_forest_unmasked)
        uu.print_log("    Tile converted and copied to s3")

    else:

        uu.print_log("  No data found. Not copying {}.".format(tile_id))


# Combines the 1x1 plantation tiles into 10x10 plantation carbon gain rate tiles, the final output of this process
def create_10x10_plantation_type(tile_id, plant_type_1x1_vrt):

    uu.print_log("Getting bounding coordinates for tile", tile_id)
    xmin, ymin, xmax, ymax = uu.coords(tile_id)
    uu.print_log("  xmin:", xmin, "; xmax:", xmax, "; ymin", ymin, "; ymax:", ymax)

    tile_10x10 = '{0}_{1}.tif'.format(tile_id, cn.pattern_planted_forest_type_unmasked)
    uu.print_log("Rasterizing", tile_10x10)
    cmd = ['gdalwarp', '-tr', '{}'.format(str(cn.Hansen_res)), '{}'.format(str(cn.Hansen_res)),
           '-co', 'COMPRESS=DEFLATE', '-tap', '-te', str(xmin), str(ymin), str(xmax), str(ymax),
           '-dstnodata', '0', '-t_srs', 'EPSG:4326', '-overwrite', '-ot', 'Byte', plant_type_1x1_vrt, tile_10x10]
    uu.log_subprocess_output_full(cmd)

    uu.print_log("Checking if {} contains any data...".format(tile_id))
    stats = uu.check_for_data(tile_10x10)
    
    if stats[0] > 0:

        uu.print_log("  Data found in {}. Copying tile to s3...".format(tile_id))
        uu.upload_final(cn.planted_forest_type_unmasked_dir, tile_id, cn.pattern_planted_forest_type_unmasked)
        uu.print_log("    Tile converted and copied to s3")

    else:

        print("  No data found. Not copying {}.".format(tile_id))


# Combines the 1x1 plantation tiles into 10x10 plantation carbon gain rate tiles, the final output of this process
def create_10x10_plantation_gain_stdev(tile_id, plant_stdev_1x1_vrt):

    print("Getting bounding coordinates for tile", tile_id)
    xmin, ymin, xmax, ymax = uu.coords(tile_id)
    print("  xmin:", xmin, "; xmax:", xmax, "; ymin", ymin, "; ymax:", ymax)

    tile_10x10 = '{0}_{1}.tif'.format(tile_id, cn.pattern_planted_forest_stdev_unmasked)
    print("Rasterizing", tile_10x10)
    cmd = ['gdalwarp', '-tr', '{}'.format(str(cn.Hansen_res)), '{}'.format(str(cn.Hansen_res)),
           '-co', 'COMPRESS=DEFLATE', '-tap', '-te', str(xmin), str(ymin), str(xmax), str(ymax),
           '-dstnodata', '0', '-t_srs', 'EPSG:4326', '-overwrite', '-ot', 'Float32', plant_stdev_1x1_vrt, tile_10x10]
    subprocess.check_call(cmd)

    print("Checking if {} contains any data...".format(tile_id))
    stats = uu.check_for_data_old(tile_10x10)

    if stats[0] > 0:

        print("  Data found in {}. Copying tile to s3...".format(tile_id))
        uu.upload_final(cn.planted_forest_stdev_unmasked_dir, tile_id, cn.pattern_planted_forest_stdev_unmasked)
        print("    Tile converted and copied to s3")

    else:

        print("  No data found. Not copying {}.".format(tile_id))

