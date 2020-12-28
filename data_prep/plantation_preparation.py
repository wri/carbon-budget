
from subprocess import Popen, PIPE, STDOUT, check_call
import os
import psycopg2
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Creates 1x1 tiles of the extent of select countries are in select latitude bands, with the defining coordinates of each tile
# in the northwest corner
def rasterize_gadm_1x1(tile_id):

    uu.print_log("Getting bounding coordinates for tile", tile_id)
    xmin, ymin, xmax, ymax = uu.coords(tile_id)
    uu.print_log("  xmin:", xmin, "; xmax:", xmax, "; ymin", ymin, "; ymax:", ymax)

    # Degrees of tile in x and y dimensions
    x_size = abs(int(xmin) - int(xmax))
    y_size = abs(int(ymin) - int(ymax))

    # Iterates through input 10x10 tile by 1x1 degree
    for x in range(x_size):

        xmin_1x1 = int(xmin) + x
        xmax_1x1 = int(xmin) + x + 1

        for y in range(y_size):

            ymin_1x1 = int(ymin) + y
            ymax_1x1 = int(ymin) + y + 1

            uu.print_log("  xmin_1x1:", xmin_1x1, "; xmax_1x1:", xmax_1x1, "; ymin_1x1", ymin_1x1, "; ymax_1x1:", ymax_1x1)

            tile_1x1 = 'GADM_{0}_{1}.tif'.format(ymax_1x1, xmin_1x1)
            uu.print_log("Rasterizing", tile_1x1)
            cmd = ['gdal_rasterize', '-tr', '{}'.format(str(cn.Hansen_res)), '{}'.format(str(cn.Hansen_res)),
                   '-co', 'COMPRESS=LZW', '-te', str(xmin_1x1), str(ymin_1x1), str(xmax_1x1), str(ymax_1x1),
                   '-burn', '1', '-a_nodata', '0', cn.gadm_iso, tile_1x1]
            # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
            process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
            with process.stdout:
                uu.log_subprocess_output(process.stdout)

            # Only keeps 1x1 GADM tiles if they actually include a country; many 1x1 tiles created out of 10x10 tiles
            # don't actually include a country.
            uu.print_log("Checking if {} contains any data...".format(tile_1x1))
            stats = uu.check_for_data(tile_1x1)

            if stats[1] > 0:
                uu.print_log("  Data found in {}. Keeping tile".format(tile_1x1))

            else:
                uu.print_log("  No data found in {}. Deleting.".format(tile_1x1))
                os.remove(tile_1x1)


# Creates 1x1 degree tiles for the entire extent of planted forest using the supplied growth rates
# (defining coordinate in the northwest corner of the tile).
# Because this iterates through all 1x1 tiles in countries with planted forests, it first checks
# whether each 1x1 tile intersects planted forests before creating a 1x1 planted forest tile for that
# 1x1 country extent tile.
def create_1x1_plantation_from_1x1_gadm(tile_1x1):

    # Gets the bounding coordinates for the 1x1 degree tile
    coords = tile_1x1.split("_")
    uu.print_log(coords)
    xmin_1x1 = str(coords[2])[:-4]
    xmax_1x1 = int(xmin_1x1) + 1
    ymax_1x1 = int(coords[1])
    ymin_1x1 = ymax_1x1 - 1

    uu.print_log("For", tile_1x1, "-- xmin_1x1:", xmin_1x1, "; xmax_1x1:", xmax_1x1, "; ymin_1x1", ymin_1x1, "; ymax_1x1:", ymax_1x1)

    # Connects Python to PostGIS using psycopg2. The credentials work on spot machines as they are currently configured
    # and are based on this: https://github.com/wri/gfw-annual-loss-processing/blob/master/1b_Summary-AOIs-to-TSV/utilities/postgis_util.py
    creds = {'host': 'localhost', 'user': 'ubuntu', 'dbname': 'ubuntu'}
    conn = psycopg2.connect(**creds)
    cursor = conn.cursor()

    # Intersects the plantations PostGIS table with the 1x1 tile, then saves any growth rates in that tile as a 1x1 tile
    # https://gis.stackexchange.com/questions/30267/how-to-create-a-valid-global-polygon-grid-in-postgis
    # https://stackoverflow.com/questions/48978616/best-way-to-run-st-intersects-on-features-inside-one-table
    # https://postgis.net/docs/ST_Intersects.html
    uu.print_log("Checking if {} has plantations in it".format(tile_1x1))

    # Does the intersect of the PostGIS table and the 1x1 GADM tile
    cursor.execute("SELECT growth FROM all_plant WHERE ST_Intersects(all_plant.wkb_geometry, ST_GeogFromText('POLYGON(({0} {1},{2} {1},{2} {3},{0} {3},{0} {1}))'))".format(
            xmin_1x1, ymax_1x1, xmax_1x1, ymin_1x1))

    # A Python list of the output of the intersection, which in this case is a list of features that were successfully intersected.
    # This is what I use to determine if any PostGIS features were intersected.
    features = cursor.fetchall()
    cursor.close()

    # If any features in the PostGIS table were intersected with the 1x1 GADM tile, then the features in this 1x1 tile
    # are converted to a planted forest gain rate tile and a plantation type tile
    if len(features) > 0:

        uu.print_log("There are plantations in {}. Converting to gain rate and plantation type rasters...".format(tile_1x1))

        # https://gis.stackexchange.com/questions/187224/how-to-use-gdal-rasterize-with-postgis-vector
        # For plantation gain rate
        cmd = ['gdal_rasterize', '-tr', '{}'.format(cn.Hansen_res), '{}'.format(cn.Hansen_res), '-co', 'COMPRESS=LZW', 'PG:dbname=ubuntu', '-l', 'all_plant', 'plant_gain_{0}_{1}.tif'.format(ymax_1x1, xmin_1x1), '-te', str(xmin_1x1), str(ymin_1x1), str(xmax_1x1), str(ymax_1x1), '-a', 'growth', '-a_nodata', '0']
        # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
        process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
        with process.stdout:
            uu.log_subprocess_output(process.stdout)

        # https://gis.stackexchange.com/questions/187224/how-to-use-gdal-rasterize-with-postgis-vector
        # For plantation type
        cmd = ['gdal_rasterize', '-tr', '{}'.format(cn.Hansen_res), '{}'.format(cn.Hansen_res), '-co', 'COMPRESS=LZW', 'PG:dbname=ubuntu', '-l', 'all_plant', 'plant_type_{0}_{1}.tif'.format(ymax_1x1, xmin_1x1), '-te', str(xmin_1x1), str(ymin_1x1), str(xmax_1x1), str(ymax_1x1), '-a', 'type_reclass', '-a_nodata', '0']
        # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
        process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
        with process.stdout:
            uu.log_subprocess_output(process.stdout)

    # If no features in the PostGIS table were intersected with the 1x1 GADM tile, nothing happens.
    else:
        uu.print_log("There are no plantations in {}. Not converting to raster.".format(tile_1x1))


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
    cmd = ['gdal_rasterize', '-tr', '{}'.format(cn.Hansen_res), '{}'.format(cn.Hansen_res), '-co', 'COMPRESS=LZW',
           'PG:dbname=ubuntu', '-l', 'all_plant', 'plant_gain_{0}_{1}.tif'.format(ymax_1x1, xmin_1x1), '-te',
           str(xmin_1x1), str(ymin_1x1), str(xmax_1x1), str(ymax_1x1), '-a', 'growth', '-a_nodata', '0']
    # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        uu.log_subprocess_output(process.stdout)


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
    cmd = ['gdal_rasterize', '-tr', '{}'.format(cn.Hansen_res), '{}'.format(cn.Hansen_res), '-co', 'COMPRESS=LZW', 'PG:dbname=ubuntu',
           '-l', 'all_plant', 'plant_type_{0}_{1}.tif'.format(ymax_1x1, xmin_1x1),
           '-te', str(xmin_1x1), str(ymin_1x1), str(xmax_1x1), str(ymax_1x1),
           '-a', 'type_reclass', '-a_nodata', '0', '-ot', 'Byte']
    # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        uu.log_subprocess_output(process.stdout)



# Makes 1x1 rasters of the removal rate standard deviation
def create_1x1_plantation_stdev_from_1x1_planted(tile_1x1):

    # Gets the bounding coordinates for the 1x1 degree tile
    coords = tile_1x1.split("_")
    xmin_1x1 = str(coords[3])[:-4]
    xmax_1x1 = int(xmin_1x1) + 1
    ymax_1x1 = int(coords[2])
    ymin_1x1 = ymax_1x1 - 1

    print "For", tile_1x1, "-- xmin_1x1:", xmin_1x1, "; xmax_1x1:", xmax_1x1, "; ymin_1x1", ymin_1x1, "; ymax_1x1:", ymax_1x1

    print "There are plantations in {}. Converting to stdev raster...".format(tile_1x1)

    # https://gis.stackexchange.com/questions/187224/how-to-use-gdal-rasterize-with-postgis-vector
    cmd = ['gdal_rasterize', '-tr', '{}'.format(cn.Hansen_res), '{}'.format(cn.Hansen_res), '-co', 'COMPRESS=LZW', 'PG:dbname=ubuntu',
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
           '-co', 'COMPRESS=LZW', '-tap', '-te', str(xmin), str(ymin), str(xmax), str(ymax),
           '-dstnodata', '0', '-t_srs', 'EPSG:4326', '-overwrite', '-ot', 'Float32', plant_gain_1x1_vrt, tile_10x10]
    # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        uu.log_subprocess_output(process.stdout)

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
           '-co', 'COMPRESS=LZW', '-tap', '-te', str(xmin), str(ymin), str(xmax), str(ymax),
           '-dstnodata', '0', '-t_srs', 'EPSG:4326', '-overwrite', '-ot', 'Byte', plant_type_1x1_vrt, tile_10x10]
    # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        uu.log_subprocess_output(process.stdout)

    uu.print_log("Checking if {} contains any data...".format(tile_id))
    stats = uu.check_for_data(tile_10x10)
    
    if stats[0] > 0:

        uu.print_log("  Data found in {}. Copying tile to s3...".format(tile_id))
        uu.upload_final(cn.planted_forest_type_unmasked_dir, tile_id, cn.pattern_planted_forest_type_unmasked)
        uu.print_log("    Tile converted and copied to s3")

    else:

        print "  No data found. Not copying {}.".format(tile_id)


# Combines the 1x1 plantation tiles into 10x10 plantation carbon gain rate tiles, the final output of this process
def create_10x10_plantation_gain_stdev(tile_id, plant_stdev_1x1_vrt):

    print "Getting bounding coordinates for tile", tile_id
    xmin, ymin, xmax, ymax = uu.coords(tile_id)
    print "  xmin:", xmin, "; xmax:", xmax, "; ymin", ymin, "; ymax:", ymax

    tile_10x10 = '{0}_{1}.tif'.format(tile_id, cn.pattern_planted_forest_stdev_unmasked)
    print "Rasterizing", tile_10x10
    cmd = ['gdalwarp', '-tr', '{}'.format(str(cn.Hansen_res)), '{}'.format(str(cn.Hansen_res)),
           '-co', 'COMPRESS=LZW', '-tap', '-te', str(xmin), str(ymin), str(xmax), str(ymax),
           '-dstnodata', '0', '-t_srs', 'EPSG:4326', '-overwrite', '-ot', 'Float32', plant_stdev_1x1_vrt, tile_10x10]
    subprocess.check_call(cmd)

    print "Checking if {} contains any data...".format(tile_id)
    stats = uu.check_for_data_old(tile_10x10)

    if stats[0] > 0:

        print "  Data found in {}. Copying tile to s3...".format(tile_id)
        uu.upload_final(cn.planted_forest_stdev_unmasked_dir, tile_id, cn.pattern_planted_forest_stdev_unmasked)
        print "    Tile converted and copied to s3"

    else:

        print "  No data found. Not copying {}.".format(tile_id)

