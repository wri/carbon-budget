
import subprocess
import os
import psycopg2
import re
from osgeo import gdal
import sys
sys.path.append('../')
import constants_and_names
import universal_util

def rasterize_gadm_1x1(tile_id):

    print "Getting bounding coordinates for tile", tile_id
    xmin, ymin, xmax, ymax = universal_util.coords(tile_id)
    print "  xmin:", xmin, "; xmax:", xmax, "; ymin", ymin, "; ymax:", ymax

    # Degrees of tile in x and y dimensions
    x_size = abs(int(xmin) - int(xmax))
    y_size = abs(int(ymin) - int(ymax))

    # Iterates through tile by 1x1 degree
    for x in range(x_size):

        xmin_1x1 = int(xmin) + x
        xmax_1x1 = int(xmin) + x + 1

        for y in range(y_size):

            ymin_1x1 = int(ymin) + y
            ymax_1x1 = int(ymin) + y + 1

            print "  xmin_1x1:", xmin_1x1, "; xmax_1x1:", xmax_1x1, "; ymin_1x1", ymin_1x1, "; ymax_1x1:", ymax_1x1

            tile_1x1 = 'GADM_{0}_{1}.tif'.format(ymax_1x1, xmin_1x1)
            print "Rasterizing", tile_1x1
            cmd = ['gdal_rasterize', '-tr', '{}'.format(str(constants_and_names.Hansen_res)), '{}'.format(str(constants_and_names.Hansen_res)),
                   '-co', 'COMPRESS=LZW', '-te', str(xmin_1x1), str(ymin_1x1), str(xmax_1x1), str(ymax_1x1),
                   '-burn', '1', '-a_nodata', '0', constants_and_names.gadm_iso, tile_1x1]
            subprocess.check_call(cmd)

            print "Checking if {} contains any data...".format(tile_1x1)
            stats = universal_util.check_for_data(tile_1x1)

            if stats[1] > 0:
                print "  Data found in {}. Keeping tile".format(tile_1x1)

            else:
                print "  No data found in {}. Deleting.".format(tile_1x1)
                os.remove(tile_1x1)

# Creates a list of 1x1 degree tiles, with the defining coordinate in the northwest corner
def create_1x1_plantation(tile_1x1):

    # Gets the bounding coordinates for the 1x1 degree tile
    coords = tile_1x1.split("_")
    print coords
    xmin_1x1 = str(coords[2])[:-4]
    xmax_1x1 = int(xmin_1x1) + 1
    ymax_1x1 = int(coords[1])
    ymin_1x1 = ymax_1x1 - 1

    print "For", tile_1x1, "-- xmin_1x1:", xmin_1x1, "; xmax_1x1:", xmax_1x1, "; ymin_1x1", ymin_1x1, "; ymax_1x1:", ymax_1x1

    # Connects Python to PostGIS using psycopg2. The credentials work on spot machines as they are currently configured
    # and are based on this: https://github.com/wri/gfw-annual-loss-processing/blob/master/1b_Summary-AOIs-to-TSV/utilities/postgis_util.py
    creds = {'host': 'localhost', 'user': 'ubuntu', 'dbname': 'ubuntu'}
    conn = psycopg2.connect(**creds)
    cursor = conn.cursor()

    # Intersects the plantations with the 1x1 tile, then saves any growth rates in that tile as a list
    # https://gis.stackexchange.com/questions/30267/how-to-create-a-valid-global-polygon-grid-in-postgis
    # https://stackoverflow.com/questions/48978616/best-way-to-run-st-intersects-on-features-inside-one-table
    # https://postgis.net/docs/ST_Intersects.html
    cursor.execute("SELECT growth FROM all_plant WHERE ST_Intersects(all_plant.wkb_geometry, ST_GeogFromText('POLYGON(({0} {1},{2} {1},{2} {3},{0} {3},{0} {1}))'))".format(
            xmin_1x1, ymax_1x1, xmax_1x1, ymin_1x1))
    features = cursor.fetchall()
    cursor.close()

    if len(features) > 0:

        print "There are plantations in {}. Converting to raster...".format(tile_1x1)

        # https://gis.stackexchange.com/questions/187224/how-to-use-gdal-rasterize-with-postgis-vector
        cmd = ['gdal_rasterize', '-tr', '{}'.format(constants_and_names.Hansen_res), '{}'.format(constants_and_names.Hansen_res), '-co', 'COMPRESS=LZW', 'PG:dbname=ubuntu', '-l', 'all_plant', 'plant_{0}_{1}.tif'.format(ymax_1x1, xmin_1x1), '-te', str(xmin_1x1), str(ymin_1x1), str(xmax_1x1), str(ymax_1x1), '-a', 'growth', '-a_nodata', '0']
        subprocess.check_call(cmd)

    else:
        print "There are no plantations in {}. Not converting to raster.".format(tile_1x1)


def create_10x10_plantation(tile_id):

    print "Getting bounding coordinates for tile", tile_id
    xmin, ymin, xmax, ymax = universal_util.coords(tile_id)
    print "  xmin:", xmin, "; xmax:", xmax, "; ymin", ymin, "; ymax:", ymax

    tile_10x10 = '{0}_{1}.tif'.format(tile_id, constants_and_names.pattern_annual_gain_AGB_planted_forest)
    print "Rasterizing", tile_10x10
    cmd = ['gdalwarp', '-tr', '{}'.format(str(constants_and_names.Hansen_res)), '{}'.format(str(constants_and_names.Hansen_res)),
           '-co', 'COMPRESS=LZW', '-te', str(xmin), str(ymin), str(xmax), str(ymax),
           '-burn', '1', '-a_nodata', '0', 'plant_1x1.vrt', tile_10x10]
    subprocess.check_call(cmd)

    print "Checking if {} contains any data...".format(tile_id)
    stats = universal_util.check_for_data(tile_10x10)

    if stats[0] > 0:

        print "  Data found in {}. Copying tile to s3...".format(tile_id)
        universal_util.upload_final(constants_and_names.annual_gain_AGB_planted_forest_dir, tile_id, constants_and_names.pattern_annual_gain_AGB_planted_forest)
        print "    Tile copied to s3"

    else:

        print "  No data found. Not copying {}.".format(tile_id)

