
import subprocess
import os
import psycopg2
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Creates 1x1 tiles of where select countries are in select latitude bands, with the defining coordinates in the
# northwest corner
def rasterize_gadm_1x1(tile_id):

    print "Getting bounding coordinates for tile", tile_id
    xmin, ymin, xmax, ymax = uu.coords(tile_id)
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
            cmd = ['gdal_rasterize', '-tr', '{}'.format(str(cn.Hansen_res)), '{}'.format(str(cn.Hansen_res)),
                   '-co', 'COMPRESS=LZW', '-te', str(xmin_1x1), str(ymin_1x1), str(xmax_1x1), str(ymax_1x1),
                   '-burn', '1', '-a_nodata', '0', cn.gadm_iso, tile_1x1]
            subprocess.check_call(cmd)

            # Only keeps 1x1 GADM tiles if they actually include a country; many 1x1 tiles created out of 10x10 tiles
            # don't actually include a country.
            print "Checking if {} contains any data...".format(tile_1x1)
            stats = uu.check_for_data(tile_1x1)

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

    # Intersects the plantations PostGIS table with the 1x1 tile, then saves any growth rates in that tile as a 1x1 tile
    # https://gis.stackexchange.com/questions/30267/how-to-create-a-valid-global-polygon-grid-in-postgis
    # https://stackoverflow.com/questions/48978616/best-way-to-run-st-intersects-on-features-inside-one-table
    # https://postgis.net/docs/ST_Intersects.html
    print "Checking if {} has plantations in it".format(tile_1x1)

    # Does the intersect of the PostGIS table and the 1x1 GADM tile
    cursor.execute("SELECT growth FROM all_plant WHERE ST_Intersects(all_plant.wkb_geometry, ST_GeogFromText('POLYGON(({0} {1},{2} {1},{2} {3},{0} {3},{0} {1}))'))".format(
            xmin_1x1, ymax_1x1, xmax_1x1, ymin_1x1))

    # A Python list of the output of the intersection, which in this case is a list of features that were successfully intersected.
    # This is what I use to determine if any PostGIS features were intersected.
    features = cursor.fetchall()
    cursor.close()

    # If any features in the PostGIS table were intersected with the 1x1 GADM tile, then the features in this 1x1 tile
    # are converted to a planted forest growth rate tile
    if len(features) > 0:

        print "There are plantations in {}. Converting to raster...".format(tile_1x1)

        # https://gis.stackexchange.com/questions/187224/how-to-use-gdal-rasterize-with-postgis-vector
        cmd = ['gdal_rasterize', '-tr', '{}'.format(cn.Hansen_res), '{}'.format(cn.Hansen_res), '-co', 'COMPRESS=LZW', 'PG:dbname=ubuntu', '-l', 'all_plant', 'plant_{0}_{1}.tif'.format(ymax_1x1, xmin_1x1), '-te', str(xmin_1x1), str(ymin_1x1), str(xmax_1x1), str(ymax_1x1), '-a', 'growth', '-a_nodata', '0']
        subprocess.check_call(cmd)

    # If no features in the PostGIS table were intersected with the 1x1 GADM tile, nothing happens.
    else:
        print "There are no plantations in {}. Not converting to raster.".format(tile_1x1)


# Combines the 1x1 plantation tiles into 10x10 plantation tiles, the final output of this process
def create_10x10_plantation(tile_id, plant_1x1_vrt):

    print "Getting bounding coordinates for tile", tile_id
    xmin, ymin, xmax, ymax = uu.coords(tile_id)
    print "  xmin:", xmin, "; xmax:", xmax, "; ymin", ymin, "; ymax:", ymax

    tile_10x10_carbon = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_AGC_planted_forest_full_extent)
    print "Rasterizing", tile_10x10_carbon
    cmd = ['gdalwarp', '-tr', '{}'.format(str(cn.Hansen_res)), '{}'.format(str(cn.Hansen_res)),
           '-co', 'COMPRESS=LZW', '-tap', '-te', str(xmin), str(ymin), str(xmax), str(ymax),
           '-dstnodata', '0', '-t_srs', 'EPSG:4326', '-overwrite', '-ot', 'Float32', plant_1x1_vrt, tile_10x10_carbon]
    subprocess.check_call(cmd)

    print "Checking if {} contains any data...".format(tile_id)
    stats = uu.check_for_data(tile_10x10_carbon)

    if stats[0] > 0:

        print "  Data found in {}. Converting carbon tile to biomass and copying tile to s3...".format(tile_id)

        tile_10x10_biomass = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_AGC_planted_forest_full_extent)

        # Equation argument for converting planted forest growth from carbon to biomass.
        calc = '--calc=A/{}'.format(cn.biomass_to_c_natrl_forest)

        # Argument for outputting file
        out = '--outfile={}'.format(tile_10x10_biomass)

        print "Converting {} from carbon to biomass...".format(tile_10x10_carbon)
        cmd = ['gdal_calc.py', '-A', tile_10x10_carbon, calc, out, '--NoDataValue=0', '--co', 'COMPRESS=LZW',
               '--overwrite']
        subprocess.check_call(cmd)

        uu.upload_final(cn.annual_gain_AGC_planted_forest_dir, tile_id, cn.pattern_annual_gain_AGC_planted_forest_full_extent)
        print "    Tile converted and copied to s3"

    else:

        print "  No data found. Not copying {}.".format(tile_id)

