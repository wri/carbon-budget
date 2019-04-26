'''
Code for creating 10x10 degree tiles of aboveground+belowground carbon accumulation rate
and plantation category out of the plantation geodatabase.
Its outputs are two sets of tiles at the full extent of planted forest features (not masked by mangrove or
non-mangrove natural forest): above+belowground carbon accumulation rate, and plantation type (oil palm, wood fiber,
other).
Unlike other carbon model scripts, this one requires prep work done outside of the script (the large, commented chunk
below this).
Once that prep work of converting the gdb to a PostGIS table has been done, there are a few entry points to this script,
unlike other carbon model scripts.
Which entry point is chosen depends on what has changed in the plantation data since it was last processed.
## NOTE: The entry point for the script is indicated by supplying two arguments following the python call.
The entry points rely on two shapefiles of the indexes (outlines) of 1x1 tiles created during plantation processing:
1. Shapefile of 1x1 tiles of the countries with planted forests (basically, countries with planted forests rasterized to 1x1 deg),
2. Shapefile of 1x1 tiles of planted forest extent (basically, planted forest extent rasterized to 1x1 deg).
These shapefiles should have been created during the previous run of the processing script and be on s3.

First entry point: Script runs from the beginning. Do this if the planted forest database now includes countries
that were not in it during the previous planted forest growth rate processing. It will take several days to run.
## NOTE: This also requires updating the list of countries with planted forests in constants_and_names.plantation_countries.
This entry point is accessed by supplying None to both arguments, i.e. mp_plantation_preparation.py None None

Second entry point: Script uses existing index shapefile of 1x1 tiles of countries with planted forests. Use this entry point
if no countries have been added to the planted forest database but some other spatial aspect of the data has changed
since the last processing, e.g., newly added planted forests in countries already in the database or the boundaries
of existing features have been altered. This entry point will use the supplied index shapefile of the 1x1 tiles of
countries with planted forests to create new 1x1 planted forest growth rate tiles. This entry point is accessed by
providing the s3 location of the index shapefile of the 1x1 country tiles,
e.g., mp_plantation_preparation.py s3://gfw2-data/climate/carbon_model/gadm_plantation_1x1_tile_index/GADM_index_1x1_20190108.shp None

Third entry point: Script uses existing index shapefile of 1x1 tiles of planted forest extent to create new 1x1 tiles
of planted forest growth rates. Use this entry point if the spatial properties of the database haven't changed but
the growth rates or forest type have. This route will iterate through only the 1x1 tiles that had planted forests previously and
create new planted forest growth rate tiles for them.
This entry point is accessed by providing the s3 location of the index shapefile of the 1x1
planted forest extent tiles,
e.g., python mp_plantation_preparation.py None s3://gfw2-data/climate/carbon_model/gadm_plantation_1x1_tile_index/plantation_index_1x1_20190108.shp
e.g., python mp_plantation_preparation.py --gadm-tile-index s3://gfw2-data/climate/carbon_model/gadm_plantation_1x1_tile_index/gadm_index_1x1_20190108.shp --planted-tile-index s3://gfw2-data/climate/carbon_model/gadm_plantation_1x1_tile_index/plantation_index_1x1_20190108.shp

All entry points conclude with creating 10x10 degree tiles of planted forest carbon accumulation rates and
planted forest type from 1x1 tiles of planted forest extent.
'''

"""
### Before running this script, the plantation gdb must be converted into a PostGIS table. That's more easily done as a series
### of commands than as a script. Below are the instructions for creating a single PostGIS table of all plantations.
### This assumes that the plantation gdb has one feature class for each country with plantations and that
### each country's feature class's attribute table has a growth rate column named "growth".

# Start a r.16xlarge spot machine
spotutil new r4.16xlarge dgibbs_wri

# Copy zipped plantation gdb with growth rate field in tables
aws s3 cp s3://gfw-files/plantations/final/global/plantations_v1_3.gdb.zip .

# Unzip the zipped plantation gdb. This can take several minutes.
unzip plantations_v1_3.gdb.zip

# Add the feature class of one country's plantations to PostGIS. This creates the "all_plant" table for other countries to be appended to.
# Using ogr2ogr requires the PG connection info but entering the PostGIS shell (psql) doesn't.
ogr2ogr -f Postgresql PG:"dbname=ubuntu" plantations_v1_3.gdb -progress -nln all_plant -sql "SELECT growth, species_simp FROM cmr_plant"

# Enter PostGIS and check that the table is there and that it has only the growth field.
psql
\d+ all_plant;

# Delete all rows from the table so that it is now empty
DELETE FROM all_plant;

# Exit the PostGIS shell
\q

# Get a list of all feature classes (countries) in the geodatabase and save it as a txt
ogrinfo plantations_v1_3.gdb | cut -d: -f2 | cut -d'(' -f1 | grep plant | grep -v Open | sed -e 's/ //g' > out.txt

# Make sure all the country tables are listed in the txt, then exit it
more out.txt
q

# Run a loop in bash that iterates through all the gdb feature classes and imports them to the all_plant PostGIS table
while read p; do echo $p; ogr2ogr -f Postgresql PG:"dbname=ubuntu" plantations_v1_3.gdb -nln all_plant -progress -append -sql "SELECT growth, species_simp FROM $p"; done < out.txt

# Create a spatial index of the plantation table to speed up the intersections with 1x1 degree tiles
psql
CREATE INDEX IF NOT EXISTS all_plant_index ON all_plant using gist(wkb_geometry);

# Adds a new column to the table and stores the plantation type reclassified as 1 (palm), 2 (wood fiber), or 3 (other)
ALTER TABLE all_plant ADD COLUMN type_reclass SMALLINT;
# Based on https://stackoverflow.com/questions/15766102/i-want-to-use-case-statement-to-update-some-records-in-sql-server-2005
UPDATE all_plant SET type_reclass = ( CASE WHEN species_simp = 'Oil Palm ' then '1' when species_simp = 'Oil Palm Mix ' then '1' when species_simp = 'Oil Palm ' then '1' when species_simp = 'Oil Palm Mix' then 1 when species_simp = 'Wood fiber / timber' then 2 when species_simp = 'Wood fiber / timber ' then 2 ELSE '3' END );

# Exit Postgres shell
\q

# Install a Python package that is needed for certain processing routes below
sudo pip install simpledbf

"""

import plantation_preparation
from multiprocessing.pool import Pool
from functools import partial
import glob
import subprocess
import argparse
import os
from simpledbf import Dbf5
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu


def main ():

    parser = argparse.ArgumentParser(description='Create planted forest carbon gain rate tiles')
    parser.add_argument('--gadm-tile-index', '-gi', required=True,
                        help='Shapefile of 1x1 degree tiles of countries that contain planted forests (i.e. countries with planted forests rasterized to 1x1 deg). If no shapefile, write None.')
    parser.add_argument('--planted-tile-index', '-pi', required=True,
                        help='Shapefile of 1x1 degree tiles of that contain planted forests (i.e. planted forest extent rasterized to 1x1 deg). If no shapefile, write None.')
    args = parser.parse_args()

    # Creates the directory and shapefile names for the two possible arguments (index shapefiles)
    gadm_index = os.path.split(args.gadm_tile_index)
    gadm_index_path = gadm_index[0]
    gadm_index_shp = gadm_index[1]
    gadm_index_shp = gadm_index_shp[:-4]
    planted_index = os.path.split(args.planted_tile_index)
    planted_index_path = planted_index[0]
    planted_index_shp = planted_index[1]
    planted_index_shp = planted_index_shp[:-4]

    # Checks the validity of the two arguments. If either one is invalid, the script ends.
    if (gadm_index_path not in cn.gadm_plant_1x1_index_dir or planted_index_path not in cn.gadm_plant_1x1_index_dir):
        raise Exception('Invalid inputs. Please provide None or s3 shapefile locations for both arguments.')

    # List of all possible 10x10 Hansen tiles except for those at very extreme latitudes (not just WHRC biomass tiles)
    total_tile_list = uu.tile_list(cn.pixel_area_dir)
    print "Number of possible 10x10 tiles to evaluate:", len(total_tile_list)

    # Removes the latitude bands that don't have any planted forests in them according to Liz Goldman.
    # i.e., Liz Goldman said by Slack on 1/2/19 that the nothernmost planted forest is 69.5146 and the southernmost is -46.938968.
    # This creates a more focused list of 10x10 tiles to iterate through (removes ones that definitely don't have planted forest).
    # NOTE: If the planted forest gdb is updated, the list of latitudes to exclude below may need to be changed to not exclude certain latitude bands.
    planted_lat_tile_list = [tile for tile in total_tile_list if '90N' not in tile]
    planted_lat_tile_list = [tile for tile in planted_lat_tile_list if '80N' not in tile]
    planted_lat_tile_list = [tile for tile in planted_lat_tile_list if '50S' not in tile]
    planted_lat_tile_list = [tile for tile in planted_lat_tile_list if '60S' not in tile]
    planted_lat_tile_list = [tile for tile in planted_lat_tile_list if '70S' not in tile]
    planted_lat_tile_list = [tile for tile in planted_lat_tile_list if '80S' not in tile]
    # planted_lat_tile_list = ['10N_080W']

    print planted_lat_tile_list
    print "Number of 10x10 tiles to evaluate after extreme latitudes have been removed:", len(planted_lat_tile_list)


    # If a planted forest extent 1x1 tile index shapefile isn't supplied
    if 'None' in args.planted_tile_index:

        ### Entry point 1:
        # If no shapefile of 1x1 tiles for countries with planted forests is supplied, 1x1 tiles of country extents will be created.
        # This runs the process from the very beginning and will take a few days.
        if 'None' in args.gadm_tile_index:

            print "No GADM 1x1 tile index shapefile provided. Creating 1x1 planted forest country tiles from scratch..."

            # Downloads and unzips the GADM shapefile, which will be used to create 1x1 tiles of land areas
            uu.s3_file_download(cn.gadm_path, '.')
            cmd = ['unzip', cn.gadm_zip]
            subprocess.check_call(cmd)

            # Creates a new GADM shapefile with just the countries that have planted forests in them.
            # This limits creation of 1x1 rasters of land area on the countries that have planted forests rather than on all countries.
            # NOTE: If the planted forest gdb is updated and has new countries added to it, the planted forest country list
            # in constants_and_names.py must be updated, too.
            print "Creating shapefile of countries with planted forests..."
            os.system('''ogr2ogr -sql "SELECT * FROM gadm_3_6_adm2_final WHERE iso IN ({0})" {1} gadm_3_6_adm2_final.shp'''.format(str(cn.plantation_countries)[1:-1], cn.gadm_iso))

            # Creates 1x1 degree tiles of countries that have planted forests in them.
            # I think this can handle using 50 processors because it's not trying to upload files to s3 and the tiles are small.
            # This takes several days to run because it iterates through at least 250 10x10 tiles.
            # For multiprocessor use.
            num_of_processes = 50
            pool = Pool(num_of_processes)
            pool.map(plantation_preparation.rasterize_gadm_1x1, planted_lat_tile_list)
            pool.close()
            pool.join()

            # # Creates 1x1 degree tiles of countries that have planted forests in them.
            # # For single processor use.
            # for tile in planted_lat_tile_list:
            #
            #     plantation_preparation.rasterize_gadm_1x1(tile)

            # Creates a shapefile of the boundaries of the 1x1 GADM tiles in countries with planted forests
            os.system('''gdaltindex {0}_{1}.shp GADM_*.tif'''.format(cn.pattern_gadm_1x1_index, uu.date_today))
            cmd = ['aws', 's3', 'cp', '.', cn.gadm_plant_1x1_index_dir, '--exclude', '*', '--include', '{}*'.format(cn.pattern_gadm_1x1_index), '--recursive']
            subprocess.check_call(cmd)

            # # Saves the 1x1 country extent tiles to s3
            # # Only use if the entire process can't run in one go on the spot machine
            # cmd = ['aws', 's3', 'cp', '.', 's3://gfw2-data/climate/carbon_model/temp_spotmachine_output/', '--exclude', '*', '--include', 'GADM_*.tif', '--recursive']
            # subprocess.check_call(cmd)

            # Delete the aux.xml files
            os.system('''rm GADM*.tif.*''')

            # List of all 1x1 degree countey extent tiles created
            gadm_list_1x1 = uu.tile_list_spot_machine(".", "GADM_")
            print "List of 1x1 degree tiles in countries that have planted forests, with defining coordinate in the northwest corner:", gadm_list_1x1
            print len(gadm_list_1x1)

        ### Entry point 2:
        # If a shapefile of the boundaries of 1x1 degree tiles of countries with planted forests is supplied,
        # a list of the 1x1 tiles is created from the shapefile.
        # This avoids creating the 1x1 country extent tiles all over again because the relevant tile extent are supplied
        # in the shapefile.
        elif cn.gadm_plant_1x1_index_dir in args.gadm_tile_index:

            print "Country extent 1x1 tile index shapefile supplied. Using that to create 1x1 planted forest tiles..."

            # Copies the shapefile of 1x1 tiles of extent of countries with planted forests
            cmd = ['aws', 's3', 'cp', '{}/'.format(gadm_index_path), '.', '--recursive', '--exclude', '*', '--include', '{}*'.format(gadm_index_shp), '--recursive']
            subprocess.check_call(cmd)

            # Gets the attribute table of the country extent 1x1 tile shapefile
            gadm = glob.glob('{}*.dbf'.format(cn.pattern_gadm_1x1_index))[0]

            # Converts the attribute table to a dataframe
            dbf = Dbf5(gadm)
            df = dbf.to_dataframe()

            # Converts the column of the dataframe with the names of the tiles (which contain their coordinates) to a list
            gadm_list_1x1 = df['location'].tolist()
            gadm_list_1x1 = [str(y) for y in gadm_list_1x1]
            print "List of 1x1 degree tiles in countries that have planted forests, with defining coordinate in the northwest corner:", gadm_list_1x1
            print "There are", len(gadm_list_1x1), "1x1 country extent tiles to iterate through."

        # In case some other arguments are provided
        else:

            raise Exception('Invalid GADM tile index shapefile provided. Please provide a valid shapefile.')

        # Creates 1x1 degree tiles of plantation growth wherever there are plantations.
        # Because this is iterating through all 1x1 tiles in countries with planted forests, it first checks
        # whether each 1x1 tile intersects planted forests before creating a 1x1 planted forest tile for that
        # 1x1 country extent tile.
        # For multiprocessor use
        num_of_processes = 30
        pool = Pool(num_of_processes)
        pool.map(plantation_preparation.create_1x1_plantation_from_1x1_gadm, gadm_list_1x1)
        pool.close()
        pool.join()

        # # Creates 1x1 degree tiles of plantation growth wherever there are plantations
        # # For single processor use
        # for tile in gadm_list_1x1:
        #
        #     plantation_preparation.create_1x1_plantation(tile)

        # Creates a shapefile in which each feature is the extent of a plantation extent tile.
        # This index shapefile can be used the next time this process is run if starting with Entry Point 3.
        os.system('''gdaltindex {0}_{1}.shp plant_*.tif'''.format(cn.pattern_plant_1x1_index, uu.date_today))
        cmd = ['aws', 's3', 'cp', '.', cn.gadm_plant_1x1_index_dir, '--exclude', '*', '--include', '{}*'.format(cn.pattern_plant_1x1_index), '--recursive']
        subprocess.check_call(cmd)

    ### Entry point 3
    # If a shapefile of the extents of 1x1 planted forest tiles is provided.
    # This is the part that actually creates the sequestration rate and forest type tiles.
    if cn.pattern_plant_1x1_index in args.planted_tile_index:

        print "Planted forest 1x1 tile index shapefile supplied. Using that to create 1x1 planted forest growth rate and forest type tiles..."

        # Copies the shapefile of 1x1 tiles of extent of planted forests
        cmd = ['aws', 's3', 'cp', '{}/'.format(planted_index_path), '.', '--recursive', '--exclude', '*', '--include',
               '{}*'.format(planted_index_shp), '--recursive']
        subprocess.check_call(cmd)

        # Gets the attribute table of the planted forest extent 1x1 tile shapefile
        gadm = glob.glob('{}*.dbf'.format(cn.pattern_plant_1x1_index))[0]

        # Converts the attribute table to a dataframe
        dbf = Dbf5(gadm)
        df = dbf.to_dataframe()

        # Converts the column of the dataframe with the names of the tiles (which contain their coordinates) to a list
        planted_list_1x1 = df['location'].tolist()
        planted_list_1x1 = [str(y) for y in planted_list_1x1]
        print "List of 1x1 degree tiles in countries that have planted forests, with defining coordinate in the northwest corner:", planted_list_1x1
        print "There are", len(planted_list_1x1), "1x1 planted forest extent tiles to iterate through."

        # Creates 1x1 degree tiles of plantation growth and type wherever there are plantations.
        # Because this is iterating through only 1x1 tiles that are known to have planted forests (from a previous run
        # of this script), it does not need to check whether there are planted forests in this tile. It goes directly
        # to intersecting the planted forest table with the 1x1 tile.

        # # For multiprocessor use
        # # This works with 30 processors on an r4.16xlarge.
        # num_of_processes = 50
        # pool = Pool(num_of_processes)
        # pool.map(plantation_preparation.create_1x1_plantation_growth_from_1x1_planted, planted_list_1x1)
        # pool.close()
        # pool.join()

        # This works with 50 processors on an r4.16xlarge marchine. Uses about 430 GB out of 480 GB.
        num_of_processes = 50
        pool = Pool(num_of_processes)
        pool.map(plantation_preparation.create_1x1_plantation_type_from_1x1_planted, planted_list_1x1)
        pool.close()
        pool.join()



    ### All script entry points meet here: creation of 10x10 degree planted forest gain rate and rtpe tiles
    ### from 1x1 degree planted forest gain rate and type tiles

    # Name of the vrt of 1x1 planted forest gain rate tiles
    plant_gain_1x1_vrt = 'plant_gain_1x1.vrt'

    # Creates a mosaic of all the 1x1 plantation gain rate tiles
    print "Creating vrt of 1x1 plantation gain rate tiles"
    os.system('gdalbuildvrt {} plant_gain_*.tif'.format(plant_gain_1x1_vrt))

    # Creates 10x10 degree tiles of plantation gain rate by iterating over the set of pixel area tiles supplied
    # at the start of the script that are in latitudes with planted forests.
    # For multiprocessor use
    num_of_processes = 20
    pool = Pool(num_of_processes)
    pool.map(partial(plantation_preparation.create_10x10_plantation_gain, plant_gain_1x1_vrt=plant_gain_1x1_vrt), planted_lat_tile_list)
    pool.close()
    pool.join()

    # # Creates 10x10 degree tiles of plantation gain rate by iterating over the set of pixel area tiles supplied
    # at the start of the script that are in latitudes with planted forests.
    # # For single processor use
    # for tile in planted_lat_tile_list:
    #
    #     plantation_preparation.create_10x10_plantation_gain(tile, plant_gain_1x1_vrt)


    # Name of the vrt of 1x1 planted forest type tiles
    plant_type_1x1_vrt = 'plant_type_1x1.vrt'

    # Creates a mosaic of all the 1x1 plantation type tiles
    print "Creating vrt of 1x1 plantation type tiles"
    os.system('gdalbuildvrt {} plant_type_*.tif'.format(plant_type_1x1_vrt))

    # Creates 10x10 degree tiles of plantation type by iterating over the set of pixel area tiles supplied
    # at the start of the script that are in latitudes with planted forests.
    # For multiprocessor use
    num_of_processes = 20
    pool = Pool(num_of_processes)
    pool.map(partial(plantation_preparation.create_10x10_plantation_type, plant_type_1x1_vrt=plant_type_1x1_vrt),
             planted_lat_tile_list)
    pool.close()
    pool.join()

    # # Creates 10x10 degree tiles of plantation type by iterating over the set of pixel area tiles supplied
    # at the start of the script that are in latitudes with planted forests.
    # # For single processor use
    # for tile in planted_lat_tile_list:
    #
    #     plantation_preparation.create_10x10_plantation_type(tile, plant_type_1x1_vrt)



if __name__ == '__main__':
    main()