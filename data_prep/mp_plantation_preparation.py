# Code for creating 10x10 degree tiles of aboveground biomass accumulation rate out of the plantation geodatabase.
# The geodatabase provides carbon gain rate; the last step of this script converts this to biomass accumulation
# in order to have the planted forest annual gain tiles be the same units as the mangrove and non-mangrove, non-
# planted forest gain rates.
# Preparing data for this script and running this script will take several days if done from the very beginning.
# If the extent of the plantations hasn't changed (e.g., no new countries added and new features are added in the
# same countries or the rates of existing features are changed) then a script could be written to use
# the GADM 1x1 index shapefile (which is on s3) to recreate the 1x1 tiles over which the growth rates will be iterated rather than having
# to create the GADM 1x1 tiles from scratch, which takes the longest out of all steps here.
# If the plantations data does include new countries, the entire script should probably be run again.

"""
### Before running this script, the plantation gdb must be converted into a PostGIS table. That's more easily done as a series
### of commands than as a script. Below are the instructions for creating a single PostGIS table of all plantations.
### This assumes that the plantation gdb has one feature class for each country with plantations and that
### each country's feature class's attribute table has a growth rate column named "growth".

# Start a 16xlarge spot machine
spotutil new m4.16xlarge

# Copy zipped plantation gdb with growth rate field in tables
aws s3 cp s3://gfw-files/plantations/final/global/plantations_final_attributes.gdb.zip .

# Unzip the zipped plantation gdb. This can take several minutes
unzip plantations_final_attributes.gdb.zip

# Add the feature class of one country's plantations to PostGIS. This creates the "all_plant" table for other countries to be appended to.
# Using ogr2ogr requires the PG connection info but entering the PostGIS shell (psql) doesn't.
ogr2ogr -f Postgresql PG:"dbname=ubuntu" plantations_final_attributes.gdb -progress -nln all_plant -sql "SELECT growth FROM cmr_plant"

# Enter PostGIS and check that the table is there and that it has only the growth field.
psql
\d+ all_plant;

# Delete all rows from the table so that it is now empty
DELETE FROM all_plant;

# Get a list of all feature classes (countries) in the geodatabase and save it as a txt
ogrinfo plantations_final_attributes.gdb | cut -d: -f2 | cut -d'(' -f1 | grep plant | grep -v Open | sed -e 's/ //g' > out.txt

# Make sure all the country tables are listed in the txt
more out.txt

# Run a loop in bash that iterates through all the gdb feature classes and imports them to the all_plant PostGIS table
while read p; do echo $p; ogr2ogr -f Postgresql PG:"dbname=ubuntu" plantations_final_attributes.gdb -nln all_plant -progress -append -sql "SELECT growth FROM $p"; done < out.txt

# Create a spatial index of the plantation table to speed up the intersections with 1x1 degree tiles
psql
CREATE INDEX IF NOT EXISTS all_plant_index ON all_plant using gist(wkb_geometry);
"""

import plantation_preparation
from multiprocessing.pool import Pool
from functools import partial
import glob
import subprocess
import argparse
import os
from simpledbf import Dbf5
from osgeo import gdal
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu


def main ():

    parser = argparse.ArgumentParser(description='Create planted forest carbon gain rate tiles')
    parser.add_argument('--gadm-tile-index', '-gi',
                        help='directory with shapefile of 1x1 degree tiles of GADM country boundaries that contain planted forests')
    parser.add_argument('--planted-tile-index', '-pi',
                        help='directory with shapefile of 1x1 degree tiles of that contain planted forests')
    args = parser.parse_args()

    list_1x1 = []

    if args.gadm_tile_index is None:

        print "No GADM 1x1 tile index shapefile provided. Creating 1x1 GADM tiles from scratch..."

        # List of all possible 10x10 Hansen tiles except for those at very extreme latitudes (not just WHRC biomass tiles)
        total_tile_list = uu.tile_list(cn.pixel_area_dir)
        print len(total_tile_list)

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
        print len(planted_lat_tile_list)

        # Downloads and unzips the GADM shapefile, which will be used to create 1x1 tiles of land areas
        uu.s3_file_download(cn.gadm_path, '.')
        cmd = ['unzip', cn.gadm_zip]
        subprocess.check_call(cmd)

        # Creates a new GADM shapefile with just the countries that have planted forests in them.
        # This focuses creating 1x1 rasters of land area on the countries that have planted forests rather than on all countries.
        # NOTE: If the planted forest gdb is updated and has new countries added to it, the planted forest country list
        # in constants_and_names.py must be updated, too.
        print "Creating shapefile of countries with planted forests..."
        os.system('''ogr2ogr -sql "SELECT * FROM gadm_3_6_adm2_final WHERE iso IN ({0})" {1} gadm_3_6_adm2_final.shp'''.format(str(cn.plantation_countries)[1:-1], cn.gadm_iso))

        # Creates 1x1 degree tiles of GADM countries that have planted forests in them.
        # I think this can handle using 50 processors because it's not trying to upload files to s3.
        # This takes several days to run because it iterates through at least 250 10x10 tiles.
        # For multiprocessor use.
        num_of_processes = 50
        pool = Pool(num_of_processes)
        pool.map(plantation_preparation.rasterize_gadm_1x1, planted_lat_tile_list)
        pool.close()
        pool.join()

        # # Creates 1x1 degree tiles of GADM countries that have planted forests in them.
        # # For single processor use.
        # for tile in planted_lat_tile_list:
        #
        #     plantation_preparation.rasterize_gadm_1x1(tile)

        # Creates a shapefile of the boundaries of the 1x1 GADM tiles in countries with planted forests
        os.system('''gdaltindex {0}_{1}.shp GADM_*.tif'''.format(cn.pattern_gadm_1x1_index, uu.date))
        cmd = ['aws', 's3', 'cp', '.', cn.gadm_plant_1x1_index_dir, '--exclude', '*', '--include', '{}*'.format(cn.pattern_gadm_1x1_index), '--recursive']
        subprocess.check_call(cmd)

        # Saves the 1x1 GADM tiles to s3
        # Only use if the entire process can't run in one go on the spot machine
        cmd = ['aws', 's3', 'cp', '.', 's3://gfw2-data/climate/carbon_model/temp_spotmachine_output/', '--exclude', '*', '--include', 'GADM_*.tif', '--recursive']
        subprocess.check_call(cmd)

        # Delete the aux.xml files
        os.system('''rm GADM*.tif.*''')

        # List of all 1x1 degree GADM tiles created
        list_1x1 = uu.tile_list_spot_machine(".", "GADM")
        print "List of 1x1 degree tiles in GADM countries that have planted forests, with defining coordinate in the northwest corner:", list_1x1
        print len(list_1x1)

    else:

        print "GADM 1x1 tile index shapefile supplied. Using that to create 1x1 planted forest tiles..."

        cmd = ['aws', 's3', 'cp', args.gadm_tile_index, '.', '--recursive', '--exclude', '*', '--include', '{}*'.format(cn.pattern_gadm_1x1_index), '--recursive']
        subprocess.check_call(cmd)

        gadm = glob.glob('{}*.dbf'.format(cn.pattern_gadm_1x1_index))[0]
        print gadm

        dbf = Dbf5(gadm)
        df = dbf.to_dataframe()

        print df.head()

        list_1x1 = df['location'].tolist()
        print "List of 1x1 degree tiles in GADM countries that have planted forests, with defining coordinate in the northwest corner:", list_1x1
        print len(list_1x1)

    # Creates 1x1 degree tiles of plantation growth wherever there are plantations
    # For multiprocessor use
    num_of_processes = 25
    pool = Pool(num_of_processes)
    pool.map(plantation_preparation.create_1x1_plantation, list_1x1)
    pool.close()
    pool.join()

    # # Creates 1x1 degree tiles of plantation growth wherever there are plantations
    # # For single processor use
    # for tile in list_1x1:
    #
    #     plantation_preparation.create_1x1_plantation(tile)

    os.system('''gdaltindex {0}_{1}.shp plant_*.tif'''.format(cn.pattern_plant_1x1_index, uu.date))
    cmd = ['aws', 's3', 'cp', '.', cn.gadm_plant_1x1_index_dir, '--exclude', '*', '--include', '{}*'.format(cn.pattern_plant_1x1_index), '--recursive']
    subprocess.check_call(cmd)

    # Name of the vrt of 1x1 planted forest tiles
    plant_1x1_vrt = 'plant_1x1.vrt'

    # Creates a mosaic of all the 1x1 plantation growth rate tiles
    print "Creating vrt of 1x1 plantation growth rate tiles"
    os.system('gdalbuildvrt {} plant_*.tif'.format(plant_1x1_vrt))

    # Creates 10x10 degree tiles of plantation growth by iterating over the pixel area tiles that are in latitudes with planted forests
    # For multiprocessor use
    num_of_processes = 20
    pool = Pool(num_of_processes)
    pool.map(partial(plantation_preparation.create_10x10_plantation, plant_1x1_vrt=plant_1x1_vrt), planted_lat_tile_list)
    pool.close()
    pool.join()

    # # Creates 10x10 degree tiles of plantation growth by iterating over the pixel area tiles that are in latitudes with planted forests
    # # For single processor use
    # for tile in planted_lat_tile_list:
    #
    #     plantation_preparation.create_10x10_plantation(tile, plant_1x1_vrt)


if __name__ == '__main__':
    main()