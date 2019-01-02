# Code for creating 10x10 degree tiles out of the plantation geodatabase

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
psql \d+ all_plant;

# Delete all rows from the table so that it is now empty
DELETE FROM all_plant;

# Get a list of all feature classes (countries) in the geodatabase and save it as a txt
ogrinfo plantations_final_attributes.gdb | cut -d: -f2 | cut -d'(' -f1 | grep plant | grep -v Open | sed -e 's/ //g' > out.txt

# Make sure all the country tables are listed in the txt
more out.txt

# Run a loop in bash that iterates through all the gdb feature classes and imports them to the all_plant PostGIS table
while read p; do echo $p; ogr2ogr -f Postgresql PG:"dbname=ubuntu" plantations_final_attributes.gdb -nln all_plant -progress -append -sql "SELECT growth FROM $p"; done < out.txt
"""

import multiprocessing
import plantation_preparation
import subprocess
import os
from osgeo import gdal
import sys
sys.path.append('../')
import constants_and_names
import universal_util


# Iterates through all possible 10x10 tiles except for those at extreme latitudes (not just WHRC biomass tiles)
total_tile_list = universal_util.tile_list(constants_and_names.pixel_area_dir)

# Removes the 10x10 tiles that don't have any planted forests in them according to Liz Goldman
# NOTE: If the planted forest gdb is updated, the list of latitudes to exclude below may need to be changed.
planted_lat_tile_list = [tile for tile in total_tile_list if '90N' not in tile]
planted_lat_tile_list = [tile for tile in planted_lat_tile_list if '80N' not in tile]
planted_lat_tile_list = [tile for tile in planted_lat_tile_list if '50S' not in tile]
planted_lat_tile_list = [tile for tile in planted_lat_tile_list if '60S' not in tile]
planted_lat_tile_list = [tile for tile in planted_lat_tile_list if '70S' not in tile]
planted_lat_tile_list = [tile for tile in planted_lat_tile_list if '80S' not in tile]
print planted_lat_tile_list

# # Downloads and unzips the GADM shapefile, which will be used to create 1x1 tiles of land areas
# universal_util.s3_file_download(constants_and_names.gadm_path, '.')
# cmd = ['unzip', constants_and_names.gadm_zip]
# subprocess.check_call(cmd)

# Creates a new shapefile with just the countries that have planted forests in them.
# This focuses rasterization on the countries that have planted forests, not all land.
# NOTE: If the planted forest gdb is updated and has new countries added to it, the planted forest country list
# in constants_and_names.py must be updated, too.
os.system('''ogr2ogr -sql "SELECT * FROM gadm_3_6_adm2_final WHERE iso IN ({0})" {1} gadm_3_6_adm2_final.shp'''.format(str(constants_and_names.plantation_countries)[1:-1], constants_and_names.gadm_iso))

# For multiprocessor use
count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(processes=count/3)
pool.map(plantation_preparation.rasterize_gadm_1x1, planted_lat_tile_list)
pool.close()
pool.join()

# # List of all 1x1 degree tiles created
# list_1x1 = universal_util.tile_list_spot_machine(".", "GADM.tif")
# print "List of 1x1 degree tiles, with defining coordinate in the northwest corner:", list_1x1
# print len(list_1x1)
#
# # Iterates through all possible 10x10 degree Hansen tiles
# for tile in list_1x1:
#
#     # Calls the function that creates all the 1x1 degree tiles
#     plantation_preparation.create_1x1_tiles(tile)
#
#
#
# # # For multiprocessor use
# # count = multiprocessing.cpu_count()
# # pool = multiprocessing.Pool(processes=count/3)
# # pool.map(plantation_preparation.create_1x1_tiles, total_tile_list)
# # pool.close()
# # pool.join()

