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
import subprocess
import os
from osgeo import gdal
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu


# List of all possible 10x10 Hansen tiles except for those at very extreme latitudes (not just WHRC biomass tiles)
total_tile_list = uu.tile_list(cn.pixel_area_dir)

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
# planted_lat_tile_list = ['00N_080W']
print planted_lat_tile_list

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
cmd = ['aws', 's3', 'cp', '.', 's3://gfw2-data/climate/carbon_model/temp_spotmachine_output/', '--exclude', '*', '--include', 'GADM_*.tif', '--recursive']
subprocess.check_call(cmd)

# To delete the aux.xml files
os.system('''rm *.tif.*''')

# List of all 1x1 degree GADM tiles created
list_1x1 = uu.tile_list_spot_machine(".", ".tif")
print "List of 1x1 degree tiles in GADM countries that have planted forests, with defining coordinate in the northwest corner:", list_1x1
print len(list_1x1)
# list_1x1 = ['GADM_0_-80.tif', 'GADM_0_-79.tif', 'GADM_0_-78.tif', 'GADM_-1_-80.tif', 'GADM_-1_-79.tif', 'GADM_-1_-78.tif']

# Creates 1x1 degree tiles of plantation growth wherever there are plantations
# For multiprocessor use
num_of_processes = 25
pool = Pool(num_of_processes)
pool.map(plantation_preparation.create_1x1_plantation, list_1x1)
pool.close()
pool.join()

# Creates 1x1 degree tiles of plantation growth wherever there are plantations
# For single processor use
for tile in list_1x1:

    plantation_preparation.create_1x1_plantation(tile)

os.system('''gdaltindex {0}_{1}.shp plant_*.tif'''.format(cn.pattern_plant_1x1_index, uu.date))
cmd = ['aws', 's3', 'cp', '.', cn.gadm_plant_1x1_index_dir, '--exclude', '*', '--include', '{}*'.format(cn.pattern_plant_1x1_index), '--recursive']
subprocess.check_call(cmd)


# plant_1x1_vrt = 'plant_1x1.vrt'
#
# # Creates a mosaic of all the 1x1 plantation growth rate tiles
# print "Creating vrt of 1x1 plantation growth rate tiles"
# os.system('gdalbuildvrt {} plant_*.tif'.format(plant_1x1_vrt))
#
# # Creates 10x10 degree tiles of plantation growth by iterating over the pixel area tiles that are in latitudes with planted forests
# # For multiprocessor use
# num_of_processes = 20
# pool = Pool(num_of_processes)
# pool.map(partial(plantation_preparation.create_10x10_plantation, plant_1x1_vrt=plant_1x1_vrt), planted_lat_tile_list)
# pool.close()
# pool.join()

# # Creates 10x10 degree tiles of plantation growth by iterating over the pixel area tiles that are in latitudes with planted forests
# # For single processor use
# for tile in planted_lat_tile_list:
#
#     plantation_preparation.create_10x10_plantation(tile, plant_1x1_vrt)