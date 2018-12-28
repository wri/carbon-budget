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

# Get a list of all feature classes in the geodatabase and save it as a txt
ogrinfo plantations_final_attributes.gdb | cut -d: -f2 | cut -d'(' -f1 | grep plant | grep -v cmr | grep -v Open | sed -e 's/ //g' > out.txt

# Make sure all the country tables are listed in the txt
more out.txt

# Run a loop in bash that iterates through all the gdb feature classes and imports them to the all_plant PostGIS table
while read p; do echo $p; ogr2ogr -f Postgresql PG:"dbname=ubuntu" plantations_final_attributes.gdb -nln all_plant -progress -append -sql "SELECT growth FROM $p"; done < out.txt
"""

import multiprocessing
import plantation_preparation
import subprocess
import os
import glob
import sys
sys.path.append('../')
import constants_and_names
import universal_util

# Creates a list of all 10x10 degree Hansen tiles on a continent (not just WHRC biomass tiles)
total_tile_list = universal_util.tile_list(constants_and_names.fao_ecozone_processed_dir)[-3:]
# total_tile_list = ['10N_070E', '10S_080W']

print total_tile_list

# Empty list to store 1x1 degree tiles
list_1x1 = []

# Iterates through all possible 10x10 degree Hansen tiles
for tile in total_tile_list:

    # Calls the function that creates all the 1x1 degree tiles
    plantation_preparation.create_1x1_tiles(tile, list_1x1)

print "List of 1x1 degree tiles, with defining coordinate in the northwest corner:", list_1x1

# # For multiprocessor use
# count = multiprocessing.cpu_count()
# pool = multiprocessing.Pool(processes=count/3)
# pool.map(plantation_preparation.create_1x1_tiles, total_tile_list)
# pool.close()
# pool.join()
