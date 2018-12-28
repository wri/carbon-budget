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

# Iterates through all possible tiles (not just WHRC biomass tiles) to create mangrove biomass tiles that don't have analogous WHRC tiles
# total_tile_list = universal_util.tile_list(constants_and_names.pixel_area_dir)
total_tile_list = ['10N_070E', '20S_110W']

# # For multiprocessor use
# count = multiprocessing.cpu_count()
# pool = multiprocessing.Pool(processes=count/3)
# pool.map(plantation_preparation.create_1x1_tiles, total_tile_list)
# pool.close()
# pool.join()

# For single processor use, for testing purposes
for tile in total_tile_list:

    plantation_preparation.create_1x1_tiles(tile)


