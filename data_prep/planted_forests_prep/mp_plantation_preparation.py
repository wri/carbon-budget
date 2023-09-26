#TODO Add establishment year from Du et al. update as another output column. Change readme accordingly.

"""
I updated the plantation rasterization scripts to use SDPT v2.0 in August 2023.
I tested them locally and they worked fine.
I was even able to ingest the entire gdb into postgres on an r5d spot machine eventually.
However, when I tried running rasterizing on the full postgres database on the spot machine, I ran into issues.
First, rasterizing 1x1 tiles was really slow, like 20 minutes per tile.
Second, a lot of tiles were throwing errors about not existing or something (I really don't remember what it was).
I couldn't figure out why rasterization was so slow and why some tiles were failing.
So I gave up on using this script and switched to rasterizing SDPT v2 using the gfw-data-api,
which also had issues but ended up seeming to work fine.
I am wondering if rasterizing with this script was very slow because I could never get PostGIS to make a spatial
index for the database after ingestion, unlike when I rasterized SDPT v1.
Also, I realized that the 1x1 tile fishnet shapefile I was using to rasterize didn't have projection information in it for
some reason, so maybe it was very hard to match with the database features.
Anyhow, those are both things to investigate in case I do try to get this script working in the future once again.
"""


"""
This script rasterizes four attributes of the Spatial Database of Planted Trees v2 (SDPT v2) geodatabase
into 10x10 degree tiles:
1) aboveground carbon removal factors (AGC RF),
2) AGC RF standard deviations,
3) general plantation type (1: oil palm, 2: wood fiber, 3: other),
4) planted forest establishment year.

Unlike other carbon model scripts, this one requires prep work be done outside the script
(the two large, commented chunks below this).

Chunk 1: Steps for getting the SDPT gdb into PostGIS for rasterization.
This must be done any time the script is tested or run.
The reason for importing the SDPT to PostGIS is that it simply merges all the SDPT country feature classes into a
single table, which makes rasterization easier.
Chunk 2: Steps for creating a 1x1 degree fishnet shapefile covering all countries that have SDPT in them.
This must be done any time the SDPT is revised in a way that changes the countries included in the SDPT.
So, it's not part of the regular workflow for SDPT rasterization.

After Chunk 1 is completed (ingesting SDPT into PostGIS), this script goes through two steps to create 10x10
SDPT property tiles:
1. Create 1x1 deg SDPT property tiles by iterating across the 1x1 fishnet that covers SDPT countries
2. Create 10x10 deg SDPT property tiles by iterating across the supplied tile_id_list

NOTE: 10N_010E is a good tile to test this script on because it includes CMR, which has a few planted forests in it,
so it processes fairly quickly. Note that setting -l in the command line doesn't limit the area used in the first step;
testing the first step (1x1 rasterization) is best done by commenting the code chunk that sets df = df.iloc[[XYZ]].

NOTE: If the column names in the SDPT gdb related to the properties attributes rasterized here change,
they must be modified in Chunk 1 below and in plantation_preparation.create_1x1_plantation_from_1x1_gadm.

python -m data_prep.mp_plantation_preparation -l 10N_010E -nu
python -m data_prep.mp_plantation_preparation -l all
"""

"""
Chunk 1: Ingesting SDPT into PostGIS
Before running this script, the SDPT gdb must be converted into a PostGIS table. That's more easily done as a series
of commands than as a script. Below are the instructions for creating a single PostGIS table of all plantations.
This assumes that the plantation gdb has one feature class for each country with plantations (and the EU can be in a combined feature class).
PostGIS column names below need to match the corresponding SDPT gdb attributes.
This can be done in a flux model Docker container locally (for testing) or in ec2 (for a full run). 

# Start a r5d.24xlarge ec2 instance (or some other size r5d machine for ec2 testing)
# Because this is a long process where there aren't really good checkpoints, it's best to use an on-demand ec2 
# instance instead of a spot instance. This would be set up in the AWS console using template
# https://us-east-1.console.aws.amazon.com/ec2/home?region=us-east-1#LaunchTemplateDetails:launchTemplateId=lt-00205de607ab6d4d9,
# version 30 (on-demand instance). 

# Change directory to where data are kept in the docker
cd /usr/local/tiles/

# Copy zipped plantation gdb with growth rate field in tables
aws s3 cp s3://gfw-files/plantations/SDPT_2.0/sdpt_v2.0_v08182023.gdb.zip .

# I tried unzipping the gdb with sdpt_v2.0_v07102023.gdb.zip but got an error about extra bytes at the beginning.
# I followed the solution at https://unix.stackexchange.com/a/115831 to fix the zipped gdb before unzipping.
zip -FFv  sdpt_v2.0_v08182023.gdb.zip --out sdpt_v2.0_v08182023.gdb.fixed.zip

# Unzip the gdb (1.5 minutes)
time unzip sdpt_v2.0_v08182023.gdb.fixed.zip

# Start the postgres service and check that it has actually started
service postgresql restart
pg_lsclusters

# Create a postgres database called ubuntu. 
createdb ubuntu

# Enter the postgres database called ubuntu and add the postgis exension to it
psql
CREATE EXTENSION postgis;
\q

# Add the feature class of one country's plantations to PostGIS. This creates the "all_plant" table for other countries to be appended to.
# Using ogr2ogr requires the PG connection info but entering the PostGIS shell (psql) doesn't.
# The fields in the SELECT statement need to change if the column names in the gdb attribute tables are changed. 
ogr2ogr -f Postgresql PG:"dbname=ubuntu" sdpt_v2.0_v08182023.gdb -progress -nln all_plant --config PG_USE_COPY YES -sql "SELECT growth, simpleName, growSDerror FROM cmr_plant_v2"

# Enter PostGIS and check that the table is there and that it has the correct fields with reasonable values.
psql
\d+ all_plant;
SELECT * FROM all_plant LIMIT 2;   # To see what the first two rows look like
SELECT COUNT (*) FROM all_plant;   # Should be 7777 for CMR in plantations v2.0

# Delete all rows from the table so that it is now empty
DELETE FROM all_plant;
\q

# Get a list of all feature classes (countries) in the geodatabase and save it as a txt
ogrinfo sdpt_v2.0_v08182023.gdb | cut -d: -f2 | cut -d'(' -f1 | grep plant | grep -v Open | sed -e 's/ //g' > out.txt

# Make sure all the country tables are listed in the txt, then exit it
more out.txt
q

# Run a loop in bash that iterates through all the gdb feature classes and imports them to the all_plant PostGIS table.
# I think it's okay that there's a message "Warning 1: organizePolygons() received a polygon with more than 100 parts. The processing may be really slow.  You can skip the processing by setting METHOD=SKIP, or only make it analyze counter-clock wise parts by setting METHOD=ONLY_CCW if you can assume that the outline of holes is counter-clock wise defined"
# It just seems to mean that the processing is slow, but the processing methods haven't changed. 
# KOR is very slowly; it paused at the last increment for about 30 minutes but did actually finish. 
# Overall, this took about 75 minutes on SDPT v2. IDN is the last feature class to be imported and it hung at
# the very last progress marker for a long time. 
# However, I knew importing was continuing because htop showed an ogr2ogr process running. 
time while read p; do echo $p; ogr2ogr -f Postgresql PG:"dbname=ubuntu" sdpt_v2.0_v08182023.gdb -nln all_plant --config PG_USE_COPY YES -progress -append -sql "SELECT growth, simpleName, growSDerror FROM $p"; done < out.txt

psql
SELECT COUNT (*) FROM all_plant;
\q
# 25,917,174 features in SDPT v2 gdb

# Create a spatial index of the plantation table to speed up the intersections with 1x1 degree tiles
# This doesn't work for v2.0 in the postgres/postgis in Docker but it does work for v2.0 in r4 instances outside Docker.
# When I try to do this in Docker, I get "column 'shape' does not exist", even though it clearly shows up in the table.
# When I do this on v2.0 in an r4 instance, swapping wkb_geometry for shape, it works fine and adds another index.
# However, I'm not sure that adding this index is necessary;
# tables in both r4 and Docker already have an index "all_plant_Shape_geom_idx" gist ("Shape") according to \d+.  
# And when I do what is described at https://gis.stackexchange.com/questions/241599/finding-postgis-tables-that-are-missing-indexes,
# no tables are shown as missing indexes. 
psql
CREATE INDEX IF NOT EXISTS all_plant_index ON all_plant using gist(shape);

# Adds a new column to the table and stores the plantation type reclassified as 1 (palm), 2 (wood fiber), or 3 (other)
# The SDPT planted forest category field name must be changed below to match the gdb. 
ALTER TABLE all_plant ADD COLUMN type_reclass SMALLINT;
# Based on https://stackoverflow.com/questions/15766102/i-want-to-use-case-statement-to-update-some-records-in-sql-server-2005
UPDATE all_plant SET type_reclass = (CASE WHEN simpleName = 'Oil palm' THEN 1 WHEN simpleName = 'Oil palm mix' THEN 1 WHEN simpleName = 'Wood fiber or timber' THEN 2 WHEN simpleName = 'Wood fiber or timber mix' THEN 2 ELSE 3 END);
\q
# Takes about 10 minutes to reclassify type_reclass

"""

"""
Chunk 2: Preparing 1x1 degree fishnet shapefile for countries that have SDPT
Did these steps locally 8/18/23.
They are a combination of ArcMap and command line in the Docker container. 
It's a mishmash but for a task that rarely needs to occur (changing the countries in the SDPT), it's fine to not script it.
The 1x1 degree shapefile is what's actually used for the script below; the PostGIS parts in this chunk are just a simple
way to select the relevant countries from the GADM iso shapefile compared to using ArcMap but all of this could be 
done in ArcMap. 

# Made 1x1 deg global fishnet in ArcMap 
arcpy.CreateFishnet_management(out_feature_class="C:/GIS/Carbon_model/test_tiles/docker_output/1x1_deg_fishnet_global__20230818.shp", origin_coord="-180 -90", y_axis_coord="-180 -80", cell_width="1", cell_height="1", number_rows="", number_columns="", corner_coord="180 90", labels="NO_LABELS", template="-180 -90 180 90", geometry_type="POLYGON")

# Set up Postgres in Docker container (as in Chunk 1)
service postgresql restart
pg_lsclusters
createdb ubuntu
psql
CREATE EXTENSION postgis;
\q

# Imported GADM iso to postgres in Docker container command line
ogr2ogr -f Postgresql PG:"dbname=ubuntu" gadm_3_6_by_iso.shp -progress -nln gadm_3_6_iso_final -nlt PROMOTE_TO_MULTI;

# Imported 1x1 fishnet to postgres in Docker container command line
ogr2ogr -f Postgresql PG:"dbname=ubuntu" 1x1_deg_fishnet_global__20230818.shp -progress -nln fishnet_1x1_deg -s_srs EPSG:4326 -t_srs EPSG:4326;

psql

# Select all the GADM features that have plantations in them according to cn.SDPT_v2_iso_codes and make a new table in Docker container command line
# Replace cn.SDPT_v2_iso_codes with the actual list of codes in constants_and_names.
# This list differs from cn.SDPT_v2_feature_classes because this one lists all the countries contained in EU, 
# plus some other countries that may have SDPT but aren't actually in the EU (mostly in Europe, though). 
CREATE TABLE gadm_sdpt_v2 AS (SELECT * FROM gadm_3_6_iso_final WHERE iso IN (cn.SDPT_v2_iso_codes));
# e.g., CREATE TABLE gadm_sdpt_v2 AS (SELECT * FROM gadm_3_6_iso_final WHERE iso IN ('AGO', 'ARG', 'ARM', 'AUS', 'AZE'...));
\q
# 157 countries selected for SDPT v2

# Export the countries that have SDPT v2 to a shapefile (just for QC/reference) in Docker container command line. Takes a few minutes, pauses at a few places. 
ogr2ogr -f "ESRI Shapefile" gadm_3_6_by_iso__with_SDPT_v2__20230821.shp PG:"dbname=ubuntu" gadm_sdpt_v2 -progress

# Select all the 1x1 degree cells that intersect GADM with SDPT v2 and make a new table in ArcMap
arcpy.SelectLayerByLocation_management(in_layer="1x1_deg_fishnet_global__20230818", overlap_type="INTERSECT", select_features="gadm_3_6_by_iso__with_SDPT_v2__20230821", search_distance="", selection_type="NEW_SELECTION", invert_spatial_relationship="NOT_INVERT")
# Then saved the selection to fishnet_1x1_deg_SDPTv2_extent__20230821.shp in ArcMap (17,127 features)

# Add bounding coordinate attributes to shapefile of 1x1 boxes that are within SDPT v2 countries in ArcMap
arcpy.AddGeometryAttributes_management(Input_Features="1x1_deg_fishnet_SDPTv2_extent__20230821", Geometry_Properties="EXTENT", Length_Unit="", Area_Unit="", Coordinate_System="")

# Add field that will store NW corner of each feature in ArcMap
arcpy.AddField_management(in_table="fishnet_1x1_deg_SDPTv2_extent__20230821", field_name="NW_corner", field_type="TEXT", field_precision="", field_scale="", field_length="", field_alias="", field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED", field_domain="")

# Populate field with coordinate of NW corner of 1x1 cells that overlap with SDPT v2 countries in ArcMap
arcpy.CalculateField_management(in_table="fishnet_1x1_deg_SDPTv2_extent__20230821", field="NW_corner", expression="str(!EXT_MAX_Y!) + '_' + str(!EXT_MIN_X!)", expression_type="PYTHON_9.3", code_block="")

# Ultimately, the list of northwest corners of the 1x1 cells is what's used in the first stage of the script below. 
"""

from multiprocessing.pool import Pool
from functools import partial
from simpledbf import Dbf5
import glob
import datetime
import argparse
import os
import sys

import constants_and_names as cn
import universal_util as uu
from data_prep.planted_forests_prep import plantation_preparation


def mp_plantation_preparation(tile_id_list):

    os.chdir(cn.docker_tile_dir)

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # List of all possible 10x10 Hansen tiles except for those at very extreme latitudes (not just WHRC biomass tiles)
        tile_id_list = uu.tile_list_s3(cn.pixel_area_dir)

    uu.print_log(tile_id_list)
    uu.print_log(f'There are {str(len(tile_id_list))} tiles to process', "\n")


    # List of output directories and output file name patterns
    output_dir_list = [cn.annual_gain_AGC_BGC_planted_forest_dir,
                       cn.stdev_annual_gain_AGC_BGC_planted_forest_dir,
                       cn.planted_forest_type_dir,
                       cn.planted_forest_estab_year_dir]

    output_pattern_list = [cn.pattern_annual_gain_AGC_BGC_planted_forest,
                           cn.pattern_stdev_annual_gain_AGC_BGC_planted_forest,
                           cn.pattern_planted_forest_type,
                           cn.pattern_planted_forest_estab_year]

    # If the model run isn't the standard one, the output directory and file names are changed
    if cn.SENSIT_TYPE != 'std':
        uu.print_log('Changing output directory and file name pattern based on sensitivity analysis')
        output_dir_list = uu.alter_dirs(cn.SENSIT_TYPE, output_dir_list)
        output_pattern_list = uu.alter_patterns(cn.SENSIT_TYPE, output_pattern_list)

    # A date can optionally be provided by the full model script or a run of this script.
    # This replaces the date in constants_and_names.
    # Only done if output upload is enabled.
    if cn.RUN_DATE is not None and cn.NO_UPLOAD is False:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, cn.RUN_DATE)


    ### Step 1: Creation of 1x1 degree planted forest property tiles

    # Downloads and unzips the GADM shapefile, which will be used to create 1x1 tiles of land areas
    uu.s3_file_download(os.path.join(cn.plantations_dir, f'{cn.pattern_gadm_1x1_index}.zip'), cn.docker_tile_dir, 'std')
    if not os.path.exists(f'{cn.pattern_gadm_1x1_index}.zip'):
        cmd = ['unzip', f'{cn.pattern_gadm_1x1_index}.zip']
        uu.log_subprocess_output_full(cmd)

    # Gets the attribute table of the country extent 1x1 tile shapefile
    gadm = glob.glob(f'{cn.pattern_gadm_1x1_index}*.dbf')[0]

    # Converts the attribute table to a dataframe
    dbf = Dbf5(gadm)
    df = dbf.to_dataframe()

    # To select one cell to test methods on
    df = df.iloc[[1513]]
    uu.print_log('Testing on ', df)

    # Converts the column of the dataframe with the names of the tiles (which contain their coordinates) to a list
    gadm_list_1x1 = df['NW_corner'].tolist()
    gadm_list_1x1 = [str(y) for y in gadm_list_1x1]
    # uu.print_log("List of 1x1 degree tiles in countries that have planted forests, with defining coordinate in the northwest corner:", gadm_list_1x1)
    uu.print_log("There are", len(gadm_list_1x1), "1x1 country extent tiles to iterate through.")

    # Creates 1x1 degree tiles of plantation attributes wherever there are plantations
    # by iterating through all 1x1 tiles that intersect with countries that have planted forests
    if cn.SINGLE_PROCESSOR:
        for tile in gadm_list_1x1:
            plantation_preparation.create_1x1_plantation_from_1x1_gadm(tile)
    else:
        processes = 35 #40 processors=730 GB peak
        uu.print_log('Create 1x1 plantation attributes from 1x1 gadm max processors=', processes)
        pool = Pool(processes)
        pool.map(plantation_preparation.create_1x1_plantation_from_1x1_gadm, gadm_list_1x1)
        pool.close()
        pool.join()


    ### Step 2: Creation of 10x10 degree planted forest property tiles
    ### from 1x1 degree planted forest property tiles

    # Creates a mosaic of all the 1x1 plantation removal factor tiles
    plant_RF_1x1_vrt = 'plant_RF_1x1.vrt'
    uu.print_log('Creating vrt of 1x1 planted forest removal factor tiles')
    os.system(f'gdalbuildvrt {plant_RF_1x1_vrt} *{cn.pattern_annual_gain_AGC_BGC_planted_forest}.tif')

    # Creates a mosaic of all the 1x1 plantation removals factor standard deviation tiles
    plant_stdev_1x1_vrt = 'plant_stdev_1x1.vrt'
    uu.print_log('Creating vrt of 1x1 planted forest removal factor standard deviation tiles')
    os.system(f'gdalbuildvrt {plant_stdev_1x1_vrt} *{cn.pattern_stdev_annual_gain_AGC_BGC_planted_forest}.tif')

    # Creates a mosaic of all the 1x1 planted forest type tiles
    plant_type_1x1_vrt = 'plant_type_1x1.vrt'
    uu.print_log('Creating vrt of 1x1 planted forest type tiles')
    os.system(f'gdalbuildvrt {plant_type_1x1_vrt} *{cn.pattern_planted_forest_type}.tif')

    # Creates a mosaic of all the 1x1 planted forest establishment year tiles
    plant_estab_year_1x1_vrt = 'plant_estab_year_1x1.vrt'
    uu.print_log('Creating vrt of 1x1 planted forest establishment year tiles')
    os.system(f'gdalbuildvrt {plant_estab_year_1x1_vrt} *{cn.pattern_planted_forest_estab_year}.tif')

    # Creates 10x10 degree tiles of plantation attributes iterating over the set of tiles supplied
    # at the start of the script that are in latitudes with planted forests.
    if cn.SINGLE_PROCESSOR:
        for tile_id in tile_id_list:
            plantation_preparation.create_10x10_plantation_tiles(tile_id, plant_RF_1x1_vrt, plant_stdev_1x1_vrt,
                                                                 plant_type_1x1_vrt, plant_estab_year_1x1_vrt)
    else:
        processes = 40
        uu.print_log('Create 10x10 plantation attributes max processors=', processes)
        pool = Pool(processes)
        pool.map(partial(plantation_preparation.create_10x10_plantation_tiles,
                         plant_RF_1x1_vrt=plant_RF_1x1_vrt,
                         plant_stdev_1x1_vrt=plant_stdev_1x1_vrt,
                         plant_type_1x1_vrt=plant_type_1x1_vrt,
                         plant_estab_year_1x1_vrt_1x1_vrt=plant_estab_year_1x1_vrt),
                 tile_id_list)
        pool.close()
        pool.join()

    # If cn.NO_UPLOAD flag is not activated (by choice or by lack of AWS credentials), output is uploaded
    if not cn.NO_UPLOAD:
        for output_dir, output_pattern in zip(output_dir_list, output_pattern_list):
            uu.upload_final_set(output_dir, output_pattern)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Create planted forest carbon removals rate tiles')
    parser.add_argument('--tile_id_list', '-l', required=True,
                        help='List of tile ids to use in the model. Should be of form 00N_110E or 00N_110E,00N_120E or all.')
    parser.add_argument('--run-date', '-d', required=False,
                        help='Date of run. Must be format YYYYMMDD.')
    parser.add_argument('--no-upload', '-nu', action='store_true',
                       help='Disables uploading of outputs to s3')
    parser.add_argument('--single-processor', '-sp', action='store_true',
                       help='Uses single processing rather than multiprocessing')
    args = parser.parse_args()

    # Sets global variables to the command line arguments
    cn.RUN_DATE = args.run_date
    cn.NO_UPLOAD = args.no_upload
    cn.SINGLE_PROCESSOR = args.single_processor

    tile_id_list = args.tile_id_list

    # Disables upload to s3 if no AWS credentials are found in environment
    if not uu.check_aws_creds():
        no_upload = True

    # Create the output log
    uu.initiate_log(tile_id_list)

    # Checks whether the sensitivity analysis and tile_id_list arguments are valid
    tile_id_list = uu.tile_id_list_check(tile_id_list)

    mp_plantation_preparation(tile_id_list)
