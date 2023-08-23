#TODO Add year planted from Du et al. update as another output column. Change readme accordingly. 

"""
Code for rasterizing three properties of the Spatial Database of Planted Trees v2 (SDPT v2) geodatabase
into 10x10 degree tiles:
1) aboveground carbon removal factors (AGC RF),
2)AGC RF standard deviations,
3) and general plantation type (1: oil palm, 2: wood fiber, 3: other).
Unlike other carbon model scripts, this one requires prep work done outside of the script
(the large, commented chunk below this).
Once that prep work of converting the gdb to a PostGIS table has been done, there are a few entry points to this script,
also unlike other carbon model scripts.
Which entry point is chosen depends on what has changed in the SDPT since it was last rasterized here.
## NOTE: The entry point for the script is indicated by supplying two arguments following the python call.
The entry points rely on two shapefiles of the indexes (outlines) of 1x1 tiles created during plantation processing:
1. Shapefile of 1x1 tiles of the countries with planted forests (basically, countries with planted forests rasterized to 1x1 deg),
2. Shapefile of 1x1 tiles of planted forest extent (basically, planted forest extent rasterized to 1x1 deg).
These shapefiles should have been created during the previous run of the processing script and be on s3.

First entry point: Script runs from the beginning. Do this if the planted forest database now includes countries
that were not in it during the previous planted forest growth rate processing. It will take several days to run.
## NOTE: This also requires updating the list of countries with planted forests in constants_and_names.plantation_countries.
This entry point is accessed by supplying None to both argument:
python -m planted_forests_prep.mp_plantation_preparation -l 10N_010E -gi None -pi None

Second entry point: Script uses existing index shapefile of 1x1 tiles of countries with planted forests. Use this entry point
if no countries have been added to the planted forest database but some other spatial aspect of the data has changed
since the last processing, e.g., newly added planted forests in countries already in the database or the boundaries
of existing features have been altered. This entry point will use the supplied index shapefile of the 1x1 tiles of
countries with planted forests to create new 1x1 planted forest growth rate tiles. This entry point is accessed by
providing the s3 location of the index shapefile of the 1x1 country tiles,
e.g., python -m planted_forests_prep.mp_plantation_preparation -l 10N_010E -gi s3://gfw2-data/climate/carbon_model/gadm_plantation_1x1_tile_index/gadm_index_1x1_20190108.shp -pi None

Third entry point: Script uses existing index shapefile of 1x1 tiles of planted forest extent to create new 1x1 tiles
of planted forest growth rates. Use this entry point if the spatial properties of the database haven't changed but
the growth rates or forest type have. This route will iterate through only the 1x1 tiles that had planted forests previously and
create new planted forest growth rate tiles for them.
This entry point is accessed by providing the s3 location of the index shapefile of the 1x1
planted forest extent tiles,
e.g., python -m planted_forests_prep.mp_plantation_preparation -l 10N_010E -gi None -pi s3://gfw2-data/climate/carbon_model/gadm_plantation_1x1_tile_index/plantation_index_1x1_20190813.shp
e.g., python -m planted_forests_prep.mp_plantation_preparation -l 10N_010E -gi s3://gfw2-data/climate/carbon_model/gadm_plantation_1x1_tile_index/gadm_index_1x1_20190108.shp -pi s3://gfw2-data/climate/carbon_model/gadm_plantation_1x1_tile_index/plantation_index_1x1_20190813.shp

All entry points conclude with creating 10x10 degree tiles of the three outputs from 1x1 tiles for SDPT v2 extent.

To run this for just a part of the world, create a new shapefile of 1x1 GADM or plantation tile boundaries (making sure that they
extend to 10x10 degree tiles (that is, the area being processed must match 10x10 degree tiles) and use that as a
command line argument. Supply the rest of the global tiles (the unchanged tiles) from the previous model run.
"""

"""
Before running this script, the SDPT gdb must be converted into a PostGIS table. That's more easily done as a series
of commands than as a script. Below are the instructions for creating a single PostGIS table of all plantations.
This assumes that the plantation gdb has one feature class for each country with plantations.
Some commands will need to be changed if the gdb column names related to removal factors change.

# Start a r5d.24xlarge spot machine
spotutil new r5d.24xlarge dgibbs_wri

# Only on r5d instances: Change directory to where the data are kept in the docker
cd /usr/local/tiles/

# Copy zipped plantation gdb with growth rate field in tables
aws s3 cp s3://gfw-files/plantations/SDPT_2.0/sdpt_v2.0_v08182023.gdb.zip .

# I tried unzipping the gdb with sdpt_v2.0_v07102023.gdb.zip but got an error about extra bytes at the beginning.
# I followed the solution at https://unix.stackexchange.com/a/115831 to fix the zipped gdb before unzipping.
zip -FFv  sdpt_v2.0_v08182023.gdb.zip --out sdpt_v2.0_v08182023.gdb.fixed.zip

# Unzip the gdb (1.5 minutes)
time unzip sdpt_v2.0_v08182023.gdb.fixed.zip

# Only on r5d instances: Start the postgres service and check that it has actually started
service postgresql restart
pg_lsclusters

# Only on r5d instances: Create a postgres database called ubuntu. 
# I tried adding RUN createdb ubuntu to the Dockerfile after RUN service postgresql restart
# but got the error: 
# createdb: could not connect to database template1: could not connect to server: No such file or directory.
#         Is the server running locally and accepting
# So I'm adding that step here.
createdb ubuntu

# Only on r5d instances: Enter the postgres database called ubuntu and add the postgis exension to it
psql
CREATE EXTENSION postgis;
ALTER USER postgres PASSWORD 'password';
\q


##### NOTE: I HAVEN'T ACTUALLY CHECKED THAT THE BELOW PROCEDURES REALLY RESULT IN A TABLE THAT CAN BE RASTERIZED CORRECTLY.
##### I JUST CHECKED THAT ROWS WERE BEING ADDED TO THE TABLE


# Add the feature class of one country's plantations to PostGIS. This creates the "all_plant" table for other countries to be appended to.
# Using ogr2ogr requires the PG connection info but entering the PostGIS shell (psql) doesn't.
ogr2ogr -f Postgresql PG:"dbname=ubuntu" sdpt_v2.0_v08182023.gdb -progress -nln all_plant --config PG_USE_COPY YES -sql "SELECT growth, simpleName, growSDerror FROM cmr_plant_v2"

# Enter PostGIS and check that the table is there and that it has only the growth field.
psql
\d+ all_plant;
SELECT * FROM all_plant LIMIT 2;   # To see what the first two rows look like
SELECT COUNT (*) FROM all_plant;   # Should be 7777 for CMR in plantations v2.0

# Delete all rows from the table so that it is now empty
DELETE FROM all_plant;

# Exit the PostGIS shell
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
# Overall, this took about 75 minutes. 
time while read p; do echo $p; ogr2ogr -f Postgresql PG:"dbname=ubuntu" sdpt_v2.0_v08182023.gdb -nln all_plant --config PG_USE_COPY YES -progress -append -sql "SELECT growth, simpleName, growSDerror FROM $p"; done < out.txt

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
ALTER TABLE all_plant ADD COLUMN type_reclass SMALLINT;
# Based on https://stackoverflow.com/questions/15766102/i-want-to-use-case-statement-to-update-some-records-in-sql-server-2005
UPDATE all_plant SET type_reclass = ( CASE WHEN simpleName = 'Oil Palm ' then '1' when simpleName = 'Oil Palm Mix ' then '1' when simpleName = 'Oil Palm ' then '1' when simpleName = 'Oil Palm Mix' then 1 when simpleName = 'Wood fiber / timber' then 2 when simpleName = 'Wood fiber / timber ' then 2 ELSE '3' END );

# Exit Postgres shell
\q

"""

"""
Did these steps locally 8/18/23:
# Made 1x1 deg global fishnet: 
arcpy.CreateFishnet_management(out_feature_class="C:/GIS/Carbon_model/test_tiles/docker_output/1x1_deg_fishnet_global__20230818.shp", origin_coord="-180 -90", y_axis_coord="-180 -80", cell_width="1", cell_height="1", number_rows="", number_columns="", corner_coord="180 90", labels="NO_LABELS", template="-180 -90 180 90", geometry_type="POLYGON")

# Imported GADM iso to postgres
ogr2ogr -f Postgresql PG:"dbname=ubuntu" gadm_3_6_by_iso.shp -progress -nln gadm_3_6_iso_final -nlt PROMOTE_TO_MULTI;

# Imported 1x1 fishnet to postgres
ogr2ogr -f Postgresql PG:"dbname=ubuntu" 1x1_deg_fishnet_global__20230818.shp -progress -nln fishnet_1x1_deg -s_srs EPSG:4326 -t_srs EPSG:4326;

psql

# Select all the GADM features that have plantations in them according to cn.SDPT_v2_iso_codes and make a new table
CREATE TABLE gadm_sdpt_v2 AS (SELECT * FROM gadm_3_6_iso_final WHERE iso IN (cn.SDPT_v2_iso_codes));
\q
# 157 countries selected

# Export the countries that have SDPT v2 to a shapefile (just for QC/reference). Takes a few minutes, pauses at a few places. 
ogr2ogr -f "ESRI Shapefile" gadm_3_6_by_iso__with_SDPT_v2__20230821.shp PG:"dbname=ubuntu" gadm_sdpt_v2 -progress

# Select all the 1x1 degree cells that intersect GADM with SDPT v2 and make a new table
# The PostGIS command took hours to run, so it's not worth doing, but copied here for reference:
# CREATE TABLE fishnet_1x1_deg__intersect_iso_with_SDPTv2 AS (SELECT fishnet_1x1_deg.id FROM gadm_sdpt_v2, fishnet_1x1_deg WHERE ST_Intersects(gadm_sdpt_v2.wkb_geometry, fishnet_1x1_deg.wkb_geometry));
arcpy.SelectLayerByLocation_management(in_layer="1x1_deg_fishnet_global__20230818", overlap_type="INTERSECT", select_features="gadm_3_6_by_iso__with_SDPT_v2__20230821", search_distance="", selection_type="NEW_SELECTION", invert_spatial_relationship="NOT_INVERT")
# Then saved the selection to fishnet_1x1_deg_SDPTv2_extent__20230821.shp (17,127 features)

# This only created a dbf, not a full shapefile. Leaving here for reference. 
# ogr2ogr -f "ESRI Shapefile" fishnet_1x1_deg__intersect_iso_with_SDPTv2.shp PG:"dbname=ubuntu" fishnet_1x1_deg__intersect_iso_with_SDPTv2 -progress

# Add bounding coordinate attributes to shapefile of 1x1 boxes that are within SDPT v2 countries
arcpy.AddGeometryAttributes_management(Input_Features="1x1_deg_fishnet_SDPTv2_extent__20230821", Geometry_Properties="EXTENT", Length_Unit="", Area_Unit="", Coordinate_System="")

# Add field that will store NW corner of each feature
arcpy.AddField_management(in_table="fishnet_1x1_deg_SDPTv2_extent__20230821", field_name="NW_corner", field_type="TEXT", field_precision="", field_scale="", field_length="", field_alias="", field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED", field_domain="")

# Add field with coordinate of NW corner of 1x1 cells that overlap with SDPT v2 countries
arcpy.CalculateField_management(in_table="fishnet_1x1_deg_SDPTv2_extent__20230821", field="NW_corner", expression="str(!EXT_MAX_Y!) + '_' + str(!EXT_MIN_X!)", expression_type="PYTHON_9.3", code_block="")
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
from data_prep import plantation_preparation


def mp_plantation_preparation(tile_id_list):

    os.chdir(cn.docker_tile_dir)

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # List of all possible 10x10 Hansen tiles except for those at very extreme latitudes (not just WHRC biomass tiles)
        total_tile_id_list = uu.tile_list_s3(cn.pixel_area_dir)

        # Removes the latitude bands that don't have any planted forests in them according to Liz Goldman.
        # i.e., Liz Goldman said by Slack on 1/2/19 that the nothernmost planted forest is 69.5146 and the southernmost is -46.938968.
        # This creates a more focused list of 10x10 tiles to iterate through (removes ones that definitely don't have planted forest).
        # NOTE: If the planted forest gdb is updated, the list of latitudes to exclude below may need to be changed to not exclude certain latitude bands.
        planted_lat_tile_id_list = [tile for tile in total_tile_id_list if '90N' not in tile]
        planted_lat_tile_id_list = [tile for tile in planted_lat_tile_id_list if '80N' not in tile]
        planted_lat_tile_id_list = [tile for tile in planted_lat_tile_id_list if '50S' not in tile]
        planted_lat_tile_id_list = [tile for tile in planted_lat_tile_id_list if '60S' not in tile]
        planted_lat_tile_id_list = [tile for tile in planted_lat_tile_id_list if '70S' not in tile]
        planted_lat_tile_id_list = [tile for tile in planted_lat_tile_id_list if '80S' not in tile]
        uu.print_log(planted_lat_tile_id_list)
    else:
        planted_lat_tile_id_list = tile_id_list


    # List of output directories and output file name patterns
    output_dir_list = [cn.annual_gain_AGC_planted_forest_dir,
                       cn.stdev_annual_gain_AGC_planted_forest_dir,
                       cn.planted_forest_type_dir,
                       cn.planted_forest_estab_year_dir]

    output_pattern_list = [cn.pattern_annual_gain_AGC_planted_forest,
                           cn.pattern_stdev_annual_gain_AGC_planted_forest,
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


    uu.print_log(planted_lat_tile_id_list)
    uu.print_log(f'There are {str(len(planted_lat_tile_id_list))} tiles to process', "\n")

    # Downloads and unzips the GADM shapefile, which will be used to create 1x1 tiles of land areas
    uu.s3_file_download(os.path.join(cn.plantations_dir, f'{cn.pattern_gadm_1x1_index}.zip'), cn.docker_tile_dir, 'std')
    cmd = ['unzip', f'{cn.pattern_gadm_1x1_index}.shp']
    uu.log_subprocess_output_full(cmd)

    # Gets the attribute table of the country extent 1x1 tile shapefile
    gadm = glob.glob(f'{cn.pattern_gadm_1x1_index}*.dbf')[0]

    # Converts the attribute table to a dataframe
    dbf = Dbf5(gadm)
    df = dbf.to_dataframe()

    # To select one cell to test methods on
    df = df.iloc[[4031]]
    print(df)

    # Converts the column of the dataframe with the names of the tiles (which contain their coordinates) to a list
    gadm_list_1x1 = df['NW_corner'].tolist()
    gadm_list_1x1 = [str(y) for y in gadm_list_1x1]
    uu.print_log("List of 1x1 degree tiles in countries that have planted forests, with defining coordinate in the northwest corner:", gadm_list_1x1)
    uu.print_log("There are", len(gadm_list_1x1), "1x1 country extent tiles to iterate through.")

    # Creates 1x1 degree tiles of plantation properties wherever there are plantations
    # by iterating through all 1x1 tiles that intersect with countries that have planted forests
    if cn.SINGLE_PROCESSOR:
        for tile in gadm_list_1x1:
            plantation_preparation.create_1x1_plantation_from_1x1_gadm(tile)
    else:
        processes = 48
        uu.print_log('Create 1x1 plantation properties from 1x1 gadm max processors=', processes)
        pool = Pool(processes)
        pool.map(plantation_preparation.create_1x1_plantation_from_1x1_gadm, gadm_list_1x1)
        pool.close()
        pool.join()


    os.quit()

    ### Creation of 10x10 degree planted forest removals rate and rtpe tiles
    ### from 1x1 degree planted forest removals rate and type tiles

    # Creates a mosaic of all the 1x1 plantation removals rate tiles
    plant_RF_1x1_vrt = 'plant_RF_1x1.vrt'
    uu.print_log("Creating vrt of 1x1 plantation removals factor tiles")
    os.system(f'gdalbuildvrt {plant_RF_1x1_vrt} plant_gain_*.tif')

    # Creates a mosaic of all the 1x1 plantation removals rate standard deviation tiles
    plant_stdev_1x1_vrt = 'plant_stdev_1x1.vrt'
    uu.print_log("Creating vrt of 1x1 plantation removals rate standard deviation tiles")
    os.system(f'gdalbuildvrt {plant_stdev_1x1_vrt} plant_stdev_*.tif')

    # Creates a mosaic of all the 1x1 plantation type tiles
    plant_type_1x1_vrt = 'plant_type_1x1.vrt'
    uu.print_log("Creating vrt of 1x1 plantation type tiles")
    os.system(f'gdalbuildvrt {plant_type_1x1_vrt} plant_type_*.tif')


    # Creates a mosaic of all the 1x1 plantation removals rate standard deviation tiles
    plant_estab_year_1x1_vrt = 'plant_estab_year_1x1.vrt'
    uu.print_log("Creating vrt of 1x1 plantation removals rate standard deviation tiles")
    os.system(f'gdalbuildvrt {plant_estab_year_1x1_vrt} plant_estab_year_*.tif')

    # Creates 10x10 degree tiles of plantation properties iterating over the set of tiles supplied
    # at the start of the script that are in latitudes with planted forests.
    if cn.SINGLE_PROCESSOR:
        for tile_id in planted_lat_tile_id_list:
            plantation_preparation.create_10x10_plantation_tiles(tile_id, plant_RF_1x1_vrt, plant_stdev_1x1_vrt,
                                                                 plant_type_1x1_vrt, plant_estab_year_1x1_vrt)
    else:
        processes = 20
        uu.print_log('Create 10x10 plantation properties max processors=', processes)
        pool = Pool(processes)
        pool.map(partial(plantation_preparation.create_10x10_plantation_tiles,
                         plant_gain_1x1_vrt=plant_RF_1x1_vrt,
                         plant_stdev_1x1_vrt=plant_stdev_1x1_vrt,
                         plant_type_1x1_vrt=plant_type_1x1_vrt,
                         plant_estab_year_1x1_vrt_1x1_vrt=plant_estab_year_1x1_vrt),
                 planted_lat_tile_id_list)
        pool.close()
        pool.join()

    # If no_upload flag is not activated (by choice or by lack of AWS credentials), output is uploaded
    if not cn.NO_UPLOAD:
        uu.upload_final_set(output_dir_list[0], output_pattern_list[0])


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
