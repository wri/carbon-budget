# # Charlie's general directions for how to join the plantation growth rate table to the plantation geodatabase and
# # make it into 10x10 tiles. From a Slack conversation with Charlie and Liz on 11/7/18.
# so if it were me, i would do something like this
# ```1. start giant spot
# 2. ogr2ogr GDB --> postGIS database to append everything
# 3. add Liz's growth rate lookup table, join to the global plantations table
# 4. use gdal_rasterize to make your 10x10 tiles```
#
# ## Getting geodatabase into a single PostGIS table
# # Copy zipped plantation gdb with growth rate field in tables
# aws s3 cp s3://gfw-files/plantations/final/global/plantations_final_attributes.gdb.zip
#
# # Unzip the zipped plantation gdb
# unzip plantations_final_attributes.gdb.zip
#
# # Add the feature class of one country's plantations to PostGIS. This creates the "all_plant" table for other countries to be appended to.
# # Using ogr2ogr requires the PG connection info but entering PostGIS (psql) doesn't.
# ogr2ogr -f Postgresql PG:"dbname=ubuntu" plantations_final_attributes.gdb -progress -nln all_plant -sql "SELECT growth FROM cmr_plant"
#
# # Enter PostGIS and check that the table is there.
# psql \d+ all_plant;
#
# # Get a list of all feature classes in the geodatabase and save it as a txt
# ogrinfo plantations_final_attributes.gdb | cut -d: -f2 | cut -d'(' -f1 | grep plant | grep -v cmr | grep -v Open | sed -e 's/ //g' > out.txt
#
# # Make sure all the country tables are listed in the txt
# more out.txt
#
# # Run a loop in bash that iterates through all the gdb feature classes and imports them to the all_plant PostGIS table
# while read p; do echo $p; ogr2ogr -f Postgresql PG:"dbname=ubuntu" plantations_final_attributes.gdb -nln all_plant -progress -append -sql "SELECT * FROM $p"; done < out.txt
#
# # To check how many rows are in the table of all plantations
# SELECT COUNT(*) FROM all_plant;
#
# # To convert the PostGIS table to a geojson
# ogr2ogr -f GeoJSON plantations.geojson PG:"dbname=ubuntu" -sql "SELECT * FROM all_plant"



# Charlie used Pandas on the spot machine to remove the unnecessary columns from the growth table. I don’t have steps for that. Saved the csv with just final_id and growth to filtered.csv
#
# # Export growth rate table from Pandas to Postgres
# >>> import pandas as pd
# >>> df = pd.read_csv('filtered.csv')
# >>> from sqlalchemy import create_engine
# >>> engine = create_engine('postgresql://ubuntu@localhost')
# >>> df.to_sql('filtered', engine)
#
# # I’m not sure this is actually the right join to use because of the column issues when I tried doing this before.
# # Need to look up various other SQL joins and check the results of this and others using psql \d+ growth;
# CREATE TABLE growth AS SELECT all_plant.wkb_geometry, filtered.final_id, filtered.growth FROM all_plant LEFT OUTER JOIN filtered ON all_plant.final_id = filtered.final_id;
#
# # Check that join is correct
# psql \d+ growth;
# SELECT COUNT(*) FROM growth;   # Should be the same number of rows as in all_plant
#
# ### General code tip: Reading a table from Postgres into Pandas
# >>> from sqlalchemy import create_engine                                                                                                                              │·····················
# >>> engine = create_engine('postgresql://ubuntu@localhost')                                                                                                           │·····················
# >>> all_plant = pd.read_sql(con=engine, 'SELECT final_id FROM all_plant')
# growth = pd.read_sql('SELECT final_id FROM growth', con=engine)


##### Getting plantation tiles from plantation GeoJSON

# Copy GeoJSON to s3
# aws s3 cp s3://gfw-files/plantations/final/global/geojson/final_plantations.geojson .

# Import GeoJSON to PostGIS. Gets "JSON parsing error: continue (at offset 1183876445)", among other errors
# ogr2ogr -f "PostgreSQL" PG:"dbname=ubuntu" "final_plantations.geojson" -nln plantations -append -progress -sql "SELECT growth FROM final_plantations"

# This should be able to create a tile out of a geojson but it gets the same error as the above about "JSON parsing error: continue (at offset 1183876445)"
# gdal_rasterize -tr 0.00025 0.00025 -co COMPRESS=LZW final_plantations.geojson test.tif -te -60 -30 -57 -27 -a growth

# ogr2ogr -f GeoJSON subset.geojson final_plantations.geojson -sql "SELECT growth FROM OGRGeoJSON WHERE growth>15"