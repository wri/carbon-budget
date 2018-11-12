
# # Charlie's general directions for how to join the plantation growth rate table to the plantation geodatabase and
# # make it into 10x10 tiles. From a Slack conversation with Liz Goldman on 11/7/18.
# so if it were me, i would do something like this
# ```1. start giant spot
# 2. ogr2ogr GDB --> postGIS database to append everything
# 3. add Liz's growth rate lookup table, join to the global plantations table
# 4. use gdal_rasterize to make your 10x10 tiles```

### Getting geodatabase into a single PostGIS table
# # Copy zipped plantation gdb and growth rate table to spot machine
# aws s3 cp s3://gfw2-data/climate/carbon_model/plantations/raw/ . --recursive
#
# # Unzip the zipped plantation gdb
# unzip plantations_merged_single.gdb.zip
#
# # Add the feature class of one country's plantations to PostGIS. This creates the "all_plant" table for other countries to be appended to.
# # Using ogr2ogr requires the PG connection info but entering PostGIS (psql) doesn't.
# ogr2ogr -f Postgresql PG:"dbname=ubuntu" plantations_merged_single.gdb -progress -nln all_plant -sql "SELECT final_id FROM cmr_plant"
#
# # Enter PostGIS and check that the table is there.
# psql \d+ all_plant;
#
# # Get a list of all feature classes in the geodatabase and save it as a txt
# ogrinfo plantations_merged_single.gdb | cut -d: -f2 | cut -d'(' -f1 | grep plant | grep -v cmr | grep -v Open | sed -e 's/ //g' > out.txt
#
# # Make sure the tables are listed in the txt
# more out.txt
#
# # Run a loop in bash that iterates through all the gdb feature classes and imports them to the all_plant PostGIS table
# while read p; do echo $p; ogr2ogr -f Postgresql PG:"dbname=ubuntu" plantations_merged_single.gdb -nln all_plant -progress -append -sql "SELECT final_id FROM $p"; done < out.txt
#
# # To check how many rows are in the table of all plantations
# SELECT COUNT(*) FROM all_plant;
#
# Charlie used Pandas to remove the unnecessary columns from the growth table. Saved the csv with just final_id and growth to filtered.csv
#
# # Export growth rate table from Pandas to Postgres
# >>> import pandas as pd
# >>> df = pd.read_csv('filtered.csv')
# >>> from sqlalchemy import create_engine
# >>> engine = create_engine('postgresql://ubuntu@localhost')
# >>> df.to_sql('filtered', engine)

# CREATE TABLE growth AS SELECT all_plant.wkb_geometry, filtered.final_id, filtered.growth FROM all_plant LEFT OUTER JOIN filtered ON all_plant.final_id = filtered.final_id;

# # Reading a table from Postgres into Pandas
# >>> from sqlalchemy import create_engine                                                                                                                              │·····················
# >>> engine = create_engine('postgresql://ubuntu@localhost')                                                                                                           │·····················
# >>> all_plant = pd.read_sql(con=engine, 'SELECT final_id FROM all_plant')
# growth = pd.read_sql('SELECT final_id FROM growth', con=engine)


