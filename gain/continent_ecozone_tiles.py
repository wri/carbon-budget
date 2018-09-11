import utilities
import subprocess

def create_continent_ecozone_tiles(tile_id):

    print tile_id

    output_dir = 's3://gfw2-data/climate/carbon_model/fao_ecozones/ecozone/'

    print "Getting extent of biomass tile"
    ymax, xmin, ymin, xmax = utilities.coords(tile_id)
    print "ymax:", ymax, "; xmin: ", xmin, "; ymin: ", ymin, "; xmax", xmax

    print "rasterizing ecozone"
    rasterized_eco_zone_tile = utilities.rasterize('fao_ecozones_fra_2000_continents_assigned_dissolved_FINAL_20180906.shp',
                                              "fao_ecozones_{}.tif".format(tile_id),
                                              xmin, ymin, xmax, ymax, '.008', 'Byte', 'gainEcozon', '0')

    utilities.upload_final('fao_ecozones', output_dir, rasterized_eco_zone_tile)





