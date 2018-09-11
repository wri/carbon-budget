import utilities
import subprocess

def create_continent_ecozone_tiles(tile_id):

    output_dir = 's3://gfw2-data/climate/carbon_model/fao_ecozones/ecozone/'

    print "get extent of biomass tile"
    print tile_id
    xmin, ymin, xmax, ymax = utilities.coords(tile_id)

    print "rasterizing ecozone"
    rasterized_eco_zone_tile = utilities.rasterize('fao_ecozones_fra_2000_continents_assigned_dissolved_FINAL_20180906.shp',
                                              "{}_fao_ecozones_fra_2000_continents_assigned_dissolved_FINAL_20180906.tif".format(tile_id),
                                              xmin, ymin, xmax, ymax, '.008', 'Byte', 'recode', '0')

    utilities.upload_final(output_dir, rasterized_eco_zone_tile)





