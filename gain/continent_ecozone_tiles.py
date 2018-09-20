import utilities
import subprocess

def create_continent_ecozone_tiles(tile_id):

    print "Processing:", tile_id

    output_dir = 's3://gfw2-data/climate/carbon_model/fao_ecozones/ecozone/20180912/'
    file_name_base = 'fao_ecozones_continents'

    print "Getting extent of biomass tile"
    ymax, xmin, ymin, xmax = utilities.coords(tile_id)
    print "ymax:", ymax, "; ymin: ", ymin, "; xmax", xmax, "; xmin: ", xmin

    print "Rasterizing ecozone to extent of biomass tile"
    utilities.rasterize('fao_ecozones_fra_2000_continents_assigned_dissolved_FINAL_20180906.shp',
                                              "{0}{1}".format(file_name_base, tile_id),
                                              xmin, ymin, xmax, ymax, '.00025', 'Int16', 'gainEcoCon', '0')

    utilities.upload_final(file_name_base, output_dir, tile_id)





