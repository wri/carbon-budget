import subprocess
import glob

biomass_to_c = 0.5

# Woods Hole biomass 2000 version 4 tiles
biomass_dir = 's3://gfw2-data/climate/WHRC_biomass/WHRC_V4/Processed/'

# Annual Hansen loss tiles (2001-2015)
loss_dir = 's3://gfw2-data/forest_change/hansen_2015/Loss_tiles/'

# Hansen gain tiles (2001-2012)
gain_dir = 's3://gfw2-data/forest_change/tree_cover_gain/gaindata_2012/'

# Tree cover density 2000 tiles
tcd_dir = 's3://gfw2-data/forest_cover/2000_treecover/'

# Intact forest landscape 2000 tiles
ifl_dir = 's3://gfw2-data/climate/carbon_model/other_emissions_inputs/ifl_2000/'

# Processed FAO ecozone shapefile
cont_ecozone_shp = 'fao_ecozones_fra_2000_continents_assigned_dissolved_FINAL_20180906.zip'

# Directory and names for the continent-ecozone tiles, raw and processed
pattern_cont_eco_raw = 'fao_ecozones_continents_raw'
pattern_cont_eco_processed = 'fao_ecozones_continents_processed'
cont_eco_zip = 's3://gfw2-data/climate/carbon_model/fao_ecozones/fao_ecozones_fra_2000_continents_assigned_dissolved_FINAL_20180906.zip'
cont_eco_raw_dir = 's3://gfw2-data/climate/carbon_model/fao_ecozones/ecozone_continent/20181002/raw/'
cont_eco_dir = 's3://gfw2-data/climate/carbon_model/fao_ecozones/ecozone_continent/20181002/processed/'

# Number of gain year tiles
gain_year_count_dir = 's3://gfw2-data/climate/carbon_model/gain_year_count_natural_forest/20180912/'

# Forest age category tiles
age_cat_dir = 's3://gfw2-data/climate/carbon_model/forest_age_category_natural_forest/20180921/'

# Annual gain rate tiles
annual_gain_dir = 's3://gfw2-data/climate/carbon_model/annual_gain_rate_natural_forest/20181003/'

# Cumulative natural forest gain tiles
cumul_gain_dir = 's3://gfw2-data/climate/carbon_model/cumulative_gain_natural_forest/20181003/'

# Lola Fatoyinbo aboveground mangrove biomass tiles
pattern_mangrove_biomass = 'mangrove_agb_t_ha'
mangrove_biomass_dir = 's3://gfw2-data/climate/carbon_model/mangrove_biomass/processed/20181019/'

# Mangrove aboveground biomass gain rate tiles
pattern_mangrove_annual_gain = 'annual_gain_rate_mangrove'
mangrove_annual_gain_dir = 's3://gfw2-data/climate/carbon_model/annual_gain_rate_mangrove/20181019/'

# Tile statistics output txt file core name
tile_stats = 'tile_stats'

def s3_folder_download(source, dest):
    cmd = ['aws', 's3', 'cp', source, dest, '--recursive', '--exclude', '*', '--include', '*.tif']
    subprocess.check_call(cmd)

def s3_file_download(source, dest):
    cmd = ['aws', 's3', 'cp', source, dest]
    subprocess.check_call(cmd)

# Lists the tiles in a folder in s3
def tile_list(source):

    ## For an s3 folder in a bucket using AWSCLI
    # Captures the list of the files in the folder
    out = subprocess.Popen(['aws', 's3', 'ls', source], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout, stderr = out.communicate()

    # Writes the output string to a text file for easier interpretation
    biomass_tiles = open("biomass_tiles.txt", "w")
    biomass_tiles.write(stdout)
    biomass_tiles.close()

    file_list = []

    # Iterates through the text file to get the names of the tiles and appends them to list
    with open("biomass_tiles.txt", 'r') as tile:
        for line in tile:
            num = len(line.strip('\n').split(" "))
            tile_name = line.strip('\n').split(" ")[num - 1]

            # For stripping down standard tree biomass tiles to the tile id
            if '_biomass.tif' in tile_name:

                tile_short_name = tile_name.replace('_biomass.tif', '')
                file_list = file_list[1:]

            # For stripping down mangrove biomass tiles to the tile id
            if pattern_mangrove_biomass in tile_name:

                tile_short_name = tile_name.replace('{}_'.format(pattern_mangrove_biomass), '')
                tile_short_name = tile_short_name.replace('.tif', '')
                file_list.append(tile_short_name)
                file_list = file_list[0:]

    return file_list



