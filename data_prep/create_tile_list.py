import subprocess
import sys
sys.path.append('../')
import constants_and_names
import universal_util


print "Making a fileof Woods Hole biomass 2000 tiles"
out = subprocess.Popen(['aws', 's3', 'ls', constants_and_names.natrl_forest_biomass_2000_dir], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
stdout, stderr = out.communicate()
# Writes the output string to a text file for easier interpretation
biomass_tiles = open("natrl_forest_biomass_tiles.txt", "w")
biomass_tiles.write(stdout)
biomass_tiles.close()

print "Making a file of mangrove biomass 2000 tiles"
out = subprocess.Popen(['aws', 's3', 'ls', constants_and_names.mangrove_biomass_2000_dir], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
stdout, stderr = out.communicate()
# Writes the output string to a text file for easier interpretation
biomass_tiles = open("mangrove_biomass_tiles.txt", "w")
biomass_tiles.write(stdout)
biomass_tiles.close()

# Empty lists for filling with biomass tile ids
file_list_natrl = []
file_list_mangrove = []

# Iterates through the text file to get the names of the tiles and appends them to list
with open("natrl_forest_biomass_tiles.txt", 'r') as tile:

    for line in tile:
        num = len(line.strip('\n').split(" "))
        tile_name = line.strip('\n').split(" ")[num - 1]

        # Only tifs will be in the tile list
        if '.tif' in tile_name:

            tile_short_name = tile_name[8:]
            file_list_natrl.append(tile_short_name)

# Iterates through the text file to get the names of the tiles and appends them to list
with open("mangrove_biomass_tiles.txt", 'r') as tile:

    for line in tile:
        num = len(line.strip('\n').split(" "))
        tile_name = line.strip('\n').split(" ")[num - 1]

        # Only tifs will be in the tile list
        if '.tif' in tile_name:

            tile_short_name = tile_name[8:]
            file_list_mangrove.append(tile_short_name)

all_tiles = file_list_natrl + file_list_mangrove
print all_tiles

unique_tiles = set(all_tiles)
print unique_tiles
