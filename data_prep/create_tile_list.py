import subprocess
import pandas as pd
import numpy as np
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
stdout2, stderr2 = out.communicate()
# Writes the output string to a text file for easier interpretation
biomass_tiles = open("mangrove_biomass_tiles.txt", "w")
biomass_tiles.write(stdout2)
biomass_tiles.close()

# Empty lists for filling with biomass tile ids
file_list_natrl = []
file_list_mangrove = []

# Iterates through the Woods Hole biomass text file to get the names of the tiles and appends them to list
with open("natrl_forest_biomass_tiles.txt", 'r') as tile:

    for line in tile:
        num = len(line.strip('\n').split(" "))
        tile_name = line.strip('\n').split(" ")[num - 1]

        # Only tifs will be in the tile list
        if '.tif' in tile_name:

            tile_short_name = tile_name[:8]
            file_list_natrl.append(tile_short_name)

# Iterates through the mangrove biomass text file to get the names of the tiles and appends them to list
with open("mangrove_biomass_tiles.txt", 'r') as tile:

    for line in tile:
        num = len(line.strip('\n').split(" "))
        tile_name = line.strip('\n').split(" ")[num - 1]

        # Only tifs will be in the tile list
        if '.tif' in tile_name:

            tile_short_name = tile_name[:8]
            file_list_mangrove.append(tile_short_name)

# Combines Woods Hole and mangrove biomass tile lists
all_tiles = file_list_natrl + file_list_mangrove

# Tile list with tiles found in both lists removed
unique_tiles = list(set(all_tiles))
print "There are {} unique tiles with biomass.".format(len(unique_tiles))
print unique_tiles

df = pd.DataFrame(unique_tiles, columns=['tile_id'])
df = df.sort_values(by=['tile_id'])
print df

df.to_csv(constants_and_names.pattern_biomass_tile_list, header=False, index=False, mode='w')

cmd = ['aws', 's3', 'cp', constants_and_names.pattern_biomass_tile_list, '{0}{1}'.format(constants_and_names.biomass_tile_list_dir, constants_and_names.pattern_biomass_tile_list)]
subprocess.check_call(cmd)