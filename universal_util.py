import subprocess
import constants_and_names
import datetime
import os
import re
import pandas as pd

# Prints the date as YYYYmmdd
d = datetime.datetime.today()
date = d.strftime('%Y%m%d')

# Creates a list of all the biomass tiles (WHRC non-mangrove and mangrove)
def read_biomass_tile_list():

    file_list = []

    with open('{}{}'.format(constants_and_names.biomass_tile_list_dir, constants_and_names.pattern_biomass_tile_list), 'r') as tiles:
        for tile in tiles:
            file_list.append(tile)

    return file_list

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

            # Only tifs will be in the tile list
            if '.tif' in tile_name:

                # For stripping down standard tree biomass tiles to the tile id
                if '{}.tif'.format(constants_and_names.pattern_natrl_forest_biomass_2000) in tile_name:

                    tile_short_name = tile_name.replace('_{}.tif'.format(constants_and_names.pattern_natrl_forest_biomass_2000), '')
                    file_list.append(tile_short_name)

                # For stripping down mangrove biomass tiles to the tile id
                if constants_and_names.pattern_mangrove_biomass_2000 in tile_name:

                    tile_short_name = tile_name.replace('{}_'.format(constants_and_names.pattern_mangrove_biomass_2000), '')
                    tile_short_name = tile_short_name.replace('.tif', '')
                    file_list.append(tile_short_name)
                    file_list = file_list[0:]

                # For stripping down pixel area tiles to the tile id
                if constants_and_names.pattern_pixel_area in tile_name:

                    tile_short_name = tile_name.replace('{}_'.format(constants_and_names.pattern_pixel_area), '')
                    tile_short_name = tile_short_name.replace('.tif', '')
                    file_list.append(tile_short_name)
                    file_list = file_list[0:]

    return file_list


# Lists the tiles on the spot machine
def tile_list_spot_machine(source):

    ## For an s3 folder in a bucket using AWSCLI
    # Captures the list of the files in the folder
    out = subprocess.Popen(['ls', source], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout, stderr = out.communicate()

    # Writes the output string to a text file for easier interpretation
    biomass_tiles = open("tiles.txt", "w")
    biomass_tiles.write(stdout)
    biomass_tiles.close()

    file_list = []

    # Iterates through the text file to get the names of the tiles and appends them to list
    with open("tiles.txt", 'r') as tile:
        for line in tile:
            num = len(line.strip('\n').split(" "))
            tile_name = line.strip('\n').split(" ")[num - 1]

            # Only tifs will be in the tile list
            if '.tif' in tile_name:

                file_list.append(tile_name)

    return file_list

# Creates a list of all biomass 2000 tiles-- those from WHRC and those only in the mangrove set
def create_combined_biomass_tile_list(WHRC, mangrove):

    print "Making a combined biomass tile list..."

    out = subprocess.Popen(['aws', 's3', 'ls', WHRC], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout, stderr = out.communicate()
    # Writes the output string to a text file for easier interpretation
    biomass_tiles = open("natrl_forest_biomass_tiles.txt", "w")
    biomass_tiles.write(stdout)
    biomass_tiles.close()

    out = subprocess.Popen(['aws', 's3', 'ls', mangrove], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
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

                tile_id = get_tile_id(tile_name)
                file_list_natrl.append(tile_id)

    # Iterates through the mangrove biomass text file to get the names of the tiles and appends them to list
    with open("mangrove_biomass_tiles.txt", 'r') as tile:

        for line in tile:
            num = len(line.strip('\n').split(" "))
            tile_name = line.strip('\n').split(" ")[num - 1]

            # Only tifs will be in the tile list
            if '.tif' in tile_name:

                tile_id = get_tile_id(tile_name)
                file_list_natrl.append(tile_id)

    # Combines Woods Hole and mangrove biomass tile lists
    all_tiles = file_list_natrl + file_list_mangrove

    # Tile list with tiles found in both lists removed, so only the unique tiles remain
    unique_tiles = list(set(all_tiles))
    print "  There are {} unique tiles with biomass.".format(len(unique_tiles))

    # Converts the set to a pandas dataframe to put the tiles in the correct order
    df = pd.DataFrame(unique_tiles, columns=['tile_id'])
    df = df.sort_values(by=['tile_id'])
    print "Tile list is:", df

    # Converts the pandas dataframe to a Python list so that it can be written to a txt
    unique_tiles_ordered_list = df.tile_id.tolist()

    # Writes the biomass tile list to a txt
    with open(constants_and_names.pattern_biomass_tile_list, 'w') as f:
        for item in unique_tiles_ordered_list:
            f.write("%s, " % item)

    # Copies that list to s3
    cmd = ['aws', 's3', 'cp', constants_and_names.pattern_biomass_tile_list, '{0}{1}'.format(constants_and_names.biomass_tile_list_dir, constants_and_names.pattern_biomass_tile_list)]
    subprocess.check_call(cmd)

    os.remove("natrl_forest_biomass_tiles.txt")
    os.remove("mangrove_biomass_tiles.txt")

    return unique_tiles_ordered_list


# Gets the tile id from the full tile name using a regular expression
def get_tile_id(tile_name):

    # based on https://stackoverflow.com/questions/20003025/find-1-letter-and-2-numbers-using-regex and https://docs.python.org/3.4/howto/regex.html
    tile_id = re.search("[0-9]{2}[A-Z][_][0-9]{3}[A-Z]", tile_name).group()

    return tile_id


# Counts the number of tiles in a folder in s3
def count_tiles(source):

    ## For an s3 folder in a bucket using AWSCLI
    # Captures the list of the files in the folder
    out = subprocess.Popen(['aws', 's3', 'ls', source], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout, stderr = out.communicate()

    # Writes the output string to a text file for easier interpretation
    tile_file = open("tiles.txt", "w")
    tile_file.write(stdout)
    tile_file.close()

    i=0
    with open(tile_file) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

# Gets the bounding coordinates of a tile
def coords(tile_id):
    NS = tile_id.split("_")[0][-1:]
    EW = tile_id.split("_")[1][-1:]

    if NS == 'S':
        ymax =-1*int(tile_id.split("_")[0][:2])
    else:
        ymax = int(str(tile_id.split("_")[0][:2]))

    if EW == 'W':
        xmin = -1*int(str(tile_id.split("_")[1][:3]))
    else:
        xmin = int(str(tile_id.split("_")[1][:3]))


    ymin = str(int(ymax) - 10)
    xmax = str(int(xmin) + 10)

    return str(xmin), str(ymin), str(xmax), str(ymax)


def s3_folder_download(source, dest):
    cmd = ['aws', 's3', 'cp', source, dest, '--recursive', '--exclude', '*', '--include', '*.tif']
    subprocess.check_call(cmd)


def s3_file_download(source, dest):
    cmd = ['aws', 's3', 'cp', source, dest]
    subprocess.check_call(cmd)

# Uploads tile to specified location
def upload_final(upload_dir, tile_id, pattern):

    file = '{}_{}.tif'.format(tile_id, pattern)

    print "Uploading {}".format(file)
    cmd = ['aws', 's3', 'cp', file, upload_dir]

    try:
        subprocess.check_call(cmd)
    except:
        print "Error uploading output tile"