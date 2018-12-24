import subprocess
import constants_and_names
import datetime
import re

# Prints the date as YYYYmmdd
d = datetime.datetime.today()
date = d.strftime('%Y%m%d')

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
                if '_biomass.tif' in tile_name:

                    tile_short_name = tile_name.replace('_biomass.tif', '')
                    file_list.append(tile_short_name)

                # For stripping down mangrove biomass tiles to the tile id
                if constants_and_names.pattern_mangrove_biomass in tile_name:

                    tile_short_name = tile_name.replace('{}_'.format(constants_and_names.pattern_mangrove_biomass), '')
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

# Gets the tile id from the full tile name
def get_tile_id(tile_name):

    # For getting tile id of biomass tiles
    if '_t_aboveground_biomass_ha_2000.tif' in tile_name:
        tile_id = tile_name[8:]

    # For getting tile id of all other tiles
    else:
        tile_short_name = tile_name.replace('.tif', '')
        tile_id = tile_short_name[-8:]

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

    return ymax, xmin, ymin, xmax


def s3_folder_download(source, dest):
    cmd = ['aws', 's3', 'cp', source, dest, '--recursive', '--exclude', '*', '--include', '*.tif']
    subprocess.check_call(cmd)


def s3_file_download(source, dest):
    cmd = ['aws', 's3', 'cp', source, dest]
    subprocess.check_call(cmd)