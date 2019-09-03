import subprocess
import glob
import constants_and_names as cn
import datetime
import os
from shutil import copy
import re
import pandas as pd
from osgeo import gdal

# Prints the date as YYYYmmdd_hhmmss
d = datetime.datetime.today()
date_today = d.strftime('%Y%m%d_%h%m%s')

# Calculates beloground biomass from aboveground biomass using the main equation from Mokany et al. 2006, Table 2
def calculate_belowground_biomass(AGB):

    BGB = (AGB ^ 0.89) * 0.489

    return BGB


# Gets the tile id from the full tile name using a regular expression
def get_tile_id(tile_name):

    # based on https://stackoverflow.com/questions/20003025/find-1-letter-and-2-numbers-using-regex and https://docs.python.org/3.4/howto/regex.html
    tile_id = re.search("[0-9]{2}[A-Z][_][0-9]{3}[A-Z]", tile_name).group()

    return tile_id


# Gets the tile id from the full tile name using a regular expression
def get_tile_type(tile_name):

    tile_type = tile_name[9:-4]

    return tile_type


# Creates a list of all the biomass tiles (WHRC non-mangrove and mangrove)
def read_biomass_tile_list():

    file_list = []

    with open('{}{}'.format(cn.biomass_tile_list_dir, cn.pattern_biomass_tile_list), 'r') as tiles:
        for tile in tiles:
            file_list.append(tile)

    return file_list

# Lists the tiles in a folder in s3
def tile_list(source):

    print "Creating list of tiles..."

    ## For an s3 folder in a bucket using AWSCLI
    # Captures the list of the files in the folder
    out = subprocess.Popen(['aws', 's3', 'ls', source], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
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

                tile_id = get_tile_id(tile_name)
                file_list.append(tile_id)

    return file_list


# Lists the tiles on the spot machine
def tile_list_spot_machine(source, pattern):

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

            # Only files with the specified pattern will be in the tile list
            if pattern in tile_name:

                file_list.append(tile_name)

    return file_list

# Creates a list of all tiles found in either two or three s3 folders and removes duplicates from the list
def create_combined_tile_list(set1, set2, set3=None):

    print "Making a combined tile list..."

    out = subprocess.Popen(['aws', 's3', 'ls', set1], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout, stderr = out.communicate()
    # Writes the output string to a text file for easier interpretation
    set1_tiles = open("set1.txt", "w")
    set1_tiles.write(stdout)
    set1_tiles.close()

    out = subprocess.Popen(['aws', 's3', 'ls', set2], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout2, stderr2 = out.communicate()
    # Writes the output string to a text file for easier interpretation
    set2_tiles = open("set2.txt", "w")
    set2_tiles.write(stdout2)
    set2_tiles.close()

    # Empty lists for filling with biomass tile ids
    file_list_set1 = []
    file_list_set2 = []

    # Iterates through the first text file to get the names of the tiles and appends them to list
    with open("set1.txt", 'r') as tile:

        for line in tile:
            num = len(line.strip('\n').split(" "))
            tile_name = line.strip('\n').split(" ")[num - 1]

            # Only tifs will be in the tile list
            if '.tif' in tile_name:

                tile_id = get_tile_id(tile_name)
                file_list_set1.append(tile_id)

    # Iterates through the second text file to get the names of the tiles and appends them to list
    with open("set2.txt", 'r') as tile:

        for line in tile:
            num = len(line.strip('\n').split(" "))
            tile_name = line.strip('\n').split(" ")[num - 1]

            # Only tifs will be in the tile list
            if '.tif' in tile_name:

                tile_id = get_tile_id(tile_name)
                file_list_set2.append(tile_id)

    print "There are {} tiles in {}".format(len(file_list_set1), set1)
    print "There are {} tiles in {}".format(len(file_list_set2), set2)

    # If there's a third folder supplied, iterates through that
    if set3 != None:

        print "Third set of tiles input. Adding to first two sets of tiles..."

        out = subprocess.Popen(['aws', 's3', 'ls', set3], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        stdout3, stderr3 = out.communicate()
        # Writes the output string to a text file for easier interpretation
        set3_tiles = open("set3.txt", "w")
        set3_tiles.write(stdout3)
        set3_tiles.close()

        file_list_set3 = []

        # Iterates through the text file to get the names of the tiles and appends them to list
        with open("set3.txt", 'r') as tile:

            for line in tile:
                num = len(line.strip('\n').split(" "))
                tile_name = line.strip('\n').split(" ")[num - 1]

                # Only tifs will be in the tile list
                if '.tif' in tile_name:
                    tile_id = get_tile_id(tile_name)
                    file_list_set3.append(tile_id)

        print "There are {} tiles in {}".format(len(file_list_set3), set3)

    # Combines both tile lists
    all_tiles = file_list_set1 + file_list_set2

    # If a third directory is supplied, the tiles from that list are added to the list from the first two
    if set3 != None:

        all_tiles = all_tiles + file_list_set3

    # Tile list with tiles found in multiple lists removed, so now duplicates are gone
    unique_tiles = list(set(all_tiles))

    # Converts the set to a pandas dataframe to put the tiles in the correct order
    df = pd.DataFrame(unique_tiles, columns=['tile_id'])
    df = df.sort_values(by=['tile_id'])

    # Converts the pandas dataframe to a Python list
    unique_tiles_ordered_list = df.tile_id.tolist()

    # Removes the text files with the lists of tiles
    set_txt = glob.glob("set*.txt")
    for i in set_txt:
        os.remove(i)

    return unique_tiles_ordered_list


# Counts the number of tiles in a folder in s3
def count_tiles_s3(source):

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

    return xmin, ymin, xmax, ymax


def s3_folder_download(source, dest):
    cmd = ['aws', 's3', 'cp', source, dest, '--recursive', '--exclude', '*', '--include', '*.tif']
    subprocess.check_call(cmd)


def s3_file_download(source, dest):
    cmd = ['aws', 's3', 'cp', source, dest]
    subprocess.check_call(cmd)


# Uploads all tiles of a pattern to specified location
def upload_final_set(upload_dir, pattern):

    cmd = ['aws', 's3', 'cp', '.', upload_dir, '--exclude', '*', '--include', '*{}*tif'.format(pattern), '--recursive']

    try:
        subprocess.check_call(cmd)
    except:
        print "Error uploading output tile"


# Uploads tile to specified location
def upload_final(upload_dir, tile_id, pattern):

    file = '{}_{}.tif'.format(tile_id, pattern)

    print "Uploading {}".format(file)
    cmd = ['aws', 's3', 'cp', file, upload_dir]

    try:
        subprocess.check_call(cmd)
    except:
        print "Error uploading output tile"


def check_for_data(out_tile):

    # Source: http://gis.stackexchange.com/questions/90726
    # Opens raster and chooses band to find min, max
    gtif = gdal.Open(out_tile)
    srcband = gtif.GetRasterBand(1)
    stats = srcband.GetStatistics(True, True)
    print "  Tile stats =  Minimum=%.3f, Maximum=%.3f, Mean=%.3f, StdDev=%.3f" % (stats[0], stats[1], stats[2], stats[3])

    return stats


# Prints the number of tiles that have been processed so far
def count_completed_tiles(pattern):

    mypath = os.getcwd()
    completed = len(glob.glob1(mypath, '*{}*'.format(pattern)))

    print "Number of completed or in progress tiles:", completed


# Returns the NoData value of a raster
def get_raster_nodata_value(tile):

    # Source: http://gis.stackexchange.com/questions/90726
    # Opens raster we're getting statistics on
    focus_tile = gdal.Open(tile)

    # Extracts the NoData value for the tile so it can be ignored
    srcband = focus_tile.GetRasterBand(1)
    nodata = srcband.GetNoDataValue()

    return nodata

# Prints information about the tile that was just processed: how long it took and how many tiles have been completed
def end_of_fx_summary(start, tile_id, pattern):

    end = datetime.datetime.now()
    elapsed_time = end-start
    print "Processing time for tile", tile_id, ":", elapsed_time
    count_completed_tiles(pattern)


def warp_to_Hansen(in_file, out_file, xmin, ymin, xmax, ymax, dt):

    cmd = ['gdalwarp', '-t_srs', 'EPSG:4326', '-co', 'COMPRESS=LZW', '-tr', str(cn.Hansen_res), str(cn.Hansen_res), '-tap', '-te',
            str(xmin), str(ymin), str(xmax), str(ymax), '-dstnodata', '0', '-ot', dt, '-overwrite', in_file, out_file]
    subprocess.check_call(cmd)


# Creates a tile of all 0s for any tile passed to it.
# Uses the pixel area tile for information about the tile.
# Based on https://gis.stackexchange.com/questions/220753/how-do-i-create-blank-geotiff-with-same-spatial-properties-as-existing-geotiff
def make_blank_tile(tile_id, pattern, folder):

    file = '{0}{1}_{2}.tif'.format(folder, tile_id, pattern)

    # If there's already a tile, there's no need to create a blank one
    if os.path.exists(file):

        print '{} exists. Not creating a blank tile.'.format(file)

    # If there isn't a tile, a blank one must be created
    else:

        print '{} does not exist. Creating a blank tile.'.format(file)

        # Preferentially uses Hansen loss tile as the template for creating a blank plantation tile
        # (tile extent, resolution, pixel alignment, compression, etc.).
        # If the tile is already on the spot machine, it uses the downloaded tile.
        if os.path.exists('{0}{1}_{2}.tif'.format(folder, tile_id, cn.pattern_loss_pre_2000_plant_masked)):
            print "Hansen loss tile exists for {}.".format(tile_id)
            cmd = ['gdal_merge.py', '-createonly', '-init', '0', '-co', 'COMPRESS=LZW', '-ot', 'Byte',
                   '-o', '{0}{1}_{2}.tif'.format(folder, tile_id, pattern),
                   '{0}{1}_{2}.tif'.format(folder, tile_id, cn.pattern_loss_pre_2000_plant_masked)]
            subprocess.check_call(cmd)

        # If the Hansen tile isn't already downloaded, it downloads the Hansen tile
        if not os.path.exists('{0}{1}_{2}.tif'.format(folder, tile_id, cn.pattern_loss_pre_2000_plant_masked)):

            try:
                s3_file_download('{0}{1}.tif'.format(cn.loss_dir, tile_id),
                                 '{0}{1}_{2}.tif'.format(folder, tile_id, 'empty_tile_template'))
                print "Downloaded Hansen loss tile for", tile_id

            # If there is no Hansen tile, it downloads the pixel area tile instead
            except:

                s3_file_download('{0}{1}_{2}.tif'.format(cn.pixel_area_dir, cn.pattern_pixel_area, tile_id),
                                 '{0}{1}_{2}.tif'.format(folder, tile_id, 'empty_tile_template'))
                print "Downloaded pixel area tile for", tile_id

            # Uses either the Hansen loss tile or pixel area tile as a template tile
            cmd = ['gdal_merge.py', '-createonly', '-init', '0', '-co', 'COMPRESS=LZW', '-ot', 'Byte',
                   '-o', '{0}{1}_{2}.tif'.format(folder, tile_id, pattern),
                   '{0}{1}_{2}.tif'.format(folder, tile_id, 'empty_tile_template')]
            subprocess.check_call(cmd)

            print "Created raster of all 0s for", file

        # If there's no Hansen loss tile, it uses a pixel area tile as the template for the blank plantation tile
        else:
            print "No Hansen tile for {}. Using pixel area tile instead.".format(tile_id)

            s3_file_download('{0}{1}_{2}.tif'.format(cn.pixel_area_dir, cn.pattern_pixel_area, tile_id),
                             '{0}{1}_{2}.tif'.format(folder, cn.pattern_pixel_area, tile_id))

            cmd = ['gdal_merge.py', '-createonly', '-init', '0', '-co', 'COMPRESS=LZW', '-ot', 'Byte',
                   '-o', '{0}{1}_{2}.tif'.format(folder, tile_id, pattern),
                   '{0}{1}_{2}.tif'.format(folder, cn.pattern_pixel_area, tile_id)]
            subprocess.check_call(cmd)

        print "Created raster of all 0s for", file


# Reformats the patterns for the 10x10 degree model output tiles for the aggregated output names
def name_aggregated_output(pattern, thresh):

    out_pattern = re.sub('ha_', '', pattern)
    print out_pattern
    out_pattern = re.sub('2001_15', 'per_year', out_pattern)
    print out_pattern
    out_pattern = re.sub('AGC_BGC_', 'AGCO2_BGCO2_', out_pattern)
    print out_pattern
    date = datetime.datetime.now()
    date_formatted = date.strftime("%Y_%m_%d")

    out_name = '{0}_10km_tcd{1}_modelv1_{2}'.format(out_pattern, thresh, date_formatted)

    return out_name



