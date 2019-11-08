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

# Gets the tile id from the full tile name using a regular expression
def get_tile_id(tile_name):

    # based on https://stackoverflow.com/questions/20003025/find-1-letter-and-2-numbers-using-regex and https://docs.python.org/3.4/howto/regex.html
    tile_id = re.search("[0-9]{2}[A-Z][_][0-9]{3}[A-Z]", tile_name).group()

    return tile_id


# Gets the tile id from the full tile name using a regular expression
def get_tile_type(tile_name):

    tile_type = tile_name[9:-4]

    return tile_type


# Gets the tile id from the full tile name using a regular expression
def get_tile_name(tile):

    tile_name = os.path.split(tile)[1]

    return tile_name


# Gets the directory of the tile
def get_tile_dir(tile):

    tile_dir = os.path.split(tile)[0]

    return tile_dir


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

# Downloads all tiles in an s3 folder
# Source=source file on s3
# dest=where to download onto spot machine
# sensit_type = whether the model is standard or a sensitivity analysis model run
# use_sensit = shows whether to actually replace the standard path with the sensitivity analysis path
def s3_folder_download(source, dest, sensit_type, sensit_use):

    # Changes the path to download from based on the sensitivity analysis being run and whether that particular input
    # has a sensitivity analysis path on s3
    if sensit_type != 'std' and 'standard' in source and sensit_use == 'true':

        print "Changing {} name to reflect sensitivity analysis".format(source)

        source = source.replace('standard', sensit_type)

    cmd = ['aws', 's3', 'cp', source, dest, '--recursive', '--exclude', '*tiled/*',
           '--exclude', '*geojason', '--exclude', '*vrt', '--exclude', '*csv']
    subprocess.check_call(cmd)
    print '\n'


# Downloads individual tiles
# Source=source file on s3
# dest=where to download onto spot machine
# sensit_type = whether the model is standard or a sensitivity analysis model run
# use_sensit = shows whether to actually replace the standard path with the sensitivity analysis path
def s3_file_download(source, dest, sensit_type, sensit_use):

    # Retrieves the name of the tile from the full path name
    dir = get_tile_dir(source)
    file_name = get_tile_name(source)

    # Changes the file to download based on the sensitivity analysis being run and whether that particular input
    # has a sensitivity analysis path on s3
    if sensit_type != 'std' and 'standard' in dir and sensit_use == 'true':

        dir = dir.replace('standard', sensit_type)
        file_name = file_name[:-4] + '_' + sensit_type + '.tif'
        print "Changing {0} name to reflect sensitivity analysis to {1}/{2}".format(source, dir, file_name)

    # Doesn't download the tile if it's already on the spot machine
    if os.path.exists(file_name):
        print file_name, "already downloaded" + "\n"

    # Tries to download the tile if it's not on the spot machine
    else:
        try:
            source = os.path.join(dir, file_name)
            cmd = ['aws', 's3', 'cp', source, dest]
            subprocess.check_call(cmd)
        except:
            print source, "not found."

# General download utility. Can download individual tiles or entire folders depending on how many are in the input list
def s3_flexible_download(source_dir, pattern, dest, sensit_type, sensit_use, tile_id_list):

    # For downloading test tiles (five or fewer)
    if len(tile_id_list) <= 5:

        # Creates a full download name (path and file)
        for tile_id in tile_id_list:
            if pattern == '':
                source = '{0}{1}.tif'.format(source_dir, tile_id)
            elif pattern == cn.pattern_gain:
                source = '{0}{1}_{2}.tif'.format(source_dir, pattern, tile_id)
            else:
                source = '{0}{1}_{2}.tif'.format(source_dir, tile_id, pattern)

            s3_file_download(source, dest, sensit_type, sensit_use)

    # For downloading full sets of tiles
    else:
        s3_folder_download(source_dir, dest, sensit_type, sensit_use)


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
        if os.path.exists('{0}{1}.tif'.format(folder, tile_id)):
            print "Hansen loss tile exists for {}.".format(tile_id)
            cmd = ['gdal_merge.py', '-createonly', '-init', '0', '-co', 'COMPRESS=LZW', '-ot', 'Byte',
                   '-o', '{0}{1}_{2}.tif'.format(folder, tile_id, pattern),
                   '{0}{1}.tif'.format(folder, tile_id)]
            subprocess.check_call(cmd)

        # If the Hansen loss tile isn't already on the spot machine
        else:

            # If the Hansen tile isn't already downloaded, it downloads the Hansen tile
            try:
                s3_file_download('{0}{1}.tif'.format(cn.loss_dir, tile_id),
                                 '{0}{1}_{2}.tif'.format(folder, tile_id, 'empty_tile_template'), 'std', 'false')
                print "Downloaded Hansen loss tile for", tile_id

            # If there is no Hansen tile, it downloads the pixel area tile instead
            except:

                s3_file_download('{0}{1}_{2}.tif'.format(cn.pixel_area_dir, cn.pattern_pixel_area, tile_id),
                                 '{0}{1}_{2}.tif'.format(folder, tile_id, 'empty_tile_template'), 'std', 'false')
                print "Downloaded pixel area tile for", tile_id

            # Uses either the Hansen loss tile or pixel area tile as a template tile
            cmd = ['gdal_merge.py', '-createonly', '-init', '0', '-co', 'COMPRESS=LZW', '-ot', 'Byte',
                   '-o', '{0}{1}_{2}.tif'.format(folder, tile_id, pattern),
                   '{0}{1}_{2}.tif'.format(folder, tile_id, 'empty_tile_template')]
            subprocess.check_call(cmd)

            print "Created raster of all 0s for", file

        # # If there's no Hansen loss tile, it uses a pixel area tile as the template for the blank plantation tile
        # else:
        #     print "No Hansen tile for {}. Using pixel area tile instead.".format(tile_id)
        #
        #     s3_file_download('{0}{1}_{2}.tif'.format(cn.pixel_area_dir, cn.pattern_pixel_area, tile_id),
        #                      '{0}{1}_{2}.tif'.format(folder, cn.pattern_pixel_area, tile_id))
        #
        #     cmd = ['gdal_merge.py', '-createonly', '-init', '0', '-co', 'COMPRESS=LZW', '-ot', 'Byte',
        #            '-o', '{0}{1}_{2}.tif'.format(folder, tile_id, pattern),
        #            '{0}{1}_{2}.tif'.format(folder, cn.pattern_pixel_area, tile_id)]
        #     subprocess.check_call(cmd)

        print "Created raster of all 0s for", file


# Reformats the patterns for the 10x10 degree model output tiles for the aggregated output names
def name_aggregated_output(pattern, thresh):

    out_pattern = re.sub('ha_', '', pattern)
    print out_pattern
    out_pattern = re.sub('2001_15', 'per_year', out_pattern)
    print out_pattern
    out_pattern = re.sub('gross_emis_year', 'gross_emis_per_year', out_pattern)
    print out_pattern
    date = datetime.datetime.now()
    date_formatted = date.strftime("%Y_%m_%d")

    out_name = '{0}_10km_tcd{1}_modelv1_1_2_biomass_soil_{2}'.format(out_pattern, thresh, date_formatted)

    return out_name


# Removes plantations that existed before 2000 from loss tile
def mask_pre_2000_plantation(pre_2000_plant, tile_to_mask, out_name, tile_id):

    print pre_2000_plant
    print tile_to_mask
    print out_name
    print tile_id

    if os.path.exists(pre_2000_plant):

        print "Pre-2000 plantation exists for {}. Cutting out pixels in those plantations...".format(tile_id)

        # In order to mask out the pre-2000 plantation pixels from the loss raster, the pre-2000 plantations need to
        # become a vrt. I couldn't get gdal_calc to work while keeping pre-2000 plantations as a raster; it wasn't
        # recognizing the 0s (nodata).
        # Based on https://gis.stackexchange.com/questions/238397/how-to-indicate-nodata-into-gdal-calc-formula
        # Only the pre-2000 plantation raster needed to be converted to a vrt; the loss raster did not.
        cmd = ['gdal_translate', '-of', 'VRT', pre_2000_plant,
               '{0}_{1}.vrt'.format(tile_id, cn.pattern_plant_pre_2000), '-a_nodata', 'none']
        subprocess.check_call(cmd)

        # Removes the pre-2000 plantation pixels from the loss tile
        pre_2000_vrt = '{0}_{1}.vrt'.format(tile_id, cn.pattern_plant_pre_2000)
        calc = '--calc=A*(B==0)'
        loss_outfilearg = '--outfile={}'.format(out_name)
        cmd = ['gdal_calc.py', '-A', tile_to_mask, '-B', pre_2000_vrt,
               calc, loss_outfilearg, '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW']
        subprocess.check_call(cmd)

    else:

        print "No pre-2000 plantation exists for {}. Renaming tile...".format(tile_id)

        os.rename(tile_to_mask, out_name)

    print "  Pre-2000 plantations for {} complete".format(tile_id)


# Checks whether the provided sensitivity analysis type is valid
def check_sensit_type(sensit_type):

    # Checks the validity of the two arguments. If either one is invalid, the script ends.
    if (sensit_type not in cn.sensitivity_list):
        raise Exception('Invalid model type. Please provide a model type from {}.'.format(cn.sensitivity_list))
    else:
        pass


# Changes the name of the input or output directory according to the sensitivity analysis
def alter_dirs(sensit_type, raw_dir_list):

    print "Raw output directory list:", raw_dir_list

    processed_dir_list = [d.replace('standard', sensit_type) for d in raw_dir_list]

    print "Processed output directory list:", processed_dir_list, "\n"
    return processed_dir_list


# Alters the file patterns in a list according to the sensitivity analysis
def alter_patterns(sensit_type, raw_pattern_list):

    print "Raw output pattern list:", raw_pattern_list

    processed_pattern_list = [(d + '_' + sensit_type) for d in raw_pattern_list]

    print "Processed output pattern list:", processed_pattern_list, "\n"
    return processed_pattern_list


# Creates the correct input tile name for processing based on the sensitivity analysis being done
def sensit_tile_rename(sensit_type, tile_id, raw_pattern, use_sensit):

    # If the analysis is not the standard model and the input should be renamed
    # i.e. even in sensitivity analyses, sometimes inputs should keep their standard names
    if sensit_type != 'std' and use_sensit == 'true':
        processed_name = '{0}_{1}_{2}.tif'.format(tile_id, raw_pattern, sensit_type)

    else:
        # For all tiles besides loss
        if len(raw_pattern) > 4:
            processed_name = '{0}_{1}.tif'.format(tile_id, raw_pattern)
            # print processed_pattern
        # For loss tiles, which have no pattern and never have a sensitivity type
        else:
            processed_name = '{}.tif'.format(tile_id)
            # print processed_pattern

    return processed_name


