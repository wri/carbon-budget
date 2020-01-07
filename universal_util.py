import subprocess
import glob
import constants_and_names as cn
import datetime
import rasterio
import os
import multiprocessing
from multiprocessing.pool import Pool
from functools import partial
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
def tile_list_s3(source, sensit_type='std'):

    print "Creating list of tiles..."

    # Changes the directory to list tiles in if the model run is the biomass_swap or US_removals sensitivity analyses
    # (JPL AGB extent and US extent, respectively)
    if sensit_type == 'std':
        source = source
    elif sensit_type == 'biomass_swap':
        source = cn.JPL_processed_dir
    elif sensit_type == 'US_removals':
        source = cn.US_annual_gain_AGB_natrl_forest_dir
    else:
        source = source.replace('standard', sensit_type)

    print source

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
    tiles = open("tiles.txt", "w")
    tiles.write(stdout)
    tiles.close()

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
def create_combined_tile_list(set1, set2, set3=None, sensit_type='std'):

    print "Making a combined tile list..."

    # Changes the directory to list tiles in if the model run is the biomass_swap or US_removals sensitivity analyses
    # (JPL AGB extent and US extent, respectively).
    # If the sensitivity analysis is biomass_swap or US_removals, there's no need to merge tile lists because the tile
    # list is defined by the extent of the sensitivity analysis.
    if sensit_type == 'biomass_swap':
        source = cn.JPL_processed_dir
        tile_list = tile_list_s3(source, sensit_type='std')
        return tile_list
    if sensit_type == 'US_removals':
        source = cn.US_annual_gain_AGB_natrl_forest_dir
        tile_list = tile_list_s3(source, sensit_type='std')
        return tile_list


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
    tile_list_name = "tiles.txt"
    tile_file = open(tile_list_name, "w")
    tile_file.write(stdout)
    tile_file.close()

    # Counts the number of rows in the csv
    i=0
    with open(tile_list_name) as f:
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


# General download utility. Can download individual tiles or entire folders depending on how many are in the input list
def s3_flexible_download(source_dir, pattern, dest, sensit_type, tile_id_list):

    # For downloading all tiles in a folder when the list of tiles can't be specified
    if tile_id_list == 'all':
        s3_folder_download(source_dir, dest, sensit_type)

    # For downloading test tiles (twenty or fewer). Chose 10 because the US removals sensitivity analysis uses 16 tiles.
    elif len(tile_id_list) <= 20:

        # Creates a full download name (path and file)
        for tile_id in tile_id_list:
            if pattern == '':   # For Hansen loss tiles
                source = '{0}{1}.tif'.format(source_dir, tile_id)
            elif pattern in [cn.pattern_gain, cn.pattern_tcd, cn.pattern_pixel_area]:   # For tiles that do not have the tile_id first
                source = '{0}{1}_{2}.tif'.format(source_dir, pattern, tile_id)
            else:  # For every other type of tile
                source = '{0}{1}_{2}.tif'.format(source_dir, tile_id, pattern)

            s3_file_download(source, dest, sensit_type)

    # For downloading full sets of tiles
    else:
        s3_folder_download(source_dir, dest, sensit_type)


# Downloads all tiles in an s3 folder, adpating to sensitivity analysis type
# Source=source file on s3
# dest=where to download onto spot machine
# sensit_type = whether the model is standard or a sensitivity analysis model run
def s3_folder_download(source, dest, sensit_type):

    # Changes the path to download from based on the sensitivity analysis being run and whether that particular input
    # has a sensitivity analysis path on s3
    if sensit_type != 'std':

        # Creates the appropriate path for getting sensitivity analysis tiles
        source_sens = source.replace('standard', sensit_type)

        print "Attempting to change name {0} to {1} to reflect sensitivity analysis".format(source, source_sens)

        # Counts how many tiles are in that s3 folder
        count = count_tiles_s3(source_sens)

        # If there appears to be a full set of tiles in the sensitivity analysis folder (7 is semi arbitrary),
        # the sensitivity folder is downloaded
        if count > 7:

            print "Source directory used:", source_sens

            cmd = ['aws', 's3', 'cp', source_sens, dest, '--recursive', '--exclude', '*tiled/*',
                   '--exclude', '*geojason', '--exclude', '*vrt', '--exclude', '*csv']
            subprocess.check_call(cmd)
            print '\n'

        # If there are fewer than 7 files in the sensitivity folder (i.e., either folder doesn't exist or it just has
        # a few test tiles), the standard folder is downloaded.
        # This can happen despite it being a sensitivity run because this input file type doesn't have a sensitivity version
        # for this date.
        else:

            print "Source directory used:", source

            cmd = ['aws', 's3', 'cp', source, dest, '--recursive', '--exclude', '*tiled/*',
                   '--exclude', '*geojason', '--exclude', '*vrt', '--exclude', '*csv']
            subprocess.check_call(cmd)
            print '\n'

    # For the standard model, the standard folder is downloaded.
    else:

        cmd = ['aws', 's3', 'cp', source, dest, '--recursive', '--exclude', '*tiled/*',
               '--exclude', '*geojason', '--exclude', '*vrt', '--exclude', '*csv']
        subprocess.check_call(cmd)
        print '\n'


# Downloads individual tiles from s3
# Source=source file on s3
# dest=where to download onto spot machine
# sensit_type = whether the model is standard or a sensitivity analysis model run
def s3_file_download(source, dest, sensit_type):

    # Retrieves the s3 directory and name of the tile from the full path name
    dir = get_tile_dir(source)
    file_name = get_tile_name(source)

    # Changes the file to download based on the sensitivity analysis being run and whether that particular input
    # has a sensitivity analysis path on s3
    if sensit_type != 'std' and 'standard' in dir:

        # Creates directory and file names according to sensitivity analysis type
        dir_sens = dir.replace('standard', sensit_type)
        file_name_sens = file_name[:-4] + '_' + sensit_type + '.tif'

        # First attempt is to try to download the sensitivity analysis version
        try:
            # Doesn't download the tile if it's already on the spot machine
            if os.path.exists(file_name_sens):
                print file_name, "already downloaded" + "\n"
                return

            # If not already on the spot machine, it downloads the file
            else:
                source = os.path.join(dir_sens, file_name_sens)
                cmd = ['aws', 's3', 'cp', source, dest]
                subprocess.check_call(cmd)
                print file_name_sens, "not previously downloaded. Now downloaded." + '\n'

        # Second attempt is to download the standard version of the file.
        # This can happen despite it being a sensitivity run because this input file doesn't have a sensitivity version
        # for this date.
        except:
            if os.path.exists(file_name):
                print file_name, "already downloaded" + "\n"
                return

            else:
                source = os.path.join(dir, file_name)
                try:
                    cmd = ['aws', 's3', 'cp', source, dest]
                    subprocess.check_call(cmd)
                    print file_name, "not previously downloaded. Now downloaded." + '\n'
                except:
                    print source, 'does not exist in standard model or sensitivity model' + '\n'

    # If not a sensitivity run, the standard file is downloaded
    else:
        if os.path.exists(file_name):
            print file_name, "already downloaded" + "\n"

            return

        else:
            source = os.path.join(dir, file_name)
            try:
                cmd = ['aws', 's3', 'cp', source, dest]
                subprocess.check_call(cmd)
                print file_name, "not previously downloaded. Now downloaded." + '\n'
            except:
                print source, 'does not exist-- check if this is expected to exist' + '\n'


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


# This version of checking for data is bad because it can miss tiles that have very little data in them
def check_for_data_old(out_tile):

    # Source: http://gis.stackexchange.com/questions/90726
    # Opens raster and chooses band to find min, max
    gtif = gdal.Open(out_tile)
    srcband = gtif.GetRasterBand(1)
    stats = srcband.GetStatistics(True, True)
    print "  Tile stats =  Minimum=%.3f, Maximum=%.3f, Mean=%.3f, StdDev=%.3f" % (stats[0], stats[1], stats[2], stats[3])

    return stats


# This version of checking for data in a tile is more robust
def check_for_data(tile):
    with rasterio.open(tile) as img:
        msk = img.read_masks(1).astype(bool)
    if msk[msk].size == 0:
        print "Tile {} is empty".format(tile)
        return True
    else:
        print "Tile {} is not empty".format(tile)
        return False


# Checks if there's data in a tile and, if so, uploads it to s3
def check_and_upload(tile_id, upload_dir, pattern):

    print "Checking if {} contains any data...".format(tile_id)
    out_tile = '{0}_{1}.tif'.format(tile_id, pattern)

    no_data = check_for_data(out_tile)

    if no_data:

        print "  No data found. Not copying {}.".format(tile_id)

    else:

        print "  Data found in {}. Copying tile to s3...".format(tile_id)
        upload_final(upload_dir, tile_id, pattern)
        print "    Tile copied to s3"


# Prints the number of tiles that have been processed so far
def count_completed_tiles(pattern):

    completed = len(glob.glob1('.', '*{}*'.format(pattern)))

    print "Number of completed or in-progress tiles:", completed


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


# Warps raster to Hansen tiles using multiple processors
def mp_warp_to_Hansen(tile_id, source_raster, out_pattern, dt):

    # Start time
    start = datetime.datetime.now()

    print "Getting extent of", tile_id
    xmin, ymin, xmax, ymax = coords(tile_id)

    out_file = '{0}_{1}.tif'.format(tile_id, out_pattern)

    cmd = ['gdalwarp', '-t_srs', 'EPSG:4326', '-co', 'COMPRESS=LZW', '-tr', str(cn.Hansen_res), str(cn.Hansen_res), '-tap', '-te',
            str(xmin), str(ymin), str(xmax), str(ymax), '-dstnodata', '0', '-ot', dt, '-overwrite', source_raster, out_file]
    subprocess.check_call(cmd)

    end_of_fx_summary(start, tile_id, out_pattern)


def warp_to_Hansen(in_file, out_file, xmin, ymin, xmax, ymax, dt):

    cmd = ['gdalwarp', '-t_srs', 'EPSG:4326', '-co', 'COMPRESS=LZW', '-tr', str(cn.Hansen_res), str(cn.Hansen_res), '-tap', '-te',
            str(xmin), str(ymin), str(xmax), str(ymax), '-dstnodata', '0', '-ot', dt, '-overwrite', in_file, out_file]
    subprocess.check_call(cmd)


# Rasterizes the shapefile within the bounding coordinates of a tile
def rasterize(in_shape, out_tif, xmin, ymin, xmax, ymax, tr=None, ot=None, name_field=None, anodata=None):
    cmd = ['gdal_rasterize', '-co', 'COMPRESS=LZW',

           # Input raster is ingested as 1024x1024 pixel tiles (rather than the default of 1 pixel wide strips
           '-co', 'TILED=YES', '-co', 'BLOCKXSIZE=1024', '-co', 'BLOCKYSIZE=1024',
           '-te', str(xmin), str(ymin), str(xmax), str(ymax),
           '-tr', tr, tr, '-ot', ot, '-a', name_field, '-a_nodata',
           anodata, in_shape, out_tif]

    subprocess.check_call(cmd)

    return out_tif


# Creates a tile of all 0s for any tile passed to it.
# Uses the pixel area tile for information about the tile.
# Based on https://gis.stackexchange.com/questions/220753/how-do-i-create-blank-geotiff-with-same-spatial-properties-as-existing-geotiff
def make_blank_tile(tile_id, pattern, folder, sensit_type):

    # Creates tile names for standard and sensitivity analyses.
    # Going into this, the function doesn't know whether there should be a standard tile or a sensitivity tile.
    # Thus, it has to be prepared for either one.
    file_name = '{0}{1}_{2}.tif'.format(folder, tile_id, pattern)
    file_name_sens = '{0}{1}_{2}_{3}.tif'.format(folder, tile_id, pattern, sensit_type)

    # Checks if the standard file exists. If it does, a blank tile isn't created.
    if os.path.exists(file_name):
        print '{} exists. Not creating a blank tile.'.format(os.path.join(folder, file_name))
        return

    # Checks if the sensitivity analysis file exists. If it does, a blank tile isn't created.
    elif os.path.exists(file_name_sens):
        print '{} exists. Not creating a blank tile.'.format(os.path.join(folder, file_name_sens))
        return

    # If neither a standard tile nor a sensitivity analysis tile exists, a blank tile is created.
    else:
        print '{} does not exist. Creating a blank tile.'.format(file_name)

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
                                 '{0}{1}_{2}.tif'.format(folder, tile_id, 'empty_tile_template'), 'std')
                print "Downloaded Hansen loss tile for", tile_id

            # If there is no Hansen tile, it downloads the pixel area tile instead
            except:

                s3_file_download('{0}{1}_{2}.tif'.format(cn.pixel_area_dir, cn.pattern_pixel_area, tile_id),
                                 '{0}{1}_{2}.tif'.format(folder, tile_id, 'empty_tile_template'), 'std')
                print "Downloaded pixel area tile for", tile_id

            # Determines what pattern to use (standard or sensitivity) based on the first tile in the list
            tile_list= tile_list_spot_machine(folder, pattern)
            full_pattern = get_tile_type(tile_list[0])

            # Uses either the Hansen loss tile or pixel area tile as a template tile,
            # with the output name corresponding to the model type
            cmd = ['gdal_merge.py', '-createonly', '-init', '0', '-co', 'COMPRESS=LZW', '-ot', 'Byte',
                   '-o', '{0}{1}_{2}.tif'.format(folder, tile_id, full_pattern),
                   '{0}{1}_{2}.tif'.format(folder, tile_id, 'empty_tile_template')]
            subprocess.check_call(cmd)
            print "Created raster of all 0s for", file_name


# Reformats the patterns for the 10x10 degree model output tiles for the aggregated output names
def name_aggregated_output(pattern, thresh, sensit_type):

    out_pattern = re.sub('ha_', '', pattern)
    # print out_pattern
    out_pattern = re.sub('2001_15', 'per_year', out_pattern)
    # print out_pattern
    out_pattern = re.sub('gross_emis_year', 'gross_emis_per_year', out_pattern)
    # print out_pattern
    out_pattern = re.sub('_t_', '_Mt_', out_pattern)
    # print out_pattern
    out_pattern = re.sub('_10km_', '_per_year_10km_', out_pattern)
    # print out_pattern
    out_pattern = re.sub('all_drivers_Mt_CO2e', 'all_drivers_Mt_CO2e_per_year', out_pattern)
    # print out_pattern
    date = datetime.datetime.now()
    date_formatted = date.strftime("%Y%m%d")

    # print thresh
    # print cn.pattern_aggreg
    # print sensit_type
    # print date_formatted

    out_name = '{0}_tcd{1}_{2}_{3}_{4}'.format(out_pattern, thresh, cn.pattern_aggreg, sensit_type, date_formatted)

    # print out_name

    return out_name


# Removes plantations that existed before 2000 from loss tile
def mask_pre_2000_plantation(pre_2000_plant, tile_to_mask, out_name, tile_id):

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
def sensit_tile_rename(sensit_type, tile_id, raw_pattern):

    # Uses whatever name of the tile is found on the spot machine
    if os.path.exists('{0}_{1}_{2}.tif'.format(tile_id, raw_pattern, sensit_type)):
        processed_name = '{0}_{1}_{2}.tif'.format(tile_id, raw_pattern, sensit_type)
    else:
        processed_name = '{0}_{1}.tif'.format(tile_id, raw_pattern)

    return processed_name


