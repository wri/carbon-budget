import subprocess
import glob
import os

# Location of source mangrove aboveground biomass images
mangrove_raw_dir = 's3://gfw2-data/climate/carbon_model/mangrove_biomass/raw_from_Lola_Fatoyinbo_20180911/'
mangrove_raw = 'MaskedSRTMCountriesAGB_WRI.zip'

# name of mangrove vrt
mangrove_vrt = 'mangrove_biomass.vrt'

# base name of mangrove biomass tiles
mangrove_tile_out = 'mangrove_agb_t_ha'

# output location for mangrove biomass tiles
out_dir = 's3://gfw2-data/climate/carbon_model/mangrove_biomass/processed/20181018/'

def s3_folder_download(source, dest):
    cmd = ['aws', 's3', 'cp', source, dest, '--recursive']
    subprocess.check_call(cmd)

def s3_file_download(source, dest):
    cmd = ['aws', 's3', 'cp', source, dest]
    subprocess.check_call(cmd)

def build_vrt(out_vrt):
    print "Creating vrt of mangroves"
    os.system('gdalbuildvrt {} *.tif'.format(out_vrt))

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
            tile_short_name = tile_name.replace('_biomass.tif', '')
            file_list.append(tile_short_name)

    file_list = file_list[1:]

    return file_list

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

    return xmin, xmax, ymin, ymax

# Uploads tile to specified location
def upload_final(pattern, upload_dir, tile_id):

    # Gets all files with the specified pattern
    files = glob.glob('{0}_{1}*'.format(pattern, tile_id))

    print '{0}_{1}.tif'.format(pattern, tile_id)

    for f in files:

        print "uploading {}".format(f)
        cmd = ['aws', 's3', 'cp', '{}'.format(f), upload_dir]
        print cmd

        try:
            subprocess.check_call(cmd)
        except:
            print "Error uploading output tile"