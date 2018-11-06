import subprocess
import glob

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

# Uploads tile to specified location
def upload_final(pattern, upload_dir, tile_id):

    file = '{}_{}.tif'.format(pattern, tile_id)

    print "Uploading {}".format(file)
    cmd = ['aws', 's3', 'cp', file, upload_dir]

    try:
        subprocess.check_call(cmd)
    except:
        print "Error uploading output tile"



