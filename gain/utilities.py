import subprocess
import glob

biomass_to_c = 0.5

cont_eco_dir = 's3://gfw2-data/climate/carbon_model/fao_ecozones/ecozone_continent/20181002/processed/'

# Ecozone shapefile
cont_ecozone_shp = 'fao_ecozones_fra_2000_continents_assigned_dissolved_FINAL_20180906.zip'

# Continent-ecozone and forest age category are needed for assigning gain rates
age_cat_dir = 's3://gfw2-data/climate/carbon_model/forest_age_category/20180921/'

# Annual gain rate and the number of years with gain are needed
annual_gain_dir = 's3://gfw2-data/climate/carbon_model/annual_gain_rate/20181003/'
gain_year_count_dir = 's3://gfw2-data/climate/carbon_model/gain_year_count/20180912/'

# Loss, gain, and tree cover density, intact forest landscape, biomass tiles, and continent-ecozone
# All of these are needed for the forest age decision tree
loss_dir = 's3://gfw2-data/forest_change/hansen_2015/Loss_tiles/'
gain_dir = 's3://gfw2-data/forest_change/tree_cover_gain/gaindata_2012/'
tcd_dir = 's3://gfw2-data/forest_cover/2000_treecover/'
ifl_dir = 's3://gfw2-data/climate/carbon_model/other_emissions_inputs/ifl_2000/'
biomass_dir = 's3://gfw2-data/climate/WHRC_biomass/WHRC_V4/Processed/'

# Processed abovegroung mangrove biomass tiles
mangrove_biomass_dir = 's3://gfw2-data/climate/carbon_model/mangrove_biomass/processed/20181018/'

# Spreadsheet with annual gain rates
gain_spreadsheet = 'gain_rate_continent_ecozone_age_20181017.xlsx'

def s3_folder_download(source, dest):
    cmd = ['aws', 's3', 'cp', source, dest, '--recursive']
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

    return ymax, xmin, ymin, xmax

# Rasterizes the shapefile within the bounding coordinates of a tile
def rasterize(in_shape, out_tif, xmin, ymin, xmax, ymax, tr=None, ot=None, gainEcoCon=None, anodata=None):
    cmd = ['gdal_rasterize', '-co', 'COMPRESS=LZW',

           # Input raster is ingested as 1024x1024 pixel tiles (rather than the default of 1 pixel wide strips
           '-co', 'TILED=YES', '-co', 'BLOCKXSIZE=1024', '-co', 'BLOCKYSIZE=1024',
           '-te', str(xmin), str(ymin), str(xmax), str(ymax),
           '-tr', tr, tr, '-ot', ot, '-a', gainEcoCon, '-a_nodata',
           anodata, in_shape, '{}.tif'.format(out_tif)]

    subprocess.check_call(cmd)

    return out_tif

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



##### Not currently using the below functions


def wgetloss(tile_id):
    print "download hansen loss tile"
    cmd = ['wget', r'http://glad.geog.umd.edu/Potapov/GFW_2015/tiles/{}.tif'.format(tile_id)]

    subprocess.check_call(cmd)


def wget2015data(tile_id, filetype):

    outfile = '{0}_{1}_h.tif'.format(tile_id, filetype)

    website = 'https://storage.googleapis.com/earthenginepartners-hansen/GFC-2015-v1.3/Hansen_GFC-2015-v1.3_{0}_{1}.tif'.format(filetype, tile_id)
    cmd = ['wget', website, '-O', outfile]
    print cmd

    subprocess.check_call(cmd)

    return outfile


def rasterize_shapefile(xmin, ymax, xmax, ymin, shapefile, output_tif, attribute_field):
    layer = shapefile.replace(".shp", "")
    # attribute_field = 'old_100'
    cmd= ['gdal_rasterize', '-te', str(xmin), str(ymin), str(xmax), str(ymax), '-a', attribute_field, '-co', 'COMPRESS=LZW', '-tr', '.00025', '.00025', '-tap', '-a_nodata', '0', '-l', layer, shapefile, output_tif]

    subprocess.check_call(cmd)

    return output_tif


def resample_00025(input_tif, resampled_tif):
    # resample to .00025
    cmd = ['gdal_translate', input_tif, resampled_tif, '-tr', '.00025', '.00025', '-co', 'COMPRESS=LZW']
    subprocess.check_call(cmd)
