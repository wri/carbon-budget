import multiprocessing
import clip_year_tiles
import subprocess
import glob
import shutil

import utilities

# creates a 10x10 degree wgs 84 tile of .00025 res burned year. Download all modis hv tile from s3,
# make a mosaic for each year, and clip to hansen extent. Files are uploaded to s3
for year in range(2013, 2014):

    # Input files
    # modis_burnyear_dir = 's3://gfw-files/sam/carbon_budget/burn_year_modisproj/'  ## previous location
    modis_burnyear_dir = 's3://gfw2-data/climate/carbon_model/other_emissions_inputs/burn_year/burn_year/'
    Hansen_loss_dir = 's3://gfw2-data/forest_change/hansen_2017'

    # download all hv tifs for this year
    include = '{0}_*.tif'.format(year)
    year_tifs_folder = "{}_year_tifs".format(year)
    utilities.makedir(year_tifs_folder)

    cmd = ['aws', 's3', 'cp', modis_burnyear_dir, year_tifs_folder]
    cmd += ['--recursive', '--exclude', "*", '--include', include]
    subprocess.check_call(cmd)

    # build list of vrt files (command wont take folder/*.tif)
    vrt_name = "global_vrt_{}.vrt".format(year)
    vrt_source_folder = "{}/*.tif".format(year_tifs_folder)

    with open('vrt_files.txt', 'w') as vrt_files:
        vrt_tifs = glob.glob(year_tifs_folder + "/*.tif")
        for tif in vrt_tifs:
            vrt_files.write(tif + "\n")

    # create vrt with wgs84 modis tiles
    cmd = ['gdalbuildvrt', '-input_file_list', 'vrt_files.txt', vrt_name]
    subprocess.check_call(cmd)

    # # build new vrt and virtually project it
    vrt_wgs84 = 'global_vrt_{}_wgs84.vrt'.format(year)
    cmd = ['gdalwarp', '-of', 'VRT', '-t_srs', "EPSG:4326", '-tap', '-tr', '.00025', '.00025', '-overwrite', vrt_name, vrt_wgs84]
    subprocess.check_call(cmd)

    # clip vrt to hansen tile extent
    # tile_list = ["00N_000E", "00N_010E", "00N_020E", "00N_030E", "00N_040E", "10N_000E", "10N_010E", "10N_010W", "10N_020E", "10N_020W", "10N_030E", "10N_040E", "10N_050E", "10S_010E", "10S_020E", "10S_030E", "10S_040E", "10S_050E", "20N_000E", "20N_010E", "20N_010W", "20N_020E", "20N_020W", "20N_030E", "20N_040E", "20N_050E", "20S_010E", "20S_020E", "20S_030E", "20S_040E", "20S_050E", "30N_000E", "30N_010E", "30N_010W", "30N_020W", "30N_030E", "30N_040E", "30N_050E", "30S_010E", "30S_020E", "30S_030E", "10S_110E", "10S_120E", "10S_130E", "10S_140E", "10S_160E", "10S_170E", "20S_110E", "20S_120E", "20S_130E", "20S_140E", "20S_150E", "20S_160E", "30S_110E", "30S_120E", "30S_130E", "30S_140E", "30S_150E", "30S_170E", "40S_140E", "40S_160E", "40S_170E", "30N_020E", "30N_060E", "30N_070E", "30N_080E", "30N_090E", "30N_100E", "30N_110E", "30N_120E", "40N_000E", "40N_010E", "40N_010W", "40N_020E", "40N_020W", "40N_030E", "40N_040E", "40N_050E", "40N_060E", "40N_070E", "40N_080E", "40N_090E", "40N_100E", "40N_110E", "40N_120E", "40N_130E", "40N_140E", "50N_000E", "50N_010E", "50N_010W", "50N_020E", "50N_030E", "50N_040E", "50N_050E", "50N_060E", "50N_070E", "50N_080E", "50N_090E", "50N_100E", "50N_110E", "50N_120E", "50N_130E", "50N_140E", "50N_150E", "60N_000E", "60N_010E", "60N_010W", "60N_020E", "60N_020W", "60N_030E", "60N_040E", "60N_050E", "60N_060E", "60N_070E", "60N_080E", "60N_090E", "60N_100E", "60N_110E", "60N_120E", "60N_130E", "60N_140E", "60N_150E", "60N_160E", "60N_170E", "70N_000E", "70N_010E", "70N_020E", "70N_020W", "70N_030E", "70N_030W", "70N_040E", "70N_050E", "70N_060E", "70N_070E", "70N_080E", "70N_090E", "70N_100E", "70N_110E", "70N_120E", "70N_130E", "70N_140E", "70N_150E", "70N_160E", "70N_170E", "70N_170W", "70N_180W", "80N_010E", "80N_020E", "80N_030E", "80N_050E", "80N_060E", "80N_070E", "80N_080E", "80N_090E", "80N_100E", "80N_110E", "80N_120E", "80N_130E", "80N_140E", "80N_150E", "80N_160E", "80N_170E", "80N_180W", "00N_070E", "00N_090E", "00N_100E", "00N_110E", "00N_120E", "00N_130E", "00N_140E", "00N_150E", "00N_160E", "10N_070E", "10N_080E", "10N_090E", "10N_100E", "10N_110E", "10N_120E", "10N_130E", "10N_140E", "10N_150E", "10N_160E", "10S_090E", "10S_100E", "10S_150E", "20N_070E", "20N_080E", "20N_090E", "20N_100E", "20N_110E", "20N_120E", "20N_140E", "30N_140E", "30N_150E", "20N_100W", "30N_090W", "30N_100W", "30N_110W", "30N_120W", "40N_070W", "40N_080W", "40N_090W", "40N_100W", "40N_110W", "40N_120W", "40N_130W", "50N_060W", "50N_070W", "50N_080W", "50N_090W", "50N_100W", "50N_110W", "50N_120W", "50N_130W", "60N_060W", "60N_070W", "60N_080W", "60N_090W", "60N_100W", "60N_110W", "60N_120W", "60N_130W", "60N_140W", "60N_150W", "60N_160W", "60N_170W", "60N_180W", "70N_060W", "70N_070W", "70N_080W", "70N_090W", "70N_100W", "70N_110W", "70N_120W", "70N_130W", "70N_140W", "70N_150W", "70N_160W", "80N_060W", "80N_070W", "80N_080W", "80N_090W", "80N_100W", "80N_110W", "80N_120W", "80N_130W", "80N_140W", "80N_150W", "80N_160W", "80N_170W", "00N_040W", "00N_050W", "00N_060W", "00N_070W", "00N_080W", "00N_090W", "00N_100W", "10N_030W", "10N_050W", "10N_060W", "10N_070W", "10N_080W", "10N_090W", "10N_100W", "10S_040W", "10S_050W", "10S_060W", "10S_070W", "10S_080W", "20N_060W", "20N_070W", "20N_080W", "20N_090W", "20N_110W", "20N_120W", "20S_030W", "20S_040W", "20S_050W", "20S_060W", "20S_070W", "20S_080W", "20S_090W", "20S_110W", "30N_080W", "30S_060W", "30S_070W", "30S_080W", "40S_070W", "40S_080W", "50S_060W", "50S_070W", "50S_080W"]
    tile_list = utilities.list_tiles('s3://gfw2-data/forest_change/hansen_2017/')
    # tile_list = ['10N_100E.tif', '10N_110E.tif', '10N_120E.tif']
    print tile_list
    # create a list of lists, with year and tile id to send to multi processor
    tile_year_list = []
    for tile_id in tile_list:
        tile_year_list.append([tile_id, year])

    if __name__ == '__main__':
        count = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(processes=40)
        pool.map(clip_year_tiles.clip_year_tiles, tile_year_list)

    print "Multiprocessing for year done. Moving to next year."

    year_tifs_folder = "{}_year_tifs".format(year)
    shutil.rmtree(year_tifs_folder)
