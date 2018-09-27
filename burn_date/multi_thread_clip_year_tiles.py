import multiprocessing
import clip_year_tiles
import subprocess
import glob
import shutil

import utilities

# creates a 10x10 degree wgs 84 tile of .00025 res burned year. Download all modis hv tile from s3,
# make a mosaic for each year, and clip to hansen extent. Files are uploaded to s3.
for year in range(2014, 2018):

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
    tile_list = utilities.list_tiles('s3://gfw2-data/forest_change/hansen_2017/')
    tile_list = tile_list[1:]
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
