import subprocess
#import gdal
import multiprocessing
import pandas as pd
import os
import glob


def del_tiles(tile_id):
    tiles = glob.glob('*{}*tif'.format(tile_id))
    for tile in tiles:
        os.remove(tile)

def merge_tiles(tile_id):
    mergetif = 'outdata/{}_disturbance_model.tif'.format(tile_id)
    conversion_tif = 'outdata/{}_shiftingag_model.tif'.format(tile_id)
    forestmodel_tif = 'outdata/{}_forestry_model.tif'.format(tile_id)
    wildfire_tif = 'outdata/{}_wildfire_model.tif'.format(tile_id)
    urbanization_tif = 'outdata/{}_urbanization_model.tif'.format(tile_id)
    mixed_tif = 'outdata/{}_deforestation_model.tif'.format(tile_id)

    cmd = ['gdal_merge.py', '-o', mergetif, conversion_tif, forestmodel_tif, wildfire_tif, urbanization_tif, mixed_tif, '-co', 'COMPRESS=LZW', '-a_nodata', '0']
    print "merging tiles"
    subprocess.check_call(cmd)


def upload_final(upload_dir, tile_id):
    files = ['disturbance_model', 'shiftingag_model', 'forestry_model', 'wildfire_model', 'deforestation_model', 'urbanization_model', 'node_totals']
    for f in files:
        to_upload = "outdata/{0}_{1}.tif".format(tile_id, f)
        print "uploading {}".format(to_upload)
        destination = '{0}/{1}/'.format(upload_dir, f)
        cmd = ['aws', 's3', 'mv', to_upload, destination]
        try:
            subprocess.check_call(cmd)
        except:
            print "error uploading"

def mask_loss(tile_id):
    dest_folder = 'cpp_util/'
    # modify loss tile by erasing where plantations are
    idn_plant_shp = '{0}/plant_est_2000_or_earlier.shp'.format(dest_folder)
    loss_tile = '{0}/{1}_loss.tif'.format(dest_folder, tile_id)

    cmd = ['gdal_rasterize', '-b', '1', '-burn', '0', idn_plant_shp, loss_tile]
    print cmd
    subprocess.check_call(cmd)


def download(file_dict, tile_id, carbon_pool_dir):
    carbon_pool_files = file_dict['carbon_pool']
    data_prep_file_list = file_dict['data_prep']
    dest_folder = 'cpp_util/'
    for carbon_file in carbon_pool_files:
        src = '{0}/{1}/{2}_{1}.tif'.format(carbon_pool_dir, carbon_file, tile_id)
        cmd = ['aws', 's3', 'cp', src, dest_folder]
        subprocess.check_call(cmd)

    for data_prep_file in data_prep_file_list:
        file_name = '{0}_res_{1}.tif'.format(tile_id, data_prep_file)

        if data_prep_file == 'tsc_model':
            file_name = '{0}_{1}.tif'.format(tile_id, data_prep_file)

        src = 's3://gfw-files/sam/carbon_budget/data_inputs2/{0}/{1}'.format(data_prep_file, file_name)
        cmd = ['aws', 's3', 'cp', src, dest_folder]
        subprocess.check_call(cmd)

    burned_area = file_dict['burned_area'][0]
    src = 's3://gfw-files/sam/carbon_budget/{0}/{1}_burnyear.tif'.format(burned_area, tile_id)
    cmd = ['aws', 's3', 'cp', src, dest_folder]
    subprocess.check_call(cmd)

    # Download and unzip shapefile of Indonesia and Malaysia plantations if they have not already been downloaded
    if os.path.exists('{0}/plant_est_2000_or_earlier.zip'.format(dest_folder)) == False:

        src = 's3://gfw-files/sam/carbon_budget/idn_plant_est_2000_or_earlier/plant_est_2000_or_earlier.zip'
        cmd = ['aws', 's3', 'cp', src, dest_folder]
        subprocess.check_call(cmd)

        cmd = ['unzip', '-o', '{0}/plant_est_2000_or_earlier.zip'.format(dest_folder), '-d', dest_folder]
        subprocess.check_call(cmd)

   # rename whichever peatland file was downloaded
    peat_files = ['peatland_drainage_proj', 'cifor_peat_mask', 'hwsd_histosoles']
    for peat_file in peat_files:
        one_peat = glob.glob("{0}/{1}*{2}*".format(dest_folder, tile_id, peat_file))
        if len(one_peat) == 1:
            os.rename(one_peat[0], '{0}/{1}_peat.tif'.format(dest_folder, tile_id))

def wgetloss(tile_id):
    print "download hansen loss tile"
    dest_folder = 'cpp_util/'
    hansen_tile = 's3://gfw2-data/forest_change/hansen_2016/{}.tif'.format(tile_id)
    local_hansen_tile = '{0}/{1}_loss.tif'.format(dest_folder, tile_id)

    cmd = ['aws', 's3', 'cp', hansen_tile, local_hansen_tile]

    subprocess.check_call(cmd)

    return local_hansen_tile


def rasterize_shapefile(shapefiles_to_raterize, tile_id, coords):
    rasterized_files = []

    for shapefile_dict in shapefiles_to_raterize:

        for shapefile in shapefile_dict:
            print "rasterizing {}".format(shapefile)
            rvalue = shapefile_dict[shapefile]
            rasterized_tile = "{0}_{1}.tif".format(tile_id, shapefile)
            rasterize = ['gdal_rasterize', '-co', 'COMPRESS=LZW', '-tr', '0.00025', '0.00025', '-ot',
                         'Byte', '-a', rvalue, '-a_nodata', '0', shapefile + ".shp", rasterized_tile]
                         # [-te 102.249286 1.152727 102.265341 1.165698]
            rasterize += coords
            subprocess.check_call(rasterize)

            print "resampling {}".format(rasterized_tile)

            resampled_tile = "{0}_res_{1}.tif".format(tile_id, shapefile)
            resample = ['gdal_translate', '-co', 'COMPRESS=LZW', '-a_nodata', '0',
                        '-tr', '.00025', '.00025', rasterized_tile, resampled_tile]

            cmd = ['gdalwarp', '-tr', '.00025', '.00025', '-tap', rasterized_tile, resampled_tile]
            subprocess.check_call(resample)

            rasterized_files.append(resampled_tile)

    return rasterized_files

def clip_raster(raster, tile_id, coords):
    print "clipping {}".format(raster)
    clipped_raster = '{0}_{1}.tif'.format(tile_id, raster)
    input_raster = raster + ".tif"
    base_cmd = ['gdal_translate', '-ot', 'Byte', '-co', 'COMPRESS=LZW', '-a_nodata', '0',
                input_raster, clipped_raster]

    clip_cmd = base_cmd + coords
    print clip_cmd
    subprocess.check_call(clip_cmd)
    return clipped_raster


def resample_raster(raster, tile_id):

    print "resampling {}".format(raster)
    input_raster = raster + ".tif"
    resampled_raster = '{0}_res_{1}.tif'.format(tile_id, raster)
    resample_cmd = ['gdal_translate', '-co', 'COMPRESS=LZW', '-tr', '.00025', '.00025', '-a_nodata',
                    '-9999', input_raster, resampled_raster]
    subprocess.check_call(resample_cmd)
    return resampled_raster


def resample_clip_raster(rasters_to_resample, tile_id, coords, coords_te):

    for raster in rasters_to_resample:
        print "resampling/clipping {}".format(raster)
        input_raster = raster + ".tif"
        clipped_raster = '{0}_res_{1}.tif'.format(tile_id, raster)
        # output forest model no data to -9999 and ot Int or whatever allows that
        if raster == "forest_model":
            base_cmd = ['gdal_translate', '-co', 'COMPRESS=LZW', '-a_nodata', '-9999',
                        input_raster, clipped_raster, '-tr', '.00025', '.00025']

            clip_cmd = base_cmd + coords
            print clip_cmd
            subprocess.check_call(clip_cmd)

        elif raster == 'cifor_peat_mask':
            clipped_raster = 'test1.tif'
            final_raster = '{0}_res_{1}.tif'.format(tile_id, raster)
            base_cmd = ['gdal_translate', '-co', 'COMPRESS=LZW', '-a_nodata', '-9999',
                        input_raster, clipped_raster, '-tr', '.00025', '.00025']
            cmd = base_cmd + coords
            subprocess.check_call(cmd)

            clipped_raster2 = 'test.tif'
            basecmd = ['gdalwarp', '-tr', '.00025', '.00025', '-tap', clipped_raster, clipped_raster2]
            cmd = basecmd + coords_te
            subprocess.check_call(cmd)

            base_cmd = ['gdal_translate', '-co', 'COMPRESS=LZW', '-a_nodata', '-9999',
                        clipped_raster2, final_raster, '-tr', '.00025', '.00025']
            cmd = base_cmd + coords
            subprocess.check_call(cmd)

            os.remove('test1.tif', 'test2.tif')

        else:
            base_cmd = ['gdal_translate', '-ot', 'Byte', '-co', 'COMPRESS=LZW', '-a_nodata', '-9999',
            input_raster, clipped_raster, '-tr', '.00025', '.00025']
            clip_cmd = base_cmd + coords
            print clip_cmd
            subprocess.check_call(clip_cmd)

    return clipped_raster




def download_burned_areas(window):
    window = "Win{}".format(window)
    ftp_path = 'ftp://ba1.geog.umd.edu/Collection6/TIFF/{0}/'.format(window)
    download_cmd = ['wget', '-r', '--ftp-user=user', '--ftp-password=burnt_data', '--no-directories', '--no-parent', '-A', '*burndate.tif', ftp_path]
    print download_cmd
    # subprocess.check_call(download_cmd)


def download_allburned_areas():

    ftp_path = 'ftp://ba1.geog.umd.edu/Collection6/TIFF/'
    download_cmd = ['wget', '-r', '--ftp-user=user', '--ftp-password=burnt_data', '--no-parent', '-A', '*burndate.tif', ftp_path]
    print download_cmd
    #subprocess.check_call(download_cmd)


def multiprocess_download(windows):
    window_list = []
    for w in windows:
        if w < 10:
            w = "0{}".format(w)
        w = str(w)
        window_list.append(w)
    print window_list
    if __name__ == '__main__':
     count = multiprocessing.cpu_count()
     pool = multiprocessing.Pool(processes=2)
     pool.map(download_burned_areas, window_list)


def get_windows_in_tile(tile_id):

    csv = 'burned_area_tile_index.csv'

    burned_index_df = pd.read_csv(csv)

    # find the windows for the given tile id
    window = burned_index_df.loc[burned_index_df['tile'] == tile_id, 'window']

    # convert results to list
    list_of_windows = window.values.tolist()

    # remove any duplicates
    list_of_windows = list(set(list_of_windows))

    return list_of_windows


def recode_burned_area(raster):

    outfile_name = raster.strip(".tif") + "_recode.tif"
    outfile_cmd = '--outfile={}'.format(outfile_name)
    recode_cmd = ['gdal_calc.py', '-A', raster, '--calc=A>0', 'NoDataValue=0', '--co', 'COMPRESS=LZW', outfile_cmd]
    subprocess.check_call(recode_cmd)

# Lists the tiles in a folder in s3
def tile_list(source):

    ## For an s3 folder in a bucket using AWSCLI
    # Captures the list of the files in the folder
    out = subprocess.Popen(['aws', 's3', 'ls', source], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout, stderr = out.communicate()

    # Writes the output string to a text file for easier interpretation
    aboveground_c_tiles = open("aboveground_c_tiles.txt", "w")
    aboveground_c_tiles.write(stdout)
    aboveground_c_tiles.close()

    file_list = []

    # Iterates through the text file to get the names of the tiles and appends them to list
    with open("aboveground_c_tiles.txt", 'r') as tile:
        for line in tile:

            num = len(line.strip('\n').split(" "))
            tile_name = line.strip('\n').split(" ")[num - 1]
            tile_short_name = tile_name.replace('_carbon.tif', '')
            file_list.append(tile_short_name)

    # file_list = file_list[1:]

    return file_list
