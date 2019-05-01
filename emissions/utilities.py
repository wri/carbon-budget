import subprocess
#import gdal
import multiprocessing
import pandas as pd
import os
import glob
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu


def del_tiles(tile_id):
    tiles = glob.glob('cpp_util/{}*.tif'.format(tile_id))
    for tile in tiles:
        os.remove(tile)


def upload_final(upload_dir, tile_id):
    files = ['disturbance_model_t_CO2_ha', 'shiftingag_model_t_CO2_ha', 'forestry_model_t_CO2_ha', 'wildfire_model_t_CO2_ha', 'deforestation_model_t_CO2_ha', 'urbanization_model_t_CO2_ha', 'node_totals_reclass']
    for f in files:
        to_upload = "outdata/{0}_{1}.tif".format(tile_id, f)
        print "uploading {}".format(to_upload)
        destination = '{0}/{1}/'.format(upload_dir, f)
        cmd = ['aws', 's3', 'mv', to_upload, destination]
        try:
            subprocess.check_call(cmd)
        except:
            print "error uploading"


def mask_loss_pre_2000_plantation(tile_id):
    dest_folder = 'cpp_util/'

    if os.path.exists('{0}_{1}.tif'.format(tile_id, cn.pattern_plant_pre_2000)):

        print "Pre-2000 plantation exists for {}. Cutting out loss in that area...".format(tile_id)

        # Carbon gain uses non-mangrove non-planted biomass:carbon ratio
        calc = '--calc=A*(B=0)'
        loss_outfilename = '{0}{1}_{2}.tif'.format(dest_folder, tile_id, cn.pattern_loss_pre_2000_plant_masked)
        loss_outfilearg = '--outfile={}'.format(loss_outfilename)
        cmd = ['gdal_calc.py',
               '-A', '{0}.tif'.format(tile_id),
               '-B', '{0}_{1}.tif'.format(tile_id, cn.pattern_plant_pre_2000),
               calc, loss_outfilearg, '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW']
        subprocess.check_call(cmd)

    else:

        print "No pre-2000 plantation exists for {}. Renaming loss tile...".format(tile_id)
        os.rename('{}.tif'.format(tile_id), '{0}_{1}.tif'.format(tile_id, cn.pattern_loss_pre_2000_plant_masked))

    # # modify loss tile by erasing where plantations are
    # idn_plant_shp = '{0}/plant_est_2000_or_earlier.shp'.format(dest_folder)
    # loss_tile = '{0}/{1}_loss.tif'.format(dest_folder, tile_id)
    #
    # cmd = ['gdal_rasterize', '-b', '1', '-burn', '0', idn_plant_shp, loss_tile]
    # print cmd
    # subprocess.check_call(cmd)


def download(file_dict, tile_id, carbon_pool_dir):
    carbon_pool_files = file_dict['carbon_pool']
    data_prep_file_list = file_dict['data_prep']
    fao_ecozone_file_list = file_dict['fao_ecozone']
    dest_folder = 'cpp_util/'
    for carbon_file in carbon_pool_files:
        src = '{0}/{1}/{2}_{1}.tif'.format(carbon_pool_dir, carbon_file, tile_id)
        cmd = ['aws', 's3', 'cp', src, dest_folder]
        subprocess.check_call(cmd)

    for data_prep_file in data_prep_file_list:
        file_name = '{0}_res_{1}.tif'.format(tile_id, data_prep_file)

        if data_prep_file == 'tsc_model':
            file_name = '{0}_{1}.tif'.format(tile_id, data_prep_file)

        src = 's3://gfw2-data/climate/carbon_model/other_emissions_inputs/{0}/{1}'.format(data_prep_file, file_name)
        cmd = ['aws', 's3', 'cp', src, dest_folder]
        subprocess.check_call(cmd)

    for ecozone_files in fao_ecozone_file_list:
        file_name = '{0}_res_{1}.tif'.format(tile_id, ecozone_files)

        src = 's3://gfw2-data/climate/carbon_model/inputs_for_carbon_pools/processed/{0}/{1}'.format(ecozone_files, file_name)
        cmd = ['aws', 's3', 'cp', src, dest_folder]
        subprocess.check_call(cmd)

    burned_area = file_dict['burned_area'][0]
    src = 's3://gfw2-data/climate/carbon_model/other_emissions_inputs/burn_year/{0}/{1}_burnyear.tif'.format(burned_area, tile_id)
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

    return file_list


def remove_nodata(tile_id):
    print "Removing nodata values in output tiles"
    files = ['disturbance_model', 'shiftingag_model', 'forestry_model', 'wildfire_model', 'deforestation_model', 'urbanization_model']
    for f in files:
        to_process = "outdata/{0}_{1}.tif".format(tile_id, f)
        out_reclass = "outdata/{0}_{1}_t_CO2_ha"
        cmd = ['gdal_translate', '-a_nodata', 'none', to_process, out_reclass]
        subprocess.check_call(cmd)

    cmd = ['gdal_translate', '-a_nodata', 'none', "outdata/{0}_node_totals.tif".format(tile_id), "outdata/{0}_node_totals_reclass.tif".format(tile_id)]
    subprocess.check_call(cmd)