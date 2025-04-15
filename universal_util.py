from subprocess import Popen, PIPE, STDOUT, check_call, run
import glob
import boto3
import botocore
import constants_and_names as cn
import datetime
import rasterio
import logging
import csv
import psutil
from shutil import copyfile, move
import os
import multiprocessing
from multiprocessing.pool import Pool
from shutil import copy
import re
import pandas as pd
from osgeo import gdal
import time
import tempfile
from random import seed
from random import random

# Prints the date as YYYYmmdd_hhmmss
d = datetime.datetime.today()
date_today = d.strftime('%Y_%m_%d')
date_time_today = d.strftime('%Y%m%d_%h%m%s') # for Linux
# date_time_today = d.strftime('%Y%m%d_%H%M%S') # for Windows

# Uploads the output log to the designated s3 folder
def upload_log():

    # Builds a slightly variable delay into the log uploading so that a ton of log uploads at once don't overwhelm s3
    seed()
    lag = random()*2
    time.sleep(lag)

    cmd = ['aws', 's3', 'cp', os.path.join(cn.docker_app, cn.model_log), cn.model_log_dir, '--only-show-errors']
    check_call(cmd)


# Creates the log with a starting line
def initiate_log(tile_id_list):

    # For some reason, logging gets turned off when AWS credentials aren't provided.
    # This restores logging without AWS credentials.
    if not check_aws_creds():
        # https://stackoverflow.com/a/49202811
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

    logging.basicConfig(filename=os.path.join(cn.docker_app, cn.model_log),
                        format='%(levelname)s @ %(asctime)s: %(message)s',
                        datefmt='%Y/%m/%d %I:%M:%S %p',
                        level=logging.INFO)

    if cn.SENSIT_TYPE == 'std':
        sensit_type = 'standard model'
    else:
        sensit_type = cn.SENSIT_TYPE

    logging.info(f'Log notes: {cn.LOG_NOTE}')
    logging.info(f'Model version: {cn.version}')
    logging.info(f'This is the start of the log for this model run. Below are the command line arguments for this run.')
    logging.info(f'Sensitivity analysis type: {sensit_type}')
    logging.info(f'Model stage argument: {cn.STAGE_INPUT}')
    logging.info(f'Run model stages after the initial selected stage: {cn.RUN_THROUGH}')
    logging.info(f'Run date: {cn.RUN_DATE}')
    logging.info(f'Tile ID list: {tile_id_list}')
    logging.info(f'Carbon emitted_pools to generate (optional): {cn.CARBON_POOL_EXTENT}')
    logging.info(f'Emissions emitted_pools (optional): {cn.EMITTED_POOLS}')
    logging.info(f'Standard net flux for comparison with sensitivity analysis net flux (optional): {cn.STD_NET_FLUX}')
    logging.info(f'Include mangrove removal scripts in model run (optional): {cn.INCLUDE_MANGROVES}')
    logging.info(f'Include US removal scripts in model run (optional): {cn.INCLUDE_US}')
    logging.info(f'Do not upload anything to s3: {cn.NO_UPLOAD}')
    logging.info(f'AWS credentials supplied: {check_aws_creds()}')
    logging.info(f'Save intermediate outputs: {cn.SAVE_INTERMEDIATES}')
    logging.info(f'Use single processor: {cn.SINGLE_PROCESSOR}')
    logging.info(f'AWS ec2 instance type and AMI ID:')

    # https://stackoverflow.com/questions/13735051/how-to-capture-curl-output-to-a-file
    # https://stackoverflow.com/questions/625644/how-to-get-the-instance-id-from-within-an-ec2-instance
    try:
        cmd = ['curl', 'http://169.254.169.254/latest/meta-data/instance-type', '-o', 'instance_type.txt', '--silent']
        process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
        with process.stdout:
            log_subprocess_output(process.stdout)
        cmd = ['curl', 'http://169.254.169.254/latest/meta-data/ami-id', '-o', 'ami_id.txt', '--silent']
        process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
        with process.stdout:
            log_subprocess_output(process.stdout)

        type_file = open("instance_type.txt", "r")
        type_lines = type_file.readlines()
        for line in type_lines:
            logging.info(f'  Instance type: {line.strip()}')

        ami_file = open("ami_id.txt", "r")
        ami_lines = ami_file.readlines()
        for line in ami_lines:
            logging.info(f'  AMI ID: {line.strip()}')

        os.remove("ami_id.txt")
        os.remove("instance_type.txt")

    except:
        logging.info('  Not running on AWS ec2 instance')

    logging.info(f"Available processors: {cn.count}")

    # Suppresses logging from rasterio and botocore below ERROR level for the entire model
    logging.getLogger("rasterio").setLevel(logging.ERROR)  # https://www.tutorialspoint.com/How-to-disable-logging-from-imported-modules-in-Python
    logging.getLogger("botocore").setLevel(logging.ERROR)  # "Found credentials in environment variables." is logged by botocore: https://github.com/boto/botocore/issues/1841

    # If no_upload flag is not activated, log is uploaded
    if not cn.NO_UPLOAD:
        upload_log()


# Prints the output statement in the console and adds it to the log. It can handle an indefinite number of string to print
def print_log(*args):

    # Empty string
    full_statement = str(object='')

    # Concatenates all individuals strings to the complete line to print
    for arg in args:
        full_statement = full_statement + str(arg) + " "

    logging.info(full_statement)

    # Prints to console
    print("LOG: " + full_statement)

    # # Every time a line is added to the log, it is copied to s3.
    # # NOTE: During model 1.2.1 runs, I started getting repeated errors about uploading the log to s3.
    # # I don't know why it happened, but my guess is that it's because I had too many things trying to copy
    # # to that s3 location at once. So I'm reducing the occasions for uploading the log by removing uploads
    # # whenever anything is printed. Instead, I'll upload at the end of each tile and each model stage.
    # upload_log()


# Logs fatal errors to the log txt, uploads to s3, and then terminates the program with an exception in the console
def exception_log(*args):

    # Empty string
    full_statement = str(object='')

    # Concatenates all individuals strings to the complete line to print
    for arg in args:
        full_statement = full_statement + str(arg) + " "

    # Adds the exception to the log txt
    logging.info(full_statement, stack_info=True)

    # If no_upload flag is not activated (by choice or by lack of AWS credentials), output is uploaded
    if not cn.NO_UPLOAD:

        # Need to upload log before the exception stops the script
        upload_log()

    # Prints to console, ending the program
    raise Exception(full_statement)


# Adds the subprocess output to the log and the console
# Solution is from second answer (jfs' answer) at this page: https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
def log_subprocess_output(pipe):

    # Reads all the output into a string
    for full_out in iter(pipe.readline, b''): # b"\n"-separated lines

        # Separates the string into an array, where each entry is one line of output
        line_array = full_out.splitlines()

        # For reasons I don't know, the array is backwards, so this prints it out in reverse (i.e. correct) order
        for line in reversed(line_array):
            logging.info(line.decode("utf-8")) #https://stackoverflow.com/questions/37016946/remove-b-character-do-in-front-of-a-string-literal-in-python-3, answer by krock
            print(line.decode("utf-8"))

    # # After the subprocess finishes, the log is uploaded to s3.
    # # Having too many tiles finish running subprocesses at once can cause the upload to get overwhelmed and cause
    # # an error. So, I've commented out the log upload because it's not really necessary here.
    # upload_log()


def log_subprocess_output_simple(cmd):
    # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        log_subprocess_output(process.stdout)


def log_subprocess_output_full(cmd):
    # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    pipe = process.stdout
    with pipe:

        # Reads all the output into a string
        for full_out in iter(pipe.readline, b''):  # b"\n"-separated lines

            # Separates the string into an array, where each entry is one line of output
            line_array = full_out.splitlines()

            # For reasons I don't know, the array is backwards, so this prints it out in reverse (i.e. correct) order
            for line in reversed(line_array):
                logging.info(line.decode(
                    "utf-8"))  # https://stackoverflow.com/questions/37016946/remove-b-character-do-in-front-of-a-string-literal-in-python-3, answer by krock
                print(line.decode(
                    "utf-8"))  # https://stackoverflow.com/questions/37016946/remove-b-character-do-in-front-of-a-string-literal-in-python-3, answer by krock


        # # After the subprocess finishes, the log is uploaded to s3
        # upload_log()


# Checks if Amazon Web Services credentials are in the environment. Both the access key and secret key are needed.
def check_aws_creds():

    if ('AWS_ACCESS_KEY_ID' in os.environ) and ('AWS_SECRET_ACCESS_KEY' in os.environ):
        # print_log("s3 credentials found. Uploading and downloading enabled.")
        return True
    else:
        # print_log("s3 credentials not found. Uploading to s3 disabled but downloading enabled.")
        return False


# Checks the OS for how much storage is available in the system, what's being used, and what percent is being used
# https://stackoverflow.com/questions/12027237/selecting-specific-columns-from-df-h-output-in-python
def check_storage():

    df_output_lines = [s.split() for s in os.popen("df -h").read().splitlines()]
    used_storage = df_output_lines[5][2]
    available_storage = df_output_lines[5][3]
    percent_storage_used = df_output_lines[5][4]
    print_log(f'Storage used: {used_storage}; Available storage: {available_storage}; Percent storage used: {percent_storage_used}')


# Obtains the absolute number of RAM gigabytes currently in use by the entire system (all processors).
# https://www.pragmaticlinux.com/2020/12/monitor-cpu-and-ram-usage-in-python-with-psutil/
# The outputs from this don't exactly match the memory shown in htop but I think it's close enough to be useful.
# It seems to slightly over-estimate memory usage (by ~1-2 GB).
def check_memory():

    used_memory = (psutil.virtual_memory().total - psutil.virtual_memory().available)/1024/1024/1000
    total_memory = psutil.virtual_memory().total/1024/1024/1000
    percent_memory = used_memory/total_memory*100
    print_log(f"Memory usage is: {round(used_memory,2)} GB out of {round(total_memory,2)} = {round(percent_memory,1)}% usage")

    if percent_memory > 99:
        print_log('WARNING: MEMORY USAGE DANGEROUSLY HIGH! TERMINATING PROGRAM.')  # Not sure if this is necessary
        exception_log('EXCEPTION: MEMORY USAGE DANGEROUSLY HIGH! TERMINATING PROGRAM.')


# Not currently using because it shows 1 when using with multiprocessing
# (although it seems to work fine when not using multiprocessing)
def counter(func):
    """
    A decorator that counts and prints the number of times a function has been executed
    https://stackoverflow.com/a/1594484 way down at the bottom of the post in the examples section
    """

    @functools.wraps(func)
    def wrapper_count(*args, **kwargs):
        wrapper_count.count = wrapper_count.count + 1
        print("Number of times {0} has been used: {1}".format(func.__name__, wrapper_count.count))
        res = func(*args, **kwargs)
        return res

    wrapper_count.count = 0
    return wrapper_count


# Gets the tile id from the full tile name using a regular expression
def get_tile_id(tile_name):

    # based on https://stackoverflow.com/questions/20003025/find-1-letter-and-2-numbers-using-regex and https://docs.python.org/3.4/howto/regex.html
    tile_id = re.search("[0-9]{2}[A-Z][_][0-9]{3}[A-Z]", tile_name).group()

    return tile_id


# Gets the tile id from the full tile name using a regular expression
def get_tile_type(tile_name):

    tile_type = tile_name[9:-4]

    return tile_type


# Gets the tile name from the full tile name using a regular expression
def get_tile_name(tile):

    tile_name = os.path.split(tile)[1]

    return tile_name


# Gets the directory of the tile
def get_tile_dir(tile):

    tile_dir = os.path.split(tile)[0]

    return tile_dir


# Makes a complete tile name out of component tile id and pattern
def make_tile_name(tile_id, pattern):

    return f'{tile_id}_{pattern}.tif'


# Lists the tiles in a folder in s3
def tile_list_s3(source, sensit_type='std'):

    # Changes the directory to list tiles in if the model run is the biomass_swap or US_removals sensitivity analyses
    # (JPL AGB extent and US extent, respectively)
    if sensit_type == 'std':
        new_source = source
    elif sensit_type == 'US_removals':
        new_source = cn.annual_gain_AGC_BGC_natrl_forest_US_dir
    else:
        new_source = source.replace('standard', sensit_type)

    print_log("\n" + f'Creating list of tiles in {new_source}')

    ## For an s3 folder in a bucket using AWSCLI
    # Captures the list of the files in the folder
    # out = Popen(['aws', 's3', 'ls', new_source, '--no-sign-request'], stdout=PIPE, stderr=STDOUT)
    out = Popen(['aws', 's3', 'ls', new_source], stdout=PIPE, stderr=STDOUT)
    stdout, stderr = out.communicate()

    # Writes the output string to a text file for easier interpretation
    biomass_tiles = open(os.path.join(cn.docker_tmp, 'tiles.txt'), "wb")
    biomass_tiles.write(stdout)
    biomass_tiles.close()

    file_list = []

    # Iterates through the text file to get the names of the tiles and appends them to list
    with open(os.path.join(cn.docker_tmp, 'tiles.txt'), 'r') as tile:
        for line in tile:
            num = len(line.strip("\n").split(" "))
            tile_name = line.strip("\n").split(" ")[num - 1]

            # Only tifs will be in the tile list
            if '.tif' in tile_name:

                tile_id = get_tile_id(tile_name)
                file_list.append(tile_id)

    if len(file_list) > 0:

        return file_list

    # In case the change of directories to look for sensitivity versions yields an empty folder.
    # This could be done better by using boto3 to check the potential s3 folders for files upfront but I couldn't figure
    # out how to do that.
    print_log("\n" + f'Creating list of tiles in {source}')

    ## For an s3 folder in a bucket using AWSCLI
    # Captures the list of the files in the folder
    # out = Popen(['aws', 's3', 'ls', source, '--no-sign-request'], stdout=PIPE, stderr=STDOUT)
    out = Popen(['aws', 's3', 'ls', source], stdout=PIPE, stderr=STDOUT)
    stdout, stderr = out.communicate()

    # Writes the output string to a text file for easier interpretation
    biomass_tiles = open(os.path.join(cn.docker_tmp, 'tiles.txt'), "wb")
    biomass_tiles.write(stdout)
    biomass_tiles.close()

    file_list = []

    # Iterates through the text file to get the names of the tiles and appends them to list
    with open(os.path.join(cn.docker_tmp, 'tiles.txt'), 'r') as tile:
        for line in tile:
            num = len(line.strip("\n").split(" "))
            tile_name = line.strip("\n").split(" ")[num - 1]

            # Only tifs will be in the tile list
            if '.tif' in tile_name:
                tile_id = get_tile_id(tile_name)
                file_list.append(tile_id)

    return file_list


# Lists the tiles on the spot machine
def tile_list_spot_machine(source, pattern):

    ## For an s3 folder in a bucket using AWSCLI
    # Captures the list of the files in the folder
    out = Popen(['ls', source], stdout=PIPE, stderr=STDOUT)
    stdout, stderr = out.communicate()

    # Writes the output string to a text file for easier interpretation
    tiles = open(os.path.join(cn.docker_tmp, 'tiles.txt'), "wb")
    tiles.write(stdout)
    tiles.close()

    file_list = []

    # Iterates through the text file to get the names of the tiles and appends them to list
    with open(os.path.join(cn.docker_tmp, 'tiles.txt'), 'r') as tile:
        for line in tile:
            num = len(line.strip("\n").split(" "))
            tile_name = line.strip("\n").split(" ")[num - 1]

            # Only files with the specified pattern will be in the tile list
            if pattern in tile_name:

                file_list.append(tile_name)

    return file_list


# Creates a list of all tile ids found in input s3 folders, removes duplicate tile ids from the list, and orders them
def create_combined_tile_list(list_of_tile_dirs, sensit_type='std'):

    print_log('Making a combined tile id list...')

    # Changes the directory to list tiles according to the model run.
    # If the model run is the biomass_swap or US_removals sensitivity analyses
    # (JPL AGB extent and US extent, respectively), particular sets of tiles are designated.
    # If the model run is standard, the names don't change.
    # WARNING: Other sensitivity analyses aren't included in this and may result in unintended behaviors.
    # WARNING: No sensitivity analyses have been tested with this function.
    if sensit_type == 'biomass_swap':
        source = cn.JPL_processed_dir
        tile_list = tile_list_s3(source, sensit_type='std')
        return tile_list
    if sensit_type == 'US_removals':
        source = cn.annual_gain_AGC_BGC_natrl_forest_US_dir
        tile_list = tile_list_s3(source, sensit_type='std')
        return tile_list

    # Iterates through the s3 locations and makes a txt file of tiles for each one
    for i, tile_set in enumerate(list_of_tile_dirs):

        # out = Popen(['aws', 's3', 'ls', set1, '--no-sign-request'], stdout=PIPE, stderr=STDOUT)
        out = Popen(['aws', 's3', 'ls', tile_set], stdout=PIPE, stderr=STDOUT)
        stdout, stderr = out.communicate()
        # Writes the output string to a text file for easier interpretation
        set1_tiles = open(f'tile_set_{i}.txt', "wb")
        set1_tiles.write(stdout)
        set1_tiles.close()

    # Empty lists for filling with tile ids
    file_list_set = []

    # The list of text files with tile info from s3
    tile_set_txt_list = glob.glob('tile_set_*txt')

    # Combines all tile text files into a single tile text file
    # https://stackoverflow.com/a/13613375
    with open('tile_set_consolidated.txt', 'w') as outfile:
        for fname in tile_set_txt_list:
            with open(fname) as infile:
                outfile.write(infile.read())

    # Iterates through the rows of the consolidated text file to get the tile ids and appends them to the list
    with open('tile_set_consolidated.txt', 'r') as tile:

        for line in tile:

            num = len(line.strip("\n").split(" "))
            tile_name = line.strip("\n").split(" ")[num - 1]

            # Only tifs will be in the tile list
            if '.tif' in tile_name:

                tile_id = get_tile_id(tile_name)
                file_list_set.append(tile_id)

    # Tile list with tiles found in multiple lists removed, so now duplicates are gone
    unique_tiles = list(set(file_list_set))

    # Converts the set to a pandas dataframe to put the tiles in the correct order
    df = pd.DataFrame(unique_tiles, columns=['tile_id'])
    df = df.sort_values(by=['tile_id'])

    # Converts the pandas dataframe back to a Python list
    unique_tiles_ordered_list = df.tile_id.tolist()

    # Removes the text files with the lists of tiles
    tile_set_txt_list = glob.glob('tile_set_*txt')  # Adds the consolidated tile txt to the list
    for i in tile_set_txt_list:
        os.remove(i)

    print_log(f'There are {len(unique_tiles_ordered_list)} unique tiles in {len(list_of_tile_dirs)} s3 folders ({len(file_list_set)} tiles overall)')

    return unique_tiles_ordered_list


# Counts the number of tiles in a folder in s3
def count_tiles_s3(source, pattern=None):

    ## For an s3 folder in a bucket using AWSCLI
    # Captures the list of the files in the folder
    # out = Popen(['aws', 's3', 'ls', source, '--no-sign-request'], stdout=PIPE, stderr=STDOUT)
    out = Popen(['aws', 's3', 'ls', source], stdout=PIPE, stderr=STDOUT)
    stdout, stderr = out.communicate()

    # Writes the output string to a text file for easier interpretation
    tile_list_name = "tiles.txt"
    tile_file = open(os.path.join(cn.docker_tmp, tile_list_name), "wb")
    tile_file.write(stdout)
    tile_file.close()

    file_list = []

    if 'gfw-data-lake' in source:
        #TODO: Change this function to count tiles in gfw-data-lake
        print_log("Not counting gfw-data-lake tiles... No good mechanism for it, sadly.")
        return

    # Iterates through the text file to get the names of the tiles and appends them to list
    with open(os.path.join(cn.docker_tmp, tile_list_name), 'r') as tile:
        for line in tile:
            num = len(line.strip("\n").split(" "))
            tile_name = line.strip("\n").split(" ")[num - 1]

            # For tcd, pixel area, and loss tiles (and their rewindowed versions),
            # which have the tile_id after the the pattern
            if pattern in [cn.pattern_tcd, cn.pattern_pixel_area, cn.pattern_loss]:
                if tile_name.endswith('.tif'):
                    tile_id = get_tile_id(tile_name)
                    file_list.append(tile_id)

            # If the counted tiles have to have a specific pattern
            elif pattern != None:
                if tile_name.endswith('{}.tif'.format(pattern)):
                    tile_id = get_tile_id(tile_name)
                    file_list.append(tile_id)

            # If the counted tiles just have to be tifs
            else:
                if tile_name.endswith('.tif'):
                    tile_id = get_tile_id(tile_name)
                    file_list.append(tile_id)

    # Count of tiles (ends in *tif)
    return len(file_list)


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
        s3_folder_download(source_dir, dest, sensit_type, pattern)

    # For downloading test tiles (twenty or fewer).
    elif len(tile_id_list) <= 20:

        # Creates a full download name (path and file)
        for tile_id in tile_id_list:
            if pattern in [cn.pattern_tcd, cn.pattern_pixel_area, cn.pattern_loss]:   # For tiles that do not have the tile_id first
                source = f'{source_dir}{pattern}_{tile_id}.tif'
            elif pattern in [cn.pattern_data_lake]:
                source = f'{source_dir}{tile_id}.tif'
            else:  # For every other type of tile
                source = f'{source_dir}{tile_id}_{pattern}.tif'

            s3_file_download(source, dest, sensit_type)

    # For downloading full sets of tiles
    else:
        s3_folder_download(source_dir, dest, sensit_type, pattern)


# Downloads all tiles in an s3 folder, adapating to sensitivity analysis type
# Source=source file on s3
# dest=where to download onto spot machine
# sensit_type = whether the model is standard or a sensitivity analysis model run
def s3_folder_download(source, dest, sensit_type, pattern = None):

    # The number of tiles with the given pattern on the spot machine.
    # Special cases are below.
    local_tile_count = len(glob.glob(f'*{pattern}*.tif'))

    # For data-lake tiles, which have a different pattern on the ec2 instance from s3
    if pattern == cn.pattern_data_lake:
        if source == cn.gain_dir:
            ec2_pattern = cn.pattern_gain_ec2
        elif source == cn.datalake_pf_agc_rf_dir:
            ec2_pattern = cn.pattern_pf_rf_agc_ec2
        elif source == cn.datalake_pf_agcbgc_rf_dir:
            ec2_pattern = cn.pattern_pf_rf_agcbgc_ec2
        elif source == cn.datalake_pf_agc_sd_dir:
            ec2_pattern = cn.pattern_pf_sd_agc_ec2
        elif source == cn.datalake_pf_agcbgc_sd_dir:
            ec2_pattern = cn.pattern_pf_sd_agcbgc_ec2
        elif source == cn.datalake_pf_simplename_dir:
            ec2_pattern = cn.pattern_planted_forest_type
        elif source == cn.datalake_pf_estab_year_dir:
            ec2_pattern = cn.pattern_planted_forest_estab_year

        local_tile_count = len(glob.glob(f'*{ec2_pattern}*.tif'))
        print_log(f'There are {local_tile_count} tiles on the spot machine with the pattern {ec2_pattern}')

    # For tile types that have the tile_id after the pattern
    if pattern in [cn.pattern_tcd, cn.pattern_pixel_area, cn.pattern_loss]:
        local_tile_count = len(glob.glob(f'{pattern}*.tif'))
        print_log(f'There are {local_tile_count} tiles on the spot machine with the pattern {pattern}')

    # Changes the path to download from based on the sensitivity analysis being run and whether that particular input
    # has a sensitivity analysis path on s3
    if sensit_type != 'std':

        # Creates the appropriate path for getting sensitivity analysis tiles
        source_sens = source.replace('standard', sensit_type)

        print_log(f'Attempting to change source directory {source} to {source_sens} to reflect sensitivity analysis')

        # Counts how many tiles are in the sensitivity analysis source s3 folder
        s3_count_sens = count_tiles_s3(source_sens)
        print_log(f'There are {s3_count_sens} tiles in sensitivity analysis folder {source_sens} with the pattern {pattern}')

        # Counts how many tiles are in the standard model source s3 folder
        s3_count_std = count_tiles_s3(source)
        print_log(f'There are {s3_count_std} tiles in standard model folder {source} with the pattern {pattern}')

        # Decides which source folder to use the count from: standard model or sensitivity analysis.
        # If there are sensitivity analysis tiles, that source folder should be used.
        # Otherwise, the standard folder should be used.
        if s3_count_sens != 0:
            s3_count = s3_count_sens
            source_final = source_sens
        else:
            s3_count = s3_count_std
            source_final = source

        # If there are as many tiles on the spot machine with the relevant pattern as there are on s3, no tiles are downloaded
        if local_tile_count == s3_count:
            print_log(f'Tiles with pattern {pattern} are already on spot machine. Not downloading.', "\n")
            return

        # If there appears to be a full set of tiles in the sensitivity analysis folder (7 is semi arbitrary),
        # the sensitivity folder is downloaded
        if s3_count > 7:

            print_log(f'Source directory used: {source_final}')

            cmd = ['aws', 's3', 'cp', source_final, dest, '--no-sign-request', '--exclude', '*tiled/*',
                   '--exclude', '*geojason', '--exclude', '*vrt', '--exclude', '*csv', '--no-progress', '--recursive']
            # cmd = ['aws', 's3', 'cp', source_final, dest, '--no-sign-request', '--exclude', '*tiled/*',
            #        '--exclude', '*geojason', '--exclude', '*vrt', '--exclude', '*csv', '--recursive']
            log_subprocess_output_full(cmd)

            print_log("\n")

        # If there are fewer than 7 files in the sensitivity folder (i.e., either folder doesn't exist or it just has
        # a few test tiles), the standard folder is downloaded.
        # This can happen despite it being a sensitivity run because this input file type doesn't have a sensitivity version
        # for this date.
        else:

            print_log(f'Source directory used: {source}')

            cmd = ['aws', 's3', 'cp', source, dest, '--no-sign-request', '--exclude', '*tiled/*',
                   '--exclude', '*geojason', '--exclude', '*vrt', '--exclude', '*csv', '--no-progress', '--recursive']
            # cmd = ['aws', 's3', 'cp', source, dest, '--no-sign-request', '--exclude', '*tiled/*',
            #        '--exclude', '*geojason', '--exclude', '*vrt', '--exclude', '*csv', '--recursive']
            log_subprocess_output_full(cmd)

            print_log("\n")

    # For the standard model, the standard folder is downloaded.
    else:

        # Counts how many tiles are in the source s3 folder
        if pattern == cn.pattern_data_lake:
            s3_count = count_tiles_s3(source, pattern=ec2_pattern)
            #print_log(f'There are {s3_count} tiles at {source} with the pattern {ec2_pattern}')
        else:
            s3_count = count_tiles_s3(source, pattern=pattern)
            print_log(f'There are {s3_count} tiles at {source} with the pattern {pattern}')

        # If there are as many tiles on the spot machine with the relevant pattern as there are on s3, no tiles are downloaded
        if local_tile_count == s3_count:
            print_log(f'Tiles with pattern {pattern} are already on spot machine. Not downloading.', "\n")
            return

        # Downloads tile sets from the gfw-data-lake.
        # They need a special process because they don't have a tile pattern on the data-lake,
        # so I have to download them into their own folder and then give them a pattern while moving them to the main folder
        if 'gfw-data-lake' in source:

            print_log(f'Downloading tiles with pattern {ec2_pattern}...')

            # Deletes special folder for downloads from data-lake (if it already exists)
            if os.path.exists(os.path.join(dest, 'data-lake-downloads')):
                os.rmdir(os.path.join(dest, 'data-lake-downloads'))

            # Special folder for the tile set that doesn't have a pattern when downloaded
            os.mkdir(os.path.join(dest, 'data-lake-downloads'))

            cmd = ['aws', 's3', 'cp', source, os.path.join(dest, 'data-lake-downloads'),
                   '--request-payer', 'requester', '--exclude', '*xml',
                   '--exclude', '*geojson', '--exclude', '*vrt', '--exclude', '*csv', '--no-progress', '--recursive']
            log_subprocess_output_full(cmd)

            # Copies pattern-less tiles from their special folder to main tile folder and renames them with
            # pattern along the way
            print_log("Copying tiles to main tile folder...")
            for filename in os.listdir(os.path.join(dest, 'data-lake-downloads')):
                move(os.path.join(dest, f'data-lake-downloads/{filename}'),
                     os.path.join(cn.docker_tile_dir, f'{filename[:-4]}_{ec2_pattern}.tif'))

            # Deletes special folder for downloads from data-lake
            os.rmdir(os.path.join(dest, 'data-lake-downloads'))
            print_log(f'data-lake tiles with pattern {ec2_pattern} copied to main tile folder...')

        # The --no-sign-request in the else statement below was causing the following error when trying to download the 1km drivers:
        # "An error occurred (AccessDenied) when calling the GetObject operation: Access Denied"
        #TODO update this when we move 1km drivers source to gfw-data-lake after API ingestion
        elif 'drivers_of_loss' in source:
            print_log(f'Tiles with pattern {pattern} are not on spot machine. Downloading...')

            cmd = ['aws', 's3', 'cp', source, dest, '--exclude', '*tiled/*',
                   '--exclude', '*geojson', '--exclude', '*vrt', '--exclude', '*csv', '--no-progress', '--recursive']

            log_subprocess_output_full(cmd)


        # Downloads non-data-lake inputs
        else:
            print_log(f'Tiles with pattern {pattern} are not on spot machine. Downloading...')

            cmd = ['aws', 's3', 'cp', source, dest, '--no-sign-request', '--exclude', '*tiled/*',
                   '--exclude', '*geojson', '--exclude', '*vrt', '--exclude', '*csv', '--no-progress', '--recursive']
            # cmd = ['aws', 's3', 'cp', source, dest, '--no-sign-request', '--exclude', '*tiled/*',
            #        '--exclude', '*geojson', '--exclude', '*vrt', '--exclude', '*csv', '--recursive']

            log_subprocess_output_full(cmd)

        print_log("\n")


# Downloads individual tiles from s3
# Source=source file on s3
# dest=where to download onto spot machine
# sensit_type = whether the model is standard or a sensitivity analysis model run
def s3_file_download(source, dest, sensit_type):

    # Retrieves the s3 directory and name of the tile from the full path name
    dir = get_tile_dir(source)
    file_name = get_tile_name(source)

    try:
        tile_id = get_tile_id(file_name)
    except:
        pass

    # Changes the file to download based on the sensitivity analysis being run and whether that particular input
    # has a sensitivity analysis path on s3.
    # Files that have standard and sensitivity analysis variants are handled differently from ones without variants
    # Hierarchy for getting tiles (start with #1, end with #4):
    # 1. Use sensitivity tile if already downloaded
    # 2. Download sensitivity if it exists
    # 3. Use standard tile if already downloaded
    # 4. Download standard tile if it exists
    if sensit_type != 'std' and 'standard' in dir:

        # Creates directory and file names according to sensitivity analysis type
        dir_sens = dir.replace('standard', sensit_type)
        file_name_sens = file_name[:-4] + '_' + sensit_type + '.tif'

        # Doesn't download the tile if sensitivity version is already on the spot machine
        print_log(f'Option 1: Checking if {file_name_sens} is already on spot machine...')
        if os.path.exists(file_name_sens):
            print_log(f'  Option 1 success: {file_name_sens} already downloaded', "\n")
            return
        else:
            print_log(f'  Option 1 failure: {file_name_sens} is not already on spot machine.')
            print_log(f'Option 2: Checking for sensitivity analysis tile {dir_sens[15:]}/{file_name_sens} on s3...')

            # If not already downloaded, first tries to download the sensitivity analysis version
            # cmd = ['aws', 's3', 'cp', '{0}/{1}'.format(dir_sens, file_name_sens), dest, '--no-sign-request', '--only-show-errors']
            cmd = ['aws', 's3', 'cp', '{0}/{1}'.format(dir_sens, file_name_sens), dest, '--only-show-errors']
            log_subprocess_output_full(cmd)

            if os.path.exists(file_name_sens):
                print_log(f'  Option 2 success: Sensitivity analysis tile {dir_sens}/{file_name_sens} found on s3 and downloaded', "\n")
                return
            else:
                print_log(f'  Option 2 failure: Tile {dir_sens}/{file_name_sens} not found on s3. Looking for standard model source...')


        # Next option is to use standard version of tile if on spot machine.
        # This can happen despite it being a sensitivity run because this input file doesn't have a sensitivity version
        # for this date.
        print_log(f'Option 3: Checking if standard version {file_name} is already on spot machine...')
        if os.path.exists(file_name):
            print_log(f'  Option 3 success: {file_name} already downloaded', "\n")
            return
        else:
            print_log(f'  Option 3 failure: {file_name} is not already on spot machine. ')
            print_log(f'Option 4: Looking for standard version of {file_name} to download...')

            # If not already downloaded, final option is to try to download the standard version of the tile.
            # If this doesn't work, the script throws a fatal error because no variant of this tile was found.
            # cmd = ['aws', 's3', 'cp', source, dest, '--no-sign-request', '--only-show-errors']
            cmd = ['aws', 's3', 'cp', source, dest, '--only-show-errors']
            log_subprocess_output_full(cmd)

            if os.path.exists(file_name):
                print_log(f'  Option 4 success: Standard tile {source} found on s3 and downloaded', "\n")
                return
            else:
                print_log(f'  Option 4 failure: Tile {source} not found on s3. Tile not found but it seems it should be. Check file paths and names.', "\n")

    # If not a sensitivity run or a tile type without sensitivity analysis variants, the standard file is downloaded

    # Special download procedures for gfw-data-lake datasets (tree cover gain, planted forests) because the tiles have no pattern, just an ID.
    # These tiles are renamed as they are downloaded to get a pattern added to them.
    else:
        if 'gfw-data-lake' in source:
            if dir == cn.gain_dir[:-1]: # Delete last character of gain_dir because it has the terminal / while dir does not have terminal /
                ec2_file_name = f'{tile_id}_{cn.pattern_gain_ec2}.tif'
            elif dir == cn.datalake_pf_agc_rf_dir[:-1]:
                ec2_file_name = f'{tile_id}_{cn.pattern_pf_rf_agc_ec2}.tif'
            elif dir == cn.datalake_pf_agcbgc_rf_dir[:-1]:
                ec2_file_name = f'{tile_id}_{cn.pattern_pf_rf_agcbgc_ec2}.tif'
            elif dir == cn.datalake_pf_agc_sd_dir[:-1]:
                ec2_file_name = f'{tile_id}_{cn.pattern_pf_sd_agc_ec2}.tif'
            elif dir == cn.datalake_pf_agcbgc_sd_dir[:-1]:
                ec2_file_name = f'{tile_id}_{cn.pattern_pf_sd_agcbgc_ec2}.tif'
            elif dir == cn.datalake_pf_simplename_dir[:-1]:
                ec2_file_name = f'{tile_id}_{cn.pattern_planted_forest_type}.tif'
            elif dir == cn.datalake_pf_estab_year_dir[:-1]:
                ec2_file_name = f'{tile_id}_{cn.pattern_planted_forest_estab_year}.tif'
            else:
                print_log(f'  Warning: {source} is located in the gfw-data-lake bucket but has not been assigned a file name pattern for download. Please update the constants_and_names.py file and the s3_file_download function in the universal_util.py file to include this dataset for download.')
                return
            gfw_data_lake_download(source, dest, dir, file_name, ec2_file_name)
            return


        # All other tiles besides gfw-data-lake datasets
        else:
            print_log(f'Option 1: Checking if {file_name} is already on spot machine...')
            if os.path.exists(os.path.join(dest, file_name)):
                print_log(f'  Option 1 success: {os.path.join(dest, file_name)} already downloaded', "\n")
                return
            else:
                print_log(f'  Option 1 failure: {file_name} is not already on spot machine.')
                print_log(f'Option 2: Checking for tile {source} on s3...')


                # If the tile isn't already downloaded, download is attempted
                source = os.path.join(dir, file_name)

                # cmd = ['aws', 's3', 'cp', source, dest, '--no-sign-request', '--only-show-errors']
                cmd = ['aws', 's3', 'cp', source, dest, '--only-show-errors']
                log_subprocess_output_full(cmd)
                if os.path.exists(os.path.join(dest, file_name)):
                    print_log(f'  Option 2 success: Tile {source} found on s3 and downloaded', "\n")
                    return
                else:
                    print_log(f'  Option 2 failure: Tile {source} not found on s3. Tile not found but it seems it should be. Check file paths and names.', "\n")

def gfw_data_lake_download(source, dest, dir, file_name, ec2_file_name):

    print_log(f'Option 1: Checking if {ec2_file_name} is already on spot machine...')

    if os.path.exists(os.path.join(dest, ec2_file_name)):
        print_log(f'  Option 1 success: {os.path.join(dest, ec2_file_name)} already downloaded', "\n")
        return
    else:
        print_log(f'  Option 1 failure: {ec2_file_name} is not already on spot machine.')
        print_log(f'Option 2: Checking for tile {source} on s3...')

        # If the tile isn't already downloaded, download is attempted
        source = os.path.join(dir, file_name)

        # cmd = ['aws', 's3', 'cp', source, dest, '--no-sign-request', '--only-show-errors']
        cmd = ['aws', 's3', 'cp', source, f'{dest}{ec2_file_name}',
               '--request-payer', 'requester', '--only-show-errors']

        log_subprocess_output_full(cmd)

        if os.path.exists(os.path.join(dest, ec2_file_name)):
            print_log(f'  Option 2 success: Tile {source} found on s3 and downloaded', "\n")
            return
        else:
            print_log(
                f'  Option 2 failure: Tile {source} not found on s3. Tile not found but it seems it should be. Check file paths and names.',
                "\n")

# Uploads all tiles of a pattern to specified location
def upload_final_set(upload_dir, pattern):

    print_log(f'Uploading tiles with pattern {pattern} to {upload_dir}')

    cmd = ['aws', 's3', 'cp', cn.docker_tile_dir, upload_dir, '--exclude', '*', '--include', '*{}*tif'.format(pattern),
           '--recursive', '--no-progress']
    try:
        log_subprocess_output_full(cmd)
        print_log(f'  Upload of tiles with {pattern} pattern complete!')
    except:
        print_log('Error uploading output tile(s)')

    # Uploads the log as each model output tile set is finished
    upload_log()


# Uploads tile to specified location
def upload_final(upload_dir, tile_id, pattern):

    file = '{}_{}.tif'.format(tile_id, pattern)

    print_log("Uploading {}".format(file))
    # cmd = ['aws', 's3', 'cp', file, upload_dir, '--no-sign-request', '--no-progress']
    cmd = ['aws', 's3', 'cp', file, upload_dir, '--no-progress']

    try:
        log_subprocess_output_full(cmd)
    except:
        print_log('Error uploading output tile')


# This version of checking for data is bad because it can miss tiles that have very little data in them.
# But it takes less memory than using rasterio, so it's good for local tests.
# This method creates a tif.aux.xml file that I tried to add a line to delete but couldn't get to work.
def check_and_delete_if_empty_light(tile_id, output_pattern):

    tile_name = f'{tile_id}_{output_pattern}.tif'

    # Source: http://gis.stackexchange.com/questions/90726
    # Opens raster and chooses band to find min, max
    gtif = gdal.Open(tile_name)
    srcband = gtif.GetRasterBand(1)
    stats = srcband.GetStatistics(True, True)
    print_log("  Tile stats =  Minimum=%.3f, Maximum=%.3f, Mean=%.3f, StdDev=%.3f" % (stats[0], stats[1], stats[2], stats[3]))

    if stats[0] != 0:
        print_log(f'  Data found in {tile_name}. Keeping file...')
    else:
        print_log(f'  Data not found in {tile_name}. Deleting...')
        os.remove(tile_name)

    # Using this gdal data check method creates a tif.aux.xml file that is unnecessary.
    # This does not work, however; it returns an error that there is no such file or directory.
    # os.remove("{0}{1}.aux.xml".format(cn.docker_base_dir, tile_name))


# This version of checking for data in a tile is more robust
def check_for_data(tile):
    with rasterio.open(tile) as img:
        msk = img.read_masks(1).astype(bool)
    if msk[msk].size == 0:
        # print_log(f"Tile {tile} is empty")
        return True
    else:
        # print_log(f"Tile {tile} is not empty")
        return False


def check_and_delete_if_empty(tile_id, output_pattern):

    tile_name = f'{tile_id}_{output_pattern}.tif'

    # Only checks for data if the tile exists
    if not os.path.exists(tile_name):
        print_log(f'{tile_name} does not exist. Skipping check of whether there is data.')
        return

    print_log(f'Checking if {tile_name} contains any data...')
    no_data = check_for_data(tile_name)

    if no_data:
        print_log(f'  Data not found in {tile_name}. Deleting...')
        os.remove(tile_name)
    else:
        print_log(f'  Data found in {tile_name}. Keeping tile to copy to s3...')


# Checks if there's data in a tile and, if so, uploads it to s3
def check_and_upload(tile_id, upload_dir, pattern):

    print_log(f'Checking if {tile_id} contains any data...')
    out_tile = f'{tile_id}_{pattern}.tif'

    no_data = check_for_data(out_tile)

    if no_data:

        print_log(f'  Data not found in {tile_id}. Not copying to s3...')

    else:

        print_log(f'  Data found in {tile_id}. Copying tile to s3...')
        upload_final(upload_dir, tile_id, pattern)
        print_log('    Tile copied to s3')


# Prints the number of tiles that have been processed so far
def count_completed_tiles(pattern):

    completed = len(glob.glob1(cn.docker_tile_dir, '*{}*'.format(pattern)))

    print_log(f'Number of completed or in-progress tiles: {completed}')


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

    # Checking memory at this point (end of the function) seems to record memory usage when it is at its peak
    check_memory()

    end = datetime.datetime.now()
    elapsed_time = end-start
    print_log(f'Processing time for tile {tile_id}: {elapsed_time}')

    count_completed_tiles(pattern)

    # If no_upload flag is not activated, log is uploaded
    if not cn.NO_UPLOAD:
        # Uploads the log as each tile is finished
        upload_log()


# Warps raster to Hansen tiles using multiple processors
def mp_warp_to_Hansen(tile_id, source_raster, out_pattern, dt):

    # Start time
    start = datetime.datetime.now()

    print_log("Getting extent of", tile_id)
    xmin, ymin, xmax, ymax = coords(tile_id)

    #Uses rewindowed tiles from docker container with same tile_id and same out_pattern if no source raster is given
    if source_raster == None:
        source_raster = f'{tile_id}_{out_pattern}_rewindow.tif'

    out_tile = f'{tile_id}_{out_pattern}.tif'

    # If it's the drivers raster, extract band 1 first
    if source_raster == cn.pattern_drivers_raw:
        tmpfile = tempfile.NamedTemporaryFile(suffix='.tif', delete=False).name
        translate_cmd = ['gdal_translate', '-b', '1', '-co', 'COMPRESS=LZW', '-co', 'TILED=YES', source_raster, tmpfile]
        run(translate_cmd, check=True)
        source_raster = tmpfile

    cmd = ['gdalwarp', '-t_srs', 'EPSG:4326', '-co', 'COMPRESS=DEFLATE', '-tr', str(cn.Hansen_res), str(cn.Hansen_res), '-tap', '-te',
            str(xmin), str(ymin), str(xmax), str(ymax), '-dstnodata', '0', '-ot', dt, '-overwrite', source_raster, out_tile]

    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        log_subprocess_output(process.stdout)

    if source_raster == tmpfile:
        os.remove(tmpfile)

    end_of_fx_summary(start, tile_id, out_pattern)


def warp_to_Hansen(in_file, out_file, xmin, ymin, xmax, ymax, dt):

    # If it's the drivers raster, extract band 1
    if in_file == cn.pattern_drivers_raw:
        tmpfile = tempfile.NamedTemporaryFile(suffix='.tif', delete=False).name
        translate_cmd = ['gdal_translate', '-b', '1', '-co', 'COMPRESS=LZW', '-co', 'TILED=YES', in_file, tmpfile]
        run(translate_cmd, check=True)
        in_file = tmpfile
    else:
        tmpfile = None

    cmd = ['gdalwarp', '-t_srs', 'EPSG:4326', '-co', 'COMPRESS=DEFLATE', '-tr', str(cn.Hansen_res), str(cn.Hansen_res), '-tap', '-te',
            str(xmin), str(ymin), str(xmax), str(ymax), '-dstnodata', '0', '-ot', dt, '-overwrite', in_file, out_file]

    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        log_subprocess_output(process.stdout)

    if tmpfile:
        os.remove(tmpfile)

# Rasterizes the shapefile within the bounding coordinates of a tile
def rasterize(in_shape, out_tif, xmin, ymin, xmax, ymax, blocksizex, blocksizey, tr=None, ot=None, name_field=None, anodata=None):
    cmd = ['gdal_rasterize', '-co', 'COMPRESS=DEFLATE',

           # Input raster is ingested as 1024x1024 pixel tiles (rather than the default of 1 pixel wide strips
           '-co', 'TILED=YES', '-co', 'BLOCKXSIZE={}'.format(blocksizex), '-co', 'BLOCKYSIZE={}'.format(blocksizey),
           '-te', str(xmin), str(ymin), str(xmax), str(ymax),
           '-tr', tr, tr, '-ot', ot, '-a', name_field, '-a_nodata',
           anodata, in_shape, out_tif]
    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        log_subprocess_output(process.stdout)

    return out_tif


# Creates a tile of all 0s for any tile passed to it.
# Uses the Hansen loss tile for information about the tile.
# Based on https://gis.stackexchange.com/questions/220753/how-do-i-create-blank-geotiff-with-same-spatial-properties-as-existing-geotiff
def make_blank_tile(tile_id, pattern, folder):

    # Creates tile names for standard and sensitivity analyses.
    # Going into this, the function doesn't know whether there should be a standard tile or a sensitivity tile.
    # Thus, it has to be prepared for either one.
    file_name = f'{folder}{tile_id}_{pattern}.tif'
    file_name_sens = f'{folder}{tile_id}_{pattern}_{cn.SENSIT_TYPE}.tif'

    # Checks if the standard file exists. If it does, a blank tile isn't created.
    if os.path.exists(file_name):
        print_log(f'{os.path.join(folder, file_name)} exists. Not creating a blank tile.')
        return

    # Checks if the sensitivity analysis file exists. If it does, a blank tile isn't created.
    elif os.path.exists(file_name_sens):
        print_log(f'{os.path.join(folder, file_name_sens)} exists. Not creating a blank tile.')
        return

    # If neither a standard tile nor a sensitivity analysis tile exists, a blank tile is created.
    else:
        print_log(f'{file_name} does not exist. Creating a blank tile.')

        with open(os.path.join(cn.docker_tmp, cn.blank_tile_txt), 'a') as f:
            f.write('{0}_{1}.tif'.format(tile_id, pattern))
            f.write("\n")
            f.close()

        # Preferentially uses Hansen loss tile as the template for creating a blank plantation tile
        # (tile extent, resolution, pixel alignment, compression, etc.).
        # If the tile is already on the spot machine, it uses the downloaded tile.
        if os.path.exists(os.path.join(folder, f'{cn.pattern_loss}_{tile_id}.tif')):
            print_log(f'Hansen loss tile exists for {tile_id}. Using that as template for blank tile.')
            cmd = ['gdal_merge.py', '-createonly', '-init', '0', '-co', 'COMPRESS=DEFLATE', '-ot', 'Byte',
                   '-o', '{0}{1}_{2}.tif'.format(folder, tile_id, pattern),
                   '{0}{1}_{2}.tif'.format(folder, cn.pattern_loss, tile_id)]
            check_call(cmd)

        # If the Hansen loss tile isn't already on the spot machine
        else:

            s3_file_download('{0}{1}_{2}.tif'.format(cn.pixel_area_dir, cn.pattern_pixel_area, tile_id),
                             os.path.join(folder, '{0}_{1}.tif'.format(tile_id, 'empty_tile_template')), 'std')
            print_log(f'Downloaded pixel area tile for {tile_id} to create a blank tile')

            # Determines what pattern to use (standard or sensitivity) based on the first tile in the list
            tile_list= tile_list_spot_machine(folder, pattern)
            full_pattern = get_tile_type(tile_list[0])

            # Uses either the Hansen loss tile or pixel area tile as a template tile,
            # with the output name corresponding to the model type
            cmd = ['gdal_merge.py', '-createonly', '-init', '0', '-co', 'COMPRESS=DEFLATE', '-ot', 'Byte',
                   '-o', '{0}/{1}_{2}.tif'.format(folder, tile_id, full_pattern),
                   '{0}/{1}_{2}.tif'.format(folder, tile_id, 'empty_tile_template')]
            check_call(cmd)
            print_log(f'Created raster of all 0s for {file_name}')


# Creates a txt that will have blank dummy tiles listed in it for certain scripts that need those
def create_blank_tile_txt():

    blank_tiles = open(os.path.join(cn.docker_tmp, cn.blank_tile_txt), "wb")
    blank_tiles.close()


# Delete all blank tiles and the txt that listed them
def list_and_delete_blank_tiles():

    blank_tiles_list = open(os.path.join(cn.docker_tmp, cn.blank_tile_txt)).read().splitlines()
    print_log(f'Blank tile list: {blank_tiles_list}')

    print_log('Deleting blank tiles...')
    for blank_tile in blank_tiles_list:
        os.remove(blank_tile)

    print_log('Deleting blank tile textfile...')
    os.remove(os.path.join(cn.docker_tmp, cn.blank_tile_txt))


# Reformats the patterns for the 10x10 degree model output tiles for the aggregated output names
def name_aggregated_output(pattern):

    # print(pattern)
    out_pattern = re.sub('ha_', '', pattern)
    # print(out_pattern)
    out_pattern = re.sub(f'2001_{cn.loss_years}', '', out_pattern)
    # print(out_pattern)
    out_pattern = re.sub('_Mg_', '_Mt_per_year_', out_pattern)
    # print(out_pattern)
    out_pattern = re.sub('all_drivers_Mt_CO2e', 'all_drivers_Mt_CO2e_per_year', out_pattern)
    # print(out_pattern)
    date = datetime.datetime.now()
    date_formatted = date.strftime("%Y%m%d")

    out_name = f'{out_pattern}_tcd{cn.canopy_threshold}_{cn.pattern_aggreg}_{cn.SENSIT_TYPE}_{date_formatted}'
    # print(out_name)

    return out_name


# Checks whether the provided sensitivity analysis type is valid
def check_sensit_type(sensit_type):

    # Checks the validity of the two arguments. If either one is invalid, the script ends.
    if (sensit_type not in cn.sensitivity_list):
        exception_log(f'Invalid model type. Please provide a model type from {cn.sensitivity_list}.')
    else:
        pass


# Changes the name of the input or output directory according to the sensitivity analysis
def alter_dirs(sensit_type, raw_dir_list):

    print_log(f'Raw output directory list: {raw_dir_list}')

    processed_dir_list = [d.replace('standard', sensit_type) for d in raw_dir_list]

    print_log(f'Processed output directory list: {processed_dir_list}', "\n")
    return processed_dir_list


# Alters the file patterns in a list according to the sensitivity analysis
def alter_patterns(sensit_type, raw_pattern_list):

    print_log(f'Raw output pattern list: {raw_pattern_list}')

    processed_pattern_list = [(d + '_' + sensit_type) for d in raw_pattern_list]

    print_log(f'Processed output pattern list: {processed_pattern_list}', "\n")
    return processed_pattern_list


# Creates the correct input tile name for processing based on the sensitivity analysis being done
def sensit_tile_rename(sensit_type, tile_id, raw_pattern):

    # Uses whatever name of the tile is found on the spot machine
    if os.path.exists(f'{tile_id}_{raw_pattern}_{sensit_type}.tif'):
        processed_name = f'{tile_id}_{raw_pattern}_{sensit_type}.tif'
    else:
        processed_name = f'{tile_id}_{raw_pattern}.tif'

    return processed_name

# Creates the correct input biomass tile name for processing based on the sensitivity analysis being done.
# Because there are actual different input biomass tiles, this doesn't fit well within sensit_tile_rename().
def sensit_tile_rename_biomass(sensit_type, tile_id):

    if cn.SENSIT_TYPE == 'biomass_swap':
        natrl_forest_biomass_2000 = f'{tile_id}_{cn.pattern_JPL_unmasked_processed}.tif'
        print_log(f'Using JPL biomass tile {tile_id} for {sensit_type} sensitivity analysis')
    else:
        natrl_forest_biomass_2000 = f'{tile_id}_{cn.pattern_WHRC_biomass_2000_unmasked}.tif'
        print_log(f'Using WHRC biomass tile {tile_id} for {sensit_type} model run')

    return natrl_forest_biomass_2000

# Determines what stages should actually be run
def analysis_stages(stage_list, stage_input, run_through, sensit_type,
                    include_mangroves = None, include_us = None):

    # If user wants all stages, all named stages (i.e. everything except 'all') are returned
    if stage_input == 'all':

        stage_output = stage_list[1:]

    # If the user selected a specific stage, the run_through argument is evaluated
    else:

        # If the user wants to run through all stages after the selected one, a new list is created
        if run_through:

            stage_output = stage_list[stage_list.index(stage_input):]

        # If the user wants only the named stage, only that is returned
        else:

            stage_output = stage_input.split()

    # Flags to include mangrove forest removal rates and US-specific removal rates in the stages to run
    if include_us:
        stage_output.insert(0, 'annual_removals_us')

    if include_mangroves:
        stage_output.insert(0, 'annual_removals_mangrove')

    # Step create_supplementary_outputs only run for standard model
    if sensit_type != 'std':
        stage_output.remove('create_supplementary_outputs')

    return stage_output


# Checks whether the tile ids provided are valid
def tile_id_list_check(tile_id_list):

    if tile_id_list == 'all':
        print_log('All tiles will be run through model. Actual list of tiles will be listed for each model stage as it begins...')
        return tile_id_list
    # Checks tile id list input validity against the pixel area tiles
    else:
        tile_id_list = list(tile_id_list.split(","))

        creds = check_aws_creds()

        # Stops checking tiles list against s3 if connection to s3 is disabled
        if not creds:
            return tile_id_list

        # Continues to check submitted tile list against s3 if connection to s3 is enabled
        possible_tile_list = tile_list_s3(cn.pixel_area_dir)

        for tile_id in tile_id_list:
            if tile_id not in possible_tile_list:
                exception_log(f'Tile_id {tile_id} not valid')
        else:
            print_log(f'{str(len(tile_id_list))} tiles have been supplied for running through the model', "\n")
            return tile_id_list


# Replaces the date specified in constants_and_names with the date provided by the model run-through
def replace_output_dir_date(output_dir_list, run_date):

    print_log('Changing output directory date based on date provided with model run-through')
    output_dir_list = [output_dir.replace(output_dir[-9:-1], run_date) for output_dir in output_dir_list]
    print_log(output_dir_list, "\n")
    return output_dir_list


# Adds various metadata tags to the raster
def add_universal_metadata_rasterio(output_dst):

    # based on https://rasterio.readthedocs.io/en/latest/topics/tags.html

    if cn.SENSIT_TYPE == 'std':
        sensit_type = 'standard model'
    else:
        sensit_type = cn.SENSIT_TYPE

    output_dst.update_tags(
        model_version=cn.version)
    output_dst.update_tags(
        date_created=date_today)
    output_dst.update_tags(
        model_type=sensit_type)
    output_dst.update_tags(
        originator='Global Forest Watch at the World Resources Institute')
    output_dst.update_tags(
        citation='Harris et al. 2021 Nature Climate Change https://www.nature.com/articles/s41558-020-00976-6')
    output_dst.update_tags(
        model_year_range=f'2001 through 20{cn.loss_years}'
    )

    return output_dst


def add_universal_metadata_gdal(output_raster):

    print_log("Adding universal metadata tags to", output_raster)

    cmd = ['gdal_edit.py',
           '-mo', f'model_version={cn.version}',
           '-mo', f'date_created={date_today}',
           '-mo', f'model_type={cn.SENSIT_TYPE}',
           '-mo', 'originator=Global Forest Watch at the World Resources Institute',
           '-mo', f'model_year_range=2001 through 20{cn.loss_years}',
           output_raster]
    log_subprocess_output_full(cmd)


# Adds metadata tags to the output rasters
def add_emissions_metadata(tile_id, output_pattern):

    # Adds metadata tags to output rasters
    add_universal_metadata_gdal(f'{tile_id}_{output_pattern}.tif')

    cmd = ['gdal_edit.py', '-mo',
           f'units=Mg CO2e/ha over model duration (2001-20{cn.loss_years})',
           '-mo', 'source=many data sources',
           '-mo', 'extent=Tree cover loss pixels within model extent (and tree cover loss driver, if applicable)',
           f'{tile_id}_{output_pattern}.tif']
    log_subprocess_output_full(cmd)


# Converts 10x10 degree Hansen tiles that are in windows of 40000x1 pixels to windows of 160x160 pixels,
# which is the resolution of the output tiles. This allows the 30x30 m pixels in each window to be summed
# into 0.04x0.04 degree rasters.
def rewindow(tile_id, download_pattern_name, striped = False):

    # start time
    start = datetime.datetime.now()

    # These tiles have the tile_id after the pattern
    if download_pattern_name in [cn.pattern_pixel_area, cn.pattern_tcd, cn.pattern_loss]:
        in_tile = f'{download_pattern_name}_{tile_id}.tif'
        out_tile = f'{download_pattern_name}_rewindow_{tile_id}.tif'

    else:
        in_tile = f'{tile_id}_{download_pattern_name}.tif'
        out_tile = f'{tile_id}_{download_pattern_name}_rewindow.tif'

    check_memory()

    # Only rewindows if the rewindowed tile does not already exist in the docker container
    #if os.path.exists(out_tile):
    #    print_log(f'{out_tile} exists. No need to rewindow')
    #    return

    # Only rewindows if the tile exists
    if os.path.exists(in_tile):

        # Just using gdalwarp inflated the output rasters about 10x, even with COMPRESS=LZW.
        # Solution was to use gdal_translate instead, although, for unclear reasons, this still inflates the size
        # of the pixel area tiles but not other tiles using LZW. DEFLATE makes all outputs smaller.
        if striped == False:
            cmd = ['gdal_translate', '-co', 'COMPRESS=DEFLATE', '-co', 'TILED=YES',
                   '-co', 'BLOCKXSIZE={}'.format(cn.agg_pixel_window), '-co', 'BLOCKYSIZE={}'.format(cn.agg_pixel_window),
                   in_tile, out_tile]
            log_subprocess_output_full(cmd)
            print_log(f'{in_tile} exists. Rewindowing to {out_tile} with {cn.agg_pixel_window}x{cn.agg_pixel_window} pixel windows...')

        elif striped == True:
            cmd = ['gdal_translate', '-co', 'COMPRESS=DEFLATE', '-co', 'TILED=NO',
                   in_tile, out_tile]
            log_subprocess_output_full(cmd)
            print_log(f'{in_tile} exists. Rewindowing to {out_tile} with 40000x1 pixel windows...')


    else:
        print_log(f'{in_tile} does not exist. Not rewindowing')

    # Prints information about the tile that was just processed
    end_of_fx_summary(start, tile_id, "{}_rewindow".format(download_pattern_name))


