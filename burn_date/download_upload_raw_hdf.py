import os
import subprocess
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu


def download_ba(hv_tile):
    # creates a folder here called ex: "ba_h00v08" and downloads all available
    # burned area files (MCD64A1.A2004245.h00v08.006.2017016101006.hdf) then moves them to s3

    ftp_path = 'ftp://ba1.geog.umd.edu/Collection6/HDF/2018/*'
    outfolder = "ba_h00v08/"
    if not os.path.exists(outfolder):
        os.makedirs(outfolder)

    # use wget to download the burned area tiles from ftp site
    file_name = "*.hdf"
    cmd = ['wget', '-r', '--ftp-user=user', '--ftp-password=burnt_data', '-A', file_name]
    cmd += ['--no-directories', '--no-parent', ftp_path, '-P', outfolder]

    # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        uu.log_subprocess_output(process.stdout)

    s3_burn_raw = 's3://gfw2-data/climate/carbon_model/other_emissions_inputs/burn_year/20190322/raw_hdf/'

    cmd = ['aws', 's3', 'mv', outfolder, s3_burn_raw, '--recursive']

    # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        uu.log_subprocess_output(process.stdout)
