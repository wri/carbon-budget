import os
import subprocess


def download_ba(hv_tile):
    # creates a folder here called "ba_h00v08" and downloads all available
    # burned area files (MCD64A1.A2004245.h00v08.006.2017016101006.hdf) then moves them to s3

    ftp_path = 'ftp://fuoco.geog.umd.edu/MCD64A1/C6/{0}/'.format(hv_tile)
    outfolder = "ba_{0}/".format(hv_tile)
    if not os.path.exists(outfolder):
        os.makedirs(outfolder)
        
    file_name = "*.hdf"
    cmd = ['wget', '-r', '--ftp-user=fire', '--ftp-password=burnt', '-A', file_name, '--no-directories', '--no-parent', ftp_path, '-P', outfolder]
    
    subprocess.check_call(cmd)

    # s3_burn_raw = 's3://gfw-files/sam/carbon_budget/burn_raw/'    # previous location
    s3_burn_raw = 's3://gfw2-data/climate/carbon_model/other_emissions_inputs/burn_year/raw_hdf/'

    cmd = ['aws', 's3', 'mv', outfolder, s3_burn_raw, '--recursive']
    subprocess.check_call(cmd)