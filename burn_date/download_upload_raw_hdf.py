import os
import subprocess
import glob

def download_ba(global_grid_hv):
    ftp_path = 'ftp://fuoco.geog.umd.edu/MCD64A1/C6/{0}/'.format(global_grid_hv)
    outfolder = "ba_{0}/".format(global_grid_hv)
    if not os.path.exists(outfolder):
        os.makedirs(outfolder)
        
    file_name = "*.hdf"
    cmd = ['wget', '-r', '--ftp-user=fire', '--ftp-password=burnt', '-A', file_name, '--no-directories', '--no-parent', ftp_path, '-P', outfolder]
    
    subprocess.check_call(cmd)

    s3_burn_raw = 's3://gfw-files/sam/carbon_budget/burn_raw/'

    cmd = ['aws', 's3', 'mv', outfolder, s3_burn_raw, '--recursive']
    subprocess.check_call(cmd)