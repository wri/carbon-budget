import os
from subprocess import Popen, PIPE, STDOUT, check_call
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu


def download_ba(hv_tile):
    # creates a folder here called ex: "ba_h00v08" and downloads all available
    # burned area files (MCD64A1.A2004245.h00v08.006.2017016101006.hdf) then moves them to s3

    ftp_path = '{}*'.format(cn.burn_area_raw_ftp)

    # use wget to download the burned area tiles from ftp site
    file_name = "*.hdf"
    cmd = ['wget', '-r', '--ftp-user=user', '--ftp-password=burnt_data', '-A', file_name]
    cmd += ['--no-directories', '--no-parent', ftp_path]
    uu.log_subprocess_output_full(cmd)

    cmd = ['aws', 's3', 'cp', '.', cn.burn_year_hdf_raw_dir, '--recursive', '--exclude', '*', '--include', '*hdf']
    uu.log_subprocess_output_full(cmd)
