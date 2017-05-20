import glob
import subprocess

file_to_upload = glob.glob("60N_100E*")

for file in file_to_upload:
    cmd = ['aws', 's3', 'cp', file, 's3://gfw-files/sam/carbon_budget/']
    subprocess.check_call(cmd)
