# file to copy down
import subprocess

file_to_copy = ['_res_forest_model', '_bgc', '_carbon', '_loss', '_res_peatland_drainage_proj', '_res_hwsd_histosoles', '_res_fao_ecozones_bor_tem_tro', '_res_climate_zone', '_deadwood', '_litter', '_soil']

for file in file_to_copy:
    filename = '{0}{1}.tif'.format(tile_id, file)
    s3_file = 's3://gfw-files/sam/carbon_budget/'.format(filename)
    cmd = ['aws', 's3', 'cp', s3_file, '.']
    subprocess.check_call(cmd)
    