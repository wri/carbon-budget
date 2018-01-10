import subprocess
import glob
import os

def calc_all_inputs_exist(tile_id):
    inputs_list = [
    's3://WHRC-carbon/global_27m_tiles/final_global_27m_tiles/biomass_10x10deg/',
    's3://gfw-files/sam/carbon_budget/data_inputs2/soil/',
    's3://gfw-files/sam/carbon_budget/data_inputs2/fao_ecozones_bor_tem_tro/',
    's3://gfw-files/sam/carbon_budget/data_inputs2/srtm/',
    's3://gfw-files/sam/carbon_budget/data_inputs2/precip/',
    ]

    for input in inputs_list:
        cmd = ['aws', 's3', 'cp', input, '.', '--recursive', '--exclude' , '*', '--include', "*{}*".format(tile_id)]
        subprocess.check_call(cmd)

    print 'writing carbon, bgc, deadwood, litter, total'
    calc_all_cmd = ['./calc_all.exe', tile_id]
    subprocess.check_call(calc_all_cmd)

    print 'uploading tiles to s3'
    tile_types = ['carbon', 'bgc', 'deadwood', 'litter', 'soil', 'total_carbon']
    for tile in tile_types:
       if tile == 'total_carbon':
           tile_name = "{}_totalc.tif".format(tile_id)
       else:
           tile_name = "{0}_{1}.tif".format(tile_id, tile)
           
       tile_dest = 's3://gfw-files/sam/carbon_budget/carbon_011018/{}/'.format(tile)

       upload_tile = ['aws', 's3', 'cp', tile_name, tile_dest]
       print upload_tile
       subprocess.check_call(upload_tile)

    print "deleting intermediate data"
    intermediate_data = glob.glob('{}*'.format(tile_id))
    for file in intermediate_data:
        os.remove(file)

