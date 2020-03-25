'''
This script makes mask tiles of where peat pixels are. Peat is represented by 1s; non-peat is no-data.
Between 40N and 60S, CIFOR peat and Jukka peat (IDN and MYS) are combined to map peat.
Outside that band (>40N, since there are no tiles at >60S), SoilGrids250m is used to mask peat.
Any pixel that is marked as most likely being a histosol subgroup is classified as peat.
Between 40N and 60S, SoilGrids250m is not used.
'''


import multiprocessing
import peatland_processing
import sys
import os
import subprocess
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

os.chdir(cn.docker_base_dir)

# The argument for what kind of model run is being done: standard conditions or a sensitivity analysis run
parser = argparse.ArgumentParser(description='Create tiles of the number of years of carbon gain for mangrove forests')
parser.add_argument('--model-type', '-t', required=True,
                    help='{}'.format(cn.model_type_arg_help))
args = parser.parse_args()
sensit_type = args.model_type
# Checks whether the sensitivity analysis argument is valid
uu.check_sensit_type(sensit_type)


# Iterates through all tiles with aboveground carbon pool emissions (not just WHRC biomass tiles)
tile_list = uu.tile_list_s3(cn.AGC_emis_year_dir)
# tile_list = ['60N_020E', '70N_070E'] # test tiles
# tile_list = ['60N_020E', '00N_020E', '30N_080W', '00N_110E'] # test tiles
print(tile_list)
print("There are {} tiles to process".format(str(len(tile_list))))

# Downloads peat layers
uu.s3_file_download(os.path.join(cn.peat_unprocessed_dir, cn.cifor_peat_file), cn.docker_base_dir, sensit_type)
uu.s3_file_download(os.path.join(cn.peat_unprocessed_dir, cn.jukka_peat_zip), cn.docker_base_dir, sensit_type)
uu.s3_file_download(os.path.join(cn.peat_unprocessed_dir, cn.soilgrids250_peat_file), cn.docker_base_dir, sensit_type) # Raster of the most likely soil group

# Unzips the Jukka peat shapefile (IDN and MYS)
cmd = ['unzip', '-o', '-j', cn.jukka_peat_zip]
subprocess.check_call(cmd)

jukka_tif = 'jukka_peat.tif'

# Converts the Jukka peat shapefile to a raster
cmd= ['gdal_rasterize', '-burn', '1', '-co', 'COMPRESS=LZW', '-tr', '{}'.format(cn.Hansen_res), '{}'.format(cn.Hansen_res),
      '-tap', '-ot', 'Byte', '-a_nodata', '0', cn.jukka_peat_shp, jukka_tif]

subprocess.check_call(cmd)

# For multiprocessor use
# This script uses about 80 GB memory max, so an r4.16xlarge is big for it.
pool = multiprocessing.Pool(processes=count-10)
pool.map(peatland_processing.create_peat_mask_tiles, tile_list)

# # For single processor use, for testing purposes
# for tile in tile_list:
#
#     peatland_processing.create_peat_mask_tiles(tile)

print("Uploading output files")
uu.upload_final_set(cn.peat_mask_dir, '{}'.format(cn.pattern_peat_mask))