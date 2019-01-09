### Creates tiles in which each pixel is a combination of the continent and FAO FRA 2000 ecozone.
### The tiles are based on a shapefile which combines the FAO FRA 2000 ecozone shapefile and a continent shapefile.
### The FAO FRA 2000 shapefile is from http://www.fao.org/geonetwork/srv/en/resources.get?id=1255&fname=eco_zone.zip&access=private
### The continent shapefile is from https://www.baruch.cuny.edu/confluence/display/geoportal/ESRI+International+Data
### Various processing steps in ArcMap were used to make sure that the entirety of the ecozone shapefile had
### continents assigned to it. The creation of the continent-ecozone shapefile was done in ArcMap.
### In the resulting ecozone-continent shapefile, the final field has continent and ecozone concatenated.
### That ecozone-continent field can be parsed to get the ecozone and continent for every pixel,
### which are necessary for assigning gain rates to pixels.
### This script also breaks the input tiles into windows that are 1024 pixels on each side and assigns all pixels that
### don't have a continent-ecozone code to the most common code in that window.
### This is done to expand the extent of the continent-ecozone tiles to include pixels that don't have a continent-ecozone
### code because they are just outside the original shapefile.
### It is necessary to expand the continent-ecozone codes into those nearby areas because otherwise some forest age category
### pixels are outside the continent-ecozone pixels and can't have gain rates assigned to them.
### This maneuver provides the necessary continent-ecozone information to assign gain rates.


import multiprocessing
import utilities
import continent_ecozone_tiles
import subprocess
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Folder that stores the completed output tiles that have already been copied to s3
os.mkdir("already_copied")

# Downloads ecozone shapefile
utilities.s3_file_download('{}'.format(cn.cont_eco_s3_zip), '.', )

# Unzips ecozone shapefile
cmd = ['unzip', cn.cont_eco_zip]
subprocess.check_call(cmd)

biomass_tile_list = uu.create_combined_biomass_tile_list(cn.natrl_forest_biomass_2000_dir, cn.mangrove_biomass_2000_dir)
# biomass_tile_list = ["00N_000E", "00N_050W", "00N_060W", "00N_010E", "00N_020E", "00N_030E", "00N_040E", "10N_000E", "10N_010E", "10N_010W", "10N_020E", "10N_020W"] # test tiles
# biomass_tile_list = ['20S_110E'] # test tile
print biomass_tile_list
print "There are {} tiles to process".format(str(len(biomass_tile_list)))

count = multiprocessing.cpu_count()
cores = count / 3
pool = multiprocessing.Pool(processes=cores)

# How many tiles the spot machine will process at one time
tiles_in_chunk = cores

for chunk in uu.chunks(biomass_tile_list, tiles_in_chunk):

    print "Chunk is:", str(chunk)

    # # For multiprocessor use
    # pool.map(continent_ecozone_tiles.create_continent_ecozone_tiles, biomass_tile_list)
    # pool.close()
    # pool.join()
    #
    # # Uploads the continent-ecozone tile to s3 before the codes are expanded to pixels in 1024x1024 windows that don't have codes.
    # # These are not used for the model. They are for reference and completeness.
    # uu.upload_chunk_set(cn.cont_eco_raw_dir, cn.pattern_cont_eco_raw)
    #
    # # Uploads all processed tiles at the end
    # uu.upload_chunk_set(cn.cont_eco_dir, cn.pattern_cont_eco_processed)
    #
    # # Moves the completed tiles that have been saved to s3 out of the folder
    # os.system('''mv *_{}.tif'''.format(cn.pattern_cont_eco_raw))
    # os.system('''mv *_{}.tif'''.format(cn.pattern_cont_eco_processed))



# # For single processor use
# for tile in biomass_tile_list:
#
#     continent_ecozone_tiles.create_continent_ecozone_tiles(tile)