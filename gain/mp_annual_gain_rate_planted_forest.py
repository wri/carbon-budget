### This script assigns annual above and belowground biomass gain rates (in the units of IPCC Table 4.9 (currently tonnes
### biomass/ha/yr)) to non-mangrove planted forest pixels. It masks mangrove pixels from the planted forest carbon gain
### rate tiles so that different forest types are non-overlapping.
### To calculate the aboveground and belowground biomass gain rates from above+belowground carbon gain rate, the
### script uses the IPCC default natural forest values. Although these values don't actually apply to planted forests,
### they are the best we have for parsing planted forests into the component values.
### We want to separate the above+below rate into above and below and convert to biomass so that we can make global
### maps of annual above and below biomass gain rates separately; the natural forests and mangroves already use
### separate above and below annual biomass gain rate files, so this brings planted forests into line with them.


import multiprocessing
import annual_gain_rate_planted_forest
import pandas as pd
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

pd.options.mode.chained_assignment = None

tile_list = uu.tile_list_s3(cn.annual_gain_AGC_BGC_planted_forest_unmasked_dir)
# tile_list = ['80N_020E', '00N_000E', '00N_020E', '00N_110E'] # test tiles: no mangrove or planted forest, mangrove only, planted forest only, mangrove and planted forest
tile_list = ['00N_110E'] # test tiles: mangrove and planted forest
print tile_list
print "There are {} tiles to process".format(str(len(tile_list))) + "\n"

# For downloading all tiles in the input folders
download_list = [cn.annual_gain_AGC_BGC_planted_forest_unmasked_dir,
                 cn.mangrove_biomass_2000_dir,
                 cn.plant_pre_2000_processed_dir]

# for input in download_list:
#     uu.s3_folder_download(input, '.')

# For copying individual tiles to spot machine for testing
for tile in tile_list:

    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.annual_gain_AGC_BGC_planted_forest_unmasked_dir, tile, cn.pattern_annual_gain_AGC_BGC_planted_forest_unmasked), '.')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.mangrove_biomass_2000_dir, tile, cn.pattern_mangrove_biomass_2000), '.')
    uu.s3_file_download('{0}{1}_{2}.tif'.format(cn.plant_pre_2000_processed_dir, tile, cn.pattern_plant_pre_2000), '.')

# # For multiprocessing.
# count = multiprocessing.cpu_count()
# pool = multiprocessing.Pool(count/3)
# # Masks mangroves out of planted forests where they overlap and pre-2000 plantation pixels
# # count/3 maxes out at about 370 GB on an r4.16xlarge. Could use more processors.
# pool.map(annual_gain_rate_planted_forest.mask_mangroves_and_pre_2000_plant, tile_list)
#
# # Converts annual above+belowground carbon gain rates into aboveground biomass gain rates
# # count/3 maxes out at about 260 GB on an r4.16xlarge. Could use more processors.
# pool.map(annual_gain_rate_planted_forest.create_AGB_rate, tile_list)
#
# # Calculates belowground biomass gain rates from aboveground biomass gain rates
# # count/3 maxes out at about 260 GB on an r4.16xlarge. Could use more processors.
# pool.map(annual_gain_rate_planted_forest.create_BGB_rate, tile_list)
#
# # Deletes any planted forest annual gain rate tiles that have no planted forest in them after being masked by mangroves.
# # This keep them from unnecessarily being stored on s3.
# pool.map(annual_gain_rate_planted_forest.check_for_planted_forest, tile_list)
# pool.close()
# pool.join()

# For single processor use
for tile in tile_list:
    annual_gain_rate_planted_forest.mask_mangroves_and_pre_2000_plant(tile)

for tile in tile_list:
    annual_gain_rate_planted_forest.create_AGB_rate(tile)

for tile in tile_list:
    annual_gain_rate_planted_forest.create_BGB_rate(tile)

for tile in tile_list:
    annual_gain_rate_planted_forest.check_for_planted_forest(tile)

print "Tiles processed. Uploading to s3 now..."
uu.upload_final_set(cn.annual_gain_AGB_planted_forest_non_mangrove_dir, cn.pattern_annual_gain_AGB_planted_forest_non_mangrove)
uu.upload_final_set(cn.annual_gain_BGB_planted_forest_non_mangrove_dir, cn.pattern_annual_gain_BGB_planted_forest_non_mangrove)

