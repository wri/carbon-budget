### This script assigns annual above and belowground biomass gain rates (in the units of IPCC Table 4.9 (currently tonnes
### biomass/ha/yr)) to non-mangrove planted forest pixels. It masks mangrove pixels from the planted forest carbon gain
### rate tiles so that different forest types are non-overlapping.
### To calculate the aboveground and belowground biomass gain rates from above+belowground carbon gain rate, the
### script uses the IPCC default natural forest values. Although these values don't actually apply to planted forests,
### they are the best we have for parsing planted forests into the component values.
### We want to separate the above+below rate into above and below and convert to biomass so that we can make global
### maps of annual above and below biomass gain rates separately; the natural forests and mangroves already use
### separate above and below annual biomass gain rate files, so this brings planted forests into line with them.

import datetime
import subprocess
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def mask_mangroves_and_pre_2000_plant(tile_id):

    # Start time
    start = datetime.datetime.now()

    # Names of the unmasked planted forest and mangrove tiles
    planted_forest_full_extent = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_AGC_BGC_planted_forest_unmasked)
    mangrove_biomass = '{0}_{1}.tif'.format(tile_id, cn.pattern_mangrove_biomass_2000)

    # Name of pre-2000 plantation tile
    pre_2000_plant = '{0}_{1}.tif'.format(tile_id, cn.pattern_plant_pre_2000)

    uu.mask_pre_2000_plantation(pre_2000_plant, planted_forest_full_extent, planted_forest_full_extent, tile_id)

    # Name of the planted forest AGC/BGC gain rate tile, with mangroves masked out
    planted_forest_no_mangrove = '{0}_no_mang_AGC_BGC.tif'.format(tile_id)

    print "Evaluating whether to mask mangroves from planted forests for {}".format(tile_id)

    # If there is a mangrove tile, mangrove pixels are masked from the planted forest raster
    if os.path.exists(mangrove_biomass):

        print "  Mangrove found for {}. Masking out mangrove...".format(tile_id)

        # Name for mangrove biomass tiles that have the nodata pixels removed
        mangrove_reclass = '{0}_reclass_{1}.tif'.format(tile_id, cn.pattern_mangrove_biomass_2000)

        # Removes the nodata values in the mangrove biomass rasters because having nodata values in the mangroves didn't work
        # in gdal_calc. The gdal_calc expression didn't know how to evaluate nodata values, so I had to remove them.
        print "    Removing nodata values in mangrove biomass raster {}".format(tile_id)
        cmd = ['gdal_translate', '-a_nodata', 'none', mangrove_biomass, mangrove_reclass]
        subprocess.check_call(cmd)

        # Masks out the mangrove biomass from the planted forest gain rate
        print "    Masking mangroves from aboveground gain rate for planted forest tile {}...".format(tile_id)
        mangrove_mask_calc = '--calc=A*(B==0)'
        mask_outfilename = planted_forest_no_mangrove
        mask_outfilearg = '--outfile={}'.format(mask_outfilename)
        cmd = ['gdal_calc.py', '-A', planted_forest_full_extent, '-B', mangrove_reclass, mangrove_mask_calc, mask_outfilearg,
               '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW']
        subprocess.check_call(cmd)

    # If there is no mangrove tile, the planted forest AGC/BGC tile is renamed to have the same name as comes out of
    # the masking option
    else:

        print "  No mangrove found for {}. Renaming file.".format(tile_id)

        os.rename(planted_forest_full_extent, planted_forest_no_mangrove)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, 'no_mang_AGC_BGC')


# Converts the combined annual aboveground carbon and belowground carbon gain rates into aboveground biomass rates.
# This uses the natural forest ratios simply for expediency-- we don't have data on biomass:C or AGB:BGB for most
# planted forest types.
def create_AGB_rate(tile_id):

    print "Creating aboveground biomass gain rate for tile {}".format(tile_id)

    # Start time
    start = datetime.datetime.now()

    # The name of the tile that comes out of the masking function above
    planted_forest_no_mangrove = '{0}_no_mang_AGC_BGC.tif'.format(tile_id)

    # Equation converts above+below to just above and carbon to biomass
    AGB_calc = '--calc=A/(1+{})*(1/{})'.format(cn.below_to_above_non_mang, cn.biomass_to_c_non_mangrove)
    AGB_outfilename = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_AGB_planted_forest_non_mangrove)
    AGB_outfilearg = '--outfile={}'.format(AGB_outfilename)
    cmd = ['gdal_calc.py', '-A', planted_forest_no_mangrove, AGB_calc, AGB_outfilearg,
           '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW']
    subprocess.check_call(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_annual_gain_AGB_planted_forest_non_mangrove)


# Converts the annual aboveground biomass gain rate into belowground biomass gain rate.
# This uses the natural forest ratios simply for expediency-- we don't have data on AGB:BGB for most
# planted forest types.
def create_BGB_rate(tile_id):

    print "Creating belowground biomass gain rate for tile {}".format(tile_id)

    # Start time
    start = datetime.datetime.now()

    planted_forest_AGB_rate = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_AGB_planted_forest_non_mangrove)

    # Calculates belowground biomass gain rate from aboveground biomass gain rate
    print "  Creating belowground biomass gain rate for tile {}".format(tile_id)
    above_to_below_calc = '--calc=A*{}'.format(cn.below_to_above_non_mang)
    below_outfilename = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_BGB_planted_forest_non_mangrove)
    below_outfilearg = '--outfile={}'.format(below_outfilename)
    cmd = ['gdal_calc.py', '-A', planted_forest_AGB_rate, above_to_below_calc, below_outfilearg,
           '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW']
    subprocess.check_call(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_annual_gain_BGB_planted_forest_non_mangrove)

# Deletes any tiles that don't have data planted forest data in them after the mangroves are masked out.
# That way, empty tiles aren't copied to s3.
# Ideally, this would be part of the initial masking function but deleting tiles that later functions expect to
# iterate through would mess things up, so this is going as a final pre-upload check, not as a way to prevent
# unnecessary processing.
def check_for_planted_forest(tile_id):

    print "Checking whether there is planted forest after masking out mangroves..."

    print "Checking if {} contains any data...".format(tile_id)
    stats = uu.check_for_data('{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_AGB_planted_forest_non_mangrove))

    if stats[0] > 0:

        print "  Data found in {}. Keeping tile to copy...".format(tile_id)

    else:

        print "  No data found. Deleting aboveground and belowground biomass gain rates...".format(tile_id)

        os.remove('{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_AGB_planted_forest_non_mangrove))
        os.remove('{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_BGB_planted_forest_non_mangrove))