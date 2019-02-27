### This script assigns annual above and belowground biomass gain rates (in the units of IPCC Table 4.9 (currently tonnes
### biomass/ha/yr)) to non-mangrove planted forest pixels. It masks mangrove pixels from the planted forest carbon gain
### rate tiles so that different forest types are non-overlapping. These are then used in the next step of the carbon model.

import datetime
import subprocess
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def mask_mangroves(tile_id):

    print "Processing:", tile_id

    # Start time
    start = datetime.datetime.now()

    # Names of the forest age category, continent-ecozone, and mangrove biomass tiles
    planted_forest_full_extent = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_AGC_BGC_planted_forest_unmasked)
    mangrove_biomass = '{0}_{1}.tif'.format(tile_id, cn.pattern_mangrove_biomass_2000)

    # Name of the output planted forest gain rate tile, with mangroves masked out
    planted_forest_no_mangrove = '{0}_no_mang_AGC_BGC.tif'.format(tile_id)

    print "  Reading input files and creating aboveground biomass gain rate for {}".format(tile_id)

    if os.path.exists(mangrove_biomass):

        print "    Mangrove found for {}. Masking out mangrove...".format(tile_id)

        # Name for mangrove biomass tiles that have the nodata pixels removed
        mangrove_reclass = '{0}_reclass_{1}.tif'.format(tile_id, cn.pattern_mangrove_biomass_2000)

        # Removes the nodata values in the mangrove biomass rasters because having nodata values in the mangroves didn't work
        # in gdal_calc. The gdal_calc expression didn't know how to evaluate nodata values, so I had to remove them.
        print "      Removing nodata values in mangrove biomass raster {}".format(tile_id)
        cmd = ['gdal_translate', '-a_nodata', 'none', mangrove_biomass, mangrove_reclass]
        subprocess.check_call(cmd)

        # Masks out the mangrove biomass from the planted forest gain rate
        print "      Masking mangroves from aboveground gain rate for planted forest tile {} and converting from carbon to biomass".format(tile_id)
        mangrove_mask_calc = '--calc=A*(B==0)'
        mask_outfilename = planted_forest_no_mangrove
        mask_outfilearg = '--outfile={}'.format(mask_outfilename)
        cmd = ['gdal_calc.py', '-A', planted_forest_full_extent, '-B', mangrove_reclass, mangrove_mask_calc, mask_outfilearg,
               '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW']
        subprocess.check_call(cmd)

    else:

        print "    No mangrove found for {}. Renaming file.".format(tile_id)

        os.rename(planted_forest_full_extent, planted_forest_no_mangrove)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_age_cat_natrl_forest)


def create_AGB_rate(tile_id):

    print "Processing:", tile_id

    # Start time
    start = datetime.datetime.now()

    # Calculates aboveground biomass gain rate from aboveground carbon gain rate
    print "  Creating aboveground biomass gain rate for tile {}".format(tile_id)

    planted_forest_no_mangrove = '{0}_no_mang_AGC_BGC.tif'.format(tile_id)

    AGB_calc = '--calc=A/(1+{})*(1/{})'.format(cn.below_to_above_natrl_forest, cn.biomass_to_c_natrl_forest)
    AGB_outfilename = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_AGB_planted_forest_non_mangrove)
    AGB_outfilearg = '--outfile={}'.format(AGB_outfilename)
    cmd = ['gdal_calc.py', '-A', planted_forest_no_mangrove, AGB_calc, AGB_outfilearg,
           '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW']
    subprocess.check_call(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_age_cat_natrl_forest)


def create_BGB_rate(tile_id):

    print "Processing:", tile_id

    # Start time
    start = datetime.datetime.now()

    planted_forest_AGB_rate = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_AGB_planted_forest_non_mangrove)

    # Calculates belowground biomass rate from aboveground biomass rate
    print "  Creating belowground biomass gain rate for tile {}".format(tile_id)
    above_to_below_calc = '--calc=A*{}'.format(cn.below_to_above_natrl_forest)
    below_outfilename = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_BGB_planted_forest_non_mangrove)
    below_outfilearg = '--outfile={}'.format(below_outfilename)
    cmd = ['gdal_calc.py', '-A', planted_forest_AGB_rate, above_to_below_calc, below_outfilearg,
           '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW']
    subprocess.check_call(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_age_cat_natrl_forest)
