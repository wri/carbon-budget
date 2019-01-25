### This script assigns annual above and belowground biomass gain rates (in the units of IPCC Table 4.9 (currently tonnes
### biomass/ha/yr)) to non-mangrove planted forest pixels. It masks mangrove pixels from the planted forest carbon gain
### rate tiles so that different forest types are non-overlapping. These are then used in the next step of the carbon model.

import datetime
import subprocess
import os
import sys
sys.path.append('../')
import constants_and_names as cn

def annual_gain_rate(tile_id):

    print "Processing:", tile_id

    # Start time
    start = datetime.datetime.now()

    # Names of the forest age category, continent-ecozone, and mangrove biomass tiles
    planted_forest_full_extent = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_AGC_planted_forest_full_extent)
    mangrove_biomass = '{0}_{1}.tif'.format(tile_id, cn.pattern_mangrove_biomass_2000)

    # Name of the output planted forest gain rate tile, with mangroves masked out
    planted_forest_no_mangrove = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_AGB_planted_forest_non_mangrove)

    print "  Reading input files and creating aboveground biomass gain rate for {}".format(tile_id)

    if os.path.exists(mangrove_biomass):

        # Name for mangrove biomass tiles that have the nodata pixels removed
        mangrove_reclass = '{0}_reclass_{1}.tif'.format(tile_id, cn.pattern_mangrove_biomass_2000)

        # Removes the nodata values in the mangrove biomass rasters because having nodata values in the mangroves didn't work
        # in gdal_calc. The gdal_calc expression didn't know how to evaluate nodata values, so I had to remove them.
        print "  Removing nodata values in mangrove biomass raster {}".format(tile_id)
        cmd = ['gdal_translate', '-a_nodata', 'none', mangrove_biomass, mangrove_reclass]
        subprocess.check_call(cmd)

        # Masks out the mangrove biomass from the planted forest gain rate
        print "  Masking mangroves from aboveground gain rate for planted forest tile {}".format(tile_id)
        mangrove_mask_calc = '--calc=A*(B==0)'
        mask_outfilename = planted_forest_no_mangrove
        mask_outfilearg = '--outfile={}'.format(mask_outfilename)
        cmd = ['gdal_calc.py', '-A', planted_forest_full_extent, '-B', mangrove_reclass, mangrove_mask_calc, mask_outfilearg,
               '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW']
        subprocess.check_call(cmd)

    else:

        os.rename(planted_forest_full_extent, planted_forest_no_mangrove)

    # Calculates belowground biomass rate from aboveground biomass rate
    print "  Creating belowground biomass gain rate for tile {}".format(tile_id)
    above_to_below_calc = '--calc=(A>0)*A*{}'.format(cn.below_to_above_natrl_forest)
    below_outfilename = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_BGB_planted_forest_non_mangrove)
    below_outfilearg = '--outfile={}'.format(below_outfilename)
    cmd = ['gdal_calc.py', '-A', planted_forest_no_mangrove, above_to_below_calc, below_outfilearg,
           '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW']
    subprocess.check_call(cmd)

    end = datetime.datetime.now()
    elapsed_time = end-start

    print "  Processing time for tile", tile_id, ":", elapsed_time
