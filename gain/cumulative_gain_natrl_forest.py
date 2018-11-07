### This script calculates the cumulative above and belowground carbon gain in non-mangrove natural forest pixels from 2001-2015.
### It multiplies the annual biomass gain rate by the number of years of gain by the biomass-to-carbon conversion.

import utilities
import datetime
import subprocess
import sys
sys.path.append('../')
import constants_and_names


def cumulative_gain(tile_id):

    print "Processing:", tile_id

    # Start time
    start = datetime.datetime.now()

    # Names of the annual gain rate and gain year count tiles
    gain_rate_AGB = '{0}_{1}.tif'.format(constants_and_names.pattern_annual_gain_AGB_natrl_forest, tile_id)
    gain_rate_BGB = '{0}_{1}.tif'.format(constants_and_names.pattern_annual_gain_BGB_natrl_forest, tile_id)
    gain_year_count = '{0}_{1}.tif'.format(constants_and_names.pattern_gain_year_count_natrl_forest, tile_id)

    print "  Reading input files and calculating cumulative aboveground gain for natural forest tile {}".format(tile_id)
    accum_calc = '--calc=A*B*{}'.format(constants_and_names.biomass_to_c)
    accum_outfilename = '{0}_{1}.tif'.format(constants_and_names.pattern_cumul_gain_AGC_natrl_forest, tile_id)
    accum_outfilearg = '--outfile={}'.format(accum_outfilename)
    cmd = ['gdal_calc.py', '-A', gain_rate_AGB, '-B', gain_year_count, accum_calc, accum_outfilearg, '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW']
    subprocess.check_call(cmd)

    utilities.upload_final(constants_and_names.pattern_cumul_gain_AGC_natrl_forest, constants_and_names.cumul_gain_AGC_natrl_forest_dir, tile_id)

    print "  Reading input files and calculating cumulative belowground gain for natural forest tile {}".format(tile_id)
    accum_calc = '--calc=A*B*{}'.format(constants_and_names.biomass_to_c)
    accum_outfilename = '{0}_{1}.tif'.format(constants_and_names.pattern_cumul_gain_BGC_natrl_forest, tile_id)
    accum_outfilearg = '--outfile={}'.format(accum_outfilename)
    cmd = ['gdal_calc.py', '-A', gain_rate_BGB, '-B', gain_year_count, accum_calc, accum_outfilearg, '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW']
    subprocess.check_call(cmd)

    utilities.upload_final(constants_and_names.pattern_cumul_gain_BGC_natrl_forest, constants_and_names.cumul_gain_BGC_natrl_forest_dir, tile_id)

    end = datetime.datetime.now()
    elapsed_time = end-start

    print "  Processing time for tile", tile_id, ":", elapsed_time