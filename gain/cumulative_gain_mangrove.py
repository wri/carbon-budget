### This script calculates the cumulative above and belowground carbon gain in mangrove forest pixels from 2001-2015.
### It multiplies the annual biomass gain rate by the number of years of gain by the biomass-to-carbon conversion.

import utilities
import datetime
import subprocess
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Calculates cumulative aboveground carbon gain in mangroves
def cumulative_gain_AGC(tile_id):

    print "Calculating cumulative aboveground carbon gain:", tile_id

    # Start time
    start = datetime.datetime.now()

    # Names of the annual gain rate and gain year count tiles
    gain_rate_AGB = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_AGB_mangrove)
    gain_year_count = '{0}_{1}.tif'.format(tile_id, cn.pattern_gain_year_count_mangrove)

    # Carbon gain uses special mangrove biomass:carbon ratio
    accum_calc = '--calc=A*B*{}'.format(cn.biomass_to_c_mangrove)
    AGC_accum_outfilename = '{0}_{1}.tif'.format(tile_id, cn.pattern_cumul_gain_AGC_mangrove)
    AGC_accum_outfilearg = '--outfile={}'.format(AGC_accum_outfilename)
    cmd = ['gdal_calc.py', '-A', gain_rate_AGB, '-B', gain_year_count, accum_calc, AGC_accum_outfilearg, '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW']
    subprocess.check_call(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_cumul_gain_AGC_mangrove)


# Calculates cumulative belowground carbon gain in mangroves
def cumulative_gain_BGC(tile_id):

    print "Calculating cumulative belowground carbon gain:", tile_id

    # Start time
    start = datetime.datetime.now()

    # Names of the annual gain rate and gain year count tiles
    gain_rate_BGB = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_BGB_mangrove)
    gain_year_count = '{0}_{1}.tif'.format(tile_id, cn.pattern_gain_year_count_mangrove)

    # Carbon gain uses special mangrove biomass:carbon ratio
    accum_calc = '--calc=A*B*{}'.format(cn.biomass_to_c_mangrove)
    BGC_accum_outfilename = '{0}_{1}.tif'.format(tile_id, cn.pattern_cumul_gain_BGC_mangrove)
    BGC_accum_outfilearg = '--outfile={}'.format(BGC_accum_outfilename)
    cmd = ['gdal_calc.py', '-A', gain_rate_BGB, '-B', gain_year_count, accum_calc, BGC_accum_outfilearg, '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW']
    subprocess.check_call(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_cumul_gain_BGC_mangrove)