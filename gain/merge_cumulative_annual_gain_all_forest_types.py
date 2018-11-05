### This script combines the annual gain rate tiles from different forest types (non-mangrove natural forests, mangroves,
### plantations, into combined tiles. It does the same for cumulative gain over the study period.

import utilities
import datetime
import subprocess

def gain_merge(tile_id):

    print "Processing:", tile_id

    # Start time
    start = datetime.datetime.now()

    # Names of the annual gain rate and cumulative gain tiles
    annual_gain_AGB_natrl_forest = '{0}_{1}.tif'.format(utilities.pattern_annual_gain_AGB_natrl_forest, tile_id)
    annual_gain_AGB_mangrove = '{0}_{1}.tif'.format(utilities.pattern_annual_gain_AGB_mangrove, tile_id)

    cumul_gain_AGC_natrl_forest = '{0}_{1}.tif'.format(utilities.pattern_cumul_gain_AGC_natrl_forest, tile_id)
    cumul_gain_AGC_mangrove = '{0}_{1}.tif'.format(utilities.pattern_cumul_gain_AGC_mangrove, tile_id)

    annual_gain_BGB_natrl_forest = '{0}_{1}.tif'.format(utilities.pattern_annual_gain_BGB_natrl_forest, tile_id)
    annual_gain_BGB_mangrove = '{0}_{1}.tif'.format(utilities.pattern_annual_gain_BGB_mangrove, tile_id)

    cumul_gain_BGC_natrl_forest = '{0}_{1}.tif'.format(utilities.pattern_cumul_gain_BGC_natrl_forest, tile_id)
    cumul_gain_BGC_mangrove = '{0}_{1}.tif'.format(utilities.pattern_cumul_gain_BGC_mangrove, tile_id)

    print "Combining annual above and belowground biomass gain rate tiles from different forest types for {}".format(tile_id)
    biomass_rate_sum_calc = '--calc=A+B+C+D'
    rate_sum_outfilename = '{0}_{1}.tif'.format(utilities.pattern_annual_gain_combo, tile_id)
    rate_sum_outfilearg = '--outfile={}'.format(rate_sum_outfilename)
    cmd = ['gdal_calc.py', '-A', annual_gain_AGB_natrl_forest, '-B', annual_gain_AGB_mangrove, '-C', annual_gain_BGB_natrl_forest, '-D', annual_gain_BGB_mangrove,
           biomass_rate_sum_calc, rate_sum_outfilearg, '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW']
    subprocess.check_call(cmd)

    utilities.upload_final(utilities.pattern_annual_gain_combo, utilities.annual_gain_combo_dir, tile_id)

    print "Combining cumulative above and belowground carbon gain tiles from different forest types for {}".format(tile_id)
    biomass_rate_sum_calc = '--calc=A+B+C+D'
    rate_sum_outfilename = '{0}_{1}.tif'.format(utilities.pattern_cumul_gain_combo, tile_id)
    rate_sum_outfilearg = '--outfile={}'.format(rate_sum_outfilename)
    cmd = ['gdal_calc.py', '-A', cumul_gain_AGC_natrl_forest, '-B', cumul_gain_BGC_mangrove, '-C', cumul_gain_BGC_natrl_forest, '-D', cumul_gain_BGC_mangrove,
           biomass_rate_sum_calc, rate_sum_outfilearg, '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW']
    subprocess.check_call(cmd)

    utilities.upload_final(utilities.pattern_cumul_gain_combo, utilities.cumul_gain_combo_dir, tile_id)

    end = datetime.datetime.now()
    elapsed_time = end-start

    print "  Processing time for tile", tile_id, ":", elapsed_time