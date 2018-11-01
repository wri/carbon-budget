### This script combines the annual gain rate tiles from different forest types (non-mangrove natural forests, mangroves,
### plantations, into combined tiles. It does the same for cumulative gain over the study period.

import utilities
import datetime
import subprocess

def gain_merge(tile_id):

    print "Processing:", tile_id

    # Start time
    start = datetime.datetime.now()

    # Names of the forest age category and continent-ecozone tiles
    annual_gain_AGB_natrl_forest = '{0}_{1}.tif'.format(utilities.pattern_annual_gain_AGB_natrl_forest, tile_id)
    annual_gain_AGB_mangrove = '{0}_{1}.tif'.format(utilities.pattern_annual_gain_AGB_mangrove, tile_id)

    cumul_gain_AGC_natrl_forest = '{0}_{1}.tif'.format(utilities.pattern_cumul_gain_AGC_natrl_forest, tile_id)
    cumul_gain_AGC_mangrove = '{0}_{1}.tif'.format(utilities.pattern_cumul_gain_AGC_AGC_mangrove, tile_id)

    print "Combining annual gain rate rasters from different forest types"
    annual_gain_rate_combo_outfile = '{}_{}.tif'.format(utilities.pattern_annual_gain_combo, tile_id)
    cmd = ['gdal_merge.py', '-o', annual_gain_rate_combo_outfile, annual_gain_AGB_natrl_forest, annual_gain_AGB_mangrove, '-co', 'COMPRESS=LZW', '-a_nodata', '0']
    subprocess.check_call(cmd)

    utilities.upload_final(utilities.pattern_annual_gain_combo, utilities.annual_gain_combo_dir, tile_id)

    print "Combining cumulative gain rate rasters from different forest types"
    cumul_gain_rate_combo_outfile = '{}_{}.tif'.format(utilities.pattern_cumul_gain_combo, tile_id)
    cmd = ['gdal_merge.py', '-o', cumul_gain_rate_combo_outfile, cumul_gain_AGC_natrl_forest, cumul_gain_AGC_mangrove, '-co', 'COMPRESS=LZW', '-a_nodata', '0']
    subprocess.check_call(cmd)

    utilities.upload_final(utilities.pattern_cumul_gain_combo, utilities.cumul_gain_combo_dir, tile_id)

    end = datetime.datetime.now()
    elapsed_time = end-start

    print "  Processing time for tile", tile_id, ":", elapsed_time