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
    gain_rate_natrl_forest = '{0}_{1}.tif'.format(utilities.pattern_annual_gain_natrl_forest, tile_id)
    gain_rate_mangrove = '{0}_{1}.tif'.format(utilities.pattern_annual_gain_mangrove, tile_id)

    cumul_natrl_forest = '{0}_{1}.tif'.format(utilities.pattern_cumul_gain_natrl_forest, tile_id)
    cumul_mangrove = '{0}_{1}.tif'.format(utilities.pattern_cumul_gain_mangrove, tile_id)

    # Pixels with both loss and gain and not in mangroves
    print "Combining annual gain rate rasters "
    annual_gain_rate_combo_outfile = '{}_{}.tif'.format(, tile_id)
    cmd = ['gdal_merge.py', '-o', age_outfile, loss_outfilename, gain_outfilename, no_change_outfilename, loss_and_gain_outfilename, '-co', 'COMPRESS=LZW', '-a_nodata', '0']
    subprocess.check_call(cmd)

    utilities.upload_final(utilities.pattern_cumul_gain_natrl_forest, utilities.cumul_gain_natrl_forest_dir, tile_id)

    end = datetime.datetime.now()
    elapsed_time = end-start

    print "  Processing time for tile", tile_id, ":", elapsed_time