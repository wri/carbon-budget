### This script calculates the cumulative above and belowground CO2 gain in non-mangrove planted forest pixels from 2001-2015.
### It multiplies the annual biomass gain rate by the number of years of gain by the biomass-to-carbon conversion and C to CO2 conversion.

import datetime
import subprocess
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Calculates cumulative aboveground carbon dioxide gain in non-mangrove planted forests
def cumulative_gain_AGC(tile_id, pattern, sensit_type):

    print("Calculating cumulative aboveground CO2 gain:", tile_id)

    # Start time
    start = datetime.datetime.now()

    # Names of the input tiles, modified according to sensitivity analysis
    gain_rate_AGB = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_annual_gain_AGB_planted_forest_non_mangrove)
    gain_year_count = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_gain_year_count_planted_forest_non_mangrove)

    # CO2 gain uses non-mangrove non-planted biomass:carbon ratio
    accum_calc = '--calc=A*B*{0}*{1}'.format(cn.biomass_to_c_non_mangrove, cn.c_to_co2)
    AGCO2_accum_outfilename = '{0}_{1}.tif'.format(tile_id, pattern)
    AGCO2_accum_outfilearg = '--outfile={}'.format(AGCO2_accum_outfilename)
    cmd = ['gdal_calc.py', '-A', gain_rate_AGB, '-B', gain_year_count, accum_calc, AGCO2_accum_outfilearg, '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW']
    subprocess.check_call(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, pattern)


# Calculates cumulative belowground carbon gain in non-mangrove planted forests
def cumulative_gain_BGC(tile_id, pattern, sensit_type):

    print("Calculating cumulative belowground CO2 gain:", tile_id)

    # Start time
    start = datetime.datetime.now()

    # Names of the input tiles, modified according to sensitivity analysis
    gain_rate_BGB = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_annual_gain_BGB_planted_forest_non_mangrove)
    gain_year_count = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_gain_year_count_planted_forest_non_mangrove)

    # CO2 gain uses non-mangrove non-planted biomass:carbon ratio
    accum_calc = '--calc=A*B*{0}*{1}'.format(cn.biomass_to_c_non_mangrove, cn.c_to_co2)
    BGCO2_accum_outfilename = '{0}_{1}.tif'.format(tile_id, pattern)
    BGCO2_accum_outfilearg = '--outfile={}'.format(BGCO2_accum_outfilename)
    cmd = ['gdal_calc.py', '-A', gain_rate_BGB, '-B', gain_year_count, accum_calc, BGCO2_accum_outfilearg, '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW']
    subprocess.check_call(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, pattern)