### Creates tiles in which each natural non-mangrove non-planted forest biomass pixel is the number of years that trees are believed to have been growing there between 2001 and 2015.
### It is based on the annual Hansen loss data and the 2000-2012 Hansen gain data (as well as the 2000 tree cover density data).
### First it calculates rasters of gain years for non-mangrove non-planted forest biomass pixels that had loss only, gain only, neither loss nor gain, and both loss and gain.
### The gain years for each of these conditions are calculated according to rules that are found in the function called by the multiprocessor command.
### Then it combines those four rasters into a single gain year raster for each tile.
### Only the merged raster is used later in the model; the 4 intermediates are saved just for checking.
### If different input rasters for loss (e.g., 2001-2017) and gain (e.g., 2000-2018) are used, the constants in create_gain_year_count_natrl_forest.py must be changed.

import subprocess
import datetime
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Gets the names of the input tiles
def tile_names(tile_id, sensit_type):

    # Names of the input files

    if sensit_type == 'legan_Amazon_loss':
        loss = '{0}_{1}.tif'.format(tile_id, cn.pattern_Brazil_annual_loss_processed)
    elif sensit_type == 'Mekong_loss':
        loss = '{0}_{1}.tif'.format(tile_id, cn.pattern_Mekong_loss_processed)
    else:
        loss = '{}.tif'.format(tile_id)

    gain = '{0}_{1}.tif'.format(cn.pattern_gain, tile_id)
    tcd = '{0}_{1}.tif'.format(cn.pattern_tcd, tile_id)
    biomass = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_WHRC_biomass_2000_non_mang_non_planted)

    return loss, gain, tcd, biomass


# Creates gain year count tiles for pixels that only had loss
def create_gain_year_count_loss_only(tile_id, sensit_type):

    print "Gain year count for loss only pixels:", tile_id

    # Names of the input tiles
    loss, gain, tcd, biomass = tile_names(tile_id, sensit_type)

    # start time
    start = datetime.datetime.now()

    # Pixels with loss only
    loss_calc = '--calc=(A>0)*(B==0)*(A-1)'
    loss_outfilename = '{}_growth_years_loss_only.tif'.format(tile_id)
    loss_outfilearg = '--outfile={}'.format(loss_outfilename)
    cmd = ['gdal_calc.py', '-A', loss, '-B', gain, loss_calc, loss_outfilearg,
           '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW', '--type', 'Byte']
    subprocess.check_call(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, 'growth_years_loss_only')


# Creates gain year count tiles for pixels that only had gain
def create_gain_year_count_gain_only_standard(tile_id, sensit_type):

    print "Gain year count for gain only pixels using standard function:", tile_id

    # start time
    start = datetime.datetime.now()

    # Names of the loss, gain and tree cover density tiles
    loss, gain, tcd, biomass = tile_names(tile_id, sensit_type)
    print loss
    print gain
    print tcd
    print biomass

    # Pixels with gain only
    gain_calc = '--calc=(A==0)*(B==1)*({}/2)'.format(cn.gain_years)
    gain_outfilename = '{}_growth_years_gain_only.tif'.format(tile_id)
    gain_outfilearg = '--outfile={}'.format(gain_outfilename)
    cmd = ['gdal_calc.py', '-A', loss, '-B', gain, gain_calc, gain_outfilearg,
           '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW', '--type', 'Byte']
    subprocess.check_call(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, 'growth_years_gain_only')


# Creates gain year count tiles for pixels that only had gain
def create_gain_year_count_gain_only_maxgain(tile_id, sensit_type):

    print "Gain pixel-only processing using maxgain function:", tile_id

    # start time
    start = datetime.datetime.now()

    # Names of the loss, gain and tree cover density tiles
    loss, gain, tcd, biomass = tile_names(tile_id, sensit_type)

    # Pixels with gain only
    gain_calc = '--calc=(A==0)*(B==1)*({})'.format(cn.loss_years)
    gain_outfilename = '{}_growth_years_gain_only.tif'.format(tile_id)
    gain_outfilearg = '--outfile={}'.format(gain_outfilename)
    cmd = ['gdal_calc.py', '-A', loss, '-B', gain, gain_calc, gain_outfilearg,
           '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW', '--type', 'Byte']
    subprocess.check_call(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, 'growth_years_gain_only')


# Creates gain year count tiles for pixels that had neither loss not gain
def create_gain_year_count_no_change(tile_id, sensit_type):

    print "No change pixel processing:", tile_id

    # start time
    start = datetime.datetime.now()

    # Names of the loss, gain and tree cover density tiles
    loss, gain, tcd, biomass = tile_names(tile_id, sensit_type)

    # Pixels with neither loss nor gain but in areas with tree cover density >0 and biomass >0 (so that oceans aren't included)
    no_change_calc = '--calc=(A==0)*(B==0)*(C>0)*(D>0)*{}'.format(cn.loss_years)
    no_change_outfilename = '{}_growth_years_no_change.tif'.format(tile_id)
    no_change_outfilearg = '--outfile={}'.format(no_change_outfilename)
    cmd = ['gdal_calc.py', '-A', loss, '-B', gain, '-C', tcd, '-D', biomass, no_change_calc,
           no_change_outfilearg, '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW', '--type', 'Byte']
    subprocess.check_call(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, 'growth_years_no_change')


# Creates gain year count tiles for pixels that had both loss and gain
def create_gain_year_count_loss_and_gain_standard(tile_id, sensit_type):

    print "Loss and gain pixel processing using standard function:", tile_id

    # start time
    start = datetime.datetime.now()

    # Names of the loss, gain and tree cover density tiles
    loss, gain, tcd, biomass = tile_names(tile_id, sensit_type)

    # Pixels with both loss and gain
    loss_and_gain_calc = '--calc=((A>0)*(B==1)*((A-1)+({}+1-A)/2))'.format(cn.loss_years)
    loss_and_gain_outfilename = '{}_growth_years_loss_and_gain.tif'.format(tile_id)
    loss_and_gain_outfilearg = '--outfile={}'.format(loss_and_gain_outfilename)
    cmd = ['gdal_calc.py', '-A', loss, '-B', gain, loss_and_gain_calc,
           loss_and_gain_outfilearg, '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW', '--type', 'Byte']
    subprocess.check_call(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, 'growth_years_loss_and_gain')


# Creates gain year count tiles for pixels that had both loss and gain
def create_gain_year_count_loss_and_gain_maxgain(tile_id, sensit_type):

    print "Loss and gain pixel processing using maxgain function:", tile_id

    # start time
    start = datetime.datetime.now()

    # Names of the loss, gain and tree cover density tiles
    loss, gain, tcd, biomass = tile_names(tile_id, sensit_type)

    # Pixels with both loss and gain
    loss_and_gain_calc = '--calc=((A>0)*(B==1)*({}-1))'.format(cn.loss_years)
    loss_and_gain_outfilename = '{}_growth_years_loss_and_gain.tif'.format(tile_id)
    loss_and_gain_outfilearg = '--outfile={}'.format(loss_and_gain_outfilename)
    cmd = ['gdal_calc.py', '-A', loss, '-B', gain, loss_and_gain_calc,
           loss_and_gain_outfilearg, '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW', '--type', 'Byte']
    subprocess.check_call(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, 'growth_years_loss_and_gain')


# Merges the four gain year count tiles above to create a single gain year count tile
def create_gain_year_count_merge(tile_id, pattern):

    print "Merging loss, gain, no change, and loss/gain pixels into single raster for {}".format(tile_id)

    # start time
    start = datetime.datetime.now()

    # The four rasters from above that are to be merged
    loss_outfilename = '{}_growth_years_loss_only.tif'.format(tile_id)
    gain_outfilename = '{}_growth_years_gain_only.tif'.format(tile_id)
    no_change_outfilename = '{}_growth_years_no_change.tif'.format(tile_id)
    loss_and_gain_outfilename = '{}_growth_years_loss_and_gain.tif'.format(tile_id)

    # All four components are merged together to the final output raster
    age_outfile = '{}_{}.tif'.format(tile_id, pattern)
    cmd = ['gdal_merge.py', '-o', age_outfile, loss_outfilename, gain_outfilename, no_change_outfilename, loss_and_gain_outfilename,
           '-co', 'COMPRESS=LZW', '-a_nodata', '0', '-ot', 'Byte']
    subprocess.check_call(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, pattern)