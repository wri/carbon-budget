### Creates tiles in which each non-mangrove planted forest pixel is the number of years that trees are believed to have been growing there between 2001 and 2015.
### It is based on the annual Hansen loss data and the 2000-2012 Hansen gain data.
### First it separately calculates rasters of gain years for non-mangrove planted forest pixels that had loss only,
### gain only, neither loss nor gain, and both loss and gain.
### The gain years for each of these conditions are calculated according to rules that are found in the function called by the multiprocessor commands.
### More gdalcalc commands can be run at the same time than gdalmerge so that's why the number of processors being used is higher
### for the first four processing steps (which use gdalcalc).
### At this point, those rules are the same as for mangrove forests.
### Then it combines those four rasters into a single gain year raster for each tile using gdalmerge.
### If different input rasters for loss (e.g., 2001-2017) and gain (e.g., 2000-2018) are used, the year count constants in constants_and_names.py must be changed.

import subprocess
import datetime
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Gets the names of the input tiles
def tile_names(tile_id):

    # Names of the loss, gain, tree cover density, intact forest landscape, mangrove biomass and planted forest tiles
    loss = '{0}.tif'.format(tile_id)
    gain = '{0}_{1}.tif'.format(cn.pattern_gain, tile_id)
    planted_forest = '{0}_{1}.tif'.format(tile_id, cn.pattern_annual_gain_AGB_planted_forest_non_mangrove)

    return loss, gain, planted_forest


# Creates gain year count tiles for pixels that only had loss
def create_gain_year_count_loss_only(tile_id):

    print "Gain year count for loss only pixels:", tile_id

    # start time
    start = datetime.datetime.now()

    # Names of the loss, gain and tree cover density tiles
    loss, gain, planted_forest = tile_names(tile_id)

    # Pixels with loss only
    loss_calc = '--calc=(A>0)*(B==0)*(C>0)*(A-1)'
    loss_outfilename = '{}_growth_years_loss_only.tif'.format(tile_id)
    loss_outfilearg = '--outfile={}'.format(loss_outfilename)
    cmd = ['gdal_calc.py', '-A', loss, '-B', gain, '-C', planted_forest, loss_calc, loss_outfilearg,
           '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW', '--type', 'Byte']
    subprocess.check_call(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, 'growth_years_loss_only')


# Creates gain year count tiles for pixels that only had gain
def create_gain_year_count_gain_only_standard(tile_id):

    print "Gain year count for gain only pixels using standard function:", tile_id

    # start time
    start = datetime.datetime.now()

    # Names of the loss, gain and tree cover density tiles
    loss, gain, planted_forest = tile_names(tile_id)

    # Pixels with gain only
    gain_calc = '--calc=(A==0)*(B==1)*(C>0)*({}/2)'.format(cn.gain_years)
    gain_outfilename = '{}_growth_years_gain_only.tif'.format(tile_id)
    gain_outfilearg = '--outfile={}'.format(gain_outfilename)
    cmd = ['gdal_calc.py', '-A', loss, '-B', gain, '-C', planted_forest, gain_calc, gain_outfilearg,
           '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW', '--type', 'Byte']
    subprocess.check_call(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, 'growth_years_gain_only')


# Creates gain year count tiles for pixels that only had gain
def create_gain_year_count_gain_only_maxgain(tile_id):

    print "Gain year count for gain only pixels using maxgain function:", tile_id

    # Names of the loss, gain and tree cover density tiles
    loss, gain, planted_forest = tile_names(tile_id)

    # start time
    start = datetime.datetime.now()

    # Pixels with gain only
    gain_calc = '--calc=(A==0)*(B==1)*(C>0)*({})'.format(cn.loss_years)
    gain_outfilename = '{}_growth_years_gain_only.tif'.format(tile_id)
    gain_outfilearg = '--outfile={}'.format(gain_outfilename)
    cmd = ['gdal_calc.py', '-A', loss, '-B', gain, '-C', planted_forest, gain_calc, gain_outfilearg,
           '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW', '--type', 'Byte']
    subprocess.check_call(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, 'growth_years_gain_only')


# Creates gain year count tiles for pixels that had neither loss not gain
def create_gain_year_count_no_change(tile_id):

    print "Gain year count for pixels with neither loss nor gain:", tile_id

    # Names of the loss, gain and tree cover density tiles
    loss, gain, planted_forest = tile_names(tile_id)

    # start time
    start = datetime.datetime.now()

    # Pixels with neither loss nor gain
    no_change_calc = '--calc=(A==0)*(B==0)*(C>0)*{}'.format(cn.loss_years)
    no_change_outfilename = '{}_growth_years_no_change.tif'.format(tile_id)
    no_change_outfilearg = '--outfile={}'.format(no_change_outfilename)
    cmd = ['gdal_calc.py', '-A', loss, '-B', gain, '-C', planted_forest, no_change_calc,
           no_change_outfilearg, '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW', '--type', 'Byte']
    subprocess.check_call(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, 'growth_years_no_change')


# Creates gain year count tiles for pixels that had both loss and gain
def create_gain_year_count_loss_and_gain_standard(tile_id):

    print "Loss and gain pixel processing using standard function:", tile_id

    # Names of the loss, gain and tree cover density tiles
    loss, gain, planted_forest = tile_names(tile_id)

    # start time
    start = datetime.datetime.now()

    # Pixels with both loss and gain
    loss_and_gain_calc = '--calc=((A>0)*(B==1)*(C>0)*((A-1)+({}+1-A)/2))'.format(cn.loss_years)
    loss_and_gain_outfilename = '{}_growth_years_loss_and_gain.tif'.format(tile_id)
    loss_and_gain_outfilearg = '--outfile={}'.format(loss_and_gain_outfilename)
    cmd = ['gdal_calc.py', '-A', loss, '-B', gain, '-C', planted_forest, loss_and_gain_calc,
           loss_and_gain_outfilearg, '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW', '--type', 'Byte']
    subprocess.check_call(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, 'growth_years_loss_and_gain')


# Creates gain year count tiles for pixels that had both loss and gain
def create_gain_year_count_loss_and_gain_maxgain(tile_id):

    print "Loss and gain pixel processing using maxgain function:", tile_id

    # Names of the loss, gain and tree cover density tiles
    loss, gain, planted_forest = tile_names(tile_id)

    # start time
    start = datetime.datetime.now()

    # Pixels with both loss and gain
    loss_and_gain_calc = '--calc=((A>0)*(B==1)*(C>0)*(A-1))'.format(cn.loss_years)
    loss_and_gain_outfilename = '{}_growth_years_loss_and_gain.tif'.format(tile_id)
    loss_and_gain_outfilearg = '--outfile={}'.format(loss_and_gain_outfilename)
    cmd = ['gdal_calc.py', '-A', loss, '-B', gain, '-C', planted_forest, loss_and_gain_calc,
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