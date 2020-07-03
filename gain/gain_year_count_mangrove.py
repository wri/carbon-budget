### Creates tiles in which each mangrove pixel is the number of years that trees are believed to have been growing there between 2001 and 2015.
### It is based on the annual Hansen loss data and the 2000-2012 Hansen gain data (as well as the 2000 tree cover density data).
### First it calculates rasters of gain years for mangrove pixels that had loss only, gain only, neither loss nor gain, and both loss and gain.
### The gain years for each of these conditions are calculated according to rules that are found in the function called by the multiprocessor command.
### At this point, those rules are the same as for non-mangrove natural forests, except that no change pixels don't use tcd as a factor.
### Then it combines those four rasters into a single gain year raster for each tile.
### This is one of the mangrove inputs for the carbon gain model.
### If different input rasters for loss (e.g., 2001-2017) and gain (e.g., 2000-2018) are used, the constants in create_gain_year_count_mangrove.py must be changed.

from subprocess import Popen, PIPE, STDOUT, check_call
import datetime
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Gets the names of the input tiles
def tile_names(tile_id):

    # Names of the loss, gain and tree cover density tiles
    loss = '{0}.tif'.format(tile_id)
    gain = '{0}_{1}.tif'.format(cn.pattern_gain, tile_id)
    mangrove = '{0}_{1}.tif'.format(tile_id, cn.pattern_mangrove_biomass_2000)

    return loss, gain, mangrove


# Creates gain year count tiles for pixels that only had loss
def create_gain_year_count_loss_only(tile_id):

    uu.print_log("Gain year count for loss only pixels:", tile_id)

    # Names of the loss, gain and tree cover density tiles
    loss, gain, mangrove = tile_names(tile_id)

    # start time
    start = datetime.datetime.now()

    # Pixels with loss only
    loss_calc = '--calc=(A>0)*(B==0)*(C>0)*(A-1)'
    loss_outfilename = '{}_growth_years_loss_only.tif'.format(tile_id)
    loss_outfilearg = '--outfile={}'.format(loss_outfilename)
    cmd = ['gdal_calc.py', '-A', loss, '-B', gain, '-C', mangrove, loss_calc, loss_outfilearg,
           '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW', '--type', 'Byte', '--quiet']
    # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        uu.log_subprocess_output(process.stdout)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, 'growth_years_loss_only')


# Creates gain year count tiles for pixels that only had gain
def create_gain_year_count_gain_only_standard(tile_id):

    uu.print_log("Gain year count for gain only pixels using standard function:", tile_id)

    # start time
    start = datetime.datetime.now()

    # Names of the loss, gain and tree cover density tiles
    loss, gain, mangrove = tile_names(tile_id)

    # Pixels with gain only
    gain_calc = '--calc=(A==0)*(B==1)*(C>0)*({}/2)'.format(cn.gain_years)
    gain_outfilename = '{}_growth_years_gain_only.tif'.format(tile_id)
    gain_outfilearg = '--outfile={}'.format(gain_outfilename)
    cmd = ['gdal_calc.py', '-A', loss, '-B', gain, '-C', mangrove, gain_calc, gain_outfilearg, '--NoDataValue=0',
           '--overwrite', '--co', 'COMPRESS=LZW', '--type', 'Byte', '--quiet']
    # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        uu.log_subprocess_output(process.stdout)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, 'growth_years_gain_only')


# Creates gain year count tiles for pixels that only had gain
def create_gain_year_count_gain_only_maxgain(tile_id):

    uu.print_log("Gain pixel-only processing using maxgain function:", tile_id)

    # start time
    start = datetime.datetime.now()

    # Names of the loss, gain and tree cover density tiles
    loss, gain, mangrove = tile_names(tile_id)

    # Pixels with gain only
    gain_calc = '--calc=(A==0)*(B==1)*(C>0)*({})'.format(cn.loss_years)
    gain_outfilename = '{}_growth_years_gain_only.tif'.format(tile_id)
    gain_outfilearg = '--outfile={}'.format(gain_outfilename)
    cmd = ['gdal_calc.py', '-A', loss, '-B', gain, '-C', mangrove, gain_calc, gain_outfilearg, '--NoDataValue=0',
           '--overwrite', '--co', 'COMPRESS=LZW', '--type', 'Byte', '--quiet']
    # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        uu.log_subprocess_output(process.stdout)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, 'growth_years_gain_only')


# Creates gain year count tiles for pixels that had neither loss not gain
def create_gain_year_count_no_change(tile_id):

    uu.print_log("Gain year count for pixels with neither loss nor gain:", tile_id)

    # start time
    start = datetime.datetime.now()

    # Names of the loss, gain and tree cover density tiles
    loss, gain, mangrove = tile_names(tile_id)

    # Pixels with neither loss nor gain but in areas with mangroves.
    # This is the only equation which really differs from the non-mangrove equations; it does not invoke tcd since that is irrelevant for mangroves.
    no_change_calc = '--calc=(A==0)*(B==0)*(C>0)*{}'.format(cn.loss_years)
    no_change_outfilename = '{}_growth_years_no_change.tif'.format(tile_id)
    no_change_outfilearg = '--outfile={}'.format(no_change_outfilename)
    cmd = ['gdal_calc.py', '-A', loss, '-B', gain, '-C', mangrove, no_change_calc, no_change_outfilearg,
           '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW', '--type', 'Byte', '--quiet']
    # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        uu.log_subprocess_output(process.stdout)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, 'growth_years_no_change')


# Creates gain year count tiles for pixels that had both loss and gain
def create_gain_year_count_loss_and_gain_standard(tile_id):

    uu.print_log("Loss and gain pixel processing using standard function:", tile_id)

    # start time
    start = datetime.datetime.now()

    # Names of the loss, gain and tree cover density tiles
    loss, gain, mangrove = tile_names(tile_id)

    # Pixels with both loss and gain
    loss_and_gain_calc = '--calc=((A>0)*(B==1)*(C>0)*((A-1)+floor(({}+1-A)/2)))'.format(cn.loss_years)
    loss_and_gain_outfilename = '{}_growth_years_loss_and_gain.tif'.format(tile_id)
    loss_and_gain_outfilearg = '--outfile={}'.format(loss_and_gain_outfilename)
    cmd = ['gdal_calc.py', '-A', loss, '-B', gain, '-C', mangrove, loss_and_gain_calc, loss_and_gain_outfilearg,
           '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW', '--type', 'Byte', '--quiet']
    # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        uu.log_subprocess_output(process.stdout)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, 'growth_years_loss_and_gain')


# Creates gain year count tiles for pixels that had both loss and gain
def create_gain_year_count_loss_and_gain_maxgain(tile_id):

    uu.print_log("Loss and gain pixel processing using maxgain function:", tile_id)

    # start time
    start = datetime.datetime.now()

    # Names of the loss, gain and tree cover density tiles
    loss, gain, mangrove = tile_names(tile_id)

    # Pixels with both loss and gain
    loss_and_gain_calc = '--calc=((A>0)*(B==1)*(C>0)*({}-1))'.format(cn.loss_years)
    loss_and_gain_outfilename = '{}_growth_years_loss_and_gain.tif'.format(tile_id)
    loss_and_gain_outfilearg = '--outfile={}'.format(loss_and_gain_outfilename)
    cmd = ['gdal_calc.py', '-A', loss, '-B', gain, '-C', mangrove, loss_and_gain_calc, loss_and_gain_outfilearg,
           '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW', '--type', 'Byte', '--quiet']
    # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        uu.log_subprocess_output(process.stdout)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, 'growth_years_loss_and_gain')


# Merges the four gain year count tiles above to create a single gain year count tile
def create_gain_year_count_merge(tile_id, pattern):

    uu.print_log("Merging loss, gain, no change, and loss/gain pixels into single raster for {}".format(tile_id))

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
    # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        uu.log_subprocess_output(process.stdout)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, pattern)