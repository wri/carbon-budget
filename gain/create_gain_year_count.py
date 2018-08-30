import utilities
import subprocess
import datetime

def create_gain_year_count(tile_id):

    print "Processing:", tile_id

    # start time
    start = datetime.datetime.now()

    # Names of the loss, gain and tree cover density tiles
    loss = '{}.tif'.format(tile_id)
    gain = 'Hansen_GFC2015_gain_{}.tif'.format(tile_id)
    tcd = 'Hansen_GFC2014_treecover2000_{}.tif'.format(tile_id)

    # Location to upload files to
    upload_dir = 's3://gfw2-data/climate/carbon_model/forest_age/20180830/'

    print 'Loss tile is', loss
    print 'Gain tile is', gain
    print 'tcd tile is', tcd

    # calc year tile values to be equal to year
    calc = '--calc=(A>0)*(B==0)*(A-1)'
    loss_outfile1 = 'growth_years_loss_only_{}.tif'.format(tile_id)
    loss_outfile2 = '--outfile={}'.format(loss_outfile1)
    cmd = ['gdal_calc.py', '-A', loss, '-B', gain, calc, loss_outfile2, '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW']
    subprocess.check_call(cmd)


    # # Creates four separate rasters for the four tree cover loss/gain combinations for pixels. Then merges the rasters.
    # # In all rasters, 0 is NoData value.
    # # Pixels with loss only
    # print "Creating raster of growth years for loss-only pixels"
    # loss_calc = '(A>0)*(B==0)*(A-1)'
    # loss_outfile = 'growth_years_loss_only_{}.tif'.format(tile_id)
    # #gdal_calc.py -A 00N_050W.tif -B Hansen_GFC2015_gain_00N_050W.tif --calc="(A>0)*(B==0)*(A-1)" --outfile=loss_only.tif --NoDataValue=0 --overwrite
    # cmd = ['gdal_calc.py', '-A', loss, '-B', gain, '--calc={}'.format(loss_calc), '--outfile={}'.format(loss_outfile), '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW']
    # subprocess.check_call(cmd)
    #
    # # Pixels with gain only
    # print "Creating raster of growth years for gain-only pixels"
    # gain_calc = '(A==0)*(B==1)*6'
    # gain_outfile = 'growth_years_gain_only_{}.tif'.format(tile_id)
    # #gdal_calc.py -A 00N_050W.tif -B Hansen_GFC2015_gain_00N_050W.tif --calc="(A==0)*(B==1)*6" --outfile=gain_only.tif --NoDataValue=0 --overwrite
    # cmd = ['gdal_calc.py', '-A', loss, '-B', gain, '--calc={}'.format(gain_calc), '--outfile={}'.format(gain_outfile), '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW']
    # subprocess.check_call(cmd)
    #
    # # Pixels with neither loss nor gain but in areas with tree cover density >0
    # print "Creating raster of growth years for no change pixels"
    # no_change_calc = '(A==0)*(B==0)*(C>0)*15'
    # no_change_outfile = 'growth_years_no_change_{}.tif'.format(tile_id)
    # #gdal_calc.py -A 00N_050W.tif -B Hansen_GFC2015_gain_00N_050W.tif -C Hansen_GFC2014_treecover2000_00N_050W.tif --calc "(A==0)*(B==0)*(C>0)*15" --outfile=no_change.tif --NoDataValue=0 --overwrite
    # cmd = ['gdal_calc.py', '-A', loss, '-B', gain, '-C', tcd, '--calc={}'.format(no_change_calc), '--outfile={}'.format(no_change_outfile), '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW']
    # subprocess.check_call(cmd)
    #
    # # Pixels with both loss and gain
    # print "Creating raster of growth years for loss and gain pixels"
    # loss_and_gain_calc = '((A>0)*(B==1)*((A-1)+(16-A)/2))'
    # loss_and_gain_outfile = 'growth_years_loss_and_gain_{}.tif'.format(tile_id)
    # #gdal_calc.py -A 00N_050W.tif -B Hansen_GFC2015_gain_00N_050W.tif --calc="((A>0)*(B==1)*((A-1)+(16-A)/2))" --outfile=gain_and_loss.tif --NoDataValue=0 --overwrite
    # cmd = ['gdal_calc.py', '-A', loss, '-B', gain, '--calc={}'.format(loss_and_gain_calc), '--outfile={}'.format(loss_and_gain_outfile), '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW']
    # subprocess.check_call(cmd)
    #
    # print "Merging loss, gain, no change, and loss/gain pixels into single raster"
    # age_outfile = 'growth_years_total_{}.tif'.format(tile_id)
    # cmd = ['gdal_merge.py', '-o', age_outfile, loss_outfile, gain_outfile, no_change_outfile, loss_and_gain_outfile, '-co', 'COMPRESS=LZW', '-n', '0', '-a_nodata', '0']
    # subprocess.check_call(cmd)
    #
    # utilities.upload_final(upload_dir, tile_id)

    end = datetime.datetime.now()
    elapsed_time = end-start

    print "Processing time for tile", tile_id, ":", elapsed_time




