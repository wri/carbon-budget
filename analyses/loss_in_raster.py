import subprocess
import datetime
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Calculates a range of tile statistics
def loss_in_raster(tile_id, raster_type, output_name, lat, mask):

    print "Calculating loss area for tile id {0}...".format(tile_id)
    print mask

    xmin, ymin, xmax, ymax = uu.coords(tile_id)

    # start time
    start = datetime.datetime.now()

    # Name of the loss time
    loss_tile = '{0}.tif'.format(tile_id)

    # The raster that loss is being analyzed inside
    raster_of_interest = '{0}_{1}.tif'.format(tile_id, raster_type)

    # Output file name
    outname = '{0}_{1}.tif'.format(tile_id, output_name)

    # Only processes the tile if it is inside the latitude band (north of the specified latitude)
    if ymax > lat and os.path.exists(raster_of_interest):

        print "{} inside latitude band and peat tile exists. Processing tile.".format(tile_id)

        # If the user has asked to create just a mask of loss as opposed to the actual output values
        if mask == True:

            print "masked"

            calc = '--calc=A/A*B'

        # If the user has asked to output the actual loss values
        if mask == False:

            print "not masked"

            # Equation argument for converting emissions from per hectare to per pixel.
            # First, multiplies the per hectare emissions by the area of the pixel in m2, then divides by the number of m2 in a hectare.
            calc = '--calc=A*B'

        # Argument for outputting file
        out = '--outfile={}'.format(outname)

        print "Masking loss in {} by raster of interest...".format(tile_id)
        cmd = ['gdal_calc.py', '-A', loss_tile, '-B', raster_of_interest, calc, out, '--NoDataValue=0', '--co', 'COMPRESS=LZW',
               '--overwrite']
        subprocess.check_call(cmd)
        print "{} masked".format(tile_id)

    else:

        print "{} outside of latitude band. Skipped tile.".format(tile_id)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, output_name)