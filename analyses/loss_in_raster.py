import subprocess
import datetime
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Calculates a range of tile statistics
def loss_in_raster(tile, raster_type, output_name, lat):

    # Extracts the tile id from the full tile name
    tile_id = uu.get_tile_id(tile)

    print "Calculating loss area for {0}, tile id {1}...".format(tile, tile_id)

    xmin, ymin, xmax, ymax = uu.coords(tile_id)

    # start time
    start = datetime.datetime.now()

    # Only processes the tile if it is inside the latitude band (north of the specified latitude)
    if ymax > lat:

        print "{} inside latitude band. Processing tile.".format(tile)

        # The raster that loss is being analyzed inside
        raster_of_interest = '{0}_{1}.tif'.format(tile_id, raster_type)

        # Output file name
        outname = '{0}_{1}.tif'.format(tile_id, output_name)

        # Equation argument for converting emissions from per hectare to per pixel.
        # First, multiplies the per hectare emissions by the area of the pixel in m2, then divides by the number of m2 in a hectare.
        calc = '--calc=A*B'

        # Argument for outputting file
        out = '--outfile={}'.format(outname)

        print "Masking loss in {} by raster of interest...".format(tile)
        cmd = ['gdal_calc.py', '-A', tile, '-B', raster_of_interest, calc, out, '--NoDataValue=0', '--co', 'COMPRESS=LZW',
               '--overwrite']
        subprocess.check_call(cmd)
        print "{} masked".format(tile)

    else:

        print "{} outside of latitude band. Skipped tile.".format(tile_id)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, output_name)