import os
from subprocess import Popen, PIPE, STDOUT, check_call
import sys
import utilities
sys.path.append('../')
import universal_util as uu
import constants_and_names as cn

currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)

def clip_year_tiles(tile_year_list):

    # Start time
    start = datetime.datetime.now()

    tile_id = tile_year_list[0].strip('.tif')
    year = tile_year_list[1]

    vrt_name = "global_vrt_{}_wgs84.vrt".format(year)

    # Gets coordinates of hansen tile
    uu.print_log("Getting coordinates of", tile_id)
    xmin, ymin, xmax, ymax = uu.coords(tile_id)

    # Clips vrt to tile extent
    uu.print_log("Clipping burn year vrt to {0} for {1}".format(tile_id, year))

    clipped_raster = "ba_{0}_{1}_clipped.tif".format(year, tile_id)
    cmd = ['gdal_translate', '-ot', 'Byte', '-co', 'COMPRESS=LZW', '-a_nodata', '0']
    cmd += [vrt_name, clipped_raster, '-tr', '.00025', '.00025']
    cmd += ['-projwin', str(xmin), str(ymax), str(xmax), str(ymin)]
    uu.log_subprocess_output_full(cmd)

    # Calculates year tile values to be equal to year. ex: 17*1
    calc = '--calc={}*(A>0)'.format(int(year)-2000)
    recoded_output = "ba_{0}_{1}.tif".format(year, tile_id)
    outfile = '--outfile={}'.format(recoded_output)

    cmd = ['gdal_calc.py', '-A', clipped_raster, calc, outfile, '--NoDataValue=0', '--co', 'COMPRESS=LZW', '--quiet']
    uu.log_subprocess_output_full(cmd)

    # Only copies to s3 if the tile has data.
    # No tiles for 2000 have data because the burn year is coded as 0, which is NoData.
    uu.print_log("Checking if {} contains any data...".format(tile_id))
    empty = uu.check_for_data(recoded_output)

    if empty:
        uu.print_log("  No data found. Not copying {}.".format(tile_id))

    else:
        uu.print_log("  Data found in {}. Copying tile to s3...".format(tile_id))
        cmd = ['aws', 's3', 'cp', recoded_output, cn.burn_year_warped_to_Hansen_dir]
        uu.log_subprocess_output_full(cmd)
        uu.print_log("    Tile copied to s3")

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, "ba_{}".format(year))


