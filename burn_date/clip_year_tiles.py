import os
from subprocess import Popen, PIPE, STDOUT, check_call
import sys

import utilities
sys.path.append('../')
import universal_util as uu

currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)


def clip_year_tiles(tile_year_list):
    # Given a list of tiles and years ['00N_000E', 2017] and a VRT of burn data,
    # the global vrt has pixels representing burned or not. This process clips the global VRT
    # and changes the pixel value to represent the year the pixel was burned. Each tile has value of
    # year burned and NoData

    tile_id = tile_year_list[0].strip('.tif')
    year = tile_year_list[1]

    vrt_name = "global_vrt_{}_wgs84.vrt".format(year)

    # get coords of hansen tile
    uu.print_log("Getting coordinates of", tile_id)
    xmin, ymin, xmax, ymax = uu.coords(tile_id)

    # clip vrt to tile extent
    clipped_raster = "ba_{0}_{1}_clipped.tif".format(year, tile_id)

    uu.print_log("Clipping burn year vrt to", tile_id)

    cmd = ['gdal_translate', '-ot', 'Byte', '-co', 'COMPRESS=LZW', '-a_nodata', '0']
    cmd += [vrt_name, clipped_raster, '-tr', '.00025', '.00025']
    cmd += ['-projwin', str(xmin), str(ymax), str(xmax), str(ymin)]
    # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        uu.log_subprocess_output(process.stdout)

    # calc year tile values to be equal to year. ex: 17*1
    calc = '--calc={}*(A>0)'.format(int(year)-2000)
    recoded_output = "ba_{0}_{1}.tif".format(year, tile_id)
    outfile = '--outfile={}'.format(recoded_output)

    cmd = ['gdal_calc.py', '-A', clipped_raster, calc, outfile, '--NoDataValue=0', '--co', 'COMPRESS=LZW', '--quiet']
    # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        uu.log_subprocess_output(process.stdout)

    # upload file
    cmd = ['aws', 's3', 'mv', recoded_output,
           's3://gfw2-data/climate/carbon_model/other_emissions_inputs/burn_year/20190322/burn_year_10x10_clip/']

    # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        uu.log_subprocess_output(process.stdout)

    # remove files
    uu.print_log("Removing files")
    files_to_remove = [clipped_raster, recoded_output]
    utilities.remove_list_files(files_to_remove)

    uu.print_log("Done removing individual files")

