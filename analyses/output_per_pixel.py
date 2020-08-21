### Calculates the net emissions over the study period, with units of Mg CO2/ha on a pixel-by-pixel basis

from subprocess import Popen, PIPE, STDOUT, check_call
import os
import datetime
import rasterio
from shutil import copyfile
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def output_per_pixel(tile_id, input_pattern, output_pattern, sensit_type):

    uu.print_log("Calculating per pixel values for", tile_id)

    # Start time
    start = datetime.datetime.now()

    # Names of the input biomass and TCD tiles
    input_model_tile = '{0}_{1}.tif'.format(tile_id, input_pattern)
    area_tile = 'hanson_2013_area_{}.tif'.format(tile_id)
    output_model_tile_no_tag = '{0}_{1}_no_tag.tif'.format(tile_id, output_pattern)
    output_model_tile = '{0}_{1}.tif'.format(tile_id, output_pattern)

    uu.print_log("Converting {} from Mg CO2/ha to Mg CO2/pixel...".format(input_model_tile))
    # Equation argument for converting emissions from per hectare to per pixel.
    # First, multiplies the per hectare emissions by the area of the pixel in m2, then divides by the number of m2 in a hectare.
    calc = '--calc=A*B/{}'.format(cn.m2_per_ha)
    out = '--outfile={}'.format(output_model_tile_no_tag)
    cmd = ['gdal_calc.py', '-A', input_model_tile, '-B', area_tile, calc, out, '--NoDataValue=0', '--co', 'COMPRESS=LZW',
           '--overwrite', '--quiet']
    uu.log_subprocess_output_full(cmd)

    uu.print_log("  Per pixel values calculated for {}".format(output_model_tile))


    uu.print_log("Adding metadata tags to", output_model_tile)
    # The tiles that are used. out_tile_no_tag is the output before metadata tags are added. out_tile is the output
    # once metadata tags have been added.

    copyfile(output_model_tile_no_tag, output_model_tile)

    with rasterio.open(output_model_tile_no_tag) as out_tile_no_tag_src:
        # Grabs metadata about the tif, like its location/projection/cellsize
        kwargs = out_tile_no_tag_src.meta  #### Use profile instead

        # Grabs the windows of the tile (stripes) so we can iterate over the entire tif without running out of memory
        windows = out_tile_no_tag_src.block_windows(1)

        kwargs.update(compress='lzw')

        out_tile_tagged = rasterio.open(output_model_tile, 'w', **kwargs)

        # Adds metadata tags to the output raster
        uu.add_rasterio_tags(out_tile_tagged, sensit_type)
        out_tile_tagged.update_tags(units='Mg CO2e/pixel over model duration (2001-20{})'.format(cn.loss_years),
                                    extent='Model extent',
                                    pixel_areas='Pixel areas depend on the latitude at which the pixel is found',
                                    scale='If this is for net flux, negative values are net sinks and positive values are net sources')

        # Iterates across the windows (1 pixel strips) of the input tile
        for idx, window in windows:
            in_window = out_tile_no_tag_src.read(1, window=window)

            # Writes the output window to the output
            out_tile_tagged.write_band(1, in_window, window=window)

    # Without this, the untagged version is counted and eventually copied to s3 if it has data in it
    os.remove(output_model_tile_no_tag)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, output_pattern)