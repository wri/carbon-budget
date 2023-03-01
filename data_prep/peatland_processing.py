'''
This script makes mask tiles of where peat pixels are. Peat is represented by 1s; non-peat is no-data.
Between 40N and 60S, Gumbricht et al. 2017 (CIFOR) peat is used.
Miettinen et al. 2016 and Dargie et al. 2017 supplement it in IDN/MYS and the Congo basin, respectively.
Outside that band (>40N, since there are no tiles at >60S), Xu et al. 2018 is used to mask peat.
Between 40N and 60S, Xu et al. 2018 is not used.
'''

from subprocess import Popen, PIPE, STDOUT, check_call
import os
import rasterio
from shutil import copyfile
import datetime
import sys

import constants_and_names as cn
import universal_util as uu


def create_peat_mask_tiles(tile_id):
    """
    :param tile_id: tile to be processed, identified by its tile id
    :return: Peat mask: 1 is peat, 0 is no peat
    """

    # Start time
    start = datetime.datetime.now()

    uu.print_log("Getting bounding coordinates for tile", tile_id)
    xmin, ymin, xmax, ymax = uu.coords(tile_id)
    uu.print_log("  ymax:", ymax, "; ymin:", ymin, "; xmax", xmax, "; xmin:", xmin)

    out_tile_no_tag = f'{tile_id}_{cn.pattern_peat_mask}_no_tag.tif'
    out_tile = f'{tile_id}_{cn.pattern_peat_mask}.tif'

    # If the tile is outside the band covered by the Gumbricht 2017/CIFOR peat raster, Xu et al. 2018 is used.
    if ymax > 40 or ymax < -60:

        uu.print_log(f'{tile_id} is outside Gumbricht band. Using Xu et al. 2018 peat map...')

        # Converts the Xu >40N peat shapefile to a raster
        cmd = ['gdal_rasterize', '-burn', '1', '-co', 'COMPRESS=DEFLATE', '-tr', str(cn.Hansen_res), str(cn.Hansen_res),
               '-te', str(xmin), str(ymin), str(xmax), str(ymax),
               '-tap', '-ot', 'Byte', '-a_nodata', '0', cn.Xu_peat_shp, out_tile_no_tag]
        uu.log_subprocess_output_full(cmd)
        uu.print_log(f'{tile_id} created.')

    # If the tile is inside the band covered by Gumbricht 2017/CIFOR, Gumbricht is used.
    # Miettinen is added in IDN and MYS and Dargie is added in the Congo basin.
    # For some reason, the Gumbricht raster has a color scheme that makes it symbolized from 0 to 255. This carries
    # over to the output file but that seems like a problem with the output symbology, not the values.
    # gdalinfo shows that the min and max values are 1, as they should be, and it visualizes correctly in ArcMap.
    else:

        uu.print_log(f"{tile_id} is inside Gumbricht band. Using Gumbricht/Miettinen/Dargie combination...")

        # Combines Gumbricht/CIFOR with Miettinen and Dargie (where they occur)
        cmd = ['gdalwarp', '-t_srs', 'EPSG:4326', '-co', 'COMPRESS=DEFLATE', '-tr', str(cn.Hansen_res), str(cn.Hansen_res),
               '-tap', '-te', str(xmin), str(ymin), str(xmax), str(ymax),
               '-dstnodata', '0', '-overwrite',
               cn.Gumbricht_peat_name, cn.Miettinen_peat_tif, cn.Dargie_peat_name, out_tile_no_tag]
        uu.log_subprocess_output_full(cmd)
        uu.print_log(f'{tile_id} created.')

    # All of the below is to add metadata tags to the output peat masks.
    # For some reason, just doing what's at https://rasterio.readthedocs.io/en/latest/topics/tags.html
    # results in the data getting removed.
    # I found it necessary to copy the peat mask and read its windows into a new copy of the file, to which the
    # metadata tags are added. I'm sure there's an easier way to do this but I couldn't figure out how.
    # I know it's very convoluted but I really couldn't figure out how to add the tags without erasing the data.
    # To make it even stranger, adding the tags before the gdal processing seemed to work fine for the non-tropical
    # (SoilGrids) tiles but not for the tropical (Gumbricht/Miettinen) tiles (i.e. data didn't disappear in the non-tropical
    # tiles if I added the tags before the GDAL steps but the tropical data did disappear).

    copyfile(out_tile_no_tag, out_tile)

    uu.print_log("Adding metadata tags to", tile_id)
    # Opens the output tile, only so that metadata tags can be added
    # Based on https://rasterio.readthedocs.io/en/latest/topics/tags.html
    with rasterio.open(out_tile_no_tag) as out_tile_no_tag_src:

        # Grabs metadata about the tif, like its location/projection/cellsize
        kwargs = out_tile_no_tag_src.meta

        # Grabs the windows of the tile (stripes) so we can iterate over the entire tif without running out of memory
        windows = out_tile_no_tag_src.block_windows(1)

        # Updates kwargs for the output dataset
        kwargs.update(
            driver='GTiff',
            count=1,
            compress='DEFLATE',
            nodata=0
        )

        out_tile_tagged = rasterio.open(out_tile, 'w', **kwargs)

        # Adds metadata tags to the output raster
        uu.add_universal_metadata_rasterio(out_tile_tagged)
        out_tile_tagged.update_tags(
            key='1 = peat. 0 = not peat.')
        out_tile_tagged.update_tags(
            source='Gumbricht et al. 2017 for <40N>; Miettinen et al. and Dargie et al. where they occur; Xu et al. for >40N')
        out_tile_tagged.update_tags(
            extent='Full extent of input datasets')

        # Iterates across the windows (1 pixel strips) of the input tile
        for idx, window in windows:

            peat_mask_window = out_tile_no_tag_src.read(1, window=window)

            # Writes the output window to the output
            out_tile_tagged.write_band(1, peat_mask_window, window=window)

    # Otherwise, the untagged version is counted and eventually copied to s3 if it has data in it
    os.remove(out_tile_no_tag)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_peat_mask)




