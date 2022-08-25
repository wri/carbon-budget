'''
This script makes mask tiles of where peat pixels are. Peat is represented by 1s; non-peat is no-data.
Between 40N and 60S, CIFOR peat and Jukka peat (IDN and MYS) are combined to map peat.
Outside that band (>40N, since there are no tiles at >60S), SoilGrids250m is used to mask peat.
Any pixel that is marked as most likely being a histosol subgroup is classified as peat.
'''

from subprocess import Popen, PIPE, STDOUT, check_call
import os
import rasterio
from shutil import copyfile
import datetime
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def create_peat_mask_tiles(tile_id):

    # Start time
    start = datetime.datetime.now()

    uu.print_log("Getting bounding coordinates for tile", tile_id)
    xmin, ymin, xmax, ymax = uu.coords(tile_id)
    uu.print_log("  ymax:", ymax, "; ymin:", ymin, "; xmax", xmax, "; xmin:", xmin)

    out_tile_no_tag = '{0}_{1}_no_tag.tif'.format(tile_id, cn.pattern_peat_mask)
    out_tile = '{0}_{1}.tif'.format(tile_id, cn.pattern_peat_mask)

    # If the tile is outside the band covered by the CIFOR peat raster, SoilGrids250m is used
    if ymax > 40 or ymax < -60:

        uu.print_log("{} is outside CIFOR band. Using SoilGrids250m organic soil mask...".format(tile_id))

        out_intermediate = '{0}_intermediate.tif'.format(tile_id, cn.pattern_peat_mask)

        # Cuts the SoilGrids250m global raster to the focal tile
        uu.warp_to_Hansen('most_likely_soil_class.vrt', out_intermediate, xmin, ymin, xmax, ymax, 'Byte')

        # Removes all non-histosol sub-groups from the SoilGrids raster.
        # Ideally, this would be done once on the entire SoilGrids raster in the main function but I didn't think of that.
        # Code 14 is the histosol subgroup in SoilGrids250 (https://files.isric.org/soilgrids/latest/data/wrb/MostProbable.qml).
        calc = '--calc=(A==14)'
        peat_mask_out_filearg = '--outfile={}'.format(out_tile_no_tag)
        cmd = ['gdal_calc.py', '-A', out_intermediate, calc, peat_mask_out_filearg,
               '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=DEFLATE', '--type=Byte', '--quiet']
        uu.log_subprocess_output_full(cmd)

        uu.print_log("{} created.".format(tile_id))

    # If the tile is inside the band covered by CIFOR, CIFOR is used (and Jukka in the tiles where it occurs).
    # For some reason, the CIFOR raster has a color scheme that makes it symbolized from 0 to 255. This carries
    # over to the output file but that seems like a problem with the output symbology, not the values.
    # gdalinfo shows that the min and max values are 1, as they should be, and it visualizes correctly in ArcMap.
    else:

        uu.print_log("{} is inside CIFOR band. Using CIFOR/Jukka combination...".format(tile_id))

        # Combines CIFOR and Jukka (if it occurs there)
        cmd = ['gdalwarp', '-t_srs', 'EPSG:4326', '-co', 'COMPRESS=DEFLATE', '-tr', '{}'.format(cn.Hansen_res), '{}'.format(cn.Hansen_res),
               '-tap', '-te', str(xmin), str(ymin), str(xmax), str(ymax),
               '-dstnodata', '0', '-overwrite', '{}'.format(cn.cifor_peat_file), 'jukka_peat.tif', out_tile_no_tag]
        uu.log_subprocess_output_full(cmd)

        uu.print_log("{} created.".format(tile_id))

    # All of the below is to add metadata tags to the output peat masks.
    # For some reason, just doing what's at https://rasterio.readthedocs.io/en/latest/topics/tags.html
    # results in the data getting removed.
    # I found it necessary to copy the peat mask and read its windows into a new copy of the file, to which the
    # metadata tags are added. I'm sure there's an easier way to do this but I couldn't figure out how.
    # I know it's very convoluted but I really couldn't figure out how to add the tags without erasing the data.
    # To make it even stranger, adding the tags before the gdal processing seemed to work fine for the non-tropical
    # (SoilGrids) tiles but not for the tropical (CIFOR/Jukka) tiles (i.e. data didn't disappear in the non-tropical
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
            source='Jukka for IDN and MYS; CIFOR for rest of tropics; SoilGrids250 (May 2020) most likely histosol for outside tropics')
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




