'''
This script makes mask tiles of where peat pixels are. Peat is represented by 1s; non-peat is no-data.
Between 40N and 60S, CIFOR peat and Jukka peat (IDN and MYS) are combined to map peat.
Outside that band (>40N, since there are no tiles at >60S), SoilGrids250m is used to mask peat.
Any pixel that is marked as most likely being a histosol subgroup is classified as peat.
'''

from subprocess import Popen, PIPE, STDOUT, check_call
import os
import rasterio
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

    out_tile = '{0}_{1}.tif'.format(tile_id, cn.pattern_peat_mask)

    uu.print_log("Adding metadata tags to", tile_id)
    gain = '{0}_{1}.tif'.format(cn.pattern_gain, tile_id)
    # Opens the output tile, only so that metadata tags can be added
    # Based on https://rasterio.readthedocs.io/en/latest/topics/tags.html
    with rasterio.open(gain) as gain_src:

        # Grabs metadata about the tif, like its location/projection/cellsize
        kwargs = gain_src.meta

        # Updates kwargs for the output dataset
        kwargs.update(
            driver='GTiff',
            count=1,
            compress='lzw',
            nodata=0
        )

        out_tile_tagged = rasterio.open(out_tile, 'w', **kwargs)

        # Adds metadata tags to the output raster
        uu.add_rasterio_tags(out_tile_tagged, 'std')
        out_tile_tagged.update_tags(
            units='unitless. 1 = peat. 0 = not peat')
        out_tile_tagged.update_tags(
            source='Jukka for IDN and MYS; CIFOR for rest of tropics; SoilGrids250 (May 2020) most likely histosol for outside tropics')
        out_tile_tagged.update_tags(
            extent='Full extent of input datasets')

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
        peat_mask_out_filearg = '--outfile={}'.format(out_tile)
        cmd = ['gdal_calc.py', '-A', out_intermediate, calc, peat_mask_out_filearg,
               '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW', '--type=Byte', '--quiet']
        uu.log_subprocess_output_full(cmd)

        uu.print_log("{} created.".format(tile_id))

    # If the tile is inside the band covered by CIFOR, CIFOR is used (and Jukka in the tiles where it occurs).
    # For some reason, the CIFOR raster has a color scheme that makes it symbolized from 0 to 255. This carries
    # over to the output file but that seems like a problem with the output symbology, not the values.
    # gdalinfo shows that the min and max values are 1, as they should be, and it visualizes correctly in ArcMap.
    else:

        uu.print_log("{} is inside CIFOR band. Using CIFOR/Jukka combination...".format(tile_id))

        # Combines CIFOR and Jukka (if it occurs there)
        cmd = ['gdalwarp', '-t_srs', 'EPSG:4326', '-co', 'COMPRESS=LZW', '-tr', '{}'.format(cn.Hansen_res), '{}'.format(cn.Hansen_res),
               '-tap', '-te', str(xmin), str(ymin), str(xmax), str(ymax),
               '-dstnodata', '0', '-overwrite', '{}'.format(cn.cifor_peat_file), 'jukka_peat.tif', out_tile]
        uu.log_subprocess_output_full(cmd)

        uu.print_log("{} created.".format(tile_id))




    # with rasterio.open(out_tile) as out_tile_src:
    #
    #     # Grabs metadata about the tif, like its location/projection/cellsize
    #     kwargs = out_tile_src.meta
    #
    #     out_tile_tagged = rasterio.open(out_tile, 'w', **kwargs)
    #
    #     # Adds metadata tags to the output raster
    #     uu.add_rasterio_tags(out_tile_tagged, 'std')
    #     out_tile_tagged.update_tags(
    #         units='unitless. 1 = peat. 0 = not peat')
    #     out_tile_tagged.update_tags(
    #         source='Jukka for IDN and MYS; CIFOR for rest of tropics; SoilGrids250 (May 2020) most likely histosol for outside tropics')
    #     out_tile_tagged.update_tags(
    #         extent='Full extent of input datasets')




    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_peat_mask)




