'''
This script creates tiles of soil carbon density, one of the carbon emitted_pools.
At this time, mineral soil carbon is for the top 30 cm of soil.
Mangrove soil carbon gets precedence over mineral soil carbon where there is mangrove biomass.
Where there is no mangrove biomass, mineral soil C is used.
Peatland carbon is not recognized or involved in any way.
This is a convoluted way of doing this processing. Originally, I tried making mangrove soil tiles masked to
mangrove AGB tiles, then making a vrt of all those mangrove soil C tiles and the mineral soil raster, and then
using gdal_warp on that to get combined tiles.
However, for reasons I couldn't figure out, the gdalbuildvrt step in which I combined the mangrove 10x10 tiles
and the mineral soil raster never actually combined the mangrove tiles with the mineral soil raste; I just kept
getting mineral soil C values out.
So, I switched to this somewhat more convoluted method that uses both gdal and rasterio/numpy.
'''

import datetime
import numpy as np
import rasterio
import os

import universal_util as uu
import constants_and_names as cn

# Creates 10x10 mangrove soil C tiles
def create_mangrove_soil_C(tile_id):

    # Start time
    start = datetime.datetime.now()

    # Checks if mangrove biomass exists. If not, it won't create a mangrove soil C tile.
    if os.path.exists(f'{tile_id}_{cn.pattern_mangrove_biomass_2000}.tif'):

        uu.print_log("Mangrove aboveground biomass tile found for", tile_id)

        uu.print_log("Getting extent of", tile_id)
        xmin, ymin, xmax, ymax = uu.coords(tile_id)

        uu.print_log("Clipping mangrove soil C from mangrove soil vrt for", tile_id)
        uu.warp_to_Hansen('mangrove_soil_C.vrt', f'{tile_id}_mangrove_full_extent.tif', xmin, ymin, xmax, ymax, 'Int16')

        mangrove_soil = f'{tile_id}_mangrove_full_extent.tif'
        mangrove_biomass = f'{tile_id}_{cn.pattern_mangrove_biomass_2000}.tif'
        outname = f'{tile_id}_{cn.pattern_soil_C_mangrove}.tif'
        out = '--outfile={}'.format(outname)
        calc = '--calc=A*(B>0)'
        datatype = '--type={}'.format('Int16')

        uu.print_log("Masking mangrove soil to mangrove biomass for", tile_id)
        cmd = ['gdal_calc.py', '-A', mangrove_soil, '-B', mangrove_biomass,
               calc, out, '--NoDataValue=0', '--co', 'COMPRESS=DEFLATE', '--overwrite', datatype, '--quiet']
        uu.log_subprocess_output_full(cmd)

    else:

        uu.print_log("Mangrove aboveground biomass tile not found for", tile_id)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_soil_C_mangrove)


# Overlays the mangrove soil C tiles with the mineral soil C tiles, giving precedence to the mangrove soil C
def create_combined_soil_C(tile_id):

    # Start time
    start = datetime.datetime.now()

    # Input files
    mangrove_soil = f'{tile_id}_{cn.pattern_soil_C_mangrove}.tif'
    mineral_soil = f'{tile_id}_{cn.pattern_soil_C_full_extent_2000_non_mang}.tif'

    # Output file
    combined_soil = f'{tile_id}_{cn.pattern_soil_C_full_extent_2000}.tif'

    # Checks if mangrove AGB tile exists. If not, mangrove soil C is not combined with mineral soil C.
    if os.path.exists(f'{tile_id}_{cn.pattern_mangrove_biomass_2000}.tif'):

        uu.print_log("Mangrove aboveground biomass tile found for", tile_id)

        mangrove_soil_src = rasterio.open(mangrove_soil)
        # Grabs metadata for one of the input tiles, like its location/projection/cellsize
        kwargs = mangrove_soil_src.meta
        # Grabs the windows of the tile (stripes) to iterate over the entire tif without running out of memory
        windows = mangrove_soil_src.block_windows(1)

        mineral_soil_src = rasterio.open(mineral_soil)

        # Updates kwargs for the output dataset.
        # Need to update data type to float 32 so that it can handle fractional removal rates
        kwargs.update(
            driver='GTiff',
            count=1,
            compress='DEFLATE',
            nodata=0
        )

        # The output file: soil C with mangrove soil C taking precedence over mineral soil C
        dst_combined_soil = rasterio.open(combined_soil, 'w', **kwargs)

        uu.print_log("Replacing mineral soil C pixels with mangrove soil C pixels for", tile_id)

        # Iterates across the windows (1 pixel strips) of the input tiles
        for idx, window in windows:

            mangrove_soil_window = mangrove_soil_src.read(1, window=window)
            mineral_soil_window = mineral_soil_src.read(1, window=window)

            combined_soil_window = np.where(mangrove_soil_window>0, mangrove_soil_window, mineral_soil_window)

            dst_combined_soil.write_band(1, combined_soil_window, window=window)

    else:

        uu.print_log("Mangrove aboveground biomass tile not found for", tile_id)

        # If there is no mangrove soil C tile, the final output of the mineral soil function needs to receive the
        # correct final name.
        os.rename(f'{tile_id}_{cn.pattern_soil_C_full_extent_2000_non_mang}.tif', combined_soil)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_soil_C_full_extent_2000)
