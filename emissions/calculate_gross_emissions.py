from subprocess import Popen, PIPE, STDOUT, check_call
import datetime
import rasterio
from shutil import copyfile
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Calls the c++ script to calculate gross emissions
def calc_emissions(tile_id, emitted_pools, sensit_type, folder):

    uu.print_log("Calculating gross emissions for", tile_id, "using", sensit_type, "model type...")

    start = datetime.datetime.now()

    # Runs the correct c++ script given the emitted_pools (biomass+soil or soil_only) and model type selected.
    # soil_only, no_shiftin_ag, and convert_to_grassland have special gross emissions C++ scripts.
    # The other sensitivity analyses and the standard model all use the same gross emissions C++ script.
    if (emitted_pools == 'soil_only') & (sensit_type == 'std'):
        cmd = ['{0}/calc_gross_emissions_soil_only.exe'.format(cn.c_emis_compile_dst), tile_id, sensit_type, folder]

    elif (emitted_pools == 'biomass_soil') & (sensit_type in ['convert_to_grassland', 'no_shifting_ag']):
        cmd = ['{0}/calc_gross_emissions_{1}.exe'.format(cn.c_emis_compile_dst, sensit_type), tile_id, sensit_type, folder]

    # This C++ script has an extra argument that names the input carbon emitted_pools and output emissions correctly
    elif (emitted_pools == 'biomass_soil') & (sensit_type not in ['no_shifting_ag', 'convert_to_grassland']):
        cmd = ['{0}/calc_gross_emissions_generic.exe'.format(cn.c_emis_compile_dst), tile_id, sensit_type, folder]

    else:
        uu.exception_log('Pool and/or sensitivity analysis option not valid')

    uu.log_subprocess_output_full(cmd)


    # Identifies which pattern to use for counting tile completion
    pattern = cn.pattern_gross_emis_commod_biomass_soil
    if (emitted_pools == 'biomass_soil') & (sensit_type == 'std'):
        pattern = pattern

    elif (emitted_pools == 'biomass_soil') & (sensit_type != 'std'):
        pattern = pattern + "_" + sensit_type

    elif emitted_pools == 'soil_only':
        pattern = pattern.replace('biomass_soil', 'soil_only')

    else:
        uu.exception_log('Pool option not valid')

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, pattern)


# Adds metadata tags to the output rasters
def add_metadata_tags(tile_id, pattern, sensit_type):

    # The tiles that are used. out_tile_no_tag is the output before metadata tags are added. out_tile is the output
    # once metadata tags have been added.
    out_tile_no_tag = uu.sensit_tile_rename(sensit_type, tile_id, '{}_no_tag'.format(pattern))
    out_tile = uu.sensit_tile_rename(sensit_type, tile_id, pattern)

    uu.print_log("Adding metadata tags to {}".format(out_tile))

    copyfile(out_tile_no_tag, out_tile)

    with rasterio.open(out_tile_no_tag) as out_tile_no_tag_src:
        # Grabs metadata about the tif, like its location/projection/cellsize
        kwargs = out_tile_no_tag_src.meta  #### Use profile instead

        # Grabs the windows of the tile (stripes) so we can iterate over the entire tif without running out of memory
        windows = out_tile_no_tag_src.block_windows(1)

        kwargs.update(
            compress='lzw'
        )

        out_tile_tagged = rasterio.open(out_tile, 'w', **kwargs)

        # Adds metadata tags to the output raster
        uu.add_rasterio_tags(out_tile_tagged, sensit_type)
        out_tile_tagged.update_tags(units='Mg CO2e/ha over model duration (2001-20{})'.format(cn.loss_years),
                        source='Many data sources',
                        extent='Tree cover loss pixels within model extent')

        # Iterates across the windows (1 pixel strips) of the input tile
        for idx, window in windows:
            in_window = out_tile_no_tag_src.read(1, window=window)

            # Writes the output window to the output
            out_tile_tagged.write_band(1, in_window, window=window)

    # Without this, the untagged version is counted and eventually copied to s3 if it has data in it
    os.remove(out_tile_no_tag)


    ### These were Thomas Maschler's suggestions for how to add metadata tags but they didn't work.
    ### The first one produced an error at rasterio.open because there weren't the necessary arguments for the w+ option
    ### (e.g., driver, bands).
    ### The second one ran fine but over-wrote the existing data so that there was no data in the raster after.
    ### That happened at the rasterio.open line.
    # out_tile = '{0}_{1}.tif'.format(tile_id, pattern)
    # uu.print_log("Adding metadata tags to", out_tile)
    #
    # # with rasterio.open(out_tile, 'w+') as src:
    # #
    # #     src.update_tags(units='Mg CO2e over model duration (2001-20{})'.format(cn.loss_years),
    # #                     source='Many data sources',
    # #                     extent='Tree cover loss pixels')
    #
    #
    #
    # with rasterio.open(out_tile, 'r') as src:
    #
    #     profile = src.profile
    #     src.close()
    #
    # with rasterio.open(out_tile, 'w+', **profile) as dst:
    #
    #     dst.update_tags(units='Mg CO2e over model duration (2001-20{})'.format(cn.loss_years),
    #                     source='Many data sources',
    #                     extent='Tree cover loss pixels')
    # #     dst.close()
