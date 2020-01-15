import numpy as np
import datetime
import rasterio
import os
import subprocess
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu


def legal_Amazon_forest_age_category(tile_id, sensit_type, output_pattern):
    # Start time
    start = datetime.datetime.now()

    loss = '{0}_{1}.tif'.format(tile_id, cn.pattern_Brazil_annual_loss_processed)
    gain = '{0}_{1}.tif'.format(cn.pattern_gain, tile_id)
    extent = '{0}_{1}.tif'.format(tile_id, cn.pattern_Brazil_forest_extent_2000_processed)
    biomass = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_WHRC_biomass_2000_non_mang_non_planted)
    plantations = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_planted_forest_type_unmasked)
    mangroves = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_mangrove_biomass_2000)

    # Opens biomass tile
    with rasterio.open(loss) as loss_src:

        # Grabs metadata about the tif, like its location/projection/cellsize
        kwargs = loss_src.meta

        # Grabs the windows of the tile (stripes) so we can iterate over the entire tif without running out of memory
        windows = loss_src.block_windows(1)

        # Opens tiles
        gain_src = rasterio.open(gain)
        extent_src = rasterio.open(extent)
        biomass_src = rasterio.open(biomass)

        # Checks whether there are mangrove or planted forest tiles. If so, they are opened.
        try:
            plantations_src = rasterio.open(plantations)
            print "    Planted forest tile found for {}".format(tile_id)
        except:
            print "    No planted forest tile for {}".format(tile_id)

        try:
            mangroves_src = rasterio.open(mangroves)
            print "    Mangrove tile found for {}".format(tile_id)
        except:
            print "    No mangrove tile for {}".format(tile_id)

        # Updates kwargs for the output dataset
        kwargs.update(
            driver='GTiff',
            count=1,
            compress='lzw',
            nodata=0
        )

        # Opens the output tile, giving it the arguments of the input tiles
        dst = rasterio.open('{0}_{1}.tif'.format(tile_id, output_pattern), 'w', **kwargs)

        # Iterates across the windows (1 pixel strips) of the input tile
        for idx, window in windows:

            # Creates windows for each input raster
            loss_window = loss_src.read(1, window=window)
            gain_window = gain_src.read(1, window=window)
            extent_window = extent_src.read(1, window=window)
            biomass_window = biomass_src.read(1, window=window)

            # Create a 0s array for the output
            dst_data = np.zeros((window.height, window.width), dtype='uint8')

            # No change pixels (no loss or gain)
            dst_data[np.where((biomass_window > 0) & (extent_window == 1) & (loss_window == 0))] = 3  # primary forest

            # Loss-only pixels
            dst_data[np.where((biomass_window > 0) & (extent_window == 1) & (loss_window > 0))] = 6   # primary forest

            # Loss-and-gain pixels
            dst_data[np.where((extent_window == 1) & (gain_window == 1) & (loss_window > 0))] = 8   # young secondary forest

            if os.path.exists(mangroves):
                mangroves_window = mangroves_src.read(1, window=window)
                dst_data = np.ma.masked_where(mangroves_window != 0, dst_data).filled(0).astype('uint8')

            if os.path.exists(plantations):
                plantations_window = plantations_src.read(1, window=window)
                dst_data = np.ma.masked_where(plantations_window != 0, dst_data).filled(0).astype('uint8')

            # Writes the output window to the output
            dst.write_band(1, dst_data, window=window)

    uu.end_of_fx_summary(start, tile_id, output_pattern)


# Gets the names of the input tiles
def tile_names(tile_id, sensit_type):

    # Names of the input files
    loss = '{0}_{1}.tif'.format(tile_id, cn.pattern_Brazil_annual_loss_processed)
    gain = '{0}_{1}.tif'.format(cn.pattern_gain, tile_id)
    extent = '{0}_{1}.tif'.format(tile_id, cn.pattern_Brazil_forest_extent_2000_processed)
    biomass = uu.sensit_tile_rename(sensit_type, tile_id, cn.pattern_WHRC_biomass_2000_non_mang_non_planted)

    return loss, gain, extent, biomass


# Creates gain year count tiles for pixels that only had loss
def legal_Amazon_create_gain_year_count_loss_only(tile_id, sensit_type):

    print "Gain year count for loss only pixels:", tile_id

    # Names of the input tiles
    loss, gain, extent, biomass = tile_names(tile_id, sensit_type)

    # start time
    start = datetime.datetime.now()

    # Pixels with loss only
    loss_calc = '--calc=(A>0)*(B==0)*(C==1)*(A-1)'
    loss_outfilename = '{}_growth_years_loss_only.tif'.format(tile_id)
    loss_outfilearg = '--outfile={}'.format(loss_outfilename)
    cmd = ['gdal_calc.py', '-A', loss, '-B', gain, '-C', extent, loss_calc, loss_outfilearg,
           '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW', '--type', 'Byte']
    subprocess.check_call(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, 'growth_years_loss_only')


# Creates gain year count tiles for pixels that had neither loss not gain
def legal_Amazon_create_gain_year_count_no_change(tile_id, sensit_type):

    print "No change pixel processing:", tile_id

    # start time
    start = datetime.datetime.now()

    # Names of the loss, gain and tree cover density tiles
    loss, gain, extent, biomass = tile_names(tile_id, sensit_type)

    loss_vrt = '{}_loss.vrt'.format(tile_id)
    os.system('gdalbuildvrt -vrtnodata None {0} {1}'.format(loss_vrt, loss))

    # Pixels with neither loss nor gain but in areas with tree cover density >0 and biomass >0 (so that oceans aren't included)
    no_change_calc = '--calc=(A==0)*(B==1)*(C>0)*{}'.format(cn.loss_years)
    no_change_outfilename = '{}_growth_years_no_change.tif'.format(tile_id)
    no_change_outfilearg = '--outfile={}'.format(no_change_outfilename)
    cmd = ['gdal_calc.py', '-A', loss_vrt, '-B', extent, '-C', biomass, no_change_calc,
           no_change_outfilearg, '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW', '--type', 'Byte']
    subprocess.check_call(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, 'growth_years_no_change')


# Creates gain year count tiles for pixels that had both loss and gain
def legal_Amazon_create_gain_year_count_loss_and_gain_standard(tile_id, sensit_type):

    print "Loss and gain pixel processing using standard function:", tile_id

    # start time
    start = datetime.datetime.now()

    # Names of the loss, gain and tree cover density tiles
    loss, gain, extent, biomass = tile_names(tile_id, sensit_type)

    # Pixels with both loss and gain
    loss_and_gain_calc = '--calc=((A>0)*(B==1)*(C==1)*((A-1)+({}+1-A)/2))'.format(cn.loss_years)
    loss_and_gain_outfilename = '{}_growth_years_loss_and_gain.tif'.format(tile_id)
    loss_and_gain_outfilearg = '--outfile={}'.format(loss_and_gain_outfilename)
    cmd = ['gdal_calc.py', '-A', loss, '-B', gain, '-C', extent, loss_and_gain_calc,
           loss_and_gain_outfilearg, '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=LZW', '--type', 'Byte']
    subprocess.check_call(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, 'growth_years_loss_and_gain')


# Merges the four gain year count tiles above to create a single gain year count tile
def legal_Amazon_create_gain_year_count_merge(tile_id, output_pattern):

    print "Merging loss, gain, no change, and loss/gain pixels into single raster for {}".format(tile_id)

    # start time
    start = datetime.datetime.now()

    # The four rasters from above that are to be merged
    loss_outfilename = '{}_growth_years_loss_only.tif'.format(tile_id)
    no_change_outfilename = '{}_growth_years_no_change.tif'.format(tile_id)
    loss_and_gain_outfilename = '{}_growth_years_loss_and_gain.tif'.format(tile_id)

    # All four components are merged together to the final output raster
    age_outfile = '{}_{}.tif'.format(tile_id, output_pattern)
    cmd = ['gdal_merge.py', '-o', age_outfile, loss_outfilename, no_change_outfilename, loss_and_gain_outfilename,
           '-co', 'COMPRESS=LZW', '-a_nodata', '0', '-ot', 'Byte']
    subprocess.check_call(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, output_pattern)





