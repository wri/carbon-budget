"""
Functions to create tiles with the number of years of carbon accumulation
"""

import datetime
import numpy as np
import rasterio
import os

import constants_and_names as cn
import universal_util as uu

def tile_names(tile_id):
    """
    Gets the names of the input tiles
    :param tile_id: tile to be processed, identified by its tile id
    :return: names of input tiles
    """

    # Names of the loss, gain, and model extent tiles
    if cn.SENSIT_TYPE == 'legal_Amazon_loss':
        loss = f'{tile_id}_{cn.pattern_Brazil_annual_loss_processed}.tif'
    else:
        loss = f'{cn.pattern_loss}_{tile_id}.tif'
    gain = f'{tile_id}_{cn.pattern_gain_ec2}.tif'
    model_extent = uu.sensit_tile_rename(cn.SENSIT_TYPE, tile_id, cn.pattern_model_extent)

    return loss, gain, model_extent


def create_gain_year_count_loss_only(tile_id):
    """
    Creates gain year count tiles for pixels that only had loss
    :param tile_id: tile to be processed, identified by its tile id
    :return: tile with number of years of carbon accumulation in pixels that only had tree cover loss
    """

    uu.print_log(f'Gain year count for loss-only pixels: {tile_id}')

    # Names of the loss, gain and tree cover density tiles
    loss, gain, model_extent = tile_names(tile_id)

    # start time
    start = datetime.datetime.now()

    uu.check_memory()

    if os.path.exists(loss):
        uu.print_log(f'  Loss tile found for {tile_id}. Using it in loss-only pixel gain year count.')
        loss_calc = '--calc=(A>0)*(B==0)*(C>0)*(A-1)'
        loss_outfilename = f'{tile_id}_gain_year_count_loss_only.tif'
        loss_outfilearg = f'--outfile={loss_outfilename}'
        cmd = ['gdal_calc.py', '-A', loss, '-B', gain, '-C', model_extent, loss_calc, loss_outfilearg,
               '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=DEFLATE', '--type', 'Byte', '--quiet',
               '--hideNoData'] # Need --hideNoData because the non-gain pixels are NoData, not 0.
        uu.log_subprocess_output_full(cmd)
    else:
        uu.print_log(f'  Loss tile not found for {tile_id}. Skipping loss-only pixel gain year count.')

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, 'gain_year_count_loss_only')


def create_gain_year_count_gain_only_standard(tile_id):
    """
    Creates gain year count tiles for pixels that only had gain (standard model only)
    :param tile_id: tile to be processed, identified by its tile id
    :return: tile with number of years of carbon accumulation in pixels that only had tree cover gain
    """

    uu.print_log(f'Gain year count for gain-only pixels using standard function: {tile_id}')

    # Names of the loss, gain and tree cover density tiles
    loss, gain, model_extent = tile_names(tile_id)

    # start time
    start = datetime.datetime.now()

    uu.check_memory()

    # Need to check if gain tile exists.
    if not os.path.exists(gain):
        uu.print_log(f'  Gain tile not found for {tile_id}. Skipping gain-only pixel gain year count.')

    # Need to check if loss tile exists because the calc string depends on the presence/absence of the loss tile
    elif os.path.exists(loss):
        uu.print_log(f'  Loss tile found for {tile_id}. Using it in gain-only pixel gain year count.')
        gain_calc = f'--calc=(A==0)*(B==1)*(C>0)*({cn.gain_years}/2)'
        gain_outfilename = f'{tile_id}_gain_year_count_gain_only.tif'
        gain_outfilearg = f'--outfile={gain_outfilename}'
        cmd = ['gdal_calc.py', '-A', loss, '-B', gain, '-C', model_extent, gain_calc, gain_outfilearg,
               '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=DEFLATE', '--type', 'Byte', '--quiet']
        uu.log_subprocess_output_full(cmd)
    else:
        uu.print_log(f'  Loss tile not found for {tile_id}. Not using it for gain-only pixel gain year count.')
        gain_calc = f'--calc=(A==1)*(B>0)*({cn.gain_years}/2)'
        gain_outfilename = f'{tile_id}_gain_year_count_gain_only.tif'
        gain_outfilearg = f'--outfile={gain_outfilename}'
        cmd = ['gdal_calc.py', '-A', gain, '-B', model_extent, gain_calc, gain_outfilearg,
               '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=DEFLATE', '--type', 'Byte', '--quiet']
        uu.log_subprocess_output_full(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, 'gain_year_count_gain_only')


def create_gain_year_count_gain_only_maxgain(tile_id):
    """
    Creates gain year count tiles for pixels that only had gain (maximum gain year sensitivity analysis only)
    :param tile_id: tile to be processed, identified by its tile id
    :return: tile with number of years of carbon accumulation in pixels that only had tree cover gain
    """

    uu.print_log(f'Gain year count for gain-only pixels using maxgain function: {tile_id}')

    # Names of the loss, gain and tree cover density tiles
    loss, gain, model_extent = tile_names(tile_id)

    # start time
    start = datetime.datetime.now()

    uu.check_memory()

    if os.path.exists(loss):
        uu.print_log(f'  Loss tile found for {tile_id}. Using it in gain-only pixel gain year count.')
        gain_calc = f'--calc=(A==0)*(B==1)*(C>0)*({cn.loss_years})'
        gain_outfilename = f'{tile_id}_gain_year_count_gain_only.tif'
        gain_outfilearg = f'--outfile={gain_outfilename}'
        cmd = ['gdal_calc.py', '-A', loss, '-B', gain, '-C', model_extent, gain_calc, gain_outfilearg,
               '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=DEFLATE', '--type', 'Byte', '--quiet']
        uu.log_subprocess_output_full(cmd)
    else:
        uu.print_log(f'  Loss tile not found for {tile_id}. Not using loss for gain-only pixel gain year count.')
        gain_calc = f'--calc=(A==1)*(B>0)*({cn.loss_years})'
        gain_outfilename = f'{tile_id}_gain_year_count_gain_only.tif'
        gain_outfilearg = f'--outfile={gain_outfilename}'
        cmd = ['gdal_calc.py', '-A', gain, '-B', model_extent, gain_calc, gain_outfilearg,
               '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=DEFLATE', '--type', 'Byte', '--quiet']
        uu.log_subprocess_output_full(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, 'gain_year_count_gain_only')


def create_gain_year_count_no_change_standard(tile_id):
    """
    Creates gain year count tiles for pixels that had neither loss not gain.
    For all models except legal_Amazon_loss.
    :param tile_id: tile to be processed, identified by its tile id
    :return: tile with number of years of carbon accumulation in pixels that had neither loss nor gain
    """

    uu.print_log("Gain year count for pixels with neither loss nor gain:", tile_id)

    # Names of the loss, gain and tree cover density tiles
    loss, gain, model_extent = tile_names(tile_id)

    # start time
    start = datetime.datetime.now()

    uu.check_memory()

    if os.path.exists(loss) and os.path.exists(gain):
        uu.print_log(f'  Loss and gain tiles found for {tile_id}. Using them in no-change pixel gain year count.')
        no_change_calc = f'--calc=(A==0)*(B==0)*(C>0)*{cn.loss_years}'
        no_change_outfilename = f'{tile_id}_gain_year_count_no_change.tif'
        no_change_outfilearg = f'--outfile={no_change_outfilename}'
        cmd = ['gdal_calc.py', '-A', loss, '-B', gain, '-C', model_extent, no_change_calc,
               no_change_outfilearg, '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=DEFLATE', '--type', 'Byte', '--quiet',
               '--hideNoData'] # Need --hideNoData because the non-gain pixels are NoData, not 0.
        uu.log_subprocess_output_full(cmd)
    elif os.path.exists(loss):
        uu.print_log(f'  Gain tile not found for {tile_id}. Not using it for no-change pixel gain year count.')
        no_change_calc = f'--calc=(A>0)*{cn.loss_years}'
        no_change_outfilename = f'{tile_id}_gain_year_count_no_change.tif'
        no_change_outfilearg = f'--outfile={no_change_outfilename}'
        cmd = ['gdal_calc.py', '-A', loss, '-B', model_extent, no_change_calc,
               no_change_outfilearg, '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=DEFLATE', '--type', 'Byte',
               '--quiet',
               '--hideNoData']  # Need --hideNoData because the non-gain pixels are NoData, not 0.
        uu.log_subprocess_output_full(cmd)
    elif os.path.exists(gain):
        uu.print_log(f'  Loss tile not found for {tile_id}. Not using it for no-change pixel gain year count.')
        no_change_calc = f'--calc=(A==0)*(B>0)*{cn.loss_years}'
        no_change_outfilename = f'{tile_id}_gain_year_count_no_change.tif'
        no_change_outfilearg = f'--outfile={no_change_outfilename}'
        cmd = ['gdal_calc.py', '-A', gain, '-B', model_extent, no_change_calc,
               no_change_outfilearg, '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=DEFLATE', '--type', 'Byte', '--quiet',
               '--hideNoData'] # Need --hideNoData because the non-gain pixels are NoData, not 0.
        uu.log_subprocess_output_full(cmd)
    else:
        uu.print_log(f'  Loss and gain tiles not found for {tile_id}. Not using them for no-change pixel gain year count.')
        no_change_calc = f'--calc=(A>0)*{cn.loss_years}'
        no_change_outfilename = f'{tile_id}_gain_year_count_no_change.tif'
        no_change_outfilearg = f'--outfile={no_change_outfilename}'
        cmd = ['gdal_calc.py', '-A', model_extent, no_change_calc,
               no_change_outfilearg, '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=DEFLATE', '--type', 'Byte', '--quiet',
               '--hideNoData'] # Need --hideNoData because the non-gain pixels are NoData, not 0.
        uu.log_subprocess_output_full(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, 'gain_year_count_no_change')


def create_gain_year_count_no_change_legal_Amazon_loss(tile_id):
    """
    Creates gain year count tiles for pixels that did not have loss (doesn't matter if they had gain or not)
    For legal_Amazon_loss sensitivity analysis.
    :param tile_id: tile to be processed, identified by its tile id
    :return: tile with number of years of carbon accumulation in pixels that did not have loss
    """

    uu.print_log(f'Gain year count for pixels without loss for legal_Amazon_loss: {tile_id}')

    # Names of the loss, gain and tree cover density tiles
    loss, gain, model_extent = tile_names(tile_id)

    # start time
    start = datetime.datetime.now()

    uu.check_memory()

    # For unclear reasons, gdal_calc doesn't register the 0 (NoData) pixels in the loss tile, so I have to convert it
    # to a vrt so that the 0 pixels are recognized.
    # This was the case with PRODES loss in model v.1.1.2.
    loss_vrt = f'{tile_id}_loss.vrt'
    os.system(f'gdalbuildvrt -vrtnodata None {loss_vrt} {loss}')

    no_change_calc = f'--calc=(A==0)*(B>0)*{cn.loss_years}'
    no_change_outfilename = f'{tile_id}_gain_year_count_no_change.tif'
    no_change_outfilearg = f'--outfile={no_change_outfilename}'
    cmd = ['gdal_calc.py', '-A', loss_vrt, '-B', model_extent, no_change_calc,
           no_change_outfilearg, '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=DEFLATE', '--type', 'Byte', '--quiet']
    uu.log_subprocess_output_full(cmd)

    os.remove(loss_vrt)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, 'gain_year_count_no_change')


def create_gain_year_count_loss_and_gain_standard(tile_id):
    """
    Creates gain year count tiles for pixels that had both loss-and-gain (standard model only)
    :param tile_id: tile to be processed, identified by its tile id
    :return: tile with number of years of carbon accumulation in pixels that had both loss-and-gain
    """

    uu.print_log(f'Loss and gain pixel processing using standard function: {tile_id}')

    # Names of the loss, gain and tree cover density tiles
    loss, gain, model_extent = tile_names(tile_id)

    # start time
    start = datetime.datetime.now()

    uu.check_memory()

    if not os.path.exists(loss) or not os.path.exists(gain):
        uu.print_log(f'  Loss and gain tiles not found for {tile_id}. Skipping loss-and-gain pixel gain year count.')
    else:
        uu.print_log(f'  Loss and gain tiles found for {tile_id}. Using them in loss-and-gain pixel gain year count.')
        loss_and_gain_calc = f'--calc=((A>0)*(B==1)*(C>0)*((A-1)+floor(({cn.loss_years}+1-A)/2)))'
        loss_and_gain_outfilename = f'{tile_id}_gain_year_count_loss_and_gain.tif'
        loss_and_gain_outfilearg = f'--outfile={loss_and_gain_outfilename}'
        cmd = ['gdal_calc.py', '-A', loss, '-B', gain, '-C', model_extent, loss_and_gain_calc,
               loss_and_gain_outfilearg, '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=DEFLATE', '--type', 'Byte', '--quiet']
        uu.log_subprocess_output_full(cmd)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, 'gain_year_count_loss_and_gain')


def create_gain_year_count_loss_and_gain_maxgain(tile_id):
    """
    Creates gain year count tiles for pixels that had both loss-and-gain (maxgain sensitivity model only)
    :param tile_id: tile to be processed, identified by its tile id
    :return: tile with number of years of carbon accumulation in pixels that had both loss-and-gain
    """

    uu.print_log(f'Loss and gain pixel processing using maxgain function: {tile_id}')

    # Names of the loss, gain and tree cover density tiles
    loss, gain, model_extent = tile_names(tile_id)

    # start time
    start = datetime.datetime.now()

    uu.check_memory()

    if os.path.exists(loss):
        uu.print_log(f'  Loss tile found for {tile_id}. Using it in loss-and-gain pixel gain year count')
        loss_and_gain_calc = f'--calc=((A>0)*(B==1)*(C>0)*({cn.loss_years}-1))'
        loss_and_gain_outfilename = f'{tile_id}_gain_year_count_loss_and_gain.tif'
        loss_and_gain_outfilearg = f'--outfile={loss_and_gain_outfilename}'
        cmd = ['gdal_calc.py', '-A', loss, '-B', gain, '-C', model_extent, loss_and_gain_calc,
               loss_and_gain_outfilearg, '--NoDataValue=0', '--overwrite', '--co', 'COMPRESS=DEFLATE', '--type', 'Byte', '--quiet']
        uu.log_subprocess_output_full(cmd)
    else:
        uu.print_log(f'  Loss tile not found for {tile_id}. Skipping loss-and-gain pixel gain year count.')

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, 'gain_year_count_loss_and_gain')


def create_gain_year_count_merge(tile_id, pattern):
    """
    Merges the four gain year count tiles above to create a single gain year count tile
    :param tile_id: tile to be processed, identified by its tile id
    :param pattern: pattern for output tile names
    :return: tile with number of years of carbon accumulation in all pixels
    """

    uu.print_log(f'Merging loss-only, gain-only, no-change, and loss/gain pixels into single gain year count raster for {tile_id}')

    # start time
    start = datetime.datetime.now()

    # The four rasters from above that are to be merged
    no_change_gain_years = f'{tile_id}_gain_year_count_no_change.tif'
    loss_only_gain_years = f'{tile_id}_gain_year_count_loss_only.tif'
    gain_only_gain_years = f'{tile_id}_gain_year_count_gain_only.tif'
    loss_and_gain_gain_years = f'{tile_id}_gain_year_count_loss_and_gain.tif'

    # Names of the output tiles
    gain_year_count_merged = uu.make_tile_name(tile_id, pattern)

    # Opens no-change gain year count tile. This should exist for all tiles.
    with rasterio.open(no_change_gain_years) as no_change_gain_years_src:

        # Grabs metadata about the tif, like its location/projection/cellsize
        kwargs = no_change_gain_years_src.meta

        # Grabs the windows of the tile (stripes) so we can iterate over the entire tif without running out of memory
        windows = no_change_gain_years_src.block_windows(1)

        # Updates kwargs for the output dataset
        kwargs.update(
            driver='GTiff',
            count=1,
            compress='DEFLATE',
            nodata=0
        )

        uu.print_log(f'  No-change tile exists for {tile_id} by default')

        # Opens the other gain year count tiles. They may not exist for all other tiles.
        try:
            loss_only_gain_years_src = rasterio.open(loss_only_gain_years)
            uu.print_log(f'  Loss-only tile found for {tile_id}')
        except rasterio.errors.RasterioIOError:
            uu.print_log(f'  Loss-only tile not found for {tile_id}')

        try:
            gain_only_gain_years_src = rasterio.open(gain_only_gain_years)
            uu.print_log(f'  Gain-only tile found for {tile_id}')
        except rasterio.errors.RasterioIOError:
            uu.print_log(f'  Gain-only tile not found for {tile_id}')

        try:
            loss_and_gain_gain_years_src = rasterio.open(loss_and_gain_gain_years)
            uu.print_log(f'  Loss-and-gain tile found for {tile_id}')
        except rasterio.errors.RasterioIOError:
            uu.print_log(f'  Loss-and-gain tile not found for {tile_id}')

        # Opens the output tile, giving it the arguments of the input tiles
        gain_year_count_merged_dst = rasterio.open(gain_year_count_merged, 'w', **kwargs)

        # Adds metadata tags to the output raster
        uu.add_universal_metadata_rasterio(gain_year_count_merged_dst)
        gain_year_count_merged_dst.update_tags(
            units='years')
        gain_year_count_merged_dst.update_tags(
            min_possible_value='0')
        gain_year_count_merged_dst.update_tags(
            max_possible_value=cn.loss_years)
        gain_year_count_merged_dst.update_tags(
            source='Gain years are assigned based on the combination of Hansen loss-and-gain in each pixel. There are four combinations: neither loss nor gain, loss-only, gain-only, loss-and-gain.')
        gain_year_count_merged_dst.update_tags(
            extent='Full model extent')

        uu.check_memory()

        # Iterates across the windows (1 pixel strips) of the input tile
        for idx, window in windows:

            no_change_gain_years_window = no_change_gain_years_src.read(1, window=window)

            try:
                loss_only_gain_years_window = loss_only_gain_years_src.read(1, window=window)
            except UnboundLocalError:
                loss_only_gain_years_window = np.zeros((window.height, window.width), dtype='uint8')

            try:
                gain_only_gain_years_window = gain_only_gain_years_src.read(1, window=window)
            except UnboundLocalError:
                gain_only_gain_years_window = np.zeros((window.height, window.width), dtype='uint8')

            try:
                loss_and_gain_gain_years_window = loss_and_gain_gain_years_src.read(1, window=window)
            except UnboundLocalError:
                loss_and_gain_gain_years_window = np.zeros((window.height, window.width), dtype='uint8')


            gain_year_count_merged_window = loss_only_gain_years_window + gain_only_gain_years_window + \
                                            no_change_gain_years_window + loss_and_gain_gain_years_window

            gain_year_count_merged_dst.write_band(1, gain_year_count_merged_window, window=window)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, pattern)
