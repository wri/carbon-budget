"""
Function to create forest age category tiles
"""

import datetime
import numpy as np
import rasterio

import constants_and_names as cn
import universal_util as uu

def forest_age_category(tile_id, gain_table_dict, pattern):
    """
    :param tile_id: tile to be processed, identified by its tile id
    :param gain_table_dict: dictionary of removal factors by continent, ecozone, and forest age category
    :param pattern: pattern for output tile names
    :return: tile denoting three broad forest age categories: 1- young (<20), 2- middle, 3- old/primary
    """

    uu.print_log("Assigning forest age categories:", tile_id)

    # Start time
    start = datetime.datetime.now()

    # Gets the bounding coordinates of each tile. Needed to determine if the tile is in the tropics (within 30 deg of the equator)
    xmin, ymin, xmax, ymax = uu.coords(tile_id)

    # Default value is that the tile is not in the tropics
    tropics = 0

    # Criteria for assigning a tile to the tropics
    if (ymax > -30) & (ymax <= 30):

        tropics = 1

    uu.print_log(f'  Tile {tile_id} in tropics: {tropics}')

    # Names of the input tiles
    gain = f'{tile_id}_{cn.pattern_gain_ec2}.tif'
    model_extent = uu.sensit_tile_rename(cn.SENSIT_TYPE, tile_id, cn.pattern_model_extent)
    ifl_primary = uu.sensit_tile_rename(cn.SENSIT_TYPE, tile_id, cn.pattern_ifl_primary)
    cont_eco = uu.sensit_tile_rename(cn.SENSIT_TYPE, tile_id, cn.pattern_cont_eco_processed)
    biomass = uu.sensit_tile_rename_biomass(cn.SENSIT_TYPE, tile_id)  # Biomass tile name depends on the sensitivity analysis

    if cn.SENSIT_TYPE == 'legal_Amazon_loss':
        loss = f'{tile_id}_{cn.pattern_Brazil_annual_loss_processed}.tif'
        uu.print_log(f'Using PRODES loss tile {tile_id} for {cn.SENSIT_TYPE} sensitivity analysis')
    elif cn.SENSIT_TYPE == 'Mekong_loss':
        loss = f'{tile_id}_{cn.pattern_Mekong_loss_processed}.tif'
    else:
        loss = f'{cn.pattern_loss}_{tile_id}.tif'
        uu.print_log(f'Using Hansen loss tile {tile_id} for {cn.SENSIT_TYPE} model run')

    # Opens biomass tile
    with rasterio.open(model_extent) as model_extent_src:

        # Grabs metadata about the tif, like its location/projection/cellsize
        kwargs = model_extent_src.meta

        # Grabs the windows of the tile (stripes) so we can iterate over the entire tif without running out of memory
        windows = model_extent_src.block_windows(1)

        # Opens the input tiles if they exist
        try:
            cont_eco_src = rasterio.open(cont_eco)
            uu.print_log(f'   Continent-ecozone tile found for {tile_id}')
        except rasterio.errors.RasterioIOError:
            uu.print_log(f'   No continent-ecozone tile found for {tile_id}')

        try:
            gain_src = rasterio.open(gain)
            uu.print_log(f'   Gain tile found for {tile_id}')
        except rasterio.errors.RasterioIOError:
            uu.print_log(f'   No gain tile found for {tile_id}')

        try:
            biomass_src = rasterio.open(biomass)
            uu.print_log(f'   Biomass tile found for {tile_id}')
        except rasterio.errors.RasterioIOError:
            uu.print_log(f'   No biomass tile found for {tile_id}')

        try:
            loss_src = rasterio.open(loss)
            uu.print_log(f'   Loss tile found for {tile_id}')
        except rasterio.errors.RasterioIOError:
            uu.print_log(f'   No loss tile found for {tile_id}')

        try:
            ifl_primary_src = rasterio.open(ifl_primary)
            uu.print_log(f'   IFL-primary forest tile found for {tile_id}')
        except rasterio.errors.RasterioIOError:
            uu.print_log(f'   No IFL-primary forest tile found for {tile_id}')

        # Updates kwargs for the output dataset
        kwargs.update(
            driver='GTiff',
            count=1,
            compress='DEFLATE',
            nodata=0
        )

        # Opens the output tile, giving it the arguments of the input tiles
        dst = rasterio.open(f'{tile_id}_{pattern}.tif', 'w', **kwargs)

        # Adds metadata tags to the output raster
        uu.add_universal_metadata_rasterio(dst)
        dst.update_tags(
            key='1: young (<20 year) secondary forest; 2: old (>20 year) secondary forest; 3: primary forest or IFL')
        dst.update_tags(
            source='Decision tree that uses Hansen gain and loss, IFL/primary forest extent, and aboveground biomass to assign an age category')
        dst.update_tags(
            extent='Full model extent, even though these age categories will not be used over the full model extent. They apply to just the rates from IPCC defaults.')

        uu.print_log(f'    Assigning IPCC age categories for {tile_id}')

        uu.check_memory()

        # Iterates across the windows (1 pixel strips) of the input tile
        for idx, window in windows:

            # Creates windows for each input raster. Only model_extent_src is guaranteed to exist
            model_extent_window = model_extent_src.read(1, window=window)

            try:
                loss_window = loss_src.read(1, window=window)
            except UnboundLocalError:
                loss_window = np.zeros((window.height, window.width), dtype='uint8')

            try:
                gain_window = gain_src.read(1, window=window)
            except UnboundLocalError:
                gain_window = np.zeros((window.height, window.width), dtype='uint8')

            try:
                cont_eco_window = cont_eco_src.read(1, window=window)
            except UnboundLocalError:
                cont_eco_window = np.zeros((window.height, window.width), dtype='uint8')

            try:
                biomass_window = biomass_src.read(1, window=window)
            except UnboundLocalError:
                biomass_window = np.zeros((window.height, window.width), dtype='float32')

            try:
                ifl_primary_window = ifl_primary_src.read(1, window=window)
            except UnboundLocalError:
                ifl_primary_window = np.zeros((window.height, window.width), dtype='uint8')

            # Creates a numpy array that has the <=20 year secondary forest growth rate x 20
            # based on the continent-ecozone code of each pixel (the dictionary).
            # This is used to assign pixels to the correct age category.
            gain_20_years = np.vectorize(gain_table_dict.get)(cont_eco_window)*20

            # Create a 0s array for the output
            dst_data = np.zeros((window.height, window.width), dtype='uint8')

            # Logic tree for assigning age categories begins here
            # Code 1 = young (<20 years) secondary forest, code 2 = old (>20 year) secondary forest, code 3 = primary forest
            # model_extent_window ensures that there is both biomass and tree cover in 2000 OR mangroves OR tree cover gain

            # For every model version except legal_Amazon_loss sensitivity analysis, which has its own rules about age assignment

            #### Try using this in the future: https://gis.stackexchange.com/questions/419445/comparing-two-rasters-based-on-a-complex-set-of-rules

            if cn.SENSIT_TYPE != 'legal_Amazon_loss':
                # No change pixels- no loss or gain
                if tropics == 0:

                    dst_data[np.where((model_extent_window > 0) & (gain_window == 0) & (loss_window == 0))] = 2

                if tropics == 1:

                    dst_data[np.where((model_extent_window > 0) & (gain_window == 0) & (loss_window == 0) & (ifl_primary_window != 1))] = 2
                    dst_data[np.where((model_extent_window > 0) & (gain_window == 0) & (loss_window == 0) & (ifl_primary_window == 1))] = 3

                # Loss-only pixels
                dst_data[np.where((model_extent_window > 0) & (gain_window == 0) & (loss_window > 0) & (ifl_primary_window != 1) & (biomass_window <= gain_20_years))] = 1
                dst_data[np.where((model_extent_window > 0) & (gain_window == 0) & (loss_window > 0) & (ifl_primary_window != 1) & (biomass_window > gain_20_years))] = 2
                dst_data[np.where((model_extent_window > 0) & (gain_window == 0) & (loss_window > 0) & (ifl_primary_window ==1))] = 3

                # Gain-only pixels
                # If there is gain, the pixel doesn't need biomass or canopy cover.
                dst_data[np.where((model_extent_window > 0) & (gain_window == 1) & (loss_window == 0))] = 1

                # Pixels with loss and gain
                # If there is gain with loss, the pixel doesn't need biomass or canopy cover.
                dst_data[np.where((model_extent_window > 0) & (gain_window == 1) & (loss_window > 0))] = 1

            # For legal_Amazon_loss sensitivity analysis
            else:

                # Non-loss pixels (could have gain or not. Assuming that if within PRODES extent in 2000, there can't be
                # gain, so it's a faulty detection. Thus, gain-only pixels are ignored and become part of no change.)
                dst_data[np.where((model_extent_window == 1) & (loss_window == 0))] = 3  # primary forest

                # Loss-only pixels
                dst_data[np.where((model_extent_window == 1) & (loss_window > 0) & (gain_window == 0))] = 3  # primary forest

                # Loss-and-gain pixels
                dst_data[np.where((model_extent_window == 1) & (loss_window > 0) & (gain_window == 1))] = 2  # young secondary forest


            # Writes the output window to the output
            dst.write_band(1, dst_data, window=window)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, pattern)
