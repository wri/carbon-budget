"""
Function to create removal factor tiles with all removal factor sources combined
"""

import datetime
import numpy as np
import rasterio

import constants_and_names as cn
import universal_util as uu

def annual_gain_rate_AGC_BGC_all_forest_types(tile_id, output_pattern_list):
    """
    :param tile_id: tile to be processed, identified by its tile id
    :param output_pattern_list: patterns for output tile names
    :return: 5 tiles: removal factor source, aboveground rate, belowground rate, aboveground+belowground rate,
        standard deviation for aboveground rate (all removal factor sources combined)
        Units: Mg carbon/ha/yr (including for standard deviation tiles)
    """

    uu.print_log(f'Mapping removal rate source and AGB and BGB removal rates: {tile_id}')

    # Start time
    start = datetime.datetime.now()

    # Names of the input tiles
    # Removal factors
    model_extent = uu.sensit_tile_rename(cn.SENSIT_TYPE, tile_id, cn.pattern_model_extent)
    mangrove_AGB = uu.sensit_tile_rename(cn.SENSIT_TYPE, tile_id, cn.pattern_annual_gain_AGB_mangrove)
    mangrove_BGB = uu.sensit_tile_rename(cn.SENSIT_TYPE, tile_id, cn.pattern_annual_gain_BGB_mangrove)
    europe_AGC_BGC = uu.sensit_tile_rename(cn.SENSIT_TYPE, tile_id, cn.pattern_annual_gain_AGC_BGC_natrl_forest_Europe)
    plantations_AGC_BGC = uu.sensit_tile_rename(cn.SENSIT_TYPE, tile_id, cn.pattern_annual_gain_AGC_BGC_planted_forest)
    us_AGC_BGC = uu.sensit_tile_rename(cn.SENSIT_TYPE, tile_id, cn.pattern_annual_gain_AGC_BGC_natrl_forest_US)
    young_AGC = uu.sensit_tile_rename(cn.SENSIT_TYPE, tile_id, cn.pattern_annual_gain_AGC_natrl_forest_young)
    age_category = uu.sensit_tile_rename(cn.SENSIT_TYPE, tile_id, cn.pattern_age_cat_IPCC)
    ipcc_AGB_default = uu.sensit_tile_rename(cn.SENSIT_TYPE, tile_id, cn.pattern_annual_gain_AGB_IPCC_defaults)
    BGB_AGB_ratio = uu.sensit_tile_rename(cn.SENSIT_TYPE, tile_id, cn.pattern_BGB_AGB_ratio)

    # Removal factor standard deviations
    mangrove_AGB_stdev = uu.sensit_tile_rename(cn.SENSIT_TYPE, tile_id, cn.pattern_stdev_annual_gain_AGB_mangrove)
    europe_AGC_BGC_stdev = uu.sensit_tile_rename(cn.SENSIT_TYPE, tile_id, cn.pattern_stdev_annual_gain_AGC_BGC_natrl_forest_Europe)
    plantations_AGC_BGC_stdev = uu.sensit_tile_rename(cn.SENSIT_TYPE, tile_id, cn.pattern_stdev_annual_gain_AGC_BGC_planted_forest)
    us_AGC_BGC_stdev = uu.sensit_tile_rename(cn.SENSIT_TYPE, tile_id, cn.pattern_stdev_annual_gain_AGC_BGC_natrl_forest_US)
    young_AGC_stdev = uu.sensit_tile_rename(cn.SENSIT_TYPE, tile_id, cn.pattern_stdev_annual_gain_AGC_natrl_forest_young)
    ipcc_AGB_default_stdev = uu.sensit_tile_rename(cn.SENSIT_TYPE, tile_id, cn.pattern_stdev_annual_gain_AGB_IPCC_defaults)

    # Names of the output tiles
    removal_forest_type = uu.make_tile_name(tile_id, output_pattern_list[0])
    annual_gain_AGC_all_forest_types = uu.make_tile_name(tile_id, output_pattern_list[1])
    annual_gain_BGC_all_forest_types = uu.make_tile_name(tile_id, output_pattern_list[2])
    annual_gain_AGC_BGC_all_forest_types = uu.make_tile_name(tile_id, output_pattern_list[3]) # Not used further in the model. Created just for reference.
    stdev_annual_gain_AGC_all_forest_types = uu.make_tile_name(tile_id, output_pattern_list[4])

    # Opens biomass tile
    with rasterio.open(model_extent) as model_extent_src:

        # Grabs metadata about the tif, like its location/projection/cellsize
        kwargs = model_extent_src.meta

        # Grabs the windows of the tile (stripes) so we can iterate over the entire tif without running out of memory
        windows = model_extent_src.block_windows(1)

        # Updates kwargs for the output dataset
        kwargs.update(
            driver='GTiff',
            count=1,
            compress='DEFLATE',
            nodata=0
        )

        # Checks whether there are mangrove or planted forest tiles. If so, they are opened.
        try:
            mangrove_AGB_src = rasterio.open(mangrove_AGB)
            mangrove_BGB_src = rasterio.open(mangrove_BGB)
            mangrove_AGB_stdev_src = rasterio.open(mangrove_AGB_stdev)
            uu.print_log(f'    Mangrove tiles (AGB and BGB) found for {tile_id}')
        except rasterio.errors.RasterioIOError:
            uu.print_log(f'    Mangrove tiles (AGB and BGB) not found for {tile_id}')

        try:
            europe_AGC_BGC_src = rasterio.open(europe_AGC_BGC)
            europe_AGC_BGC_stdev_src = rasterio.open(europe_AGC_BGC_stdev)
            uu.print_log(f'    Europe removal factor tile found for {tile_id}')
        except rasterio.errors.RasterioIOError:
            uu.print_log(f'    Europe removal factor tile not found for {tile_id}')

        try:
            plantations_AGC_BGC_src = rasterio.open(plantations_AGC_BGC)
            plantations_AGC_BGC_stdev_src = rasterio.open(plantations_AGC_BGC_stdev)
            uu.print_log(f'    Planted forest tile found for {tile_id}')
        except rasterio.errors.RasterioIOError:
            uu.print_log(f'    Planted forest tile not found for {tile_id}')

        try:
            us_AGC_BGC_src = rasterio.open(us_AGC_BGC)
            us_AGC_BGC_stdev_src = rasterio.open(us_AGC_BGC_stdev)
            uu.print_log(f'    US removal factor tile found for {tile_id}')
        except rasterio.errors.RasterioIOError:
            uu.print_log(f'    US removal factor tile not found for {tile_id}')

        try:
            young_AGC_src = rasterio.open(young_AGC)
            young_AGC_stdev_src = rasterio.open(young_AGC_stdev)
            uu.print_log(f'    Young forest removal factor tile found for {tile_id}')
        except rasterio.errors.RasterioIOError:
            uu.print_log(f'    Young forest removal factor tile not found for {tile_id}')

        try:
            age_category_src = rasterio.open(age_category)
            uu.print_log(f'    Age category tile found for {tile_id}')
        except rasterio.errors.RasterioIOError:
            uu.print_log(f'    Age category tile not found for {tile_id}')

        try:
            ipcc_AGB_default_src = rasterio.open(ipcc_AGB_default)
            ipcc_AGB_default_stdev_src = rasterio.open(ipcc_AGB_default_stdev)
            uu.print_log(f'    IPCC default removal rate tile found for {tile_id}')
        except rasterio.errors.RasterioIOError:
            uu.print_log(f'    IPCC default removal rate tile not found for {tile_id}')

        try:
            BGB_AGB_ratio_src = rasterio.open(BGB_AGB_ratio)
            uu.print_log(f'    BGB:AGB tile found for {tile_id}')
        except rasterio.errors.RasterioIOError:
            uu.print_log(f'    BGB:AGB tile not found for {tile_id}. Using default BGB:AGB from Mokany instead.')

        # Opens the output tile, giving it the arguments of the input tiles
        removal_forest_type_dst = rasterio.open(removal_forest_type, 'w', **kwargs)

        # Adds metadata tags to the output raster
        uu.add_universal_metadata_rasterio(removal_forest_type_dst)
        removal_forest_type_dst.update_tags(
            key='6: mangroves. 5: European-specific rates. 4: planted forests. 3: US-specific rates. 2: young (<20 year) secondary forests. 1: old (>20 year) secondary forests and primary forests. Priority goes to the highest number.')
        removal_forest_type_dst.update_tags(
            source='Mangroves: IPCC wetlands supplement. Europe: Liz Goldman. Planted forests: Spatial Database of Planted Forests. USA: US FIA, via Rich Birdsey. Young natural forests: Cook-Patton et al. 2020. Old natural forests: IPCC Forests table 4.9')
        removal_forest_type_dst.update_tags(
            extent='Full model extent')

        # Updates kwargs for the removal rate outputs-- just need to change datatype
        kwargs.update(dtype='float32')

        annual_gain_AGC_all_forest_types_dst = rasterio.open(annual_gain_AGC_all_forest_types, 'w', **kwargs)
        annual_gain_BGC_all_forest_types_dst = rasterio.open(annual_gain_BGC_all_forest_types, 'w', **kwargs)
        annual_gain_AGC_BGC_all_forest_types_dst = rasterio.open(annual_gain_AGC_BGC_all_forest_types, 'w', **kwargs)
        stdev_annual_gain_AGC_all_forest_types_dst = rasterio.open(stdev_annual_gain_AGC_all_forest_types, 'w', **kwargs)

        # Adds metadata tags to the output raster
        uu.add_universal_metadata_rasterio(annual_gain_AGC_all_forest_types_dst)
        annual_gain_AGC_all_forest_types_dst.update_tags(
            units='megagrams aboveground carbon/ha/yr')
        annual_gain_AGC_all_forest_types_dst.update_tags(
            source='Mangroves: IPCC wetlands supplement Table 4.4. Europe: Liz Goldman. Planted forests: Spatial Database of Planted Forests. USA: US FIA, via Rich Birdsey. Young natural forests: Cook-Patton et al. 2020. Old natural forests: IPCC Forests table 4.9')
        annual_gain_AGC_all_forest_types_dst.update_tags(
            extent='Full model extent')

        # Adds metadata tags to the output raster
        uu.add_universal_metadata_rasterio(annual_gain_BGC_all_forest_types_dst)
        annual_gain_BGC_all_forest_types_dst.update_tags(
            units='megagrams belowground carbon/ha/yr')
        annual_gain_BGC_all_forest_types_dst.update_tags(
            source='Mangroves: IPCC wetlands supplement Table 4.4. Europe: Liz Goldman. Planted forests: Spatial Database of Planted Forests. USA: US FIA, via Rich Birdsey. Young natural forests: Cook-Patton et al. 2020. Old natural forests: IPCC Forests table 4.9')
        annual_gain_BGC_all_forest_types_dst.update_tags(
            extent='Full model extent')

        # Adds metadata tags to the output raster
        uu.add_universal_metadata_rasterio(annual_gain_AGC_BGC_all_forest_types_dst)
        annual_gain_AGC_BGC_all_forest_types_dst.update_tags(
            units='megagrams aboveground + belowground carbon/ha/yr')
        annual_gain_AGC_BGC_all_forest_types_dst.update_tags(
            source='Mangroves: IPCC wetlands supplement Table 4.4. Europe: Liz Goldman. Planted forests: Spatial Database of Planted Forests. USA: US FIA, via Rich Birdsey. Young natural forests: Cook-Patton et al. 2020. Old natural forests: IPCC Forests table 4.9')
        annual_gain_AGC_BGC_all_forest_types_dst.update_tags(
            extent='Full model extent')

        # Adds metadata tags to the output raster
        uu.add_universal_metadata_rasterio(stdev_annual_gain_AGC_all_forest_types_dst)
        stdev_annual_gain_AGC_all_forest_types_dst.update_tags(
            units='standard deviation for removal factor, in terms of megagrams aboveground carbon/ha/yr')
        stdev_annual_gain_AGC_all_forest_types_dst.update_tags(
            source='Mangroves: IPCC wetlands supplement Table 4.4. Europe: Liz Goldman. Planted forests: Spatial Database of Planted Forests. USA: US FIA, via Rich Birdsey. Young natural forests: Cook-Patton et al. 2020. Old natural forests: IPCC Forests table 4.9')
        stdev_annual_gain_AGC_all_forest_types_dst.update_tags(
            extent='Full model extent')

        uu.print_log(f'  Creating removal model forest type tile, AGC removal factor tile, BGC removal factor tile, and AGC removal factor standard deviation tile for {tile_id}')

        uu.check_memory()

        # Iterates across the windows (1 pixel strips) of the input tile
        for idx, window in windows:

            model_extent_window = model_extent_src.read(1, window=window)

            # Output rasters' windows
            removal_forest_type_window = np.zeros((window.height, window.width), dtype='uint8')
            annual_gain_AGC_all_forest_types_window = np.zeros((window.height, window.width), dtype='float32')
            annual_gain_BGC_all_forest_types_window = np.zeros((window.height, window.width), dtype='float32')
            stdev_annual_gain_AGC_all_forest_types_window = np.zeros((window.height, window.width), dtype='float32')

            try:
                age_category_window = age_category_src.read(1, window=window)
            except UnboundLocalError:
                age_category_window = np.zeros((window.height, window.width), dtype='uint8')

            try:
                BGB_AGB_ratio_window = BGB_AGB_ratio_src.read(1, window=window)
            except UnboundLocalError:
                BGB_AGB_ratio_window = np.empty((window.height, window.width), dtype='float32')
                BGB_AGB_ratio_window[:] = cn.below_to_above_non_mang

            # Lowest priority
            try:
                ipcc_AGB_default_rate_window = ipcc_AGB_default_src.read(1, window=window)
                ipcc_AGB_default_stdev_window = ipcc_AGB_default_stdev_src.read(1, window=window)
                # In no_primary_gain, the AGB_default_rate_window = 0, so primary forest pixels would not be
                # assigned a removal forest type and therefore get exclude from the model later.
                # That is incorrect, so using model_extent as the criterion instead allows the primary forest pixels
                # that don't have rates under this sensitivity analysis to still be included in the model.
                # Unfortunately, model_extent is slightly different from the IPCC rate extent (no IPCC rates where
                # there is no ecozone information), but this is a very small difference and not worth worrying about.
                if cn.SENSIT_TYPE == 'no_primary_gain':
                    removal_forest_type_window = np.where(model_extent_window != 0,
                                                          cn.old_natural_rank,
                                                          removal_forest_type_window).astype('uint8')
                else:
                    removal_forest_type_window = np.where(ipcc_AGB_default_rate_window != 0,
                                                          cn.old_natural_rank,
                                                          removal_forest_type_window).astype('uint8')
                annual_gain_AGC_all_forest_types_window = np.where(ipcc_AGB_default_rate_window != 0,
                                                                   ipcc_AGB_default_rate_window * cn.biomass_to_c_non_mangrove,
                                                                   annual_gain_AGC_all_forest_types_window).astype('float32')
                annual_gain_BGC_all_forest_types_window = np.where(ipcc_AGB_default_rate_window != 0,
                                                                   ipcc_AGB_default_rate_window * cn.biomass_to_c_non_mangrove * BGB_AGB_ratio_window,
                                                                   annual_gain_BGC_all_forest_types_window).astype('float32')
                stdev_annual_gain_AGC_all_forest_types_window = np.where(ipcc_AGB_default_stdev_window != 0,
                                                                   ipcc_AGB_default_stdev_window * cn.biomass_to_c_non_mangrove,
                                                                   stdev_annual_gain_AGC_all_forest_types_window).astype('float32')
            except UnboundLocalError:
                pass

            try: # young_AGC_rate_window uses > because of the weird NaN in the tiles. If != is used, the young rate NaN overwrites the IPCC arrays
                young_AGC_rate_window = young_AGC_src.read(1, window=window)
                young_AGC_stdev_window = young_AGC_stdev_src.read(1, window=window)
                # Using the > with the NaN results in non-fatal "RuntimeWarning: invalid value encountered in greater".
                # This isn't actually a problem, so the "with" statement suppresses it, per https://stackoverflow.com/a/58026329/10839927
                with np.errstate(invalid='ignore'):
                    removal_forest_type_window = np.where((young_AGC_rate_window > 0) & (age_category_window == 1),
                                                          cn.young_natural_rank,
                                                          removal_forest_type_window).astype('uint8')
                    annual_gain_AGC_all_forest_types_window = np.where((young_AGC_rate_window > 0) & (age_category_window == 1),
                                                                       young_AGC_rate_window,
                                                                       annual_gain_AGC_all_forest_types_window).astype('float32')
                    annual_gain_BGC_all_forest_types_window = np.where((young_AGC_rate_window > 0) & (age_category_window == 1),
                                                                       young_AGC_rate_window * BGB_AGB_ratio_window,
                                                                       annual_gain_BGC_all_forest_types_window).astype('float32')
                    stdev_annual_gain_AGC_all_forest_types_window = np.where((young_AGC_stdev_window > 0) & (age_category_window == 1),
                                                                       young_AGC_stdev_window,
                                                                       stdev_annual_gain_AGC_all_forest_types_window).astype('float32')

            except UnboundLocalError:
                pass

            if cn.SENSIT_TYPE != 'US_removals':
                try:
                    us_AGC_BGC_rate_window = us_AGC_BGC_src.read(1, window=window)
                    us_AGC_BGC_stdev_window = us_AGC_BGC_stdev_src.read(1, window=window)
                    removal_forest_type_window = np.where(us_AGC_BGC_rate_window != 0, cn.US_rank, removal_forest_type_window).astype('uint8')
                    annual_gain_AGC_all_forest_types_window = np.where(us_AGC_BGC_rate_window != 0,
                                                                       us_AGC_BGC_rate_window / (1 + BGB_AGB_ratio_window),
                                                                       annual_gain_AGC_all_forest_types_window).astype('float32')
                    annual_gain_BGC_all_forest_types_window = np.where(us_AGC_BGC_rate_window != 0,
                                                                       (us_AGC_BGC_rate_window) -
                                                                       (us_AGC_BGC_rate_window / (1 + BGB_AGB_ratio_window)),
                                                                       annual_gain_BGC_all_forest_types_window).astype('float32')
                    stdev_annual_gain_AGC_all_forest_types_window = np.where(us_AGC_BGC_stdev_window != 0,
                                                                       us_AGC_BGC_stdev_window / (1 + BGB_AGB_ratio_window),
                                                                       stdev_annual_gain_AGC_all_forest_types_window).astype('float32')
                except UnboundLocalError:
                    pass

            try:
                plantations_AGC_BGC_rate_window = plantations_AGC_BGC_src.read(1, window=window)
                plantations_AGC_BGC_stdev_window = plantations_AGC_BGC_stdev_src.read(1, window=window)
                removal_forest_type_window = np.where(plantations_AGC_BGC_rate_window != 0, cn.planted_forest_rank, removal_forest_type_window).astype('uint8')
                annual_gain_AGC_all_forest_types_window = np.where(plantations_AGC_BGC_rate_window != 0,
                                                                   plantations_AGC_BGC_rate_window / (1 + BGB_AGB_ratio_window),
                                                                   annual_gain_AGC_all_forest_types_window).astype('float32')
                annual_gain_BGC_all_forest_types_window = np.where(plantations_AGC_BGC_rate_window != 0,
                                                                   (plantations_AGC_BGC_rate_window ) -
                                                                   (plantations_AGC_BGC_rate_window / (1 + BGB_AGB_ratio_window)),
                                                                   annual_gain_BGC_all_forest_types_window).astype('float32')
                stdev_annual_gain_AGC_all_forest_types_window = np.where(plantations_AGC_BGC_stdev_window != 0,
                                                                   plantations_AGC_BGC_stdev_window / (1 + BGB_AGB_ratio_window),
                                                                   stdev_annual_gain_AGC_all_forest_types_window).astype('float32')
            except UnboundLocalError:
                pass

            try:
                europe_AGC_BGC_rate_window = europe_AGC_BGC_src.read(1, window=window)
                europe_AGC_BGC_stdev_window = europe_AGC_BGC_stdev_src.read(1, window=window)
                removal_forest_type_window = np.where(europe_AGC_BGC_rate_window != 0, cn.europe_rank, removal_forest_type_window).astype('uint8')
                annual_gain_AGC_all_forest_types_window = np.where(europe_AGC_BGC_rate_window != 0,
                                                                   europe_AGC_BGC_rate_window / (1 + BGB_AGB_ratio_window),
                                                                   annual_gain_AGC_all_forest_types_window).astype('float32')
                annual_gain_BGC_all_forest_types_window = np.where(europe_AGC_BGC_rate_window != 0,
                                                                   (europe_AGC_BGC_rate_window) -
                                                                   (europe_AGC_BGC_rate_window / (1 + BGB_AGB_ratio_window)),
                                                                   annual_gain_BGC_all_forest_types_window).astype('float32')
                # NOTE: Nancy Harris thought that the European removal standard deviations were 2x too large,
                # per email on 8/30/2020. Thus, simplest fix is to leave original tiles 2x too large and
                # correct them only where composited with other stdev sources.
                stdev_annual_gain_AGC_all_forest_types_window = np.where(europe_AGC_BGC_stdev_window != 0,
                                                                   (europe_AGC_BGC_stdev_window/2) / (1 + BGB_AGB_ratio_window),
                                                                   stdev_annual_gain_AGC_all_forest_types_window).astype('float32')
            except UnboundLocalError:
                pass

            # Highest priority
            try:
                mangroves_AGB_rate_window = mangrove_AGB_src.read(1, window=window)
                mangroves_BGB_rate_window = mangrove_BGB_src.read(1, window=window)
                mangroves_AGB_stdev_window = mangrove_AGB_stdev_src.read(1, window=window)
                removal_forest_type_window = np.where(mangroves_AGB_rate_window != 0, cn.mangrove_rank, removal_forest_type_window).astype('uint8')
                annual_gain_AGC_all_forest_types_window = np.where(mangroves_AGB_rate_window != 0,
                                                                   mangroves_AGB_rate_window * cn.biomass_to_c_mangrove,
                                                                   annual_gain_AGC_all_forest_types_window).astype('float32')
                annual_gain_BGC_all_forest_types_window = np.where(mangroves_BGB_rate_window != 0,
                                                                   mangroves_BGB_rate_window * cn.biomass_to_c_mangrove,
                                                                   annual_gain_BGC_all_forest_types_window).astype('float32')
                stdev_annual_gain_AGC_all_forest_types_window = np.where(mangroves_AGB_stdev_window != 0,
                                                                   mangroves_AGB_stdev_window * cn.biomass_to_c_mangrove,
                                                                   stdev_annual_gain_AGC_all_forest_types_window).astype('float32')
            except UnboundLocalError:
                pass

            # Masks outputs to model output extent
            removal_forest_type_window = np.where(model_extent_window == 1, removal_forest_type_window, 0)
            annual_gain_AGC_all_forest_types_window = np.where(model_extent_window == 1, annual_gain_AGC_all_forest_types_window, 0)
            annual_gain_BGC_all_forest_types_window = np.where(model_extent_window == 1, annual_gain_BGC_all_forest_types_window, 0)
            annual_gain_AGC_BGC_all_forest_types_window = annual_gain_AGC_all_forest_types_window + annual_gain_BGC_all_forest_types_window
            stdev_annual_gain_AGC_all_forest_types_window = np.where(model_extent_window == 1, stdev_annual_gain_AGC_all_forest_types_window, 0)

            # Writes the outputs window to the output files
            removal_forest_type_dst.write_band(1, removal_forest_type_window, window=window)
            annual_gain_AGC_all_forest_types_dst.write_band(1, annual_gain_AGC_all_forest_types_window, window=window)
            annual_gain_BGC_all_forest_types_dst.write_band(1, annual_gain_BGC_all_forest_types_window, window=window)
            annual_gain_AGC_BGC_all_forest_types_dst.write_band(1, annual_gain_AGC_BGC_all_forest_types_window, window=window)
            stdev_annual_gain_AGC_all_forest_types_dst.write_band(1, stdev_annual_gain_AGC_all_forest_types_window, window=window)

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, cn.pattern_removal_forest_type)
