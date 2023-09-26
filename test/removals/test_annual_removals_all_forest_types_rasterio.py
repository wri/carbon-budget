import glob
import numpy as np
import os
import pytest
import rasterio
import sys
import universal_util as uu
import constants_and_names as cn
from unittest.mock import patch
from removals.annual_gain_rate_AGC_BGC_all_forest_types import annual_gain_rate_AGC_BGC_all_forest_types

import test.test_utilities as tu


# run from /usr/local/app
# pytest -m all_removals -s

# @pytest.mark.xfail
@patch("universal_util.sensit_tile_rename")
@patch("universal_util.make_tile_name")
@patch("universal_util.upload_log")
@pytest.mark.rasterio
@pytest.mark.all_removals
@pytest.mark.parametrize("comparison_dict", [
                                             {cn.removal_forest_type_dir: cn.pattern_removal_forest_type},
                                             {cn.annual_gain_AGC_all_types_dir: cn.pattern_annual_gain_AGC_all_types},
                                             {cn.annual_gain_BGC_all_types_dir: cn.pattern_annual_gain_BGC_all_types},
                                             {cn.annual_gain_AGC_BGC_all_types_dir: cn.pattern_annual_gain_AGC_BGC_all_types},
                                             {cn.stdev_annual_gain_AGC_all_types_dir: cn.pattern_stdev_annual_gain_AGC_all_types}
                                             ])

def test_rasterio_runs(upload_log_dummy, make_tile_name_fake, sensit_tile_rename_fake,
                       delete_old_outputs, comparison_dict):

    ### arrange
    # # tile_id for testing and the extent that should be tested within it

    # # For 40N_020E, AGC changes with using BGB:AGB map because European removal factor tiles are AGC+BGC,
    # # so making the composite AGC tiles from that depends on the BGC ratio. 40N_020E seems to work fine.
    # tile_id = "40N_020E"
    # xmin = 20
    # ymax = 40
    # xmax = xmin + 10
    # ymin = ymax - 0.005

    # For 40N_090W, AGC changes with using BGB:AGB map because US removal factor tiles are AGC+BGC,
    # so making the composite AGC tiles from that depends on the BGC ratio. 40N_090W seems to work fine.
    tile_id = "40N_090W"
    xmin = -90
    ymax = 40
    xmax = xmin + 10
    ymin = ymax - 0.005

    # tile_id = "00N_000E"
    # xmin = 0
    # ymax = 0
    # xmax = 10
    # ymin = -0.005


    # Dictionary of tiles needed for test
    input_dict = {
        cn.model_extent_dir: cn.pattern_model_extent,
        cn.annual_gain_AGB_mangrove_dir: cn.pattern_annual_gain_AGB_mangrove,
        cn.annual_gain_BGB_mangrove_dir: cn.pattern_annual_gain_BGB_mangrove,
        cn.annual_gain_AGC_BGC_natrl_forest_Europe_dir: cn.pattern_annual_gain_AGC_BGC_natrl_forest_Europe,
        cn.annual_gain_AGC_BGC_planted_forest_dir: cn.pattern_annual_gain_AGC_BGC_planted_forest,
        cn.annual_gain_AGC_BGC_natrl_forest_US_dir: cn.pattern_annual_gain_AGC_BGC_natrl_forest_US,
        cn.annual_gain_AGC_natrl_forest_young_dir: cn.pattern_annual_gain_AGC_natrl_forest_young,
        cn.age_cat_IPCC_dir: cn.pattern_age_cat_IPCC,
        cn.annual_gain_AGB_IPCC_defaults_dir: cn.pattern_annual_gain_AGB_IPCC_defaults,
        cn.BGB_AGB_ratio_dir: cn.pattern_BGB_AGB_ratio,

        cn.stdev_annual_gain_AGB_mangrove_dir: cn.pattern_stdev_annual_gain_AGB_mangrove,
        cn.stdev_annual_gain_AGC_BGC_natrl_forest_Europe_dir: cn.pattern_stdev_annual_gain_AGC_BGC_natrl_forest_Europe,
        cn.stdev_annual_gain_AGC_BGC_planted_forest_dir: cn.pattern_stdev_annual_gain_AGC_BGC_planted_forest,
        cn.stdev_annual_gain_AGC_BGC_natrl_forest_US_dir: cn.pattern_stdev_annual_gain_AGC_BGC_natrl_forest_US,
        cn.stdev_annual_gain_AGC_natrl_forest_young_dir: cn.pattern_stdev_annual_gain_AGC_natrl_forest_young,
        cn.stdev_annual_gain_AGB_IPCC_defaults_dir: cn.pattern_stdev_annual_gain_AGB_IPCC_defaults
    }

    output_pattern_list = [cn.pattern_removal_forest_type, cn.pattern_annual_gain_AGC_all_types,
                           cn.pattern_annual_gain_BGC_all_types, cn.pattern_annual_gain_AGC_BGC_all_types,
                           cn.pattern_stdev_annual_gain_AGC_all_types]

    # Makes input tiles for process being tested in specified test area
    tu.make_test_tiles(tile_id, input_dict, cn.pattern_test_suffix, cn.test_data_dir, xmin, ymin, xmax, ymax)

    test_input_pattern = list(comparison_dict.values())[0]

    # Makes comparison tiles for output in specified test area
    uu.print_log("Making comparison tile for output tile type")
    tu.make_test_tiles(tile_id, comparison_dict, cn.pattern_comparison_suffix, cn.test_data_dir, xmin, ymin, xmax, ymax)

    # Deletes outputs of previous run if they exist.
    # Only runs before first parametrized run to avoid deleting the difference raster created from previous parametrizations
    print(delete_old_outputs)

    # Renames the input test tiles with the test suffix (except for biomass, which has its own rule)
    def fake_impl_sensit_tile_rename(sensit_type, tile_id, raw_pattern):
        return f"test/test_data/{tile_id}_{raw_pattern}_{cn.pattern_test_suffix}.tif"
    sensit_tile_rename_fake.side_effect = fake_impl_sensit_tile_rename

    # Makes the output tile names with the test suffix
    def fake_impl_make_tile_name(tile_id, out_pattern):
        return f"test/test_data/tmp_out/{tile_id}_{out_pattern}_{cn.pattern_test_suffix}.tif"
    make_tile_name_fake.side_effect = fake_impl_make_tile_name

    ### act
    # Creates the fragment output tiles
    annual_gain_rate_AGC_BGC_all_forest_types(tile_id=tile_id,
                output_pattern_list = output_pattern_list)


    ### assert
    # The original and new rasters that need to be compared
    original_raster = f'{cn.test_data_dir}{tile_id}_{test_input_pattern}_{cn.pattern_comparison_suffix}.tif'
    # original_raster = f'{cn.test_data_dir}{tile_id}_{cn.pattern_deadwood_emis_year_2000}_{cn.pattern_comparison_suffix}.tif'  # For forcing failure of litter test (compares litter to deadwood)
    new_raster = f'{cn.test_data_out_dir}{tile_id}_{test_input_pattern}_{cn.pattern_test_suffix}.tif'
    # new_raster = f'{cn.test_data_out_dir}{tile_id}_{cn.pattern_litter_emis_year_2000}_{cn.pattern_test_suffix}.tif'   # For forcing failure of deadwood test (compares deadwood to litter)

    # # Converts the original and new rasters into numpy arrays for comparison.
    # # Also creates a difference raster for visualization (not used in testing).
    # # original_raster is from the previous run of the model. new_raster is the developmental version.
    tu.assert_make_test_arrays_and_difference(original_raster, new_raster, tile_id, test_input_pattern)
