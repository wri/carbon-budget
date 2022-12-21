import glob
import numpy as np
import os
import pytest
import rasterio
import sys
import universal_util as uu
import constants_and_names as cn
from unittest.mock import patch
from carbon_pools.create_carbon_pools import create_deadwood_litter

import test.test_helpers as th

# from test import test_helpers as th

pytestmark = pytest.mark.integration

# run from /usr/local/carbon-budget
# pytest -m integration -s
# Good test coordinates in GIS are -0.0002 S, 9.549 E (has two mangrove loss pixels adjacent to a few non-mangrove loss pixels)

# @pytest.mark.xfail
@patch("universal_util.sensit_tile_rename")
@patch("universal_util.sensit_tile_rename_biomass")
@patch("universal_util.make_tile_name")
@patch("universal_util.upload_log")
@pytest.mark.parametrize("comparison_dict", [{cn.deadwood_emis_year_2000_dir: cn.pattern_deadwood_emis_year_2000}
                                            ,{cn.litter_emis_year_2000_dir: cn.pattern_litter_emis_year_2000}
                         ])
def test_rasterio_runs(upload_log_dummy, make_tile_name_fake, sensit_tile_rename_biomass_fake, sensit_tile_rename_fake,
                 delete_old_outputs, create_deadwood_dictionary, create_litter_dictionary, comparison_dict):

    ### arrange
    # tile_id for testing and the extent that should be tested within it
    tile_id = "00N_000E"
    xmin = 0
    ymin = -0.005
    xmax = 10
    ymax = 0

    # Dictionary of tiles needed for test
    input_dict = {cn.mangrove_biomass_2000_dir: cn.pattern_mangrove_biomass_2000,
                  cn.cont_eco_dir: cn.pattern_cont_eco_processed,
                  cn.precip_processed_dir: cn.pattern_precip,
                  cn.elevation_processed_dir: cn.pattern_elevation,
                  cn.bor_tem_trop_processed_dir: cn.pattern_bor_tem_trop_processed,
                  cn.WHRC_biomass_2000_unmasked_dir: cn.pattern_WHRC_biomass_2000_unmasked,
                  cn.AGC_emis_year_dir: cn.pattern_AGC_emis_year}

    # Makes input tiles for process being tested in specified test area
    # th.make_test_tiles(tile_id, input_dict, cn.pattern_test_suffix, cn.test_data_dir, 0, -0.005, 10, 0)
    th.make_test_tiles(tile_id, input_dict, cn.pattern_test_suffix, cn.test_data_dir, xmin, ymin, xmax, ymax)

    test_input_pattern = list(comparison_dict.values())[0]

    # Makes comparison tiles for output in specified test area
    # th.make_test_tiles(tile_id, comparison_dict, cn.pattern_comparison_suffix, cn.test_data_dir, 0, -0.005, 10, 0)
    th.make_test_tiles(tile_id, comparison_dict, cn.pattern_comparison_suffix, cn.test_data_dir, xmin, ymin, xmax, ymax)

    # Deletes outputs of previous run if they exist.
    # Only runs before first parametrized run to avoid deleting the difference raster created from previous parametrizations
    print(delete_old_outputs)

    # Makes mangrove deadwood:AGC and litter:AGC dictionaries for different continent-ecozone combinations
    deadwood_dict = create_deadwood_dictionary
    litter_dict = create_litter_dictionary

    # Renames the input test tiles with the test suffix (except for biomass, which has its own rule)
    def fake_impl_sensit_tile_rename(sensit_type, tile_id, raw_pattern):
        return f"test/test_data/{tile_id}_{raw_pattern}_{cn.pattern_test_suffix}.tif"
    sensit_tile_rename_fake.side_effect = fake_impl_sensit_tile_rename

    # Renames the input biomass tile with the test suffix
    def fake_impl_sensit_tile_rename_biomass(sensit_type, tile_id):
        return f"test/test_data/{tile_id}_t_aboveground_biomass_ha_2000_{cn.pattern_test_suffix}.tif"
    sensit_tile_rename_biomass_fake.side_effect = fake_impl_sensit_tile_rename_biomass

    # Makes the output tile names with the test suffix
    def fake_impl_make_tile_name(tile_id, out_pattern):
        return f"test/test_data/tmp_out/{tile_id}_{out_pattern}_{cn.pattern_test_suffix}.tif"
    make_tile_name_fake.side_effect = fake_impl_make_tile_name

    ### act
    # Creates the fragment output tiles
    create_deadwood_litter(tile_id=tile_id,
                            mang_deadwood_AGB_ratio=deadwood_dict,
                            mang_litter_AGB_ratio=litter_dict,
                            carbon_pool_extent=['loss'])


    ### assert
    # The original and new rasters that need to be compared
    original_raster = f'{cn.test_data_dir}{tile_id}_{test_input_pattern}_{cn.pattern_comparison_suffix}.tif'
    # original_raster = f'{cn.test_data_dir}{tile_id}_{cn.pattern_deadwood_emis_year_2000}_{cn.pattern_comparison_suffix}.tif'  # For forcing failure of litter test (compares litter to deadwood)
    new_raster = f'{cn.test_data_out_dir}{tile_id}_{test_input_pattern}_{cn.pattern_test_suffix}.tif'
    # new_raster = f'{cn.test_data_out_dir}{tile_id}_{cn.pattern_litter_emis_year_2000}_{cn.pattern_test_suffix}.tif'   # For forcing failure of deadwood test (compares deadwood to litter)

    # # Converts the original and new rasters into numpy arrays for comparison.
    # # Also creates a difference raster for visualization (not used in testing).
    # # original_raster is from the previous run of the model. new_raster is the developmental version.
    th.assert_make_test_arrays_and_difference(original_raster, new_raster, tile_id, test_input_pattern)
