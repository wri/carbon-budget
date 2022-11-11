import glob
import os
import pytest
from unittest.mock import patch
from carbon_pools.create_carbon_pools import create_deadwood_litter, mangrove_pool_ratio_dict

pytestmark = pytest.mark.integration

# run from /usr/local/app/test
# pytest -m integration

# @pytest.mark.xfail
@patch("universal_util.sensit_tile_rename")
@patch("universal_util.sensit_tile_rename_biomass")
@patch("universal_util.make_tile_name")
@patch("universal_util.upload_log")
def test_it_runs(upload_log_dummy, make_tile_name_fake, sensit_tile_rename_biomass_fake, sensit_tile_rename_fake):

    out_tests = glob.glob('test_data/tmp_out/*.tif')
    for f in out_tests:
        os.remove(f)
        print(f"Deleted {f}")

    # arrange
    def fake_impl_sensit_tile_rename(sensit_type, tile_id, raw_pattern):
        return f"test_data/{tile_id}_{raw_pattern}_top_005deg.tif"
    sensit_tile_rename_fake.side_effect = fake_impl_sensit_tile_rename

    def fake_impl_sensit_tile_rename_biomass(sensit_type, tile_id):
        return f"test_data/{tile_id}_t_aboveground_biomass_ha_2000_top_005deg.tif"
    sensit_tile_rename_biomass_fake.side_effect = fake_impl_sensit_tile_rename_biomass

    def fake_impl_make_tile_name(tile_id, out_pattern):
        return f"test_data/tmp_out/{tile_id}_{out_pattern}_top_005deg.tif"
    make_tile_name_fake.side_effect = fake_impl_make_tile_name

    # act
    result = create_deadwood_litter(tile_id="00N_000E",
                                    # mang_deadwood_AGB_ratio= {'1': 0.5, '2': 0.4, '3': 0.2, '4': 100},
                                    # mang_litter_AGB_ratio={'1': 0.8, '2': 0.7, '3': 0.6, '4': 100},
                                    mang_deadwood_AGB_ratio= {'1': 0.123, '2': 0.258, '3': 0.258, '4': 100},
                                    mang_litter_AGB_ratio={'1': 0.008, '2': 0.0169, '3': 0.0169, '4': 100},
                                    carbon_pool_extent=['loss'])

    #assert
    assert result == "gary"
