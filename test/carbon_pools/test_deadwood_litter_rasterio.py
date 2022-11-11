import pytest
from unittest.mock import patch
from carbon_pools.create_carbon_pools import create_deadwood_litter, mangrove_pool_ratio_dict

pytestmark = pytest.mark.integration

# @pytest.mark.xfail
@patch("universal_util.sensit_tile_rename")
@patch("universal_util.sensit_tile_rename_biomass")
@patch("universal_util.upload_log")
def test_it_runs(upload_log_dummy, sensit_tile_rename_biomass_fake, sensit_tile_rename_fake):
    # arrange
    def fake_impl_sensit_tile_rename(sensit_type, tile_id, raw_pattern):
        return f"test/test_data/{tile_id}_{raw_pattern}_top_005deg.tif"
    sensit_tile_rename_fake.side_effect = fake_impl_sensit_tile_rename

    def fake_impl_sensit_tile_rename_biomass(sensit_type, tile_id):
        return f"test/test_data/{tile_id}_t_aboveground_biomass_ha_2000_top_005deg.tif"
    sensit_tile_rename_biomass_fake.side_effect = fake_impl_sensit_tile_rename_biomass

    # act
    result = create_deadwood_litter(tile_id="00N_000E",
                                    mang_deadwood_AGB_ratio= {'1': 0.5, '2': 0.4, '3': 0.2, '4': 100},
                                    mang_litter_AGB_ratio={'1': 0.8, '2': 0.7, '3': 0.6, '4': 100},
                                    carbon_pool_extent=['loss'])

    #assert
    assert result == "gary"
