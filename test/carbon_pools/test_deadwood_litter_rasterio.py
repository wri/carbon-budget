import pytest
from carbon_pools.create_carbon_pools import create_deadwood_litter, mangrove_pool_ratio_dict

pytestmark = pytest.mark.integration

@pytest.mark.xfail
def test_it_runs():
    # arrange

    # act
    result = create_deadwood_litter(tile_id="00N_000E",
                                    mang_deadwood_AGB_ratio= {'1': 0.5, '2': 0.4, '3': 0.2, '4': 100},
                                    mang_litter_AGB_ratio={'1': 0.8, '2': 0.7, '3': 0.6, '4': 100},
                                    carbon_pool_extent=['loss'])

    #assert
    assert result == "gary"
