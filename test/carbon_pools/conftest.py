import glob
import numpy as np
import os
import pytest
import rasterio
import constants_and_names as cn
from carbon_pools.create_carbon_pools import prepare_gain_table, mangrove_pool_ratio_dict

# Makes mangrove BGC:AGC dictionary for different continent-ecozone combinations
@pytest.fixture(scope='session')
def create_BGC_dictionary():

    gain_table_simplified = prepare_gain_table()

    mang_BGB_AGB_ratio = mangrove_pool_ratio_dict(gain_table_simplified,
                                                    cn.below_to_above_trop_dry_mang,
                                                    cn.below_to_above_trop_wet_mang,
                                                    cn.below_to_above_subtrop_mang)

    return mang_BGB_AGB_ratio


# Makes mangrove deadwood:AGC dictionary for different continent-ecozone combinations
@pytest.fixture(scope='session')
def create_deadwood_dictionary():

    gain_table_simplified = prepare_gain_table()

    mang_deadwood_AGB_ratio = mangrove_pool_ratio_dict(gain_table_simplified,
                                                        cn.deadwood_to_above_trop_dry_mang,
                                                        cn.deadwood_to_above_trop_wet_mang,
                                                        cn.deadwood_to_above_subtrop_mang)

    return mang_deadwood_AGB_ratio


# Makes mangrove litter:AGC dictionary for different continent-ecozone combinations
@pytest.fixture(scope='session')
def create_litter_dictionary():

    gain_table_simplified = prepare_gain_table()

    mang_litter_AGB_ratio = mangrove_pool_ratio_dict(gain_table_simplified,
                                                        cn.litter_to_above_trop_dry_mang,
                                                        cn.litter_to_above_trop_wet_mang,
                                                        cn.litter_to_above_subtrop_mang)

    return mang_litter_AGB_ratio
