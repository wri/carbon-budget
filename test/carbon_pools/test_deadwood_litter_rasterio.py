import glob
import os
import pytest
import universal_util as uu
import constants_and_names as cn
from unittest.mock import patch
from carbon_pools.create_carbon_pools import prepare_gain_table, create_deadwood_litter, mangrove_pool_ratio_dict

pytestmark = pytest.mark.integration

# run from /usr/local/app/test
# pytest -m integration
# Good test coordinates in GIS are -0.0002 S, 9.549 E (has two mangrove loss pixels adjacent to a few non-mangrove loss pixels)

# Deletes outputs of previous run if they exist
@pytest.fixture
def delete_old_outputs():

    out_tests = glob.glob(f'{cn.test_data_dir}tmp_out/*.tif')
    for f in out_tests:
        os.remove(f)
        print(f"Deleted {f}")

# Makes mangrove deadwood:AGC dictionary for different continent-ecozone combinations
@pytest.fixture
def create_deadwood_dictionary():

    gain_table_simplified = prepare_gain_table()

    mang_deadwood_AGB_ratio = mangrove_pool_ratio_dict(gain_table_simplified,
                                                        cn.deadwood_to_above_trop_dry_mang,
                                                        cn.deadwood_to_above_trop_wet_mang,
                                                        cn.deadwood_to_above_subtrop_mang)

    return mang_deadwood_AGB_ratio

# Makes mangrove litter:AGC dictionary for different continent-ecozone combinations
@pytest.fixture
def create_litter_dictionary():

    gain_table_simplified = prepare_gain_table()

    mang_litter_AGB_ratio = mangrove_pool_ratio_dict(gain_table_simplified,
                                                        cn.litter_to_above_trop_dry_mang,
                                                        cn.litter_to_above_trop_wet_mang,
                                                        cn.litter_to_above_subtrop_mang)

    return mang_litter_AGB_ratio


# @pytest.mark.xfail
@patch("universal_util.sensit_tile_rename")
@patch("universal_util.sensit_tile_rename_biomass")
@patch("universal_util.make_tile_name")
@patch("universal_util.upload_log")
def test_it_runs(upload_log_dummy, make_tile_name_fake, sensit_tile_rename_biomass_fake, sensit_tile_rename_fake,
                 delete_old_outputs, create_deadwood_dictionary, create_litter_dictionary):

    # arrange
    # Dictionary of tiles needed for test
    input_dict = {cn.mangrove_biomass_2000_dir: cn.pattern_mangrove_biomass_2000,
                  cn.cont_eco_dir: cn.pattern_cont_eco_processed,
                  cn.precip_processed_dir: cn.pattern_precip,
                  cn.elevation_processed_dir: cn.pattern_elevation,
                  cn.bor_tem_trop_processed_dir: cn.pattern_bor_tem_trop_processed,
                  cn.WHRC_biomass_2000_unmasked_dir: cn.pattern_WHRC_biomass_2000_unmasked,
                  cn.AGC_emis_year_dir: cn.pattern_AGC_emis_year}

    # Makes tiles in specified test area
    uu.make_test_tiles("00N_000E", input_dict, cn.pattern_test_suffix, cn.test_data_dir, 0, -0.005, 10, 0)

    # Dictionary of tiles previously made for this step, for comparison
    comparison_dict = {cn.deadwood_emis_year_2000_dir: cn.pattern_deadwood_emis_year_2000,
                  cn.litter_emis_year_2000_dir: cn.pattern_litter_emis_year_2000}

    # Makes comparison tiles in specified test area
    uu.make_test_tiles("00N_000E", comparison_dict, f'comparison_{cn.pattern_test_suffix}', cn.test_data_dir, 0, -0.005, 10, 0)

    # Deletes outputs of previous run if they exist
    print(delete_old_outputs)

    # Makes mangrove deadwood:AGC and litter:AGC dictionaries for different continent-ecozone combinations
    deadwood_dict = create_deadwood_dictionary
    litter_dict = create_litter_dictionary

    # Renames the input test tiles with the test suffix (except for biomass, which has its own rule)
    def fake_impl_sensit_tile_rename(sensit_type, tile_id, raw_pattern):
        return f"test_data/{tile_id}_{raw_pattern}_{cn.pattern_test_suffix}.tif"
    sensit_tile_rename_fake.side_effect = fake_impl_sensit_tile_rename

    # Renames the input biomass tile with the test suffix
    def fake_impl_sensit_tile_rename_biomass(sensit_type, tile_id):
        return f"test_data/{tile_id}_t_aboveground_biomass_ha_2000_{cn.pattern_test_suffix}.tif"
    sensit_tile_rename_biomass_fake.side_effect = fake_impl_sensit_tile_rename_biomass

    # Makes the output tile names with the test suffix
    def fake_impl_make_tile_name(tile_id, out_pattern):
        return f"test_data/tmp_out/{tile_id}_{out_pattern}_{cn.pattern_test_suffix}.tif"
    make_tile_name_fake.side_effect = fake_impl_make_tile_name

    # act
    result = create_deadwood_litter(tile_id="00N_000E",
                                    mang_deadwood_AGB_ratio=deadwood_dict,
                                    mang_litter_AGB_ratio=litter_dict,
                                    carbon_pool_extent=['loss'])

    #assert
    assert result == "emissions"
