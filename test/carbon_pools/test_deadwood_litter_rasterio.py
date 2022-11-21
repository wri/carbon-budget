import glob
import os
import pytest
import universal_util as uu
import constants_and_names as cn
from unittest.mock import patch
from carbon_pools.create_carbon_pools import create_deadwood_litter, mangrove_pool_ratio_dict

pytestmark = pytest.mark.integration

# run from /usr/local/app/test
# pytest -m integration
# Good test coordinates in GIS are -0.0002 S, 9.549 E (has two mangrove loss pixels adjacent to a few non-mangrove loss pixels)

# @pytest.mark.xfail
@patch("universal_util.sensit_tile_rename")
@patch("universal_util.sensit_tile_rename_biomass")
@patch("universal_util.make_tile_name")
@patch("universal_util.upload_log")
def test_it_runs(upload_log_dummy, make_tile_name_fake, sensit_tile_rename_biomass_fake, sensit_tile_rename_fake):

    input_dir_list = [cn.mangrove_biomass_2000_dir, cn.cont_eco_dir,
                      cn.precip_processed_dir, cn.elevation_processed_dir, cn.bor_tem_trop_processed_dir,
                      cn.WHRC_biomass_2000_unmasked_dir, cn.AGC_emis_year_dir]
    input_pattern_list = [cn.pattern_mangrove_biomass_2000, cn.pattern_cont_eco_processed,
                                cn.pattern_precip, cn.pattern_elevation, cn.pattern_bor_tem_trop_processed,
                                cn.pattern_WHRC_biomass_2000_unmasked, cn.pattern_AGC_emis_year]

    input_dict = {cn.mangrove_biomass_2000_dir: cn.pattern_mangrove_biomass_2000,
                  cn.cont_eco_dir: cn.pattern_cont_eco_processed,
                  cn.precip_processed_dir: cn.pattern_precip,
                  cn.elevation_processed_dir: cn.pattern_elevation,
                  cn.bor_tem_trop_processed_dir: cn.pattern_bor_tem_trop_processed,
                  cn.WHRC_biomass_2000_unmasked_dir: cn.pattern_WHRC_biomass_2000_unmasked,
                  cn.AGC_emis_year_dir: cn.pattern_AGC_emis_year}

    for key, value in input_dict.items():
        uu.make_test_tile("00N_000E", key, value, cn.pattern_test_suffix, cn.test_data_dir, 0, -0.005, 10, 0)

    out_tests = glob.glob('test_data/tmp_out/*.tif')
    for f in out_tests:
        os.remove(f)
        print(f"Deleted {f}")

    # arrange
    def fake_impl_sensit_tile_rename(sensit_type, tile_id, raw_pattern):
        return f"test_data/{tile_id}_{raw_pattern}_{cn.pattern_test_suffix}.tif"
    sensit_tile_rename_fake.side_effect = fake_impl_sensit_tile_rename

    def fake_impl_sensit_tile_rename_biomass(sensit_type, tile_id):
        return f"test_data/{tile_id}_t_aboveground_biomass_ha_2000_{cn.pattern_test_suffix}.tif"
    sensit_tile_rename_biomass_fake.side_effect = fake_impl_sensit_tile_rename_biomass

    def fake_impl_make_tile_name(tile_id, out_pattern):
        return f"test_data/tmp_out/{tile_id}_{out_pattern}_{cn.pattern_test_suffix}.tif"
    make_tile_name_fake.side_effect = fake_impl_make_tile_name

    # act
    result = create_deadwood_litter(tile_id="00N_000E",
                                    mang_deadwood_AGB_ratio= {2001.0: 0.258, 4001.0: 0.258, 7001.0: 0.258, 2002.0: 0.258, 4002.0: 0.258, 7002.0: 0.258, 2003.0: 0.258, 4003.0: 0.258, 7003.0: 0.258, 1004.0: 0.258, 3004.0: 0.258, 4004.0: 0.258, 7004.0: 0.258, 8004.0: 0.258, 2004.0: 0.258, 1014.0: 0.258, 2014.0: 0.258, 1018.0: 0.123, 2018.0: 0.123, 2005.0: 0.258, 3005.0: 0.258, 4005.0: 0.258, 7005.0: 0.258, 2006.0: 0.258, 4006.0: 0.258, 1007.0: 0.258, 2007.0: 0.258, 5007.0: 0.258, 6007.0: 0.258, 7007.0: 0.258, 1008.0: 0.258, 2008.0: 0.258, 4008.0: 0.258, 8008.0: 0.258, 1009.0: 0.258, 2009.0: 0.258, 4009.0: 0.258, 7009.0: 0.258, 1010.0: 0.258, 2010.0: 0.258, 5010.0: 0.258, 6010.0: 0.258, 2011.0: 0.258, 4011.0: 0.258, 7011.0: 0.258, 2012.0: 0.258, 4012.0: 0.258, 7012.0: 0.258, 2013.0: 0.258, 4013.0: 0.258, 7013.0: 0.258, 8013.0: 0.258, 4014.0: 0.258, 7014.0: 0.258, 8014.0: 0.258, 2015.0: 0.258, 4015.0: 0.258, 7015.0: 0.258, 1016.0: 0.258, 2016.0: 0.258, 4016.0: 0.258, 1017.0: 0.258, 2017.0: 0.258, 4017.0: 0.258, 4018.0: 0.123, 1019.0: 0.123, 2019.0: 0.123, 4019.0: 0.123, 1020.0: 0.123, 2020.0: 0.123, 4020.0: 0.123, 1021.0: 0.258, 2021.0: 0.258, 5021.0: 0.258, 6021.0: 0.258, 1022.0: 0.258, 2022.0: 0.258, 4022.0: 0.258, 7022.0: 0.258, 0.0: 0},
                                    mang_litter_AGB_ratio={2001.0: 0.0169, 4001.0: 0.0169, 7001.0: 0.0169, 2002.0: 0.0169, 4002.0: 0.0169, 7002.0: 0.0169, 2003.0: 0.0169, 4003.0: 0.0169, 7003.0: 0.0169, 1004.0: 0.0169, 3004.0: 0.0169, 4004.0: 0.0169, 7004.0: 0.0169, 8004.0: 0.0169, 2004.0: 0.0169, 1014.0: 0.0169, 2014.0: 0.0169, 1018.0: 0.008, 2018.0: 0.008, 2005.0: 0.0169, 3005.0: 0.0169, 4005.0: 0.0169, 7005.0: 0.0169, 2006.0: 0.0169, 4006.0: 0.0169, 1007.0: 0.0169, 2007.0: 0.0169, 5007.0: 0.0169, 6007.0: 0.0169, 7007.0: 0.0169, 1008.0: 0.0169, 2008.0: 0.0169, 4008.0: 0.0169, 8008.0: 0.0169, 1009.0: 0.0169, 2009.0: 0.0169, 4009.0: 0.0169, 7009.0: 0.0169, 1010.0: 0.0169, 2010.0: 0.0169, 5010.0: 0.0169, 6010.0: 0.0169, 2011.0: 0.0169, 4011.0: 0.0169, 7011.0: 0.0169, 2012.0: 0.0169, 4012.0: 0.0169, 7012.0: 0.0169, 2013.0: 0.0169, 4013.0: 0.0169, 7013.0: 0.0169, 8013.0: 0.0169, 4014.0: 0.0169, 7014.0: 0.0169, 8014.0: 0.0169, 2015.0: 0.0169, 4015.0: 0.0169, 7015.0: 0.0169, 1016.0: 0.0169, 2016.0: 0.0169, 4016.0: 0.0169, 1017.0: 0.0169, 2017.0: 0.0169, 4017.0: 0.0169, 4018.0: 0.008, 1019.0: 0.008, 2019.0: 0.008, 4019.0: 0.008, 1020.0: 0.008, 2020.0: 0.008, 4020.0: 0.008, 1021.0: 0.0169, 2021.0: 0.0169, 5021.0: 0.0169, 6021.0: 0.0169, 1022.0: 0.0169, 2022.0: 0.0169, 4022.0: 0.0169, 7022.0: 0.0169, 0.0: 0},
                                    carbon_pool_extent=['loss'])

    #assert
    assert result == "gary"
