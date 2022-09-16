import numpy as np
import pytest

from carbon_pools.create_carbon_pools import create_deadwood_litter, \
    deadwood_litter_equations


def arid_pools(**kwargs):
    pass


@pytest.mark.xfail
def test_can_call_function():
    result = create_deadwood_litter("", {}, {}, [])
    assert result is None


@pytest.mark.xfail
def test_can_call_with_biomass_swap():
    result = create_deadwood_litter("", {}, {}, [], "biomass_swap", True)
    assert result is None


@pytest.mark.xfail
def test_arid_pools():
    result = arid_pools(
        elevation_window=2000,
        precip_window=1000,
        bor_tem_trop_window=1,
        natrl_forest_biomass_window=np.ma.array([1]),
        deadwood_2000_output=np.ma.array([1]),
        litter_2000_output=np.ma.array([1])
    )
    assert result == (np.ma.array([1.0094]), np.ma.array([1.0148]))


@pytest.mark.xfail
def test_arid_pools_with_no_deadwood_or_litter():
    result = arid_pools(
        elevation_window=2000,
        precip_window=1000,
        bor_tem_trop_window=1,
        natrl_forest_biomass_window=np.ma.array([1]),
        deadwood_2000_output=np.ma.array([0]),
        litter_2000_output=np.ma.array([0])
    )
    assert result == (np.ma.array([0.0094]), np.ma.array([0.0148]))


@pytest.mark.xfail
def test_arid_pools_no_biomass_means_none_is_added():
    result = arid_pools(
        elevation_window=2000,
        precip_window=1000,
        bor_tem_trop_window=1,
        natrl_forest_biomass_window=np.ma.array([0]),
        deadwood_2000_output=np.ma.array([1]),
        litter_2000_output=np.ma.array([1])
    )
    assert result == (np.ma.array([1]), np.ma.array([1]))


@pytest.mark.xfail
def test_arid_pools_fraction_of_biomass():
    result = arid_pools(
        elevation_window=2000,
        precip_window=1000,
        bor_tem_trop_window=1,
        natrl_forest_biomass_window=np.ma.array([0.5]),
        deadwood_2000_output=np.ma.array([1]),
        litter_2000_output=np.ma.array([1])
    )
    assert result == (np.ma.array([1.0047]), np.ma.array([1.0074]))


def test_deadwood_litter_equations_can_be_called():
    result = deadwood_litter_equations(
        bor_tem_trop_window=np.zeros((1, 1), dtype='float32'),
        deadwood_2000_output=np.zeros((1, 1), dtype='float32'),
        elevation_window=np.zeros((1, 1), dtype='float32'),
        litter_2000_output=np.zeros((1, 1), dtype='float32'),
        natrl_forest_biomass_window=np.zeros((1, 1), dtype='float32'),
        precip_window=np.zeros((1, 1), dtype='float32')
    )


def test_deadwood_litter_equations_return_zero_deadwood_for_zero_biomass():
    deadwood, _ = deadwood_litter_equations(
    bor_tem_trop_window=np.zeros((1, 1), dtype='float32'),
    deadwood_2000_output=np.zeros((1, 1), dtype='float32'),
    elevation_window=np.zeros((1, 1), dtype='float32'),
    litter_2000_output=np.zeros((1, 1), dtype='float32'),
    natrl_forest_biomass_window=np.zeros((1, 1), dtype='float32'),
    precip_window=np.zeros((1, 1), dtype='float32')
    )
    assert deadwood == np.array([0.])


def test_deadwood_litter_equations_return_zero_litter_for_zero_biomass():
    _, litter = deadwood_litter_equations(
        bor_tem_trop_window=np.zeros((1, 1), dtype='float32'),
        deadwood_2000_output=np.zeros((1, 1), dtype='float32'),
        elevation_window=np.zeros((1, 1), dtype='float32'),
        litter_2000_output=np.zeros((1, 1), dtype='float32'),
        natrl_forest_biomass_window=np.zeros((1, 1), dtype='float32'),
        precip_window=np.zeros((1, 1), dtype='float32')
    )
    assert litter == np.array([0.])
