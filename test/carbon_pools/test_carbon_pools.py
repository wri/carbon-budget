import numpy as np
import pytest as pytest

from ...carbon_pools.create_carbon_pools import create_deadwood_litter, arid_pools


# Use @pytest.mark.skip to skip tests if needed.

def test_can_call_function():
    result = create_deadwood_litter("", {}, {}, [], "", True)
    assert result is None


def test_can_call_with_biomass_swap():
    result = create_deadwood_litter("", {}, {}, [], "biomass_swap", True)
    assert result is None


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
