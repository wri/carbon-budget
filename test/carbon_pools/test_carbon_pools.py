import numpy as np
import pytest

from ...carbon_pools.create_carbon_pools import create_deadwood_litter, deadwood_litter_equations


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


def test_deadwood_litter_equations_return_zero_deadwood__tropical_low_elev_low_precip():
    deadwood, _ = deadwood_litter_equations(
        bor_tem_trop_window=np.array([1], dtype='float32'),
        deadwood_2000_output=np.array([0], dtype='float32'),
        elevation_window=np.array([1], dtype='float32'),
        litter_2000_output=np.array([0], dtype='float32'),
        natrl_forest_biomass_window=np.array([1], dtype='float32'),
        precip_window=np.array([1], dtype='float32')
    )
    assert deadwood == np.array([0.02])


def test_deadwood_litter_equations_return_zero_litter__tropical_low_elev_low_precip():
    _, litter = deadwood_litter_equations(
        bor_tem_trop_window=np.array([1], dtype='float32'),
        deadwood_2000_output=np.array([0], dtype='float32'),
        elevation_window=np.array([1], dtype='float32'),
        litter_2000_output=np.array([0], dtype='float32'),
        natrl_forest_biomass_window=np.array([1], dtype='float32'),
        precip_window=np.array([1], dtype='float32')
    )
    assert litter == np.array([0.04])
