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


# Scenario 1- tropical, low elevation, low precipitation
def test_deadwood_litter_equations_return_zero_deadwood__tropical_low_elev_low_precip():
    deadwood, _ = deadwood_litter_equations(
        bor_tem_trop_window=np.array([1], dtype='float32'),
        deadwood_2000_output=np.array([0], dtype='float32'),
        elevation_window=np.array([1], dtype='float32'),
        litter_2000_output=np.array([0], dtype='float32'),
        natrl_forest_biomass_window=np.array([1], dtype='float32'),
        precip_window=np.array([1], dtype='float32')
    )
    assert deadwood == np.array([0.0094], dtype='float32')

def test_deadwood_litter_equations_return_zero_litter__tropical_low_elev_low_precip():
    _, litter = deadwood_litter_equations(
        bor_tem_trop_window=np.array([1], dtype='float32'),
        deadwood_2000_output=np.array([0], dtype='float32'),
        elevation_window=np.array([1], dtype='float32'),
        litter_2000_output=np.array([0], dtype='float32'),
        natrl_forest_biomass_window=np.array([1], dtype='float32'),
        precip_window=np.array([1], dtype='float32')
    )
    assert litter == np.array([0.0148], dtype='float32')


# Scenario 2- tropical, low elevation, moderate precipitation
def test_deadwood_litter_equations_return_zero_deadwood__tropical_low_elev_mod_precip():
    deadwood, _ = deadwood_litter_equations(
        bor_tem_trop_window=np.array([1], dtype='float32'),
        deadwood_2000_output=np.array([0], dtype='float32'),
        elevation_window=np.array([1], dtype='float32'),
        litter_2000_output=np.array([0], dtype='float32'),
        natrl_forest_biomass_window=np.array([100], dtype='float32'),
        precip_window=np.array([1600], dtype='float32')
    )
    assert deadwood == np.array([0.47], dtype='float32')

def test_deadwood_litter_equations_return_zero_litter__tropical_low_elev_mod_precip():
    _, litter = deadwood_litter_equations(
        bor_tem_trop_window=np.array([1], dtype='float32'),
        deadwood_2000_output=np.array([0], dtype='float32'),
        elevation_window=np.array([1], dtype='float32'),
        litter_2000_output=np.array([0], dtype='float32'),
        natrl_forest_biomass_window=np.array([100], dtype='float32'),
        precip_window=np.array([1600], dtype='float32')
    )
    assert litter == np.array([0.37], dtype='float32')


# Scenario 3- tropical, low elevation, high precipitation
def test_deadwood_litter_equations_return_zero_deadwood__tropical_low_elev_high_precip():
    deadwood, _ = deadwood_litter_equations(
        bor_tem_trop_window=np.array([1], dtype='float32'),
        deadwood_2000_output=np.array([0], dtype='float32'),
        elevation_window=np.array([1], dtype='float32'),
        litter_2000_output=np.array([0], dtype='float32'),
        natrl_forest_biomass_window=np.array([100], dtype='float32'),
        precip_window=np.array([1601], dtype='float32')
    )
    assert deadwood == np.array([2.82], dtype='float32')

def test_deadwood_litter_equations_return_zero_litter__tropical_low_elev_high_precip():
    _, litter = deadwood_litter_equations(
        bor_tem_trop_window=np.array([1], dtype='float32'),
        deadwood_2000_output=np.array([0], dtype='float32'),
        elevation_window=np.array([1], dtype='float32'),
        litter_2000_output=np.array([0], dtype='float32'),
        natrl_forest_biomass_window=np.array([100], dtype='float32'),
        precip_window=np.array([1601], dtype='float32')
    )
    assert litter == np.array([0.37], dtype='float32')


# Scenario 4- tropical, high elevation, any precipitation
def test_deadwood_litter_equations_return_zero_deadwood__tropical_high_elev_any_precip():
    deadwood, _ = deadwood_litter_equations(
        bor_tem_trop_window=np.array([1], dtype='float32'),
        deadwood_2000_output=np.array([0], dtype='float32'),
        elevation_window=np.array([2001], dtype='float32'),
        litter_2000_output=np.array([0], dtype='float32'),
        natrl_forest_biomass_window=np.array([100], dtype='float32'),
        precip_window=np.array([1], dtype='float32')
    )
    assert deadwood == np.array([3.29], dtype='float32')

def test_deadwood_litter_equations_return_zero_litter__tropical_high_elev_any_precip():
    _, litter = deadwood_litter_equations(
        bor_tem_trop_window=np.array([1], dtype='float32'),
        deadwood_2000_output=np.array([0], dtype='float32'),
        elevation_window=np.array([2001], dtype='float32'),
        litter_2000_output=np.array([0], dtype='float32'),
        natrl_forest_biomass_window=np.array([100], dtype='float32'),
        precip_window=np.array([1], dtype='float32')
    )
    assert litter == np.array([0.37], dtype='float32')


# Scenario 5- non-tropical, any elevation, any precipitation
def test_deadwood_litter_equations_return_zero_deadwood__non_tropical_any_elev_any_precip():
    deadwood, _ = deadwood_litter_equations(
        bor_tem_trop_window=np.array([2], dtype='float32'),
        deadwood_2000_output=np.array([0], dtype='float32'),
        elevation_window=np.array([1], dtype='float32'),
        litter_2000_output=np.array([0], dtype='float32'),
        natrl_forest_biomass_window=np.array([100], dtype='float32'),
        precip_window=np.array([1], dtype='float32')
    )
    assert deadwood == np.array([3.76], dtype='float32')

def test_deadwood_litter_equations_return_zero_litter__non_tropical_any_elev_any_precip():
    _, litter = deadwood_litter_equations(
        bor_tem_trop_window=np.array([2], dtype='float32'),
        deadwood_2000_output=np.array([0], dtype='float32'),
        elevation_window=np.array([1], dtype='float32'),
        litter_2000_output=np.array([0], dtype='float32'),
        natrl_forest_biomass_window=np.array([100], dtype='float32'),
        precip_window=np.array([1], dtype='float32')
    )
    assert litter == np.array([1.48], dtype='float32')


def test_create_deadwood_litter():
    result = create_deadwood_litter(
        tile_id="00N_000E",
        mang_deadwood_AGB_ratio= {'1': 0.5, '2': 0.4, '3': 0.2, '4': 100},
        mang_litter_AGB_ratio={'1': 0.8, '2': 0.7, '3': 0.6, '4': 100},
        carbon_pool_extent=['loss']
    )