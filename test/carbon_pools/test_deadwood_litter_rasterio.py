import glob
import numpy as np
import os
import pytest
import rasterio
import universal_util as uu
import constants_and_names as cn
from unittest.mock import patch
from carbon_pools.create_carbon_pools import create_deadwood_litter

import test_helpers as th

pytestmark = pytest.mark.integration

# run from /usr/local/app/test
# pytest -m integration -s
# Good test coordinates in GIS are -0.0002 S, 9.549 E (has two mangrove loss pixels adjacent to a few non-mangrove loss pixels)


# # Makes test tile fragments of specified size for testing purposes using vsis3 (rather than downloading full rasters to Docker instance)
# def make_test_tiles(tile_id, input_dict, test_suffix, out_dir, xmin, ymin, xmax, ymax):
#
#     # Iterates through all input files
#     for key, pattern in input_dict.items():
#
#         # Directory for vsis3 for input file
#         s3_dir = f'{key}'[5:]
#         vsis3_dir = f'/vsis3/{s3_dir}'
#
#         # The full tile name and the test tile fragment name
#         in_file = f'{vsis3_dir}{tile_id}_{pattern}.tif'
#         out_file = f'{out_dir}{tile_id}_{pattern}_{test_suffix}.tif'
#
#         # Skips creating the test tile fragment if it already exists
#         if os.path.exists(out_file):
#             uu.print_log(f'{out_file} already exists. Not creating.')
#             continue
#
#         uu.print_log(f'Making test tile {out_file}')
#
#         # Makes the test tile fragment
#         cmd = ['gdalwarp', '-tr', '{}'.format(str(cn.Hansen_res)), '{}'.format(str(cn.Hansen_res)),
#                '-co', 'COMPRESS=DEFLATE', '-tap', '-te', str(xmin), str(ymin), str(xmax), str(ymax),
#                '-dstnodata', '0', '-t_srs', 'EPSG:4326', '-overwrite', in_file, out_file]
#         uu.log_subprocess_output_full(cmd)
#
#
# # Converts two rasters into numpy arrays, which can be compared in an assert statement.
# # Also creates a raster that's the difference between the two compared rasters. Not used in assert statement.
# # original_raster is from the previous run of the model. new_raster is the developmental version.
# def assert_make_test_arrays_and_difference(original_raster, new_raster, tile_id, pattern):
#
#     print(f'Comparing {new_raster} to {original_raster}')
#
#     array_original = rasterio.open(original_raster).read()
#     array_new = rasterio.open(new_raster).read()
#
#     # Array that is difference between the original and new rasters. Not used for testing, just for visualization.
#     difference = array_original - array_new
#
#     # Saves the difference raster
#     with rasterio.open(original_raster) as src:
#         dsm_meta = src.profile
#
#     diff_saved = f'{cn.test_data_out_dir}{tile_id}_{pattern}_{cn.pattern_test_suffix}_difference.tif'
#
#     with rasterio.open(diff_saved, 'w', **dsm_meta) as diff_out:
#         diff_out.write(difference)
#
#     # https://numpy.org/doc/stable/reference/generated/numpy.testing.assert_equal.html#numpy.testing.assert_equal
#     np.testing.assert_equal(array_original, array_new)


# @pytest.mark.xfail
@patch("universal_util.sensit_tile_rename")
@patch("universal_util.sensit_tile_rename_biomass")
@patch("universal_util.make_tile_name")
@patch("universal_util.upload_log")
@pytest.mark.parametrize("test_input", [cn.pattern_deadwood_emis_year_2000, cn.pattern_litter_emis_year_2000])
def test_rasterio_runs(upload_log_dummy, make_tile_name_fake, sensit_tile_rename_biomass_fake, sensit_tile_rename_fake,
                 delete_old_outputs, create_deadwood_dictionary, create_litter_dictionary, test_input):

    tile_id = "00N_000E"

    ### arrange
    # Dictionary of tiles needed for test
    input_dict = {cn.mangrove_biomass_2000_dir: cn.pattern_mangrove_biomass_2000,
                  cn.cont_eco_dir: cn.pattern_cont_eco_processed,
                  cn.precip_processed_dir: cn.pattern_precip,
                  cn.elevation_processed_dir: cn.pattern_elevation,
                  cn.bor_tem_trop_processed_dir: cn.pattern_bor_tem_trop_processed,
                  cn.WHRC_biomass_2000_unmasked_dir: cn.pattern_WHRC_biomass_2000_unmasked,
                  cn.AGC_emis_year_dir: cn.pattern_AGC_emis_year}

    # Makes tiles in specified test area
    th.make_test_tiles(tile_id, input_dict, cn.pattern_test_suffix, cn.test_data_dir, 0, -0.005, 10, 0)

    # Dictionary of tiles previously made for this step, for comparison
    comparison_dict = {cn.deadwood_emis_year_2000_dir: cn.pattern_deadwood_emis_year_2000,
                  cn.litter_emis_year_2000_dir: cn.pattern_litter_emis_year_2000}

    # Makes comparison tiles in specified test area
    th.make_test_tiles(tile_id, comparison_dict, cn.pattern_comparison_suffix, cn.test_data_dir, 0, -0.005, 10, 0)

    # Deletes outputs of previous run if they exist.
    # Only runs before first parametrized run to avoid deleting the difference raster created from previous parametrizations
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

    ### act
    # Creates the fragment output tiles
    create_deadwood_litter(tile_id=tile_id,
                            mang_deadwood_AGB_ratio=deadwood_dict,
                            mang_litter_AGB_ratio=litter_dict,
                            carbon_pool_extent=['loss'])


    ### assert
    # The original and new rasters that need to be compared
    # original_raster = f'{cn.test_data_dir}{tile_id}_{test_input}_{cn.pattern_comparison_suffix}.tif'
    original_raster = f'{cn.test_data_dir}{tile_id}_{cn.pattern_deadwood_emis_year_2000}_{cn.pattern_comparison_suffix}.tif'
    new_raster = f'{cn.test_data_out_dir}{tile_id}_{test_input}_{cn.pattern_test_suffix}.tif'
    # new_raster = f'{cn.test_data_out_dir}{tile_id}_{cn.pattern_litter_emis_year_2000}_{cn.pattern_test_suffix}.tif'

    # # Converts the original and new rasters into numpy arrays for comparison.
    # # Also creates a difference raster for visualization (not used in testing).
    # # original_raster is from the previous run of the model. new_raster is the developmental version.
    th.assert_make_test_arrays_and_difference(original_raster, new_raster, tile_id, test_input)



