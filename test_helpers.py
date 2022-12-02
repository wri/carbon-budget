import glob
import numpy as np
import os
import pytest
import rasterio
import universal_util as uu
import constants_and_names as cn

# Makes test tile fragments of specified size for testing purposes using vsis3 (rather than downloading full rasters to Docker instance)
def make_test_tiles(tile_id, input_dict, test_suffix, out_dir, xmin, ymin, xmax, ymax):

    # Iterates through all input files
    for key, pattern in input_dict.items():

        # Directory for vsis3 for input file
        s3_dir = f'{key}'[5:]
        vsis3_dir = f'/vsis3/{s3_dir}'

        # The full tile name and the test tile fragment name
        in_file = f'{vsis3_dir}{tile_id}_{pattern}.tif'
        out_file = f'{out_dir}{tile_id}_{pattern}_{test_suffix}.tif'

        # Skips creating the test tile fragment if it already exists
        if os.path.exists(out_file):
            uu.print_log(f'{out_file} already exists. Not creating.')
            continue

        uu.print_log(f'Making test tile {out_file}')

        # Makes the test tile fragment
        cmd = ['gdalwarp', '-tr', '{}'.format(str(cn.Hansen_res)), '{}'.format(str(cn.Hansen_res)),
               '-co', 'COMPRESS=DEFLATE', '-tap', '-te', str(xmin), str(ymin), str(xmax), str(ymax),
               '-dstnodata', '0', '-t_srs', 'EPSG:4326', '-overwrite', in_file, out_file]
        uu.log_subprocess_output_full(cmd)


# Converts two rasters into numpy arrays, which can be compared in an assert statement.
# Also creates a raster that's the difference between the two compared rasters. Not used in assert statement.
# original_raster is from the previous run of the model. new_raster is the developmental version.
def assert_make_test_arrays_and_difference(original_raster, new_raster, tile_id, pattern):

    print(f'Comparing {new_raster} to {original_raster}')

    array_original = rasterio.open(original_raster).read()
    array_new = rasterio.open(new_raster).read()

    # Array that is difference between the original and new rasters. Not used for testing, just for visualization.
    difference = array_original - array_new

    # Saves the difference raster
    with rasterio.open(original_raster) as src:
        dsm_meta = src.profile

    diff_saved = f'{cn.test_data_out_dir}{tile_id}_{pattern}_{cn.pattern_test_suffix}_difference.tif'

    with rasterio.open(diff_saved, 'w', **dsm_meta) as diff_out:
        diff_out.write(difference)

    # https://numpy.org/doc/stable/reference/generated/numpy.testing.assert_equal.html#numpy.testing.assert_equal
    np.testing.assert_equal(array_original, array_new)

    print('\n')