import glob
import numpy as np
import os
import pytest
import rasterio
import constants_and_names as cn

# Deletes outputs of previous run if they exist.
# This fixture runs only before the first parametrized run, per https://stackoverflow.com/a/62288070.
@pytest.fixture(scope='session')
def delete_old_outputs():

    out_tests = glob.glob(f'{cn.test_data_out_dir}*.tif')
    for f in out_tests:
        os.remove(f)
        print(f"Deleted {f}")