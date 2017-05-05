import glob
import os

import utilities

# for all files matching Win*, clip, resample, and stack them (all years, months). output 1 file <tileid>_burn.tif


def process_burned_area(windows, coords, tile_id):

    # coords = ['-projwin', str(xmin), str(ymax), str(xmax), str(ymin)]
    for w in windows:
        # get all windows
        burned_list = glob.glob("*Win{}*").format(w)

        for burned_tif in burned_list:
            # clip image
            raster = os.path.basename(burned_tif)
            clipped_window = utilities.clip_raster(raster, tile_id, coords)
            # resample
        # add all resampled/clipped images