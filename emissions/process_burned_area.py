import glob
import os
import subprocess

import utilities

# for all files matching Win*, clip, resample, and stack them (all years, months). output 1 file <tileid>_burn.tif


def process_burned_area(windows, coords, tile_id):

    # coords = ['-projwin', str(xmin), str(ymax), str(xmax), str(ymin)]
    print coords
    print tile_id	
    for w in windows:
        print w
        # get all windows
        # burned_list = glob.glob("*Win{}*").format(w)
        win_glob = "MCD64monthly*Win{}*burndate.tif".format(w)
        burned_list = glob.glob(win_glob)
        print burned_list

        for burned_tif in burned_list:
            # recode > 0 to 1, all else to no data
            #recoded_burned_area = utilities.recode_burned_area(burned_tif)
            #recoded_proj = 'projected'
            #clipped_window = utilities.clip_raster(recoded_proj, tile_id, coords)
            clipped_window = '10N_100E_projected.tif'
            resampled_raster = clipped_window.strip(".tif")+"_res.tif"
            #resample_cmd = ['gdal_translate', '-co', 'COMPRESS=LZW', '-tr', '.00025', '.00025', '-a_nodata',
            #        '0', clipped_window, resampled_raster]
            #subprocess.check_call(resample_cmd)
            final_clip = utilities.clip_raster(resampled_raster.strip(".tif"), tile_id, coords)

        # add all resampled/clipped images
coords = ['-projwin', '100.0', '10.0', '110.0', '0.0']
tile_id = '10N_100E'
windows = ['19']

#process_burned_area(windows, coords, tile_id)
