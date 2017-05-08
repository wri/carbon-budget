import glob
import os

import utilities

# for all files matching Win*, clip, resample, and stack them (all years, months). output 1 file <tileid>_burn.tif


def process_burned_area(windows, coords, tile_id):

    # coords = ['-projwin', str(xmin), str(ymax), str(xmax), str(ymin)]
    print coords
    print tile_id	
    for w in windows:
        print w
# ['MCD64monthly.A2001032.Win04.006.burndate.tif', 'MCD64monthly.A2001121.Win04.006.burndate.tif', 'MCD64monthly.A2000336.Win04.006.burndate.tif', 'MCD64monthly.A2001001.Win04.006.burndate.tif', 'MCD64monthly.A2000306.Win04.006.burndate.tif', 'MCD64monthly.A2001091.Win04.006.burndate.tif', 'MCD64monthly.A2001060.Win04.006.burndate.tif']

        # get all windows
        # burned_list = glob.glob("*Win{}*").format(w)
        win_glob = "*Win{}*burndate.tif".format(w)
        burned_list = glob.glob(win_glob)
        print burned_list
        for burned_tif in burned_list:
            # recode > 0 to 1, all else to no data
            recoded_burned_area = utilities.recode_burned_area(burned_tif)
            # clip image
            raster = os.path.basename(recoded_burned_area).strip(".tif")
            print raster
            print tile_id
            
            clipped_window = utilities.clip_raster(raster, tile_id, coords)
            # resample
        # add all resampled/clipped images
coords = ['-projwin', '100.0', '10.0', '110.0', '0.0']
tile_id = '10N_100E'
windows = ['19']

process_burned_area(windows, coords, tile_id)
