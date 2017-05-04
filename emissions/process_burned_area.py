import glob
# for all files matching Win*, clip, resample, and stack them (all years, months). output 1 file <tileid>_burn.tif

def process_burned_area(windows, coords):
    for w in windows:
        # clip window to extent
        burned_list = glob.glob("*Win{}*").format(w)
        for burned_tif in burned_list:
            # clip image

            # resample
        # add all resampled/clipped images