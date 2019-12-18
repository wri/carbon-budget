import multiprocessing
from multiprocessing.pool import Pool
from functools import partial
import US_removal_rates
import argparse
import pandas as pd
import subprocess
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def main ():

    # Files to download for this script.
    download_dict = {cn.planted_forest_type_unmasked_dir: [cn.pattern_planted_forest_type_unmasked],
                     cn.gain_dir: [cn.pattern_gain],
                     cn.tcd_dir: [cn.pattern_tcd],
                     cn.annual_gain_AGB_natrl_forest_dir: [cn.pattern_annual_gain_AGB_natrl_forest]
    }

    # List of tiles to run in the model
    tile_id_list = uu.tile_list_s3(cn.annual_gain_AGB_natrl_forest_dir)
    # tile_id_list = ["00N_000E", "00N_050W", "00N_060W", "00N_010E", "00N_020E", "00N_030E", "00N_040E", "10N_000E", "10N_010E", "10N_010W", "10N_020E", "10N_020W"] # test tiles
    tile_id_list = ['50N_070W'] # test tiles
    print tile_id_list
    print "There are {} tiles to process".format(str(len(tile_id_list))) + "\n"


    # List of output directories and output file name patterns
    output_dir_list = [cn.annual_gain_AGB_natrl_forest_dir]
    output_pattern_list = [cn.pattern_annual_gain_AGB_natrl_forest]


    # By definition, this script is for US-specific removals
    sensit_type = 'US_removals'

    count = multiprocessing.cpu_count()


    # Counts how many processed FIA region tiles there are on s3 already.
    FIA_regions_tile_count = uu.count_tiles_s3(cn.FIA_regions_processed_dir)

    # Only creates FIA region tiles if they don't already exist on s3. 13 tiles cover the continental US.
    if FIA_regions_tile_count == 15:
        print "FIA region tiles already created. Copying to s3 now..."
        uu.s3_flexible_download(cn.FIA_regions_processed_dir, cn.pattern_FIA_regions_processed, '.', 'std', tile_id_list)

    else:
        print "FIA region tiles do not exist. CCreating tiles, then copying to s3 for future use..."
        uu.s3_file_download(os.path.join(cn.FIA_regions_raw_dir, cn.name_FIA_regions_raw), '.', 'std')

        cmd = ['unzip', '-o', '-j', cn.name_FIA_regions_raw]
        subprocess.check_call(cmd)

        # Converts the region shapefile to Hansen tiles
        pool = multiprocessing.Pool(count/2)
        pool.map(US_removal_rates.prep_FIA_regions, tile_id_list)


    # List of FIA region tiles on the spot machine. Only these are used for the rest of the script.
    US_tile_list = uu.tile_list_spot_machine('.', '{}.tif'.format(cn.pattern_FIA_regions_processed))
    US_tile_id_list = [i[0:8] for i in US_tile_list]
    print US_tile_id_list

    US_tile_id_list = ['50N_070W']


    # Counts how many processed FIA region tiles there are on s3 already.
    US_age_tile_count = uu.count_tiles_s3(cn.US_forest_age_cat_processed_dir)

    # Only creates FIA forest group tiles if they don't already exist on s3. 13 tiles cover the continental US.
    if US_age_tile_count == 15:
        print "Forest age category tiles already created. Copying to spot machine now..."
        uu.s3_flexible_download(cn.US_forest_age_cat_processed_dir, cn.pattern_US_forest_age_cat_processed,
                                '', 'std', US_tile_id_list)

    else:
        print "Southern forest age category tiles do not exist. Creating tiles, then copying to s3 for future use..."
        uu.s3_file_download(os.path.join(cn.US_forest_age_cat_raw_dir, cn.name_US_forest_age_cat_raw), '.', 'std')

        # Converts the national forest age category raster to Hansen tiles
        source_raster = cn.name_US_forest_age_cat_raw
        out_pattern = cn.pattern_US_forest_age_cat_processed
        dt = 'Int16'
        pool = multiprocessing.Pool(count/2)
        pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt), US_tile_id_list)

        uu.upload_final_set(cn.US_forest_age_cat_processed_dir, cn.pattern_US_forest_age_cat_processed)


    # # Counts how many processed FIA region tiles there are on s3 already.
    # FIA_forest_group_tile_count = uu.count_tiles_s3(cn.FIA_regions_processed_dir)
    #
    # # Only creates FIA forest group tiles if they don't already exist on s3. 13 tiles cover the continental US.
    # if FIA_forest_group_tile_count == 15:
    #     print "FIA forest group tiles already created. Copying to spot machine now..."
    #     uu.s3_flexible_download(cn.FIA_forest_group_processed_dir, cn.pattern_FIA_forest_group_processed, '', 'std', US_tile_id_list)
    #
    # else:
    #     print "FIA forest group tiles do not exist. Creating tiles, then copying to s3 for future use..."
    #     uu.s3_file_download(os.path.join(cn.FIA_forest_group_raw_dir, cn.name_FIA_forest_group_raw), '.', 'std')
    #
    #     # Converts the national forest grou raster to Hansen forest group tiles
    #     source_raster = cn.name_FIA_forest_group_raw
    #     out_pattern = cn.pattern_FIA_forest_group_processed
    #     dt = 'Byte'
    #     pool = multiprocessing.Pool(count/2)
    #     pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt), US_tile_id_list)
    #
    #
    # # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
    # for key, values in download_dict.iteritems():
    #     dir = key
    #     pattern = values[0]
    #     uu.s3_flexible_download(dir, pattern, '.', sensit_type, US_tile_id_list)
    #
    #
    # # If the model run isn't the standard one, the output directory and file names are changed
    # if sensit_type != 'std':
    #     print "Changing output directory and file name pattern based on sensitivity analysis"
    #     output_dir_list = uu.alter_dirs(sensit_type, output_dir_list)
    #     output_pattern_list = uu.alter_patterns(sensit_type, output_pattern_list)
    #
    #
    #  # Table with IPCC Table 4.9 default gain rates
    # uu.s3_file_download(os.path.join(cn.US_removal_rate_dir, cn.table_US_removal_rate), '.', 'US_removals')
    #
    #
    # # Creates a single filename pattern to pass to the multiprocessor call
    # pattern = output_pattern_list[0]
    #
    # # This configuration of the multiprocessing call is necessary for passing multiple arguments to the main function
    # # It is based on the example here: http://spencerimp.blogspot.com/2015/12/python-multiprocess-with-multiple.html
    # # With processes=30, peak usage was about 350 GB
    # count = multiprocessing.cpu_count()
    # pool = multiprocessing.Pool(processes=26)
    # pool.map(partial(US_removal_rates.forest_age_category, gain_table_dict=gain_table_dict,
    #                  pattern=pattern, sensit_type=sensit_type), US_tile_id_list)
    # pool.close()
    # pool.join()
    # #
    # # # # For single processor use
    # # # for tile in tile_id_list:
    # # #
    # # #     forest_age_category_natrl_forest.forest_age_category(tile, gain_table_dict, pattern, sensit_type)
    # #
    # # # Uploads output tiles to s3
    # # uu.upload_final_set(output_dir_list[0], output_pattern_list[0])


if __name__ == '__main__':
    main()
