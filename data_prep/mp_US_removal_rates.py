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
    download_dict = {cn.gain_dir: [cn.pattern_gain],
                     cn.annual_gain_AGB_natrl_forest_dir: [cn.pattern_annual_gain_AGB_natrl_forest]
    }

    # List of tiles to run in the model
    tile_id_list = uu.tile_list_s3(cn.annual_gain_AGB_natrl_forest_dir)
    # tile_id_list = ["00N_000E", "00N_050W", "00N_060W", "00N_010E", "00N_020E", "00N_030E", "00N_040E", "10N_000E", "10N_010E", "10N_010W", "10N_020E", "10N_020W"] # test tiles
    # tile_id_list = ['30N_110W'] # test tiles
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
    if FIA_regions_tile_count == 16:
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

    # US_tile_id_list = ['30N_110W']


    # Counts how many processed FIA region tiles there are on s3 already.
    US_age_tile_count = uu.count_tiles_s3(cn.US_forest_age_cat_processed_dir)

    # Only creates FIA forest group tiles if they don't already exist on s3. 13 tiles cover the continental US.
    if US_age_tile_count == 16:
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


    # Counts how many processed FIA region tiles there are on s3 already.
    FIA_forest_group_tile_count = uu.count_tiles_s3(cn.FIA_regions_processed_dir)

    # Only creates FIA forest group tiles if they don't already exist on s3. 13 tiles cover the continental US.
    if FIA_forest_group_tile_count == 16:
        print "FIA forest group tiles already created. Copying to spot machine now..."
        uu.s3_flexible_download(cn.FIA_forest_group_processed_dir, cn.pattern_FIA_forest_group_processed, '', 'std', US_tile_id_list)

    else:
        print "FIA forest group tiles do not exist. Creating tiles, then copying to s3 for future use..."
        uu.s3_file_download(os.path.join(cn.FIA_forest_group_raw_dir, cn.name_FIA_forest_group_raw), '.', 'std')

        # Converts the national forest grou raster to Hansen forest group tiles
        source_raster = cn.name_FIA_forest_group_raw
        out_pattern = cn.pattern_FIA_forest_group_processed
        dt = 'Byte'
        pool = multiprocessing.Pool(count/2)
        pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt), US_tile_id_list)


    # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
    for key, values in download_dict.iteritems():
        dir = key
        pattern = values[0]
        uu.s3_flexible_download(dir, pattern, '.', sensit_type, US_tile_id_list)



    # Table with IPCC Table 4.9 default gain rates
    cmd = ['aws', 's3', 'cp', os.path.join(cn.gain_spreadsheet_dir, cn.table_US_removal_rate), '.']
    subprocess.check_call(cmd)

    # Imports the table with the ecozone-continent codes and the carbon gain rates
    gain_table = pd.read_excel("{}".format(cn.table_US_removal_rate),
                               sheet_name="US_rates_for_model")

    # Converts gain table from wide to long, so each continent-ecozone-age category has its own row
    gain_table_group_region_by_age = pd.melt(gain_table, id_vars=['FIA_region_code', 'forest_group_code'],
                                      value_vars=['growth_young', 'growth_middle',
                                                  'growth_old'])
    gain_table_group_region_by_age = gain_table_group_region_by_age.dropna()

    # Creates a code for each age category so that each continent-ecozone-age combo can have its own unique value
    age_dict = {'growth_young': 1000, 'growth_middle': 2000, 'growth_old': 3000}

    # Creates a unique value for each continent-ecozone-age category
    gain_table_group_region_age = gain_table_group_region_by_age.replace({"variable": age_dict})
    gain_table_group_region_age['age_cat'] = gain_table_group_region_age['variable']*10
    gain_table_group_region_age['combined'] = gain_table_group_region_age['age_cat'] + \
                                              gain_table_group_region_age['forest_group_code']*100 + \
                                              gain_table_group_region_age['FIA_region_code']

    print gain_table_group_region_age

    # Converts the continent-ecozone-age codes and corresponding gain rates to a dictionary
    gain_table_dict = pd.Series(gain_table_group_region_age.value.values, index=gain_table_group_region_age.combined).to_dict()

    print gain_table_dict

    # Adds a dictionary entry for where the ecozone-continent-age code is 0 (not in a continent)
    gain_table_dict[0] = 0

    print gain_table_dict


    # Creates a single filename pattern to pass to the multiprocessor call
    pattern = output_pattern_list[0]


    # # This configuration of the multiprocessing call is necessary for passing multiple arguments to the main function
    # # It is based on the example here: http://spencerimp.blogspot.com/2015/12/python-multiprocess-with-multiple.html
    # # processes=24 peaks at about 440 GB of memory on an r4.16xlarge machine
    # pool = multiprocessing.Pool(count/2)
    # pool.map(partial(US_removal_rates.US_removal_rate_calc, gain_table_dict=gain_table_dict,
    #                  pattern=pattern, sensit_type=sensit_type), US_tile_id_list)
    # pool.close()
    # pool.join()

    # For single processor use
    for tile_id in US_tile_id_list:

        US_removal_rates.US_removal_rate_calc(tile_id, gain_table_dict, pattern, sensit_type)


    # Uploads output tiles to s3
    for i in range(0, len(output_dir_list)):
        uu.upload_final_set(output_dir_list[i], output_pattern_list[i])


if __name__ == '__main__':
    main()
