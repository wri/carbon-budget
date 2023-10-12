'''
This script creates Hansen tiles of US-specific AGB and BGB removal rates for non-mangrove, non-planted forests for the
US removal rate sensitivity analysis. The extent of the output is the same as the non-mangrove, non-planted forests
for the standard model. These rates are then combined with mangrove and non-mangrove planted forest
rates from the standard model in mp_merge_cumulative_annual_gain_all_forest_types.py and the model then run like for
any other sensitivity analysis.
US-specific removal rates are based on the FIA region, FIA forest group, and forest age category of each pixel. A rate
table is then applied to each combination of region-group-age to apply the correct rates, just like for the standard
model (mp_annual_gain_rate_IPCC_defaults.py).
The FIA region shapefile is Hansenized in this script.
The FIA forest group raster is created in ArcMap before this processing and Hansenized in this script. The input forest group
raster is basically the composite of the original forest group raster and the ArcMap Focal Statistics tool applied to it
at various rectangular windows, from 3x3 to 400x400. This Focal Statistics process covers the entire CONUS in
forest group characterization so that any model pixel will be covered by forest group.
The forest age raster (Pan et al.) is pre-created and then processed in this script the same as the forest group raster.
The actual age category cutoffs are different for the SE/SC regions and the rest of the US but the age category raster
as already incorporated that, so the youngest category (1000) means 0-20 years for non-south and 0-10 years for south, etc.
After Hansenizing region, group, and age category, this script creates two dictionaries: one with removal rates by
region-group-age combinations and another with the youngest rate for each region-group combination.
The first dictionary is applied to all standard removals model pixels according to their region-group-age combination
but then is overwritten for any Hansen gain pixel with the youngest rate for that region-group combination applied
(using the second dictionary). That is because we can assume that any Hansen gain pixel is in the youngest age category,
i.e. it is more specific information than the Pan et al. forest age category raster, so we give that info priority.
Wherever there is no rate in the table for a region-group-age combination, the standard model rate is used.
This has the exact same extent as the standard AGB and BGB annual removal rate layers; the pixels where it should be
different are the ones that have a region-group-age combination that's in the FIA table.
'''

import multiprocessing
from functools import partial
import datetime
from sensitivity_analysis import US_removal_rates
import pandas as pd
from subprocess import Popen, PIPE, STDOUT, check_call
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def main ():

    no_upload=False

    # Create the output log
    uu.initiate_log()

    os.chdir(cn.docker_tile_dir)

    # Files to download for this script.
    download_dict = {cn.gain_dir: [cn.pattern_data_lake],
                     cn.annual_gain_AGB_IPCC_defaults_dir: [cn.pattern_annual_gain_AGB_IPCC_defaults],
                     cn.BGB_AGB_ratio_dir: [cn.pattern_BGB_AGB_ratio]
    }

    # List of tiles that could be run. This list is only used to create the FIA region tiles if they don't already exist.
    tile_id_list = uu.tile_list_s3(cn.annual_gain_AGB_IPCC_defaults_dir)
    # tile_id_list = ["00N_000E", "00N_050W", "00N_060W", "00N_010E", "00N_020E", "00N_030E", "00N_040E", "10N_000E", "10N_010E", "10N_010W", "10N_020E", "10N_020W"] # test tiles
    # tile_id_list = ['50N_130W'] # test tiles


    # List of output directories and output file name patterns
    output_dir_list = [cn.US_annual_gain_AGB_natrl_forest_dir, cn.US_annual_gain_BGB_natrl_forest_dir]
    output_pattern_list = [cn.pattern_US_annual_gain_AGB_natrl_forest, cn.pattern_US_annual_gain_BGB_natrl_forest]


    # By definition, this script is for US-specific removals
    sensit_type = 'US_removals'

    # Counts how many processed FIA region tiles there are on s3 already. 16 tiles cover the continental US.
    FIA_regions_tile_count = uu.count_tiles_s3(cn.FIA_regions_processed_dir)

    # Only creates FIA region tiles if they don't already exist on s3.
    if FIA_regions_tile_count == 16:
        uu.print_log("FIA region tiles already created. Copying to s3 now...")
        uu.s3_flexible_download(cn.FIA_regions_processed_dir, cn.pattern_FIA_regions_processed, cn.docker_tile_dir, 'std', 'all')

    else:
        uu.print_log("FIA region tiles do not exist. Creating tiles, then copying to s3 for future use...")
        uu.s3_file_download(os.path.join(cn.FIA_regions_raw_dir, cn.name_FIA_regions_raw), cn.docker_tile_dir, 'std')

        cmd = ['unzip', '-o', '-j', cn.name_FIA_regions_raw]
        # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
        process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
        with process.stdout:
            uu.log_subprocess_output(process.stdout)

        # Converts the region shapefile to Hansen tiles
        pool = multiprocessing.Pool(int(cn.count/2))
        pool.map(US_removal_rates.prep_FIA_regions, tile_id_list)


    # List of FIA region tiles on the spot machine. Only this list is used for the rest of the script.
    US_tile_list = uu.tile_list_spot_machine(cn.docker_tile_dir, '{}.tif'.format(cn.pattern_FIA_regions_processed))
    US_tile_id_list = [i[0:8] for i in US_tile_list]
    # US_tile_id_list = ['50N_130W']    # For testing
    uu.print_log(US_tile_id_list)
    uu.print_log("There are {} tiles to process".format(str(len(US_tile_id_list))) + "\n")


    # Counts how many processed forest age category tiles there are on s3 already. 16 tiles cover the continental US.
    US_age_tile_count = uu.count_tiles_s3(cn.US_forest_age_cat_processed_dir)

    # Only creates FIA forest age category tiles if they don't already exist on s3.
    if US_age_tile_count == 16:
        uu.print_log("Forest age category tiles already created. Copying to spot machine now...")
        uu.s3_flexible_download(cn.US_forest_age_cat_processed_dir, cn.pattern_US_forest_age_cat_processed,
                                '', 'std', US_tile_id_list)

    else:
        uu.print_log("Southern forest age category tiles do not exist. Creating tiles, then copying to s3 for future use...")
        uu.s3_file_download(os.path.join(cn.US_forest_age_cat_raw_dir, cn.name_US_forest_age_cat_raw), cn.docker_tile_dir, 'std')

        # Converts the national forest age category raster to Hansen tiles
        source_raster = cn.name_US_forest_age_cat_raw
        out_pattern = cn.pattern_US_forest_age_cat_processed
        dt = 'Int16'
        pool = multiprocessing.Pool(int(cn.count/2))
        pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt),
                 US_tile_id_list)

        uu.upload_final_set(cn.US_forest_age_cat_processed_dir, cn.pattern_US_forest_age_cat_processed)


    # Counts how many processed FIA forest group tiles there are on s3 already. 16 tiles cover the continental US.
    FIA_forest_group_tile_count = uu.count_tiles_s3(cn.FIA_forest_group_processed_dir)

    # Only creates FIA forest group tiles if they don't already exist on s3.
    if FIA_forest_group_tile_count == 16:
        uu.print_log("FIA forest group tiles already created. Copying to spot machine now...")
        uu.s3_flexible_download(cn.FIA_forest_group_processed_dir, cn.pattern_FIA_forest_group_processed, '', 'std', US_tile_id_list)

    else:
        uu.print_log("FIA forest group tiles do not exist. Creating tiles, then copying to s3 for future use...")
        uu.s3_file_download(os.path.join(cn.FIA_forest_group_raw_dir, cn.name_FIA_forest_group_raw), cn.docker_tile_dir, 'std')

        # Converts the national forest group raster to Hansen forest group tiles
        source_raster = cn.name_FIA_forest_group_raw
        out_pattern = cn.pattern_FIA_forest_group_processed
        dt = 'Byte'
        pool = multiprocessing.Pool(int(cn.count/2))
        pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt),
                 US_tile_id_list)

        uu.upload_final_set(cn.FIA_forest_group_processed_dir, cn.pattern_FIA_forest_group_processed)


    # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
    for key, values in download_dict.items():
        dir = key
        pattern = values[0]
        uu.s3_flexible_download(dir, pattern, cn.docker_tile_dir, sensit_type, US_tile_id_list)



    # Table with US-specific removal rates
    # cmd = ['aws', 's3', 'cp', os.path.join(cn.gain_spreadsheet_dir, cn.table_US_removal_rate), cn.docker_base_dir, '--no-sign-request']
    cmd = ['aws', 's3', 'cp', os.path.join(cn.gain_spreadsheet_dir, cn.table_US_removal_rate), cn.docker_tile_dir]

    # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        uu.log_subprocess_output(process.stdout)

    # Imports the table with the region-group-age AGB removal rates
    gain_table = pd.read_excel("{}".format(cn.table_US_removal_rate),
                               sheet_name="US_rates_for_model")

    # Converts removals table from wide to long, so each region-group-age category has its own row
    gain_table_group_region_by_age = pd.melt(gain_table, id_vars=['FIA_region_code', 'forest_group_code'],
                                      value_vars=['growth_young', 'growth_middle',
                                                  'growth_old'])
    gain_table_group_region_by_age = gain_table_group_region_by_age.dropna()

    # In the forest age category raster, each category has this value
    age_dict = {'growth_young': 1000, 'growth_middle': 2000, 'growth_old': 3000}

    # Creates a unique value for each forest group-region-age category in the table.
    # Although these rates are applied to all standard removals model pixels at first, they are not ultimately used for
    # pixels that have Hansen gain (see below).
    gain_table_group_region_age = gain_table_group_region_by_age.replace({"variable": age_dict})
    gain_table_group_region_age['age_cat'] = gain_table_group_region_age['variable']*10
    gain_table_group_region_age['group_region_age_combined'] = gain_table_group_region_age['age_cat'] + \
                                              gain_table_group_region_age['forest_group_code']*100 + \
                                              gain_table_group_region_age['FIA_region_code']
    # Converts the forest group-region-age codes and corresponding removals rates to a dictionary,
    # where the key is the unique group-region-age code and the value is the AGB removal rate.
    gain_table_group_region_age_dict = pd.Series(gain_table_group_region_age.value.values, index=gain_table_group_region_age.group_region_age_combined).to_dict()
    uu.print_log(gain_table_group_region_age_dict)


    # Creates a unique value for each forest group-region category using just young forest rates.
    # These are assigned to Hansen gain pixels, which automatically get the young forest rate, regardless of the
    # forest age category raster.
    gain_table_group_region = gain_table_group_region_age.drop(gain_table_group_region_age[gain_table_group_region_age.age_cat != 10000].index)
    gain_table_group_region['group_region_combined'] = gain_table_group_region['forest_group_code']*100 + \
                                                       gain_table_group_region['FIA_region_code']
    # Converts the forest group-region codes and corresponding removals rates to a dictionary,
    # where the key is the unique group-region code (youngest age category) and the value is the AGB removal rate.
    gain_table_group_region_dict = pd.Series(gain_table_group_region.value.values, index=gain_table_group_region.group_region_combined).to_dict()
    uu.print_log(gain_table_group_region_dict)


    # count/2 on a m4.16xlarge maxes out at about 230 GB of memory (processing 16 tiles at once), so it's okay on an m4.16xlarge
    pool = multiprocessing.Pool(int(cn.count/2))
    pool.map(partial(US_removal_rates.US_removal_rate_calc, gain_table_group_region_age_dict=gain_table_group_region_age_dict,
                     gain_table_group_region_dict=gain_table_group_region_dict,
                     output_pattern_list=output_pattern_list, sensit_type=sensit_type), US_tile_id_list)
    pool.close()
    pool.join()

    # # For single processor use
    # for tile_id in US_tile_id_list:
    #
    #     US_removal_rates.US_removal_rate_calc(tile_id, gain_table_group_region_age_dict, gain_table_group_region_dict,
    #                                           output_pattern_list, sensit_type)


    # Uploads output tiles to s3
    for i in range(0, len(output_dir_list)):
        uu.upload_final_set(output_dir_list[i], output_pattern_list[i])


if __name__ == '__main__':
    main()