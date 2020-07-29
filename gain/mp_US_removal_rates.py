'''
This script creates Hansen tiles of US-specific AGB and BGB removal rates for non-mangrove, non-planted forests for the
US removal rate sensitivity analysis. The extent of the output is the same as the non-mangrove, non-planted forests
for the standard model. These rates are then combined with mangrove and non-mangrove planted forest
rates from the standard model in mp_merge_cumulative_annual_gain_all_forest_types.py and the model then run like for
any other sensitivity analysis.
US-specific removal rates are based on the FIA region, FIA forest group, and forest age category of each pixel. A rate
table is then applied to each combination of region-group-age to apply the correct rates, just like for the standard
model (mp_annual_gain_rate_natrl_forest.py).
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
The first dictionary is applied to all standard gain model pixels according to their region-group-age combination
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
import argparse
import US_removal_rates
import pandas as pd
from subprocess import Popen, PIPE, STDOUT, check_call
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def mp_US_removal_rates(sensit_type, tile_id_list, run_date):

    os.chdir(cn.docker_base_dir)

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        tile_id_list = uu.tile_list_s3(cn.FIA_regions_processed_dir)

    uu.print_log(tile_id_list)
    uu.print_log("There are {} tiles to process".format(str(len(tile_id_list))) + "\n")

    # Files to download for this script
    download_dict = {cn.gain_dir: [cn.pattern_gain],
                     cn.WHRC_biomass_2000_unmasked_dir: [cn.pattern_WHRC_biomass_2000_unmasked], # used as template/kwargs source for output tiles
                     cn.FIA_regions_processed_dir: [cn.pattern_FIA_regions_processed],
                     cn.FIA_forest_group_processed_dir: [cn.pattern_FIA_forest_group_processed],
                     cn.age_cat_natrl_forest_US_dir: [cn.pattern_age_cat_natrl_forest_US]
    }

    # List of output directories and output file name patterns
    output_dir_list = [cn.annual_gain_AGC_BGC_natrl_forest_US_dir]
    output_pattern_list = [cn.pattern_annual_gain_AGC_BGC_natrl_forest_US]

    # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
    for key, values in download_dict.items():
        dir = key
        pattern = values[0]
        uu.s3_flexible_download(dir, pattern, cn.docker_base_dir, sensit_type, tile_id_list)

    # If the model run isn't the standard one, the output directory and file names are changed
    if sensit_type != 'std':
        uu.print_log("Changing output directory and file name pattern based on sensitivity analysis")
        output_dir_list = uu.alter_dirs(sensit_type, output_dir_list)
        output_pattern_list = uu.alter_patterns(sensit_type, output_pattern_list)

    # A date can optionally be provided by the full model script or a run of this script.
    # This replaces the date in constants_and_names.
    if run_date is not None:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, run_date)




    # Table with US-specific removal rates
    cmd = ['aws', 's3', 'cp', os.path.join(cn.gain_spreadsheet_dir, cn.table_US_removal_rate), cn.docker_base_dir]
    uu.log_subprocess_output_full(cmd)


    # Imports the table with the region-group-age AGB removal rates
    gain_table = pd.read_excel("{}".format(cn.table_US_removal_rate),
                               sheet_name="US_rates_AGC+BGC")

    # Converts gain table from wide to long, so each region-group-age category has its own row
    gain_table_group_region_by_age = pd.melt(gain_table, id_vars=['FIA_region_code', 'forest_group_code'],
                                      value_vars=['growth_young', 'growth_middle',
                                                  'growth_old'])
    gain_table_group_region_by_age = gain_table_group_region_by_age.dropna()

    # In the forest age category raster, each category has this value
    age_dict = {'growth_young': 1000, 'growth_middle': 2000, 'growth_old': 3000}

    # Creates a unique value for each forest group-region-age category in the table.
    # Although these rates are applied to all standard gain model pixels at first, they are not ultimately used for
    # pixels that have Hansen gain (see below).
    gain_table_group_region_age = gain_table_group_region_by_age.replace({"variable": age_dict})
    gain_table_group_region_age['age_cat'] = gain_table_group_region_age['variable']*10
    gain_table_group_region_age['group_region_age_combined'] = gain_table_group_region_age['age_cat'] + \
                                              gain_table_group_region_age['forest_group_code']*100 + \
                                              gain_table_group_region_age['FIA_region_code']
    # Converts the forest group-region-age codes and corresponding gain rates to a dictionary,
    # where the key is the unique group-region-age code and the value is the AGB removal rate.
    gain_table_group_region_age_dict = pd.Series(gain_table_group_region_age.value.values, index=gain_table_group_region_age.group_region_age_combined).to_dict()
    uu.print_log(gain_table_group_region_age_dict)


    # Creates a unique value for each forest group-region category using just young forest rates.
    # These are assigned to Hansen gain pixels, which automatically get the young forest rate, regardless of the
    # forest age category raster.
    gain_table_group_region = gain_table_group_region_age.drop(gain_table_group_region_age[gain_table_group_region_age.age_cat != 10000].index)
    gain_table_group_region['group_region_combined'] = gain_table_group_region['forest_group_code']*100 + \
                                                       gain_table_group_region['FIA_region_code']
    # Converts the forest group-region codes and corresponding gain rates to a dictionary,
    # where the key is the unique group-region code (youngest age category) and the value is the AGB removal rate.
    gain_table_group_region_dict = pd.Series(gain_table_group_region.value.values, index=gain_table_group_region.group_region_combined).to_dict()
    uu.print_log(gain_table_group_region_dict)


    # count/2 on a m4.16xlarge maxes out at about 230 GB of memory (processing 16 tiles at once), so it's okay on an m4.16xlarge
    if cn.count == 96:
        processes = 68   # 36 processors = 200 GB peak; 54 = 260 GB peak; 62 = 290 GB peak; 68 = XXX GB peak
    else:
        processes = 24
    uu.print_log('US natural forest AGC+BGC removal rate max processors=', processes)
    # pool = multiprocessing.Pool(processes)
    # pool.map(partial(US_removal_rates.US_removal_rate_calc, gain_table_group_region_age_dict=gain_table_group_region_age_dict,
    #                  gain_table_group_region_dict=gain_table_group_region_dict,
    #                  output_pattern_list=output_pattern_list), tile_id_list)
    # pool.close()
    # pool.join()

    # For single processor use
    for tile_id in tile_id_list:

        US_removal_rates.US_removal_rate_calc(tile_id, gain_table_group_region_age_dict, gain_table_group_region_dict,
                                              output_pattern_list)


    # Uploads output tiles to s3
    for i in range(0, len(output_dir_list)):
        uu.upload_final_set(output_dir_list[i], output_pattern_list[i])


if __name__ == '__main__':

    # The arguments for what kind of model run is being run (standard conditions or a sensitivity analysis) and
    # the tiles to include
    parser = argparse.ArgumentParser(
        description='Create tiles of the annual AGB and BGB gain rates for mangrove forests')
    parser.add_argument('--model-type', '-t', required=True,
                        help='{}'.format(cn.model_type_arg_help))
    parser.add_argument('--tile_id_list', '-l', required=True,
                        help='List of tile ids to use in the model. Should be of form 00N_110E or 00N_110E,00N_120E or all.')
    parser.add_argument('--run-date', '-d', required=False,
                        help='Date of run. Must be format YYYYMMDD.')
    args = parser.parse_args()
    sensit_type = args.model_type
    tile_id_list = args.tile_id_list
    run_date = args.run_date

    # Create the output log
    uu.initiate_log(tile_id_list=tile_id_list, sensit_type=sensit_type, run_date=run_date)

    # Checks whether the sensitivity analysis and tile_id_list arguments are valid
    uu.check_sensit_type(sensit_type)
    tile_id_list = uu.tile_id_list_check(tile_id_list)

    mp_US_removal_rates(sensit_type=sensit_type, tile_id_list=tile_id_list, run_date=run_date)