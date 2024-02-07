'''
This script creates Hansen tiles of US-specific AGC+BGC removal rates and removal rate standard deviations for the standard model.
Its extent is all pixels that have a US region, US age category, and FIA forest group assigned.
Not all US pixels within the model extent have a US region, a US age category, and an FIA forest group pixel.
Those pixels are not included in these tiles; rates from other sources are eventually applied to those pixels.
Moreover, this does not use the model extent map at all, so it produces rates in pixels that may not actually be part
of the final model extent.

The combination of US region, US age category, and FIA forest group is used to assign annual removal rates and
standard deviations from a spreadsheet.
The rates in the spreadsheet were prepared by Rich Birdsey (retired US Forest Service, now at WHRC) using FIA
database queries. The standard deviations were initially prepared by Rich Birdsey, then modified and consolidated
by Nancy Harris (different issues for easter and western US forests and different age categories).
US-specific removal rates are based on the FIA region, FIA forest group, and forest age category of each pixel. A rate
table is then applied to each combination of region-group-age to apply the correct rates, just like for the standard
model (mp_annual_gain_rate_IPCC_defaults.py).
The FIA region tiles are a Hansenized version of a US region map that Thailynn Munroe made.
The FIA forest group raster is created in ArcMap before this processing and Hansenized in the data prep script.
The input forest group raster is basically the composite of the original forest group raster and the
ArcMap Focal Statistics tool applied to it at various rectangular windows, from 3x3 to 400x400.
This Focal Statistics process covers the entire CONUS in forest group characterization so that any model pixel will
be covered by forest group.
The forest age raster is based on Pan et al. and was created by Thailynn Munroe. She used the same Focal Statistics
process for forest age category as I did for forest group. The age category cutoffs are 0-20, 20-100, and >100 years
and are the same for the entire US. All Hansen gain pixels are automatically assigned the young forest rate and
standard deviation, regardless of the Pan et al. forest age category.
The pixels that don't have a rate assigned are ones where Rich Birdsey couldn't get a rate from the FIA database for
that region-group-age combination (such as exotic hardwoods, I believe).
So although almost the entire US is covered by the three input rasters, considerable areas with assigned rates can
occur when the FIA didn't have sufficient data to come up with a rate.

This script creates two dictionaries for rate and two for stdev to apply to the three input tiles: one by
region-group-age combinations and another with the youngest rate for each region-group combination.
The first dictionary is applied to all standard gain model pixels according to their region-group-age combination
but then is overwritten for any Hansen gain pixel with the youngest rate for that region-group combination applied
(using the second dictionary). That is because we can assume that any Hansen gain pixel is in the youngest age category,
i.e. it is more specific information than the Pan et al. forest age category raster, so we give that info priority.
'''

import multiprocessing
from functools import partial
import datetime
import argparse
import pandas as pd
from subprocess import Popen, PIPE, STDOUT, check_call
import os
import sys

import constants_and_names as cn
import universal_util as uu
from . import US_removal_rates

def mp_US_removal_rates(tile_id_list):

    os.chdir(cn.docker_tile_dir)

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        tile_id_list = uu.tile_list_s3(cn.FIA_regions_processed_dir)

    uu.print_log(tile_id_list)
    uu.print_log(f'There are {str(len(tile_id_list))} tiles to process', "\n")

    # Files to download for this script
    download_dict = {cn.gain_dir: [cn.pattern_data_lake],
                     cn.FIA_regions_processed_dir: [cn.pattern_FIA_regions_processed],
                     cn.FIA_forest_group_processed_dir: [cn.pattern_FIA_forest_group_processed],
                     cn.age_cat_natrl_forest_US_dir: [cn.pattern_age_cat_natrl_forest_US]
    }

    # List of output directories and output file name patterns
    output_dir_list = [cn.annual_gain_AGC_BGC_natrl_forest_US_dir, cn.stdev_annual_gain_AGC_BGC_natrl_forest_US_dir]
    output_pattern_list = [cn.pattern_annual_gain_AGC_BGC_natrl_forest_US, cn.pattern_stdev_annual_gain_AGC_BGC_natrl_forest_US]


    # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
    for key, values in download_dict.items():
        dir = key
        pattern = values[0]
        uu.s3_flexible_download(dir, pattern, cn.docker_tile_dir, cn.SENSIT_TYPE, tile_id_list)


    # If the model run isn't the standard one, the output directory and file names are changed
    if cn.SENSIT_TYPE != 'std':
        uu.print_log('Changing output directory and file name pattern based on sensitivity analysis')
        output_dir_list = uu.alter_dirs(cn.SENSIT_TYPE, output_dir_list)
        output_pattern_list = uu.alter_patterns(cn.SENSIT_TYPE, output_pattern_list)

    # A date can optionally be provided by the full model script or a run of this script.
    # This replaces the date in constants_and_names.
    if cn.RUN_DATE is not None:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, cn.RUN_DATE)


    # Table with US-specific removal rates
    # cmd = ['aws', 's3', 'cp', os.path.join(cn.gain_spreadsheet_dir, cn.table_US_removal_rate), cn.docker_base_dir, '--no-sign-request']
    cmd = ['aws', 's3', 'cp', os.path.join(cn.gain_spreadsheet_dir, cn.table_US_removal_rate), cn.docker_tile_dir]
    uu.log_subprocess_output_full(cmd)


    ### To make the removal factor dictionaries

    # Imports the table with the region-group-age AGC+BGC removal rates
    gain_table = pd.read_excel("{}".format(cn.table_US_removal_rate),
                               sheet_name="US_rates_AGC+BGC")

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


    ### To make the removal factor standard deviation dictionaries

    # Converts removals table from wide to long, so each region-group-age category has its own row
    stdev_table_group_region_by_age = pd.melt(gain_table, id_vars=['FIA_region_code', 'forest_group_code'],
                                             value_vars=['SD_young', 'SD_middle',
                                                         'SD_old'])
    stdev_table_group_region_by_age = stdev_table_group_region_by_age.dropna()

    # In the forest age category raster, each category has this value
    stdev_dict = {'SD_young': 1000, 'SD_middle': 2000, 'SD_old': 3000}

    # Creates a unique value for each forest group-region-age category in the table.
    # Although these rates are applied to all standard removals model pixels at first, they are not ultimately used for
    # pixels that have Hansen gain (see below).
    stdev_table_group_region_age = stdev_table_group_region_by_age.replace({"variable": stdev_dict})
    stdev_table_group_region_age['age_cat'] = stdev_table_group_region_age['variable'] * 10
    stdev_table_group_region_age['group_region_age_combined'] = stdev_table_group_region_age['age_cat'] + \
                                                               stdev_table_group_region_age['forest_group_code'] * 100 + \
                                                               stdev_table_group_region_age['FIA_region_code']
    # Converts the forest group-region-age codes and corresponding removals rates to a dictionary,
    # where the key is the unique group-region-age code and the value is the AGB removal rate.
    stdev_table_group_region_age_dict = pd.Series(stdev_table_group_region_age.value.values,
                                                 index=stdev_table_group_region_age.group_region_age_combined).to_dict()
    uu.print_log(stdev_table_group_region_age_dict)

    # Creates a unique value for each forest group-region category using just young forest rates.
    # These are assigned to Hansen gain pixels, which automatically get the young forest rate, regardless of the
    # forest age category raster.
    stdev_table_group_region = stdev_table_group_region_age.drop(
        stdev_table_group_region_age[stdev_table_group_region_age.age_cat != 10000].index)
    stdev_table_group_region['group_region_combined'] = stdev_table_group_region['forest_group_code'] * 100 + \
                                                       stdev_table_group_region['FIA_region_code']
    # Converts the forest group-region codes and corresponding removals rates to a dictionary,
    # where the key is the unique group-region code (youngest age category) and the value is the AGB removal rate.
    stdev_table_group_region_dict = pd.Series(stdev_table_group_region.value.values,
                                             index=stdev_table_group_region.group_region_combined).to_dict()
    uu.print_log(stdev_table_group_region_dict)


    if cn.count == 96:
        processes = 68   # 68 processors (only 16 tiles though) = 310 GB peak
    else:
        processes = 24
    uu.print_log('US natural forest AGC+BGC removal rate max processors=', processes)
    pool = multiprocessing.Pool(processes)
    pool.map(partial(US_removal_rates.US_removal_rate_calc,
                     gain_table_group_region_age_dict=gain_table_group_region_age_dict,
                     gain_table_group_region_dict=gain_table_group_region_dict,
                     stdev_table_group_region_age_dict=stdev_table_group_region_age_dict,
                     stdev_table_group_region_dict=stdev_table_group_region_dict,
                     output_pattern_list=output_pattern_list), tile_id_list)
    pool.close()
    pool.join()

    # # For single processor use
    # for tile_id in tile_id_list:
    #
    #     US_removal_rates.US_removal_rate_calc(tile_id,
    #       gain_table_group_region_age_dict,
    #       gain_table_group_region_dict,
    #       stdev_table_group_region_age_dict,
    #       stdev_table_group_region_dict,
    #       output_pattern_list)


    # Uploads output tiles to s3
    for i in range(0, len(output_dir_list)):
        uu.upload_final_set(output_dir_list[i], output_pattern_list[i])


if __name__ == '__main__':

    # The arguments for what kind of model run is being run (standard conditions or a sensitivity analysis) and
    # the tiles to include
    parser = argparse.ArgumentParser(
        description='Create tiles of removal factors for the US using US rates')
    parser.add_argument('--model-type', '-t', required=True,
                        help=f'{cn.model_type_arg_help}')
    parser.add_argument('--tile_id_list', '-l', required=True,
                        help='List of tile ids to use in the model. Should be of form 00N_110E or 00N_110E,00N_120E or all.')
    parser.add_argument('--run-date', '-d', required=False,
                        help='Date of run. Must be format YYYYMMDD.')
    parser.add_argument('--no-upload', '-nu', action='store_true',
                       help='Disables uploading of outputs to s3')
    args = parser.parse_args()

    # Sets global variables to the command line arguments
    cn.SENSIT_TYPE = args.model_type
    cn.RUN_DATE = args.run_date
    cn.NO_UPLOAD = args.no_upload

    tile_id_list = args.tile_id_list

    # Disables upload to s3 if no AWS credentials are found in environment
    if not uu.check_aws_creds():
        cn.NO_UPLOAD = True

    # Create the output log
    uu.initiate_log(tile_id_list)

    # Checks whether the sensitivity analysis and tile_id_list arguments are valid
    uu.check_sensit_type(cn.SENSIT_TYPE)
    tile_id_list = uu.tile_id_list_check(tile_id_list)

    mp_US_removal_rates(tile_id_list)