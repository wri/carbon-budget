"""
It also creates assigns aboveground removal rate standard deviations for the full model extent according to IPCC Table 4.9 defaults
(in the units of IPCC Table 4.9 (currently tonnes biomass/ha/yr)) to the entire model extent.
The standard deviation tiles are used in the uncertainty analysis.
It requires IPCC Table 4.9, formatted for easy ingestion by pandas.
Essentially, this does some processing of the IPCC removals rate table, then uses it as a dictionary that it applies
to every pixel in every tile.
Each continent-ecozo0ne-forest age category combination gets its own code, which matches the codes in the
processed IPCC table.
The extent of these removal rates is greater than what is ultimately used in the model because it assigns IPCC defaults
everywhere there's a forest age category, continent, and ecozone.
You can think of this as the IPCC default rate that would be applied if no other data were available for that pixel.
The belowground removal rates are purely the aboveground removal rates with the above:below ratio applied to them.

python -m removals.mp_annual_gain_rate_IPCC_defaults -t std -l 00N_000E -nu
python -m removals.mp_annual_gain_rate_IPCC_defaults -t std -l all
"""

import multiprocessing
from functools import partial
import argparse
import pandas as pd
import os
import sys

import constants_and_names as cn
import universal_util as uu
from . import annual_gain_rate_IPCC_defaults

os.chdir(cn.docker_tile_dir)

def mp_annual_gain_rate_IPCC_defaults(tile_id_list):
    """
    :param tile_id_list: list of tile ids to process
    :return: set of tiles with annual removal factors according to IPCC Volume 4 Table 4.9:
        aboveground rate, belowground rate, standard deviation for aboveground rate.
        Units: Mg biomass/ha/yr (including for standard deviation tiles)
    """

    os.chdir(cn.docker_tile_dir)
    pd.options.mode.chained_assignment = None


    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # List of tiles to run in the model
        tile_id_list = uu.tile_list_s3(cn.model_extent_dir, cn.SENSIT_TYPE)

    uu.print_log(tile_id_list)
    uu.print_log(f'There are {str(len(tile_id_list))} tiles to process', "\n")


    # Files to download for this script.
    download_dict = {
        cn.age_cat_IPCC_dir: [cn.pattern_age_cat_IPCC],
        cn.cont_eco_dir: [cn.pattern_cont_eco_processed],
        cn.BGB_AGB_ratio_dir: [cn.pattern_BGB_AGB_ratio]
    }


    # List of output directories and output file name patterns
    output_dir_list = [cn.annual_gain_AGB_IPCC_defaults_dir, cn.annual_gain_BGB_IPCC_defaults_dir, cn.stdev_annual_gain_AGB_IPCC_defaults_dir]
    output_pattern_list = [cn.pattern_annual_gain_AGB_IPCC_defaults, cn.pattern_annual_gain_BGB_IPCC_defaults, cn.pattern_stdev_annual_gain_AGB_IPCC_defaults]


    # If the model run isn't the standard one, the output directory and file names are changed
    if cn.SENSIT_TYPE != 'std':
        uu.print_log('Changing output directory and file name pattern based on sensitivity analysis')
        output_dir_list = uu.alter_dirs(cn.SENSIT_TYPE, output_dir_list)
        output_pattern_list = uu.alter_patterns(cn.SENSIT_TYPE, output_pattern_list)

    # A date can optionally be provided by the full model script or a run of this script.
    # This replaces the date in constants_and_names.
    # Only done if output upload is enabled.
    if cn.RUN_DATE is not None and cn.NO_UPLOAD is not None:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, cn.RUN_DATE)


    # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
    for key, values in download_dict.items():
        directory = key
        pattern = values[0]
        uu.s3_flexible_download(directory, pattern, cn.docker_tile_dir, cn.SENSIT_TYPE, tile_id_list)

    # Table with IPCC Table 4.9 default removals rates
    # cmd = ['aws', 's3', 'cp', os.path.join(cn.gain_spreadsheet_dir, cn.gain_spreadsheet), cn.docker_base_dir, '--no-sign-request']
    cmd = ['aws', 's3', 'cp', os.path.join(cn.gain_spreadsheet_dir, cn.gain_spreadsheet), cn.docker_tile_dir]
    uu.log_subprocess_output_full(cmd)


    ### To make the removal factor dictionaries

    # Special removal rate table for no_primary_gain sensitivity analysis: primary forests and IFLs have removal rate of 0
    if cn.SENSIT_TYPE == 'no_primary_gain':
        # Imports the table with the ecozone-continent codes and the carbon removals rates
        gain_table = pd.read_excel(cn.gain_spreadsheet, sheet_name = "natrl fores gain, no_prim_gain")
        uu.print_log('Using no_primary_gain IPCC default rates for tile creation')

    # All other analyses use the standard removal rates
    else:
        # Imports the table with the ecozone-continent codes and the biomass removals rates
        gain_table = pd.read_excel(cn.gain_spreadsheet, sheet_name = "natrl fores gain, for std model")

    # Removes rows with duplicate codes (N. and S. America for the same ecozone)
    gain_table_simplified = gain_table.drop_duplicates(subset='gainEcoCon', keep='first')

    # Converts removals table from wide to long, so each continent-ecozone-age category has its own row
    gain_table_cont_eco_age = pd.melt(gain_table_simplified, id_vars = ['gainEcoCon'],
                            value_vars = ['growth_primary', 'growth_secondary_greater_20', 'growth_secondary_less_20'])
    gain_table_cont_eco_age = gain_table_cont_eco_age.dropna()

    # Creates a table that has just the continent-ecozone combinations for adding to the dictionary.
    # These will be used whenever there is just a continent-ecozone pixel without a forest age pixel.
    # Assigns removal rate of 0 when there's no age category.
    gain_table_con_eco_only = gain_table_cont_eco_age
    gain_table_con_eco_only = gain_table_con_eco_only.drop_duplicates(subset='gainEcoCon', keep='first')
    gain_table_con_eco_only['value'] = 0
    gain_table_con_eco_only['cont_eco_age'] = gain_table_con_eco_only['gainEcoCon']

    # Creates a code for each age category so that each continent-ecozone-age combo can have its own unique value
    rate_age_dict = {'growth_secondary_less_20': 10000, 'growth_secondary_greater_20': 20000, 'growth_primary': 30000}

    # Creates a unique value for each continent-ecozone-age category
    gain_table_cont_eco_age = gain_table_cont_eco_age.replace({"variable": rate_age_dict})
    gain_table_cont_eco_age['cont_eco_age'] = gain_table_cont_eco_age['gainEcoCon'] + gain_table_cont_eco_age['variable']

    # Merges the table of just continent-ecozone codes and the table of  continent-ecozone-age codes
    gain_table_all_combos = pd.concat([gain_table_con_eco_only, gain_table_cont_eco_age])

    # Converts the continent-ecozone-age codes and corresponding removals rates to a dictionary
    gain_table_dict = pd.Series(gain_table_all_combos.value.values,index=gain_table_all_combos.cont_eco_age).to_dict()

    # Adds a dictionary entry for where the ecozone-continent-age code is 0 (not in a continent)
    gain_table_dict[0] = 0

    # Adds a dictionary entry for each forest age code for pixels that have forest age but no continent-ecozone
    for key, value in rate_age_dict.items():

        gain_table_dict[value] = 0

    # Converts all the keys (continent-ecozone-age codes) to float type
    gain_table_dict = {float(key): value for key, value in gain_table_dict.items()}


    ### To make the removal factor standard deviation dictionary

    # Special removal rate table for no_primary_gain sensitivity analysis: primary forests and IFLs have removal rate of 0
    if cn.SENSIT_TYPE == 'no_primary_gain':
        # Imports the table with the ecozone-continent codes and the carbon removals rates
        stdev_table = pd.read_excel(cn.gain_spreadsheet, sheet_name="natrl fores stdv, no_prim_gain")
        uu.print_log('Using no_primary_gain IPCC default standard deviations for tile creation')

    # All other analyses use the standard removal rates
    else:
        # Imports the table with the ecozone-continent codes and the biomass removals rate standard deviations
        stdev_table = pd.read_excel(cn.gain_spreadsheet, sheet_name="natrl fores stdv, for std model")

    # Removes rows with duplicate codes (N. and S. America for the same ecozone)
    stdev_table_simplified = stdev_table.drop_duplicates(subset='gainEcoCon', keep='first')

    # Converts removals table from wide to long, so each continent-ecozone-age category has its own row
    stdev_table_cont_eco_age = pd.melt(stdev_table_simplified, id_vars = ['gainEcoCon'], value_vars = ['stdev_primary', 'stdev_secondary_greater_20', 'stdev_secondary_less_20'])
    stdev_table_cont_eco_age = stdev_table_cont_eco_age.dropna()

    # Creates a table that has just the continent-ecozone combinations for adding to the dictionary.
    # These will be used whenever there is just a continent-ecozone pixel without a forest age pixel.
    # Assigns removal rate of 0 when there's no age category.
    stdev_table_con_eco_only = stdev_table_cont_eco_age
    stdev_table_con_eco_only = stdev_table_con_eco_only.drop_duplicates(subset='gainEcoCon', keep='first')
    stdev_table_con_eco_only['value'] = 0
    stdev_table_con_eco_only['cont_eco_age'] = stdev_table_con_eco_only['gainEcoCon']

    # Creates a code for each age category so that each continent-ecozone-age combo can have its own unique value
    stdev_age_dict = {'stdev_secondary_less_20': 10000, 'stdev_secondary_greater_20': 20000, 'stdev_primary': 30000}


    # Creates a unique value for each continent-ecozone-age category
    stdev_table_cont_eco_age = stdev_table_cont_eco_age.replace({"variable": stdev_age_dict})
    stdev_table_cont_eco_age['cont_eco_age'] = stdev_table_cont_eco_age['gainEcoCon'] + stdev_table_cont_eco_age['variable']

    # Merges the table of just continent-ecozone codes and the table of  continent-ecozone-age codes
    stdev_table_all_combos = pd.concat([stdev_table_con_eco_only, stdev_table_cont_eco_age])

    # Converts the continent-ecozone-age codes and corresponding removals rates to a dictionary
    stdev_table_dict = pd.Series(stdev_table_all_combos.value.values,index=stdev_table_all_combos.cont_eco_age).to_dict()

    # Adds a dictionary entry for where the ecozone-continent-age code is 0 (not in a continent)
    stdev_table_dict[0] = 0

    # Adds a dictionary entry for each forest age code for pixels that have forest age but no continent-ecozone
    for key, value in stdev_age_dict.items():

        stdev_table_dict[value] = 0

    # Converts all the keys (continent-ecozone-age codes) to float type
    stdev_table_dict = {float(key): value for key, value in stdev_table_dict.items()}

    if cn.SINGLE_PROCESSOR:
        for tile_id in tile_id_list:
            annual_gain_rate_IPCC_defaults.annual_gain_rate(tile_id, gain_table_dict, stdev_table_dict, output_pattern_list)

    # This configuration of the multiprocessing call is necessary for passing multiple arguments to the main function
    # It is based on the example here: http://spencerimp.blogspot.com/2015/12/python-multiprocess-with-multiple.html
    if cn.count == 96:
        if cn.SENSIT_TYPE == 'biomass_swap':
            processes = 24 # 24 processors = 590 GB peak
        else:
            processes = 23  # 30 processors>=740 GB peak; 25>=740 GB peak (too high); 20>=740 GB peak (risky);
            # 16>=740 GB peak; 14=420 GB peak; 17=520 GB peak; 20=610 GB peak; 23=XXX GB peak
    else:
        processes = 2
    uu.print_log(f'Annual removals rate natural forest max processors={processes}')
    with multiprocessing.Pool(processes) as pool:
        pool.map(partial(annual_gain_rate_IPCC_defaults.annual_gain_rate,
                         gain_table_dict=gain_table_dict, stdev_table_dict=stdev_table_dict,
                         output_pattern_list=output_pattern_list),
                 tile_id_list)
        pool.close()
        pool.join()


    # If no_upload flag is not activated (by choice or by lack of AWS credentials), output is uploaded
    if not cn.NO_UPLOAD:
        for output_dir, output_pattern in zip(output_dir_list, output_pattern_list):
            uu.upload_final_set(output_dir, output_pattern)


if __name__ == '__main__':

    # The arguments for what kind of model run is being run (standard conditions or a sensitivity analysis) and
    # the tiles to include
    parser = argparse.ArgumentParser(
        description='Create tiles of removal factors according to IPCC defaults')
    parser.add_argument('--model-type', '-t', required=True,
                        help=f'{cn.model_type_arg_help}')
    parser.add_argument('--tile_id_list', '-l', required=True,
                        help='List of tile ids to use in the model. Should be of form 00N_110E or 00N_110E,00N_120E or all.')
    parser.add_argument('--run-date', '-d', required=False,
                        help='Date of run. Must be format YYYYMMDD.')
    parser.add_argument('--no-upload', '-nu', action='store_true',
                       help='Disables uploading of outputs to s3')
    parser.add_argument('--single-processor', '-sp', action='store_true',
                       help='Uses single processing rather than multiprocessing')
    args = parser.parse_args()


    # Sets global variables to the command line arguments
    cn.SENSIT_TYPE = args.model_type
    cn.RUN_DATE = args.run_date
    cn.NO_UPLOAD = args.no_upload
    cn.SINGLE_PROCESSOR = args.single_processor

    tile_id_list = args.tile_id_list

    # Disables upload to s3 if no AWS credentials are found in environment
    if not uu.check_aws_creds():
        cn.NO_UPLOAD = True

    # Create the output log
    uu.initiate_log(tile_id_list)

    # Checks whether the sensitivity analysis and tile_id_list arguments are valid
    uu.check_sensit_type(cn.SENSIT_TYPE)
    tile_id_list = uu.tile_id_list_check(tile_id_list)

    mp_annual_gain_rate_IPCC_defaults(tile_id_list)
