### This script assigns annual above and belowground non-mangrove, non-planted forestbiomass gain rates
### (in the units of IPCC Table 4.9 (currently tonnes biomass/ha/yr)) to non-mangrove natural forest pixels.
### It requires IPCC Table 4.9, formatted for easy ingestion by pandas.
### Essentially, this does some processing of the IPCC gain rate table, then uses it as a dictionary that it applies
### to every pixel in every tile.
### Each continent-ecozone-forest age category combination gets its own code, which matches the codes in the
### processed IPCC table.
### Belowground biomass gain rate is a constant proportion of aboveground biomass gain rate, again according to IPCC tables.

import multiprocessing
from functools import partial
import argparse
import pandas as pd
import datetime
import subprocess
import os
import sys
sys.path.append('/usr/local/app/gain/')
import annual_gain_rate_natrl_forest
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

os.chdir(cn.docker_base_dir)

def mp_annual_gain_rate_natrl_forest(sensit_type, tile_id_list, run_date = None):

    os.chdir(cn.docker_base_dir)
    pd.options.mode.chained_assignment = None


    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # List of tiles to run in the model
        tile_id_list = uu.tile_list_s3(cn.WHRC_biomass_2000_non_mang_non_planted_dir, sensit_type)

    uu.print_log(tile_id_list)
    uu.print_log("There are {} tiles to process".format(str(len(tile_id_list))) + "\n")


    # Files to download for this script.
    download_dict = {
        cn.age_cat_natrl_forest_dir: [cn.pattern_age_cat_natrl_forest],
        cn.cont_eco_dir: [cn.pattern_cont_eco_processed],
        cn.plant_pre_2000_processed_dir: [cn.pattern_plant_pre_2000]
    }


    # List of output directories and output file name patterns
    output_dir_list = [cn.annual_gain_AGB_natrl_forest_dir, cn.annual_gain_BGB_natrl_forest_dir]
    output_pattern_list = [cn.pattern_annual_gain_AGB_natrl_forest, cn.pattern_annual_gain_BGB_natrl_forest]


    # If the model run isn't the standard one, the output directory and file names are changed
    if sensit_type != 'std':
        uu.print_log("Changing output directory and file name pattern based on sensitivity analysis")
        output_dir_list = uu.alter_dirs(sensit_type, output_dir_list)
        output_pattern_list = uu.alter_patterns(sensit_type, output_pattern_list)

    # A date can optionally be provided by the full model script or a run of this script.
    # This replaces the date in constants_and_names.
    if run_date is not None:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, run_date)


    # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
    for key, values in download_dict.items():
        dir = key
        pattern = values[0]
        uu.s3_flexible_download(dir, pattern, cn.docker_base_dir, sensit_type, tile_id_list)


    # Table with IPCC Table 4.9 default gain rates
    cmd = ['aws', 's3', 'cp', os.path.join(cn.gain_spreadsheet_dir, cn.gain_spreadsheet), cn.docker_base_dir]
    subprocess.check_call(cmd)

    # Special removal rate table for no_primary_gain sensitivity analysis: primary forests and IFLs have removal rate of 0
    if sensit_type == 'no_primary_gain':
        # Imports the table with the ecozone-continent codes and the carbon gain rates
        gain_table = pd.read_excel("{}".format(cn.gain_spreadsheet),
                                   sheet_name = "natrl fores gain, no_prim_gain")

    # All other analyses use the standard removal rates (except US_removals, which has its own script)
    else:
        # Imports the table with the ecozone-continent codes and the carbon gain rates
        gain_table = pd.read_excel("{}".format(cn.gain_spreadsheet),
                                   sheet_name = "natrl fores gain, for std model")

    # Removes rows with duplicate codes (N. and S. America for the same ecozone)
    gain_table_simplified = gain_table.drop_duplicates(subset='gainEcoCon', keep='first')

    # Converts gain table from wide to long, so each continent-ecozone-age category has its own row
    gain_table_cont_eco_age = pd.melt(gain_table_simplified, id_vars = ['gainEcoCon'], value_vars = ['growth_primary', 'growth_secondary_greater_20', 'growth_secondary_less_20'])
    gain_table_cont_eco_age = gain_table_cont_eco_age.dropna()

    # Creates a table that has just the continent-ecozone combinations for adding to the dictionary.
    # These will be used whenever there is just a continent-ecozone pixel without a forest age pixel.
    # Assigns removal rate of 0 when there's no age category.
    gain_table_con_eco_only = gain_table_cont_eco_age
    gain_table_con_eco_only = gain_table_con_eco_only.drop_duplicates(subset='gainEcoCon', keep='first')
    gain_table_con_eco_only['value'] = 0
    gain_table_con_eco_only['cont_eco_age'] = gain_table_con_eco_only['gainEcoCon']

    # Creates a code for each age category so that each continent-ecozone-age combo can have its own unique value
    age_dict = {'growth_primary': 10000, 'growth_secondary_greater_20': 20000, 'growth_secondary_less_20': 30000}

    # Creates a unique value for each continent-ecozone-age category
    gain_table_cont_eco_age = gain_table_cont_eco_age.replace({"variable": age_dict})
    gain_table_cont_eco_age['cont_eco_age'] = gain_table_cont_eco_age['gainEcoCon'] + gain_table_cont_eco_age['variable']

    # Merges the table of just continent-ecozone codes and the table of  continent-ecozone-age codes
    gain_table_all_combos = pd.concat([gain_table_con_eco_only, gain_table_cont_eco_age])

    # Converts the continent-ecozone-age codes and corresponding gain rates to a dictionary
    gain_table_dict = pd.Series(gain_table_all_combos.value.values,index=gain_table_all_combos.cont_eco_age).to_dict()

    # Adds a dictionary entry for where the ecozone-continent-age code is 0 (not in a continent)
    gain_table_dict[0] = 0

    # Adds a dictionary entry for each forest age code for pixels that have forest age but no continent-ecozone
    for key, value in age_dict.items():

        gain_table_dict[value] = 0

    # Converts all the keys (continent-ecozone-age codes) to float type
    gain_table_dict = {float(key): value for key, value in gain_table_dict.items()}


    # This configuration of the multiprocessing call is necessary for passing multiple arguments to the main function
    # It is based on the example here: http://spencerimp.blogspot.com/2015/12/python-multiprocess-with-multiple.html
    # processes=24 peaks at about 440 GB of memory on an r4.16xlarge machine
    pool = multiprocessing.Pool(processes=24)
    pool.map(partial(annual_gain_rate_natrl_forest.annual_gain_rate, sensit_type=sensit_type, gain_table_dict=gain_table_dict,
                     output_pattern_list=output_pattern_list), tile_id_list)
    pool.close()
    pool.join()

    # # For single processor use
    # for tile_id in tile_id_list:
    #
    #     annual_gain_rate_natrl_forest.annual_gain_rate(tile_id, sensit_type, gain_table_dict, output_pattern_list)


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
    script_start = datetime.datetime.now()
    uu.initiate_log(script_start)

    # Checks whether the sensitivity analysis and tile_id_list arguments are valid
    uu.check_sensit_type(sensit_type)
    tile_id_list = uu.tile_id_list_check(tile_id_list)

    mp_annual_gain_rate_natrl_forest(sensit_type=sensit_type, tile_id_list=tile_id_list, run_date=run_date)