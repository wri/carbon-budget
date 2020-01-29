### Creates tiles of annual aboveground biomass gain rates for mangroves using IPCC Wetlands Supplement Table 4.4 rates.
### Its inputs are the continent-ecozone tiles, mangrove biomass tiles (for locations of mangroves), and the IPCC
### gain rate table.

import multiprocessing
from functools import partial
import annual_gain_rate_mangrove
import argparse
import pandas as pd
import subprocess
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def main (sensit_type):

    pd.options.mode.chained_assignment = None

    download_dict = {
        cn.cont_eco_dir: [cn.pattern_cont_eco_processed],
        cn.mangrove_biomass_2000_dir: [cn.pattern_mangrove_biomass_2000],
        cn.plant_pre_2000_processed_dir: [cn.pattern_plant_pre_2000]
    }


    # Lists the tiles that have both mangrove biomass and FAO ecozone information because both of these are necessary for
    # calculating mangrove gain
    mangrove_biomass_tile_list = uu.tile_list_s3(cn.mangrove_biomass_2000_dir)
    ecozone_tile_list = uu.tile_list_s3(cn.cont_eco_dir)
    tile_id_list = list(set(mangrove_biomass_tile_list).intersection(ecozone_tile_list))
    # tile_id_list = ['10N_080W', '00N_110E'] # test tiles
    # tile_id_list = ['00N_110E'] # test tiles
    print tile_id_list
    print "There are {} tiles to process".format(str(len(tile_id_list))) + "\n"


    # List of output directories and output file name patterns
    output_dir_list = [cn.annual_gain_AGB_mangrove_dir, cn.annual_gain_BGB_mangrove_dir]
    output_pattern_list = [cn.pattern_annual_gain_AGB_mangrove, cn.pattern_annual_gain_BGB_mangrove]


    # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
    for key, values in download_dict.iteritems():
        dir = key
        pattern = values[0]
        uu.s3_flexible_download(dir, pattern, '.', sensit_type, tile_id_list)


    # Table with IPCC Wetland Supplement Table 4.4 default mangrove gain rates
    cmd = ['aws', 's3', 'cp', os.path.join(cn.gain_spreadsheet_dir, cn.gain_spreadsheet), '.']
    subprocess.check_call(cmd)

    # Imports the table with the ecozone-continent codes and the carbon gain rates
    gain_table = pd.read_excel("{}".format(cn.gain_spreadsheet),
                               sheet_name = "mangrove gain, for model")

    # Removes rows with duplicate codes (N. and S. America for the same ecozone)
    gain_table_simplified = gain_table.drop_duplicates(subset='gainEcoCon', keep='first')

    # Creates belowground:aboveground biomass ratio dictionary for the three mangrove types, where the keys correspond to
    # the "mangType" field in the gain rate spreadsheet.
    # If the assignment of mangTypes to ecozones changes, that column in the spreadsheet may need to change and the
    # keys in this dictionary would need to change accordingly.
    type_ratio_dict = {'1': cn.below_to_above_trop_dry_mang, '2'  :cn.below_to_above_trop_wet_mang, '3': cn.below_to_above_subtrop_mang}
    type_ratio_dict_final = {int(k):float(v) for k,v in type_ratio_dict.items()}

    # Applies the belowground:aboveground biomass ratios for the three mangrove types to the annual aboveground gain rates to get
    # a column of belowground annual gain rates by mangrove type
    gain_table_simplified['BGB_AGB_ratio'] = gain_table_simplified['mangType'].map(type_ratio_dict_final)
    gain_table_simplified['BGB_annual_rate'] = gain_table_simplified.AGB_gain_tons_yr * gain_table_simplified.BGB_AGB_ratio

    # Converts the continent-ecozone codes and corresponding gain rates to dictionaries for aboveground and belowground gain rates
    gain_above_dict = pd.Series(gain_table_simplified.AGB_gain_tons_yr.values,index=gain_table_simplified.gainEcoCon).to_dict()
    gain_below_dict = pd.Series(gain_table_simplified.BGB_annual_rate.values,index=gain_table_simplified.gainEcoCon).to_dict()

    # Adds a dictionary entry for where the ecozone-continent code is 0 (not in a continent)
    gain_above_dict[0] = 0
    gain_below_dict[0] = 0

    # Converts all the keys (continent-ecozone codes) to float type
    gain_above_dict = {float(key): value for key, value in gain_above_dict.iteritems()}
    gain_below_dict = {float(key): value for key, value in gain_below_dict.iteritems()}

    # This configuration of the multiprocessing call is necessary for passing multiple arguments to the main function
    # It is based on the example here: http://spencerimp.blogspot.com/2015/12/python-multiprocess-with-multiple.html
    # Ran with 18 processors on r4.16xlarge (430 GB memory peak)
    count = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=18)
    pool.map(partial(annual_gain_rate_mangrove.annual_gain_rate, sensit_type=sensit_type, output_pattern_list=output_pattern_list,
                     gain_above_dict=gain_above_dict, gain_below_dict=gain_below_dict), tile_id_list)
    pool.close()
    pool.join()

    # # For single processor use
    # for tile in tile_id_list:
    #
    #     annual_gain_rate_mangrove.annual_gain_rate(tile, sensit_type, output_pattern_list,
    #           gain_above_dict, gain_below_dict)


    for i in range(0, len(output_dir_list)):
        uu.upload_final_set(output_dir_list[i], output_pattern_list[i])


if __name__ == '__main__':

    # The argument for what kind of model run is being done: standard conditions or a sensitivity analysis run
    parser = argparse.ArgumentParser(description='Create tiles of the number of years of carbon gain for mangrove forests')
    parser.add_argument('--model-type', '-t', required=True,
                        help='{}'.format(cn.model_type_arg_help))
    args = parser.parse_args()
    sensit_type = args.model_type
    # Checks whether the sensitivity analysis argument is valid
    uu.check_sensit_type(sensit_type)