import create_carbon_pools_in_emis_year
import constants_and_names as cn
import pandas as pd

# Imports the table with the ecozone-continent codes and the carbon gain rates
gain_table = pd.read_excel("{}".format(cn.gain_spreadsheet),
                           sheet_name = "mangrove gain, for model")

# Removes rows with duplicate codes (N. and S. America for the same ecozone)
gain_table_simplified = gain_table.drop_duplicates(subset='gainEcoCon', keep='first')
mang_deadwood_AGB_ratio = create_carbon_pools_in_emis_year.mangrove_pool_ratio_dict(gain_table_simplified,
                                                                           cn.deadwood_to_above_trop_dry_mang,
                                                                           cn.deadwood_to_above_trop_wet_mang,
                                                                           cn.deadwood_to_above_subtrop_mang)
tile = "00N_110E"
create_carbon_pools_in_emis_year.create_deadwood(tile, mang_deadwood_AGB_ratio)