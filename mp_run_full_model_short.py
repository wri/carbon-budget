import multiprocessing
from functools import partial
import glob
import argparse
import pandas as pd
import subprocess
import os
import constants_and_names as cn
import universal_util as uu
import sys
# sys.path.append('../gain')
from gain.mp_forest_age_category_natrl_forest import mp_forest_age_category_natrl_forest
# import mp_gain_year_count_natrl_forest
# import mp_annual_gain_rate_natrl_forest
# import mp_cumulative_gain_natrl_forest
# import mp_merge_cumulative_annual_gain_all_forest_types
# sys.path.append('../carbon_pools')
# import mp_create_carbon_pools
# sys.path.append('../emissions')
# import mp_calculate_gross_emissions

def main ():

    model_stages = ['all', 'forest_age_category_natrl_forest', 'gain_year_count_natrl_forest',
                    'annual_gain_rate_natrl_forest', 'cumulative_gain_natrl_forest',
                     'removals_merged', 'carbon_pools', 'gross_emissions']


    # The argument for what kind of model run is being done: standard conditions or a sensitivity analysis run
    parser = argparse.ArgumentParser(description='Create tiles of the number of years of carbon gain for mangrove forests')
    parser.add_argument('--model-type', '-t', required=True, help='{}'.format(cn.model_type_arg_help))
    parser.add_argument('--stages', '-s', required=True,
                        help='Stages of creating Brazil legal Amazon-specific gross cumulative removals. Options are {}'.format(model_stages))
    parser.add_argument('--run_through', '-r', required=True,
                        help='Options: true or false. true: run named stage and following stages. false: run only named stage.')
    parser.add_argument('--tile_id_list', '-l', required=False,
                        help='Optional list of tile ids to use in the model, best used for testing. Should be of form 00N_110E')
    parser.add_argument('--carbon-pool-extent', '-ce', required=False,
                        help='Extent over which carbon pools should be calculated: loss or 2000')
    parser.add_argument('--pools-to-use', '-p', required=True,
                        help='Options are soil_only or biomass_soil. Former only considers emissions from soil. Latter considers emissions from biomass and soil.')
    args = parser.parse_args()
    sensit_type = args.model_type
    stage_input = args.stages
    run_through = args.run_through
    carbon_pool_extent = args.carbon_pool_extent
    pools = args.pools_to_use
    tile_id_list = args.tile_id_list


    # Checks whether the sensitivity analysis argument is valid
    uu.check_sensit_type(sensit_type)


    # Checks the validity of the model stage arguments. If either one is invalid, the script ends.
    if (stage_input not in model_stages):
        raise Exception('Invalid stage selection. Please provide a stage from {}.'.format(model_stages))
    else:
        pass
    if (run_through not in ['true', 'false']):
        raise Exception('Invalid run through option. Please enter true or false.')
    else:
        pass


    # Generates the list of stages to run
    actual_stages = uu.analysis_stages(model_stages, stage_input, run_through)
    print actual_stages


    if tile_id_list:
        print tile_id_list
        print "{} tiles have been supplied for running through the model".format(str(len(tile_id_list))) + "\n"
    else:
        tile_id_list = 'all'


    # List of output directories and output file name patterns
    raw_output_dir_list = [
                       cn.age_cat_natrl_forest_dir, cn.gain_year_count_natrl_forest_dir,
                       cn.annual_gain_AGB_natrl_forest_dir, cn.annual_gain_BGB_natrl_forest_dir,
                       cn.cumul_gain_AGCO2_natrl_forest_dir, cn.cumul_gain_BGCO2_natrl_forest_dir,
                       cn.annual_gain_AGB_BGB_all_types_dir, cn.cumul_gain_AGCO2_BGCO2_all_types_dir,
                       cn.AGC_emis_year_dir, cn.BGC_emis_year_dir, cn.deadwood_emis_year_2000_dir,
                       cn.litter_emis_year_2000_dir, cn.soil_C_emis_year_2000_dir, cn.total_C_emis_year_dir
                       ]

    raw_output_pattern_list = [
                           cn.pattern_age_cat_natrl_forest, cn.pattern_gain_year_count_natrl_forest,
                           cn.pattern_annual_gain_AGB_natrl_forest, cn.pattern_annual_gain_BGB_natrl_forest,
                           cn.pattern_cumul_gain_AGCO2_natrl_forest, cn.pattern_cumul_gain_BGCO2_natrl_forest,
                           cn.pattern_annual_gain_AGB_BGB_all_types, cn.pattern_cumul_gain_AGCO2_BGCO2_all_types,
                           cn.pattern_AGC_emis_year, cn.pattern_BGC_emis_year, cn.pattern_deadwood_emis_year_2000,
                           cn.pattern_litter_emis_year_2000, cn.pattern_soil_C_emis_year_2000, cn.pattern_total_C_emis_year
                           ]


    # If the model run isn't the standard one, the output directory and file names are changed.
    # Otherwise, they stay standard.
    if sensit_type != 'std':
        print "Changing output directory and file name pattern based on sensitivity analysis"
        master_output_dir_list = uu.alter_dirs(sensit_type, raw_output_dir_list)
        master_output_pattern_list = uu.alter_patterns(sensit_type, raw_output_pattern_list)
    else:
        master_output_dir_list = raw_output_dir_list
        master_output_pattern_list = raw_output_pattern_list

    count = multiprocessing.cpu_count()


    # Creates forest age category tiles
    if 'forest_age_category_natrl_forest' in actual_stages:

        print 'Creating forest age category tiles'

        mp_forest_age_category_natrl_forest(sensit_type, tile_id_list)


    # Creates tiles of the number of years of removals
    if 'gain_year_count_natrl_forest' in actual_stages:

        print 'Creating gain year count tiles for natural forest'

        # Files to download for this script
        download_dict = {
            cn.loss_dir: [''],
            cn.gain_dir: [cn.pattern_gain],
            cn.tcd_dir: [cn.pattern_tcd],
            cn.WHRC_biomass_2000_non_mang_non_planted_dir: [cn.pattern_WHRC_biomass_2000_non_mang_non_planted],
            cn.planted_forest_type_unmasked_dir: [cn.pattern_planted_forest_type_unmasked],
            cn.mangrove_biomass_2000_dir: [cn.pattern_mangrove_biomass_2000]
        }


        if tile_id_list:
            print tile_id_list
            print "There are {} tiles to process".format(str(len(tile_id_list))) + "\n"
        else:
            tile_id_list = uu.tile_list_s3(cn.WHRC_biomass_2000_non_mang_non_planted_dir, sensit_type)
            # tile_id_list = ['00N_050W']
            print tile_id_list
            print "There are {} tiles to process".format(str(len(tile_id_list))) + "\n"


        # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
        for key, values in download_dict.iteritems():
            dir = key
            pattern = values[0]
            uu.s3_flexible_download(dir, pattern, '.', sensit_type, tile_id_list)


        pattern = master_output_pattern_list[1]

        # Creates gain year count tiles using only pixels that had only loss
        # count/3 uses about 220 GB on an r4.16xlarge machine
        # count/2 uses about 330 GB on an r4.16xlarge machine
        count = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(processes=36)
        pool.map(partial(gain_year_count_natrl_forest.create_gain_year_count_loss_only, sensit_type=sensit_type),
                 tile_id_list)

        # processes=36 maxes out at about 200 GB
        pool = multiprocessing.Pool(processes=36)
        if sensit_type == 'maxgain':
            # Creates gain year count tiles using only pixels that had only gain
            # count/2 uses about 200 GB on an r4.16xlarge machine
            pool.map(
                partial(gain_year_count_natrl_forest.create_gain_year_count_gain_only_maxgain, sensit_type=sensit_type),
                tile_id_list)
        else:
            # Creates gain year count tiles using only pixels that had only gain
            pool.map(partial(gain_year_count_natrl_forest.create_gain_year_count_gain_only_standard,
                             sensit_type=sensit_type),
                     tile_id_list)

        # Creates gain year count tiles using only pixels that had neither loss nor gain pixels
        # processes=36 maxes out at about 320 GB
        pool = multiprocessing.Pool(processes=36)
        pool.map(partial(gain_year_count_natrl_forest.create_gain_year_count_no_change, sensit_type=sensit_type),
                 tile_id_list)

        # processes=36 maxes out at about 220 GB
        pool = multiprocessing.Pool(processes=36)
        if sensit_type == 'maxgain':
            # Creates gain year count tiles using only pixels that had both loss and gain pixels
            pool.map(partial(gain_year_count_natrl_forest.create_gain_year_count_loss_and_gain_maxgain,
                             sensit_type=sensit_type),
                     tile_id_list)
        else:
            # Creates gain year count tiles using only pixels that had both loss and gain pixels
            pool.map(partial(gain_year_count_natrl_forest.create_gain_year_count_loss_and_gain_standard,
                             sensit_type=sensit_type),
                     tile_id_list)

        # Merges the four above gain year count tiles for each Hansen tile into a single output tile
        # count/6 maxes out at about 230 GB
        count = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(count / 6)
        pool.map(partial(gain_year_count_natrl_forest.create_gain_year_count_merge, pattern=pattern), tile_id_list)
        pool.close()
        pool.join()

        # # For single processor use
        # for tile_id in tile_id_list:
        #     gain_year_count_natrl_forest.create_gain_year_count_loss_only(tile_id, sensit_type)
        #
        # for tile_id in tile_id_list:
        #     if sensit_type == 'maxgain':
        #         gain_year_count_natrl_forest.create_gain_year_count_gain_only_maxgain(tile_id, sensit_type)
        #     else:
        #         gain_year_count_natrl_forest.create_gain_year_count_gain_only_standard(tile_id, sensit_type)
        #
        # for tile_id in tile_id_list:
        #     gain_year_count_natrl_forest.create_gain_year_count_no_change(tile_id, sensit_type)
        #
        # for tile_id in tile_id_list:
        #     if sensit_type == 'maxgain':
        #         gain_year_count_natrl_forest.create_gain_year_count_loss_and_gain_maxgain(tile_id, sensit_type)
        #     else:
        #         gain_year_count_natrl_forest.create_gain_year_count_loss_and_gain_standard(tile_id, sensit_type)
        #
        # for tile_id in tile_id_list:
        #     gain_year_count_natrl_forest.create_gain_year_count_merge(tile_id, output_pattern_list[1])


        # Intermediate output tiles for checking outputs
        uu.upload_final_set(master_output_dir_list[1], "growth_years_loss_only")
        uu.upload_final_set(master_output_dir_list[1], "growth_years_gain_only")
        uu.upload_final_set(master_output_dir_list[1], "growth_years_no_change")
        uu.upload_final_set(master_output_dir_list[1], "growth_years_loss_and_gain")

        # Uploads output from this stage
        uu.upload_final_set(master_output_dir_list[1], master_output_pattern_list[1])


    # Creates tiles of annual AGB and BGB gain rate for non-mangrove, non-planted forest using the standard model
    # removal function
    if 'annual_gain_rate_natrl_forest' in actual_stages:

        print 'Creating annual removals for natural forest'

        # Files to download for this script.
        download_dict = {
            cn.age_cat_natrl_forest_dir: [cn.pattern_age_cat_natrl_forest],
            cn.cont_eco_dir: [cn.pattern_cont_eco_processed],
            cn.plant_pre_2000_processed_dir: [cn.pattern_plant_pre_2000]
        }


        if tile_id_list:
            print tile_id_list
            print "There are {} tiles to process".format(str(len(tile_id_list))) + "\n"
        else:
            tile_id_list = uu.tile_list_s3(cn.WHRC_biomass_2000_non_mang_non_planted_dir, sensit_type)
            # tile_id_list = ['00N_050W']
            print tile_id_list
            print "There are {} tiles to process".format(str(len(tile_id_list))) + "\n"


        # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
        for key, values in download_dict.iteritems():
            dir = key
            pattern = values[0]
            uu.s3_flexible_download(dir, pattern, '.', sensit_type, tile_id_list)


        # Table with IPCC Table 4.9 default gain rates
        cmd = ['aws', 's3', 'cp', os.path.join(cn.gain_spreadsheet_dir, cn.gain_spreadsheet), '.']
        subprocess.check_call(cmd)

        pd.options.mode.chained_assignment = None

        # Imports the table with the ecozone-continent codes and the carbon gain rates
        gain_table = pd.read_excel("{}".format(cn.gain_spreadsheet),
                                   sheet_name="natrl fores gain, for std model")

        # Removes rows with duplicate codes (N. and S. America for the same ecozone)
        gain_table_simplified = gain_table.drop_duplicates(subset='gainEcoCon', keep='first')

        # Converts gain table from wide to long, so each continent-ecozone-age category has its own row
        gain_table_cont_eco_age = pd.melt(gain_table_simplified, id_vars=['gainEcoCon'],
                                          value_vars=['growth_primary', 'growth_secondary_greater_20',
                                                      'growth_secondary_less_20'])
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
        gain_table_cont_eco_age['cont_eco_age'] = gain_table_cont_eco_age['gainEcoCon'] + gain_table_cont_eco_age[
            'variable']

        # Merges the table of just continent-ecozone codes and the table of continent-ecozone-age codes
        gain_table_all_combos = pd.concat([gain_table_con_eco_only, gain_table_cont_eco_age])

        # Converts the continent-ecozone-age codes and corresponding gain rates to a dictionary
        gain_table_dict = pd.Series(gain_table_all_combos.value.values,
                                    index=gain_table_all_combos.cont_eco_age).to_dict()

        # Adds a dictionary entry for where the ecozone-continent-age code is 0 (not in a continent)
        gain_table_dict[0] = 0

        # Adds a dictionary entry for each forest age code for pixels that have forest age but no continent-ecozone
        for key, value in age_dict.iteritems():
            gain_table_dict[value] = 0

        # Converts all the keys (continent-ecozone-age codes) to float type
        gain_table_dict = {float(key): value for key, value in gain_table_dict.iteritems()}

        print gain_table_dict


        # This configuration of the multiprocessing call is necessary for passing multiple arguments to the main function
        # It is based on the example here: http://spencerimp.blogspot.com/2015/12/python-multiprocess-with-multiple.html
        # processes=24 peaks at about 440 GB of memory on an r4.16xlarge machine
        output_pattern_list = master_output_pattern_list[2:4]
        count = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(count/2)
        pool.map(partial(annual_gain_rate_natrl_forest.annual_gain_rate, sensit_type=sensit_type,
                         gain_table_dict=gain_table_dict,
                         output_pattern_list=output_pattern_list), tile_id_list)
        pool.close()
        pool.join()

        # # For single processor use
        # for tile in tile_id_list:
        #
        #     annual_gain_rate_natrl_forest.annual_gain_rate(tile, sensit_type, gain_table_dict, master_output_pattern_list[2:4])


        # Uploads outputs from this stage
        for i in range(master_output_dir_list[2:4]):
            uu.upload_final_set(master_output_dir_list[i], master_output_pattern_list[i])


    # Creates tiles of cumulative AGCO2 and BGCO2 gain rate for non-mangrove, non-planted forest using the standard model
    # removal function
    if 'cumulative_gain_natrl_forest' in actual_stages:

        print 'Creating cumulative removals for natural forest'

        # Files to download for this script.
        download_dict = {
            cn.annual_gain_AGB_natrl_forest_dir: [cn.pattern_annual_gain_AGB_natrl_forest],
            cn.annual_gain_BGB_natrl_forest_dir: [cn.pattern_annual_gain_BGB_natrl_forest],
            cn.gain_year_count_natrl_forest_dir: [cn.pattern_gain_year_count_natrl_forest]
        }


        if tile_id_list:
            print tile_id_list
            print "There are {} tiles to process".format(str(len(tile_id_list))) + "\n"
        else:
            tile_id_list = uu.tile_list_s3(cn.WHRC_biomass_2000_non_mang_non_planted_dir, sensit_type)
            # tile_id_list = ['00N_050W']
            print tile_id_list
            print "There are {} tiles to process".format(str(len(tile_id_list))) + "\n"


        # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
        for key, values in download_dict.iteritems():
            dir = key
            pattern = values[0]
            uu.s3_flexible_download(dir, pattern, '.', sensit_type, tile_id_list)


        # Calculates cumulative aboveground carbon gain in non-mangrove planted forests
        output_pattern_list = master_output_pattern_list[4:6]
        pool = multiprocessing.Pool(count/3)
        pool.map(partial(cumulative_gain_natrl_forest.cumulative_gain_AGCO2, output_pattern_list=output_pattern_list,
                         sensit_type=sensit_type), tile_id_list)

        # Calculates cumulative belowground carbon gain in non-mangrove planted forests
        pool = multiprocessing.Pool(count/3)
        pool.map(partial(cumulative_gain_natrl_forest.cumulative_gain_BGCO2, output_pattern_list=output_pattern_list,
                         sensit_type=sensit_type), tile_id_list)
        pool.close()
        pool.join()

        # # For single processor use
        # for tile_id in tile_id_list:
        #     cumulative_gain_natrl_forest.cumulative_gain_AGCO2(tile_id, master_output_pattern_list[4], sensit_type)
        #
        # for tile_id in tile_id_list:
        #     cumulative_gain_natrl_forest.cumulative_gain_BGCO2(tile_id, master_output_pattern_list[5], sensit_type)


        # Uploads outputs from this stage
        for i in range(master_output_dir_list[4:6]):
            uu.upload_final_set(master_output_dir_list[i], master_output_pattern_list[i])


    # Creates tiles of annual gain rate and cumulative removals for all forest types (above + belowground)
    if 'removals_merged' in actual_stages:

        print 'Creating annual and cumulative removals for all forest types combined (above + belowground)'

        # Files to download for this script
        download_dict = {
            cn.annual_gain_AGB_mangrove_dir: [cn.pattern_annual_gain_AGB_mangrove],
            cn.annual_gain_AGB_planted_forest_non_mangrove_dir: [cn.pattern_annual_gain_AGB_planted_forest_non_mangrove],
            cn.annual_gain_AGB_natrl_forest_dir: [cn.pattern_annual_gain_AGB_natrl_forest],

            cn.annual_gain_BGB_mangrove_dir: [cn.pattern_annual_gain_BGB_mangrove],
            cn.annual_gain_BGB_planted_forest_non_mangrove_dir: [cn.pattern_annual_gain_BGB_planted_forest_non_mangrove],
            cn.annual_gain_BGB_natrl_forest_dir: [cn.pattern_annual_gain_BGB_natrl_forest],

            cn.cumul_gain_AGCO2_mangrove_dir: [cn.pattern_cumul_gain_AGCO2_mangrove],
            cn.cumul_gain_AGCO2_planted_forest_non_mangrove_dir: [cn.pattern_cumul_gain_AGCO2_planted_forest_non_mangrove],
            cn.cumul_gain_AGCO2_natrl_forest_dir: [cn.pattern_cumul_gain_AGCO2_natrl_forest],

            cn.cumul_gain_BGCO2_mangrove_dir: [cn.pattern_cumul_gain_BGCO2_mangrove],
            cn.cumul_gain_BGCO2_planted_forest_non_mangrove_dir: [cn.pattern_cumul_gain_BGCO2_planted_forest_non_mangrove],
            cn.cumul_gain_BGCO2_natrl_forest_dir: [cn.pattern_cumul_gain_BGCO2_natrl_forest]
        }


        if tile_id_list:
            print tile_id_list
            print "There are {} tiles to process".format(str(len(tile_id_list))) + "\n"
        else:
            tile_id_list = uu.tile_list_s3(cn.WHRC_biomass_2000_non_mang_non_planted_dir, sensit_type)
            # tile_id_list = ['00N_050W']
            print tile_id_list
            print "There are {} tiles to process".format(str(len(tile_id_list))) + "\n"


        # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
        for key, values in download_dict.iteritems():
            dir = key
            pattern = values[0]
            uu.s3_flexible_download(dir, pattern, '.', sensit_type, tile_id_list)


        # For multiprocessing
        output_pattern_list = master_output_pattern_list[6:8]
        pool = multiprocessing.Pool(count/3)
        pool.map(
            partial(merge_cumulative_annual_gain_all_forest_types.gain_merge, output_pattern_list=output_pattern_list,
                    sensit_type=sensit_type), tile_id_list)
        pool.close()
        pool.join()

        # # For single processor use
        # for tile_id in tile_id_list:
        #     merge_cumulative_annual_gain_all_forest_types.gain_merge(tile_id, output_pattern_list[6:8], sensit_type)

        # Uploads output tiles to s3
        for i in range(master_output_dir_list)[6:8]:
            uu.upload_final_set(master_output_dir_list[i], master_output_pattern_list[i])


    # Creates carbon pools in loss year
    if 'carbon_pools' in actual_stages:

        print 'Creating emissions year carbon pools'


        # Checks the validity of the carbon pool extent selection in combination with the sensitivity analysis
        if (sensit_type != 'std') & (carbon_pool_extent != 'loss'):
            raise Exception("Sensitivity analysis run must use 'loss' extent")
        # Checks the validity of the carbon_pool_extent argument
        if (carbon_pool_extent not in ['loss', '2000']):
            raise Exception("Invalid carbon_pool_extent input. Please choose loss or 2000.")


        # Files to download for this script
        download_dict = {
            cn.mangrove_biomass_2000_dir: [cn.pattern_mangrove_biomass_2000],
            cn.cont_eco_dir: [cn.pattern_cont_eco_processed],
            cn.bor_tem_trop_processed_dir: [cn.pattern_bor_tem_trop_processed],
            cn.precip_processed_dir: [cn.pattern_precip],
            cn.elevation_processed_dir: [cn.pattern_elevation],
            cn.soil_C_full_extent_2000_dir: [cn.pattern_soil_C_full_extent_2000],
            cn.gain_dir: [cn.pattern_gain],
            cn.cumul_gain_AGCO2_mangrove_dir: [cn.pattern_cumul_gain_AGCO2_mangrove],
            cn.cumul_gain_AGCO2_planted_forest_non_mangrove_dir: [cn.pattern_cumul_gain_AGCO2_planted_forest_non_mangrove],
            cn.cumul_gain_AGCO2_natrl_forest_dir: [cn.pattern_cumul_gain_AGCO2_natrl_forest],
            cn.annual_gain_AGB_mangrove_dir: [cn.pattern_annual_gain_AGB_mangrove],
            cn.annual_gain_AGB_planted_forest_non_mangrove_dir: [cn.pattern_annual_gain_AGB_planted_forest_non_mangrove],
            cn.annual_gain_AGB_natrl_forest_dir: [cn.pattern_annual_gain_AGB_natrl_forest]
        }

        # Adds the correct AGB tiles to the download dictionary depending on the model run
        if sensit_type == 'biomass_swap':
            download_dict[cn.JPL_processed_dir] = [cn.pattern_JPL_unmasked_processed]
        else:
            download_dict[cn.WHRC_biomass_2000_unmasked_dir] = [cn.pattern_WHRC_biomass_2000_unmasked]

        # Adds the correct loss tile to the download dictionary depending on the model run
        if sensit_type == 'legal_Amazon_loss':
            download_dict[cn.Brazil_annual_loss_processed_dir] = [cn.pattern_Brazil_annual_loss_processed]
        else:
            download_dict[cn.loss_dir] = ['']


        if tile_id_list:
            print tile_id_list
            print "There are {} tiles to process".format(str(len(tile_id_list))) + "\n"
        else:
            tile_id_list = uu.tile_list_s3(cn.WHRC_biomass_2000_non_mang_non_planted_dir, sensit_type)
            # tile_id_list = ['00N_050W']
            print tile_id_list
            print "There are {} tiles to process".format(str(len(tile_id_list))) + "\n"


        for key, values in download_dict.iteritems():
            dir = key
            pattern = values[0]
            uu.s3_flexible_download(dir, pattern, '.', sensit_type, tile_id_list)


        # Table with IPCC Wetland Supplement Table 4.4 default mangrove gain rates
        cmd = ['aws', 's3', 'cp', os.path.join(cn.gain_spreadsheet_dir, cn.gain_spreadsheet), '.']
        subprocess.check_call(cmd)

        pd.options.mode.chained_assignment = None

        # Imports the table with the ecozone-continent codes and the carbon gain rates
        gain_table = pd.read_excel("{}".format(cn.gain_spreadsheet),
                                   sheet_name="mangrove gain, for model")

        # Removes rows with duplicate codes (N. and S. America for the same ecozone)
        gain_table_simplified = gain_table.drop_duplicates(subset='gainEcoCon', keep='first')

        mang_BGB_AGB_ratio = create_carbon_pools.mangrove_pool_ratio_dict(gain_table_simplified,
                                                                          cn.below_to_above_trop_dry_mang,
                                                                          cn.below_to_above_trop_wet_mang,
                                                                          cn.below_to_above_subtrop_mang)

        mang_deadwood_AGB_ratio = create_carbon_pools.mangrove_pool_ratio_dict(gain_table_simplified,
                                                                               cn.deadwood_to_above_trop_dry_mang,
                                                                               cn.deadwood_to_above_trop_wet_mang,
                                                                               cn.deadwood_to_above_subtrop_mang)

        mang_litter_AGB_ratio = create_carbon_pools.mangrove_pool_ratio_dict(gain_table_simplified,
                                                                             cn.litter_to_above_trop_dry_mang,
                                                                             cn.litter_to_above_trop_wet_mang,
                                                                             cn.litter_to_above_subtrop_mang)


        if carbon_pool_extent == 'loss':

            print "Creating tiles of emitted aboveground carbon (carbon 2000 + carbon accumulation until loss year)"
            # 16 processors seems to use more than 460 GB-- I don't know exactly how much it uses because I stopped it at 460
            # 14 processors maxes out at 410-415 GB
            # Creates a single filename pattern to pass to the multiprocessor call
            pattern = master_output_pattern_list[8]
            pool = multiprocessing.Pool(count/4)
            pool.map(partial(create_carbon_pools.create_emitted_AGC,
                             pattern=pattern, sensit_type=sensit_type), tile_id_list)
            pool.close()
            pool.join()

            # # For single processor use
            # for tile_id in tile_id_list:
            #     create_carbon_pools.create_emitted_AGC(tile_id, master_output_pattern_list[8], sensit_type)

            uu.upload_final_set(master_output_dir_list[8], master_output_pattern_list[8])

        elif carbon_pool_extent == '2000':

            print "Creating tiles of aboveground carbon in 2000"
            # 16 processors seems to use more than 460 GB-- I don't know exactly how much it uses because I stopped it at 460
            # 14 processors maxes out at 415 GB
            # Creates a single filename pattern to pass to the multiprocessor call
            pattern = master_output_pattern_list[8]
            count = multiprocessing.cpu_count()
            pool = multiprocessing.Pool(processes=14)
            pool.map(partial(create_carbon_pools.create_2000_AGC,
                             pattern=pattern, sensit_type=sensit_type), tile_id_list)
            pool.close()
            pool.join()

            # # For single processor use
            # for tile_id in tile_id_list:
            #     create_carbon_pools.create_2000_AGC(tile_id, output_pattern_list[8], sensit_type)

            uu.upload_final_set(master_output_dir_list[8], master_output_pattern_list[8])

        else:
            raise Exception("Extent argument not valid")

        print "Creating tiles of belowground carbon"
        # 18 processors used between 300 and 400 GB memory, so it was okay on a r4.16xlarge spot machine
        # Creates a single filename pattern to pass to the multiprocessor call
        pattern = master_output_pattern_list[9]
        pool = multiprocessing.Pool(count/2)
        pool.map(partial(create_carbon_pools.create_BGC, mang_BGB_AGB_ratio=mang_BGB_AGB_ratio,
                         carbon_pool_extent=carbon_pool_extent,
                         pattern=pattern, sensit_type=sensit_type), tile_id_list)
        pool.close()
        pool.join()

        # # For single processor use
        # for tile_id in tile_id_list:
        #     create_carbon_pools.create_BGC(tile_id, mang_BGB_AGB_ratio, carbon_pool_extent, master_output_pattern_list[9], sensit_type)

        uu.upload_final_set(master_output_dir_list[9], master_output_pattern_list[9])

        print "Creating tiles of deadwood carbon"
        # processes=16 maxes out at about 430 GB
        # Creates a single filename pattern to pass to the multiprocessor call
        pattern = master_output_pattern_list[10]
        pool = multiprocessing.Pool(count/4)
        pool.map(
            partial(create_carbon_pools.create_deadwood, mang_deadwood_AGB_ratio=mang_deadwood_AGB_ratio,
                    carbon_pool_extent=carbon_pool_extent,
                    pattern=pattern, sensit_type=sensit_type), tile_id_list)
        pool.close()
        pool.join()

        # # For single processor use
        # for tile_id in tile_id_list:
        #     create_carbon_pools.create_deadwood(tile_id, mang_deadwood_AGB_ratio, carbon_pool_extent, master_output_pattern_list[10], sensit_type)

        uu.upload_final_set(master_output_dir_list[10], master_output_pattern_list[10])

        print "Creating tiles of litter carbon"
        # Creates a single filename pattern to pass to the multiprocessor call
        pattern = master_output_pattern_list[11]
        pool = multiprocessing.Pool(count/4)
        pool.map(partial(create_carbon_pools.create_litter, mang_litter_AGB_ratio=mang_litter_AGB_ratio,
                         carbon_pool_extent=carbon_pool_extent,
                         pattern=pattern, sensit_type=sensit_type), tile_id_list)
        pool.close()
        pool.join()

        # # For single processor use
        # for tile_id in tile_id_list:
        #     create_carbon_pools.create_litter(tile_id, mang_litter_AGB_ratio, carbon_pool_extent, master_output_pattern_list[11], sensit_type)

        uu.upload_final_set(master_output_dir_list[11], master_output_pattern_list[11])

        if carbon_pool_extent == 'loss':

            print "Creating tiles of soil carbon"
            # Creates a single filename pattern to pass to the multiprocessor call
            pattern = master_output_pattern_list[12]
            pool = multiprocessing.Pool(count/3)
            pool.map(partial(create_carbon_pools.create_soil,
                             pattern=pattern, sensit_type=sensit_type), tile_id_list)
            pool.close()
            pool.join()

            # # For single processor use
            # for tile_id in tile_id_list:
            #     create_carbon_pools.create_soil(tile_id, master_output_pattern_list[12], sensit_type)

            uu.upload_final_set(master_output_dir_list[12], master_output_pattern_list[12])

        elif carbon_pool_extent == '2000':
            print "Skipping soil for 2000 carbon pool calculation"

        else:
            raise Exception("Extent argument not valid")

        print "Creating tiles of total carbon"
        # I tried several different processor numbers for this. Ended up using 14 processors, which used about 380 GB memory
        # at peak. Probably could've handled 16 processors on an r4.16xlarge machine but I didn't feel like taking the time to check.
        # Creates a single filename pattern to pass to the multiprocessor call
        pattern = master_output_pattern_list[13]
        pool = multiprocessing.Pool(count/4)
        pool.map(partial(create_carbon_pools.create_total_C, carbon_pool_extent=carbon_pool_extent,
                         pattern=pattern, sensit_type=sensit_type), tile_id_list)
        pool.close()
        pool.join()

        # # For single processor use
        # for tile_id in tile_id_list:
        #     create_carbon_pools.create_total_C(tile_id, carbon_pool_extent, master_output_pattern_list[13], sensit_type)

        uu.upload_final_set(master_output_dir_list[13], master_output_pattern_list[13])


    if 'gross_emissions' in actual_stages:

        print 'Creating gross emissions tiles'

        # Checks the validity of the pools argument
        if (pools not in ['soil_only', 'biomass_soil']):
            raise Exception('Invalid pool input. Please choose soil_only or biomass_soil.')

        # Files to download for this script
        download_dict = {
            cn.AGC_emis_year_dir: [cn.pattern_AGC_emis_year],
            cn.BGC_emis_year_dir: [cn.pattern_BGC_emis_year],
            cn.deadwood_emis_year_2000_dir: [cn.pattern_deadwood_emis_year_2000],
            cn.litter_emis_year_2000_dir: [cn.pattern_litter_emis_year_2000],
            cn.soil_C_emis_year_2000_dir: [cn.pattern_soil_C_emis_year_2000],
            cn.peat_mask_dir: [cn.pattern_peat_mask],
            cn.ifl_primary_processed_dir: [cn.pattern_ifl_primary],
            cn.planted_forest_type_unmasked_dir: [cn.pattern_planted_forest_type_unmasked],
            cn.drivers_processed_dir: [cn.pattern_drivers],
            cn.climate_zone_processed_dir: [cn.pattern_climate_zone],
            cn.bor_tem_trop_processed_dir: [cn.pattern_bor_tem_trop_processed],
            cn.burn_year_dir: [cn.pattern_burn_year],
            cn.plant_pre_2000_processed_dir: [cn.pattern_plant_pre_2000]
        }

        # Special loss tiles for the Brazil sensitivity analysis
        if sensit_type == 'legal_Amazon_loss':
            download_dict[cn.Brazil_annual_loss_processed_dir] = [cn.pattern_Brazil_annual_loss_processed]
        else:
            download_dict[cn.loss_dir] = ['']


        if tile_id_list:
            print tile_id_list
            print "There are {} tiles to process".format(str(len(tile_id_list))) + "\n"
        else:
            tile_id_list = uu.tile_list_s3(cn.WHRC_biomass_2000_non_mang_non_planted_dir, sensit_type)
            # tile_id_list = ['00N_050W']
            print tile_id_list
            print "There are {} tiles to process".format(str(len(tile_id_list))) + "\n"


        # Checks if the correct c++ script has been compiled for the pool option selected
        if pools == 'biomass_soil':

            # Output file directories for biomass+soil. Must be in same order as output pattern directories.
            output_dir_list = [cn.gross_emis_commod_biomass_soil_dir,
                               cn.gross_emis_shifting_ag_biomass_soil_dir,
                               cn.gross_emis_forestry_biomass_soil_dir,
                               cn.gross_emis_wildfire_biomass_soil_dir,
                               cn.gross_emis_urban_biomass_soil_dir,
                               cn.gross_emis_no_driver_biomass_soil_dir,
                               cn.gross_emis_all_gases_all_drivers_biomass_soil_dir,
                               cn.gross_emis_co2_only_all_drivers_biomass_soil_dir,
                               cn.gross_emis_non_co2_all_drivers_biomass_soil_dir,
                               cn.gross_emis_nodes_biomass_soil_dir]

            output_pattern_list = [cn.pattern_gross_emis_commod_biomass_soil,
                                   cn.pattern_gross_emis_shifting_ag_biomass_soil,
                                   cn.pattern_gross_emis_forestry_biomass_soil,
                                   cn.pattern_gross_emis_wildfire_biomass_soil,
                                   cn.pattern_gross_emis_urban_biomass_soil,
                                   cn.pattern_gross_emis_no_driver_biomass_soil,
                                   cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil,
                                   cn.pattern_gross_emis_co2_only_all_drivers_biomass_soil,
                                   cn.pattern_gross_emis_non_co2_all_drivers_biomass_soil,
                                   cn.pattern_gross_emis_nodes_biomass_soil]

            # Some sensitivity analyses have specific gross emissions scripts.
            # The rest of the sensitivity analyses and the standard model can all use the same, generic gross emissions script.
            if sensit_type in ['no_shifting_ag', 'convert_to_grassland']:
                if os.path.exists('./cpp_util/calc_gross_emissions_{}.exe'.format(sensit_type)):
                    print "C++ for {} already compiled.".format(sensit_type)
                else:
                    raise Exception('Must compile standard {} model C++...'.format(sensit_type))
            else:
                if os.path.exists('./cpp_util/calc_gross_emissions_generic.exe'):
                    print "C++ for generic emissions already compiled."
                else:
                    raise Exception('Must compile generic emissions C++...')

        elif (pools == 'soil_only') & (sensit_type == 'std'):
            if os.path.exists('./cpp_util/calc_gross_emissions_soil_only.exe'):
                print "C++ for soil_only already compiled."

                # Output file directories for soil_only. Must be in same order as output pattern directories.
                output_dir_list = [cn.gross_emis_commod_soil_only_dir,
                                   cn.gross_emis_shifting_ag_soil_only_dir,
                                   cn.gross_emis_forestry_soil_only_dir,
                                   cn.gross_emis_wildfire_soil_only_dir,
                                   cn.gross_emis_urban_soil_only_dir,
                                   cn.gross_emis_no_driver_soil_only_dir,
                                   cn.gross_emis_all_gases_all_drivers_soil_only_dir,
                                   cn.gross_emis_co2_only_all_drivers_soil_only_dir,
                                   cn.gross_emis_non_co2_all_drivers_soil_only_dir,
                                   cn.gross_emis_nodes_soil_only_dir]

                output_pattern_list = [cn.pattern_gross_emis_commod_soil_only,
                                       cn.pattern_gross_emis_shifting_ag_soil_only,
                                       cn.pattern_gross_emis_forestry_soil_only,
                                       cn.pattern_gross_emis_wildfire_soil_only,
                                       cn.pattern_gross_emis_urban_soil_only,
                                       cn.pattern_gross_emis_no_driver_soil_only,
                                       cn.pattern_gross_emis_all_gases_all_drivers_soil_only,
                                       cn.pattern_gross_emis_co2_only_all_drivers_soil_only,
                                       cn.pattern_gross_emis_non_co2_all_drivers_soil_only,
                                       cn.pattern_gross_emis_nodes_soil_only]
            else:
                raise Exception('Must compile soil_only C++...')

        else:
            raise Exception('Pool and/or sensitivity analysis option not valid')

        # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
        for key, values in download_dict.iteritems():
            dir = key
            pattern = values[0]
            uu.s3_flexible_download(dir, pattern, './cpp_util/', sensit_type, tile_id_list)


        print "Removing loss pixels from plantations that existed in Indonesia and Malaysia before 2000..."
        # Pixels that were in plantations that existed before 2000 should not be included in gross emissions.
        # Pre-2000 plantations have not previously been masked, so that is done here.
        # There are only 8 tiles to process, so count/2 will cover all of them in one go.
        count = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(count / 2)
        pool.map(partial(calculate_gross_emissions.mask_pre_2000_plant, sensit_type=sensit_type), tile_id_list)

        # # For single processor use
        # for tile in tile_id_list:
        #       calculate_gross_emissions.mask_pre_2000_plant(tile)

        # The C++ code expects a plantations tile for every input 10x10.
        # However, not all Hansen tiles have plantations.
        # This function creates "dummy" plantation tiles for all Hansen tiles that do not have plantations.
        # That way, the C++ script gets all the necessary input files
        folder = 'cpp_util/'

        print "Making blank tiles for inputs that don't currently exist"
        # All of the inputs that need to have dummy tiles made in order to match the tile list of the carbon pools
        pattern_list = [cn.pattern_planted_forest_type_unmasked, cn.pattern_peat_mask, cn.pattern_ifl_primary,
                        cn.pattern_drivers, cn.pattern_bor_tem_trop_processed]

        for pattern in pattern_list:
            count = multiprocessing.cpu_count()
            pool = multiprocessing.Pool(count - 10)
            pool.map(partial(uu.make_blank_tile, pattern=pattern, folder=folder, sensit_type=sensit_type), tile_id_list)
            pool.close()
            pool.join()

        # # For single processor use
        # for pattern in pattern_list:
        #     for tile in tile_id_list:
        #         uu.make_blank_tile(tile, pattern, folder, sensit_type)

        # Calculates gross emissions for each tile
        # count/4 uses about 390 GB on a r4.16xlarge spot machine.
        # processes=18 uses about 440 GB on an r4.16xlarge spot machine.
        count = multiprocessing.cpu_count()
        # pool = multiprocessing.Pool(processes=18)
        pool = multiprocessing.Pool(processes=9)
        pool.map(partial(calculate_gross_emissions.calc_emissions, pools=pools, sensit_type=sensit_type), tile_id_list)

        # # For single processor use
        # for tile in tile_id_list:
        #       calculate_gross_emissions.calc_emissions(tile, pools, sensit_type)

        # Uploads emissions to appropriate directory for the carbon pools chosen
        for i in range(0, len(output_dir_list)):
            uu.upload_final_set(output_dir_list[i], output_pattern_list[i])


if __name__ == '__main__':
    main()