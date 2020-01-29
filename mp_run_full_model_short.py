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
from gain.mp_forest_age_category_natrl_forest import mp_forest_age_category_natrl_forest
from gain.mp_gain_year_count_natrl_forest import mp_gain_year_count_natrl_forest
from gain.mp_annual_gain_rate_natrl_forest import mp_annual_gain_rate_natrl_forest
from gain.mp_cumulative_gain_natrl_forest import mp_cumulative_gain_natrl_forest
from gain.mp_merge_cumulative_annual_gain_all_forest_types import mp_merge_cumulative_annual_gain_all_forest_types
from carbon_pools.mp_create_carbon_pools import mp_create_carbon_pools
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
    parser.add_argument('--tile_id_list', '-l', required=True,
                        help='List of tile ids to use in the model. Should be of form 00N_110E or all.')
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

    tile_id_list = uu.tile_id_list_check(tile_id_list)


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

        print 'Creating forest age category for natural forest tiles'

        mp_forest_age_category_natrl_forest(sensit_type, tile_id_list)


    # Creates tiles of the number of years of removals
    if 'gain_year_count_natrl_forest' in actual_stages:

        print 'Creating gain year count tiles for natural forest'

        mp_gain_year_count_natrl_forest(sensit_type, tile_id_list)


    # Creates tiles of annual AGB and BGB gain rate for non-mangrove, non-planted forest using the standard model
    # removal function
    if 'annual_gain_rate_natrl_forest' in actual_stages:

        print 'Creating annual removals for natural forest'

        mp_annual_gain_rate_natrl_forest(sensit_type, tile_id_list)


    # Creates tiles of cumulative AGCO2 and BGCO2 gain rate for non-mangrove, non-planted forest using the standard model
    # removal function
    if 'cumulative_gain_natrl_forest' in actual_stages:

        print 'Creating cumulative removals for natural forest'

        mp_cumulative_gain_natrl_forest(sensit_type, tile_id_list)


    # Creates tiles of annual gain rate and cumulative removals for all forest types (above + belowground)
    if 'removals_merged' in actual_stages:

        print 'Creating annual and cumulative removals for all forest types combined (above + belowground)'

        mp_merge_cumulative_annual_gain_all_forest_types(sensit_type, tile_id_list)



    # Creates carbon pools in loss year
    if 'carbon_pools' in actual_stages:

        print 'Creating emissions year carbon pools'

        mp_create_carbon_pools(sensit_type, tile_id_list, carbon_pool_extent)


    # if 'gross_emissions' in actual_stages:
    #
    #     print 'Creating gross emissions tiles'
    #
    #     # Checks the validity of the pools argument
    #     if (pools not in ['soil_only', 'biomass_soil']):
    #         raise Exception('Invalid pool input. Please choose soil_only or biomass_soil.')
    #
    #     # Files to download for this script
    #     download_dict = {
    #         cn.AGC_emis_year_dir: [cn.pattern_AGC_emis_year],
    #         cn.BGC_emis_year_dir: [cn.pattern_BGC_emis_year],
    #         cn.deadwood_emis_year_2000_dir: [cn.pattern_deadwood_emis_year_2000],
    #         cn.litter_emis_year_2000_dir: [cn.pattern_litter_emis_year_2000],
    #         cn.soil_C_emis_year_2000_dir: [cn.pattern_soil_C_emis_year_2000],
    #         cn.peat_mask_dir: [cn.pattern_peat_mask],
    #         cn.ifl_primary_processed_dir: [cn.pattern_ifl_primary],
    #         cn.planted_forest_type_unmasked_dir: [cn.pattern_planted_forest_type_unmasked],
    #         cn.drivers_processed_dir: [cn.pattern_drivers],
    #         cn.climate_zone_processed_dir: [cn.pattern_climate_zone],
    #         cn.bor_tem_trop_processed_dir: [cn.pattern_bor_tem_trop_processed],
    #         cn.burn_year_dir: [cn.pattern_burn_year],
    #         cn.plant_pre_2000_processed_dir: [cn.pattern_plant_pre_2000]
    #     }
    #
    #     # Special loss tiles for the Brazil sensitivity analysis
    #     if sensit_type == 'legal_Amazon_loss':
    #         download_dict[cn.Brazil_annual_loss_processed_dir] = [cn.pattern_Brazil_annual_loss_processed]
    #     else:
    #         download_dict[cn.loss_dir] = ['']
    #
    #
    #     if tile_id_list:
    #         print tile_id_list
    #         print "There are {} tiles to process".format(str(len(tile_id_list))) + "\n"
    #     else:
    #         tile_id_list = uu.tile_list_s3(cn.WHRC_biomass_2000_non_mang_non_planted_dir, sensit_type)
    #         # tile_id_list = ['00N_050W']
    #         print tile_id_list
    #         print "There are {} tiles to process".format(str(len(tile_id_list))) + "\n"
    #
    #
    #     # Checks if the correct c++ script has been compiled for the pool option selected
    #     if pools == 'biomass_soil':
    #
    #         # Output file directories for biomass+soil. Must be in same order as output pattern directories.
    #         output_dir_list = [cn.gross_emis_commod_biomass_soil_dir,
    #                            cn.gross_emis_shifting_ag_biomass_soil_dir,
    #                            cn.gross_emis_forestry_biomass_soil_dir,
    #                            cn.gross_emis_wildfire_biomass_soil_dir,
    #                            cn.gross_emis_urban_biomass_soil_dir,
    #                            cn.gross_emis_no_driver_biomass_soil_dir,
    #                            cn.gross_emis_all_gases_all_drivers_biomass_soil_dir,
    #                            cn.gross_emis_co2_only_all_drivers_biomass_soil_dir,
    #                            cn.gross_emis_non_co2_all_drivers_biomass_soil_dir,
    #                            cn.gross_emis_nodes_biomass_soil_dir]
    #
    #         output_pattern_list = [cn.pattern_gross_emis_commod_biomass_soil,
    #                                cn.pattern_gross_emis_shifting_ag_biomass_soil,
    #                                cn.pattern_gross_emis_forestry_biomass_soil,
    #                                cn.pattern_gross_emis_wildfire_biomass_soil,
    #                                cn.pattern_gross_emis_urban_biomass_soil,
    #                                cn.pattern_gross_emis_no_driver_biomass_soil,
    #                                cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil,
    #                                cn.pattern_gross_emis_co2_only_all_drivers_biomass_soil,
    #                                cn.pattern_gross_emis_non_co2_all_drivers_biomass_soil,
    #                                cn.pattern_gross_emis_nodes_biomass_soil]
    #
    #         # Some sensitivity analyses have specific gross emissions scripts.
    #         # The rest of the sensitivity analyses and the standard model can all use the same, generic gross emissions script.
    #         if sensit_type in ['no_shifting_ag', 'convert_to_grassland']:
    #             if os.path.exists('./cpp_util/calc_gross_emissions_{}.exe'.format(sensit_type)):
    #                 print "C++ for {} already compiled.".format(sensit_type)
    #             else:
    #                 raise Exception('Must compile standard {} model C++...'.format(sensit_type))
    #         else:
    #             if os.path.exists('./cpp_util/calc_gross_emissions_generic.exe'):
    #                 print "C++ for generic emissions already compiled."
    #             else:
    #                 raise Exception('Must compile generic emissions C++...')
    #
    #     elif (pools == 'soil_only') & (sensit_type == 'std'):
    #         if os.path.exists('./cpp_util/calc_gross_emissions_soil_only.exe'):
    #             print "C++ for soil_only already compiled."
    #
    #             # Output file directories for soil_only. Must be in same order as output pattern directories.
    #             output_dir_list = [cn.gross_emis_commod_soil_only_dir,
    #                                cn.gross_emis_shifting_ag_soil_only_dir,
    #                                cn.gross_emis_forestry_soil_only_dir,
    #                                cn.gross_emis_wildfire_soil_only_dir,
    #                                cn.gross_emis_urban_soil_only_dir,
    #                                cn.gross_emis_no_driver_soil_only_dir,
    #                                cn.gross_emis_all_gases_all_drivers_soil_only_dir,
    #                                cn.gross_emis_co2_only_all_drivers_soil_only_dir,
    #                                cn.gross_emis_non_co2_all_drivers_soil_only_dir,
    #                                cn.gross_emis_nodes_soil_only_dir]
    #
    #             output_pattern_list = [cn.pattern_gross_emis_commod_soil_only,
    #                                    cn.pattern_gross_emis_shifting_ag_soil_only,
    #                                    cn.pattern_gross_emis_forestry_soil_only,
    #                                    cn.pattern_gross_emis_wildfire_soil_only,
    #                                    cn.pattern_gross_emis_urban_soil_only,
    #                                    cn.pattern_gross_emis_no_driver_soil_only,
    #                                    cn.pattern_gross_emis_all_gases_all_drivers_soil_only,
    #                                    cn.pattern_gross_emis_co2_only_all_drivers_soil_only,
    #                                    cn.pattern_gross_emis_non_co2_all_drivers_soil_only,
    #                                    cn.pattern_gross_emis_nodes_soil_only]
    #         else:
    #             raise Exception('Must compile soil_only C++...')
    #
    #     else:
    #         raise Exception('Pool and/or sensitivity analysis option not valid')
    #
    #     # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
    #     for key, values in download_dict.iteritems():
    #         dir = key
    #         pattern = values[0]
    #         uu.s3_flexible_download(dir, pattern, './cpp_util/', sensit_type, tile_id_list)
    #
    #
    #     print "Removing loss pixels from plantations that existed in Indonesia and Malaysia before 2000..."
    #     # Pixels that were in plantations that existed before 2000 should not be included in gross emissions.
    #     # Pre-2000 plantations have not previously been masked, so that is done here.
    #     # There are only 8 tiles to process, so count/2 will cover all of them in one go.
    #     count = multiprocessing.cpu_count()
    #     pool = multiprocessing.Pool(count / 2)
    #     pool.map(partial(calculate_gross_emissions.mask_pre_2000_plant, sensit_type=sensit_type), tile_id_list)
    #
    #     # # For single processor use
    #     # for tile in tile_id_list:
    #     #       calculate_gross_emissions.mask_pre_2000_plant(tile)
    #
    #     # The C++ code expects a plantations tile for every input 10x10.
    #     # However, not all Hansen tiles have plantations.
    #     # This function creates "dummy" plantation tiles for all Hansen tiles that do not have plantations.
    #     # That way, the C++ script gets all the necessary input files
    #     folder = 'cpp_util/'
    #
    #     print "Making blank tiles for inputs that don't currently exist"
    #     # All of the inputs that need to have dummy tiles made in order to match the tile list of the carbon pools
    #     pattern_list = [cn.pattern_planted_forest_type_unmasked, cn.pattern_peat_mask, cn.pattern_ifl_primary,
    #                     cn.pattern_drivers, cn.pattern_bor_tem_trop_processed]
    #
    #     for pattern in pattern_list:
    #         count = multiprocessing.cpu_count()
    #         pool = multiprocessing.Pool(count - 10)
    #         pool.map(partial(uu.make_blank_tile, pattern=pattern, folder=folder, sensit_type=sensit_type), tile_id_list)
    #         pool.close()
    #         pool.join()
    #
    #     # # For single processor use
    #     # for pattern in pattern_list:
    #     #     for tile in tile_id_list:
    #     #         uu.make_blank_tile(tile, pattern, folder, sensit_type)
    #
    #     # Calculates gross emissions for each tile
    #     # count/4 uses about 390 GB on a r4.16xlarge spot machine.
    #     # processes=18 uses about 440 GB on an r4.16xlarge spot machine.
    #     count = multiprocessing.cpu_count()
    #     # pool = multiprocessing.Pool(processes=18)
    #     pool = multiprocessing.Pool(processes=9)
    #     pool.map(partial(calculate_gross_emissions.calc_emissions, pools=pools, sensit_type=sensit_type), tile_id_list)
    #
    #     # # For single processor use
    #     # for tile in tile_id_list:
    #     #       calculate_gross_emissions.calc_emissions(tile, pools, sensit_type)
    #
    #     # Uploads emissions to appropriate directory for the carbon pools chosen
    #     for i in range(0, len(output_dir_list)):
    #         uu.upload_final_set(output_dir_list[i], output_pattern_list[i])


if __name__ == '__main__':
    main()