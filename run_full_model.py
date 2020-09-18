'''
git clone https://github.com/wri/carbon-budget
spotutil new r4.16xlarge dgibbs_wri --disk_size 1024
c++ /usr/local/app/emissions/cpp_util/calc_gross_emissions_generic.cpp -o /usr/local/app/emissions/cpp_util/calc_gross_emissions_generic.exe -lgdal
python run_full_model.py -t std -s forest_age_category_natrl_forest -r false -d 20209999 -l 00N_000E -ce loss -p biomass_soil -tcd 30 -ln "This is a log note"
python run_full_model.py -t std -s all -r true -d 20200327 -l all -ce loss -p biomass_soil -tcd 30 -ma true -pl true -ln "This is a log note"

python run_full_model.py -t std -s all -r true -d 20200822 -l all -ce loss,2000 -p biomass_soil -tcd 30 -ma true -ln "First attempt at running full standard model on all tiles for model v1.2.0. Hopefully this will be the sole, definitive run of standard model v1.2.0."

python run_full_model.py -t no_primary_gain -s all -r true -d 20200916 -l all -ce loss -p biomass_soil -tcd 30 -sagg s3://gfw2-data/climate/carbon_model/0_4deg_output_aggregation/biomass_soil/standard/20200914/net_flux_Mt_CO2e_biomass_soil_per_year_tcd30_0_4deg_modelv1_2_0_std_20200914.tif -ln "Running sensitivity analysis for model v1.2.0"

'''

import argparse
import os
import glob
import datetime
import logging
import constants_and_names as cn
import universal_util as uu
from data_prep.mp_model_extent import mp_model_extent
from gain.mp_annual_gain_rate_mangrove import mp_annual_gain_rate_mangrove
from gain.mp_US_removal_rates import mp_US_removal_rates
from gain.mp_forest_age_category_IPCC import mp_forest_age_category_IPCC
from gain.mp_annual_gain_rate_IPCC_defaults import mp_annual_gain_rate_IPCC_defaults
from gain.mp_annual_gain_rate_AGC_BGC_all_forest_types import mp_annual_gain_rate_AGC_BGC_all_forest_types
from gain.mp_gain_year_count_all_forest_types import mp_gain_year_count_all_forest_types
from gain.mp_gross_removals_all_forest_types import mp_gross_removals_all_forest_types
from carbon_pools.mp_create_carbon_pools import mp_create_carbon_pools
from emissions.mp_calculate_gross_emissions import mp_calculate_gross_emissions
from analyses.mp_net_flux import mp_net_flux
from analyses.mp_aggregate_results_to_4_km import mp_aggregate_results_to_4_km
from analyses.mp_output_per_pixel import mp_output_per_pixel

def main ():

    os.chdir(cn.docker_base_dir)

    # List of possible model stages to run (not including mangrove and planted forest stages)
    model_stages = ['all', 'model_extent', 'forest_age_category_IPCC', 'annual_removals_IPCC',
                    'annual_removals_all_forest_types', 'gain_year_count', 'gross_removals_all_forest_types',
                    'carbon_pools', 'gross_emissions',
                    'net_flux', 'aggregate']


    # The argument for what kind of model run is being done: standard conditions or a sensitivity analysis run
    parser = argparse.ArgumentParser(description='Run the full carbon flux model')
    parser.add_argument('--model-type', '-t', required=True, help='{}'.format(cn.model_type_arg_help))
    parser.add_argument('--stages', '-s', required=True,
                        help='Stages for running the flux model. Options are {}'.format(model_stages))
    parser.add_argument('--run-through', '-r', required=True,
                        help='Options: true or false. true: run named stage and following stages. false: run only named stage.')
    parser.add_argument('--run-date', '-d', required=False,
                        help='Date of run. Must be format YYYYMMDD.')
    parser.add_argument('--tile-id-list', '-l', required=True,
                        help='List of tile ids to use in the model. Should be of form 00N_110E or 00N_110E,00N_120E or all.')
    parser.add_argument('--carbon-pool-extent', '-ce', required=False,
                        help='Time period for which carbon emitted_pools should be calculated: loss, 2000, loss,2000, or 2000,loss')
    parser.add_argument('--emitted-pools-to-use', '-p', required=False,
                        help='Options are soil_only or biomass_soil. Former only considers emissions from soil. Latter considers emissions from biomass and soil.')
    parser.add_argument('--tcd-threshold', '-tcd', required=False,
                        help='Tree cover density threshold above which pixels will be included in the aggregation.')
    parser.add_argument('--std-net-flux-aggreg', '-sagg', required=False,
                        help='The s3 standard model net flux aggregated tif, for comparison with the sensitivity analysis map')
    parser.add_argument('--mangroves', '-ma', required=False,
                        help='Include mangrove removal rate and standard deviation tile creation step (before model extent). true or false.')
    parser.add_argument('--us-rates', '-us', required=False,
                        help='Include US removal rate and standard deviation tile creation step (before model extent). true or false.')
    parser.add_argument('--per-pixel-results', '-ppr', required=False,
                        help='Include per pixel result calculations for gross emissions (all gases, all pools), gross removals, and net flux. true or false.')
    parser.add_argument('--log-note', '-ln', required=False,
                        help='Note to include in log header about model run.')
    args = parser.parse_args()

    sensit_type = args.model_type
    stage_input = args.stages
    run_through = args.run_through
    run_date = args.run_date
    tile_id_list = args.tile_id_list
    carbon_pool_extent = args.carbon_pool_extent
    emitted_pools = args.emitted_pools_to_use
    thresh = args.tcd_threshold
    if thresh is not None:
        thresh = int(thresh)
    std_net_flux = args.std_net_flux_aggreg
    include_mangroves = args.mangroves
    include_us = args.us_rates
    include_per_pixel = args.per_pixel_results
    log_note = args.log_note

    # Start time for script
    script_start = datetime.datetime.now()

    # Create the output log
    uu.initiate_log(tile_id_list=tile_id_list, sensit_type=sensit_type, run_date=run_date, stage_input=stage_input, run_through=run_through,
                    carbon_pool_extent=carbon_pool_extent, emitted_pools=emitted_pools, thresh=thresh, std_net_flux=std_net_flux,
                    include_mangroves=include_mangroves, include_us=include_us, include_per_pixel=include_per_pixel,
                    log_note=log_note)


    # Checks the validity of the model stage arguments. If either one is invalid, the script ends.
    if (stage_input not in model_stages):
        uu.exception_log('Invalid stage selection. Please provide a stage from', model_stages)
    else:
        pass
    if (run_through not in ['true', 'false']):
        uu.exception_log('Invalid run through option. Please enter true or false.')
    else:
        pass

    # Generates the list of stages to run
    actual_stages = uu.analysis_stages(model_stages, stage_input, run_through,
                                       include_mangroves = include_mangroves, include_us=include_us,
                                       include_per_pixel=include_per_pixel)
    uu.print_log("Analysis stages to run:", actual_stages)

    # Reports how much storage is being used with files
    uu.check_storage()

    # Checks whether the sensitivity analysis argument is valid
    uu.check_sensit_type(sensit_type)

    # Checks if the carbon pool type is specified if the stages to run includes carbon pool generation.
    # Does this up front so the user knows before the run begins that information is missing.
    if ('carbon_pools' in actual_stages) & (carbon_pool_extent not in ['loss', '2000', 'loss,2000', '2000,loss']):
        uu.exception_log("Invalid carbon_pool_extent input. Please choose loss, 2000, loss,2000 or 2000,loss.")

    # Checks if the correct c++ script has been compiled for the pool option selected.
    # Does this up front so that the user is prompted to compile the C++ before the script starts running, if necessary.
    if 'gross_emissions' in actual_stages:

        if emitted_pools == 'biomass_soil':
            # Some sensitivity analyses have specific gross emissions scripts.
            # The rest of the sensitivity analyses and the standard model can all use the same, generic gross emissions script.
            if sensit_type in ['no_shifting_ag', 'convert_to_grassland']:
                if os.path.exists('{0}/calc_gross_emissions_{1}.exe'.format(cn.c_emis_compile_dst, sensit_type)):
                    uu.print_log("C++ for {} already compiled.".format(sensit_type))
                else:
                    uu.exception_log('Must compile standard {} model C++...'.format(sensit_type))
            else:
                if os.path.exists('{0}/calc_gross_emissions_generic.exe'.format(cn.c_emis_compile_dst)):
                    uu.print_log("C++ for generic emissions already compiled.")
                else:
                    uu.exception_log('Must compile generic emissions C++...')

        elif (emitted_pools == 'soil_only') & (sensit_type == 'std'):
            if os.path.exists('{0}/calc_gross_emissions_soil_only.exe'.format(cn.c_emis_compile_dst)):
                uu.print_log("C++ for generic emissions already compiled.")
            else:
                uu.exception_log('Must compile soil_only C++...')

        else:
            uu.exception_log('Pool and/or sensitivity analysis option not valid for gross emissions')

    # Checks whether the canopy cover argument is valid up front.
    if 'aggregate' in actual_stages:
        if thresh < 0 or thresh > 99:
            uu.exception_log('Invalid tcd. Please provide an integer between 0 and 99.')
        else:
            pass

    # If the tile_list argument is an s3 folder, the list of tiles in it is created
    if 's3://' in tile_id_list:
        tile_id_list = uu.tile_list_s3(tile_id_list, 'std')
        uu.print_log(tile_id_list)
        uu.print_log("There are {} tiles to process".format(str(len(tile_id_list))), "\n")
    # Otherwise, check that the tile list argument is valid. "all" is the way to specify that all tiles should be processed
    else:
        tile_id_list = uu.tile_id_list_check(tile_id_list)


    # List of output directories and output file name patterns.
    # The directory list is only used for counting tiles in output folders at the end of the model
    output_dir_list = [
        cn.model_extent_dir,
        cn.age_cat_IPCC_dir,
        cn.annual_gain_AGB_IPCC_defaults_dir, cn.annual_gain_BGB_IPCC_defaults_dir, cn.stdev_annual_gain_AGB_IPCC_defaults_dir,
        cn.removal_forest_type_dir,
        cn.annual_gain_AGC_all_types_dir, cn.annual_gain_BGC_all_types_dir,
        cn.annual_gain_AGC_BGC_all_types_dir, cn.stdev_annual_gain_AGC_all_types_dir,
        cn.gain_year_count_dir,
        cn.cumul_gain_AGCO2_all_types_dir, cn.cumul_gain_BGCO2_all_types_dir,
        cn.cumul_gain_AGCO2_BGCO2_all_types_dir
    ]

    # Prepends the mangrove and US output directories if mangroves are included
    if 'annual_removals_mangrove' in actual_stages:

        output_dir_list = [cn.annual_gain_AGB_mangrove_dir, cn.annual_gain_BGB_mangrove_dir,
                           cn.stdev_annual_gain_AGB_mangrove_dir] + output_dir_list

    if 'annual_removals_us' in actual_stages:

        output_dir_list = [cn.annual_gain_AGC_BGC_natrl_forest_US_dir,
                           cn.stdev_annual_gain_AGC_BGC_natrl_forest_US_dir] + output_dir_list

    # Adds the carbon directories depending on which carbon emitted_pools are being generated: 2000 and/or emissions year
    if 'carbon_pools' in actual_stages:
        if 'loss' in carbon_pool_extent:
            output_dir_list = output_dir_list + [cn.AGC_emis_year_dir, cn.BGC_emis_year_dir,
                                                 cn.deadwood_emis_year_2000_dir, cn.litter_emis_year_2000_dir,
                                                 cn.soil_C_emis_year_2000_dir, cn.total_C_emis_year_dir]

        if '2000' in carbon_pool_extent:
            output_dir_list = output_dir_list + [cn.AGC_2000_dir, cn.BGC_2000_dir,
                                                 cn.deadwood_2000_dir, cn.litter_2000_dir,
                                                 cn.soil_C_full_extent_2000_dir, cn.total_C_2000_dir]

    # Adds the biomass_soil output directories or the soil_only output directories depending on the model run
    if 'gross_emissions' in actual_stages:
        if emitted_pools == 'biomass_soil':
            output_dir_list = output_dir_list + [cn.gross_emis_commod_biomass_soil_dir,
                               cn.gross_emis_shifting_ag_biomass_soil_dir,
                               cn.gross_emis_forestry_biomass_soil_dir,
                               cn.gross_emis_wildfire_biomass_soil_dir,
                               cn.gross_emis_urban_biomass_soil_dir,
                               cn.gross_emis_no_driver_biomass_soil_dir,
                               cn.gross_emis_all_gases_all_drivers_biomass_soil_dir,
                               cn.gross_emis_co2_only_all_drivers_biomass_soil_dir,
                               cn.gross_emis_non_co2_all_drivers_biomass_soil_dir,
                               cn.gross_emis_nodes_biomass_soil_dir]

        else:
            output_dir_list = output_dir_list + [cn.gross_emis_commod_soil_only_dir,
                                   cn.gross_emis_shifting_ag_soil_only_dir,
                                   cn.gross_emis_forestry_soil_only_dir,
                                   cn.gross_emis_wildfire_soil_only_dir,
                                   cn.gross_emis_urban_soil_only_dir,
                                   cn.gross_emis_no_driver_soil_only_dir,
                                   cn.gross_emis_all_gases_all_drivers_soil_only_dir,
                                   cn.gross_emis_co2_only_all_drivers_soil_only_dir,
                                   cn.gross_emis_non_co2_all_drivers_soil_only_dir,
                                   cn.gross_emis_nodes_soil_only_dir]

    output_dir_list = output_dir_list + [cn.net_flux_dir, cn.cumul_gain_AGCO2_BGCO2_all_types_per_pixel_dir,
                                         cn.gross_emis_all_gases_all_drivers_biomass_soil_per_pixel_dir,
                                         cn.net_flux_per_pixel_dir]


    # Output patterns aren't actually used in the script-- here just for reference.
    output_pattern_list = [
        cn.pattern_model_extent,
        cn.pattern_age_cat_IPCC,
        cn.pattern_annual_gain_AGB_IPCC_defaults, cn.pattern_annual_gain_BGB_IPCC_defaults, cn.pattern_stdev_annual_gain_AGB_IPCC_defaults,
        cn.pattern_removal_forest_type,
        cn.pattern_annual_gain_AGC_all_types, cn.pattern_annual_gain_BGC_all_types,
        cn.pattern_annual_gain_AGC_BGC_all_types, cn.pattern_stdev_annual_gain_AGC_all_types,
        cn.pattern_gain_year_count,
        cn.pattern_cumul_gain_AGCO2_all_types, cn.pattern_cumul_gain_BGCO2_all_types,
        cn.pattern_cumul_gain_AGCO2_BGCO2_all_types
    ]

    # Prepends the mangrove and US output pattern if mangroves are included
    if 'annual_removals_mangrove' in actual_stages:

        output_pattern_list = [cn.pattern_annual_gain_AGB_mangrove, cn.pattern_annual_gain_BGB_mangrove,
                               cn.pattern_stdev_annual_gain_AGB_mangrove] + output_pattern_list

    if 'annual_removals_us' in actual_stages:

        output_pattern_list = [cn.pattern_annual_gain_AGC_BGC_natrl_forest_US,
                               cn.pattern_stdev_annual_gain_AGC_BGC_natrl_forest_US] + output_pattern_list

    # Adds the soil carbon patterns depending on which carbon emitted_pools are being generated: 2000 and/or emissions year
    if 'carbon_pools' in actual_stages:
        if 'loss' in carbon_pool_extent:
            output_pattern_list = output_pattern_list + [cn.pattern_AGC_emis_year, cn.pattern_BGC_emis_year,
                                                 cn.pattern_deadwood_emis_year_2000, cn.pattern_litter_emis_year_2000,
                                                 cn.pattern_soil_C_emis_year_2000, cn.pattern_total_C_emis_year]

        if '2000' in carbon_pool_extent:
            output_pattern_list = output_pattern_list + [cn.pattern_AGC_2000, cn.pattern_BGC_2000,
                                                 cn.pattern_deadwood_2000, cn.pattern_litter_2000,
                                                 cn.pattern_soil_C_full_extent_2000, cn.pattern_total_C_2000]

    # Adds the biomass_soil output patterns or the soil_only output directories depending on the model run
    if 'gross_emissions' in actual_stages:
        if emitted_pools == 'biomass_soil':
            output_pattern_list = output_pattern_list + [cn.pattern_gross_emis_commod_biomass_soil,
                                                      cn.pattern_gross_emis_shifting_ag_biomass_soil,
                                                      cn.pattern_gross_emis_forestry_biomass_soil,
                                                      cn.pattern_gross_emis_wildfire_biomass_soil,
                                                      cn.pattern_gross_emis_urban_biomass_soil,
                                                      cn.pattern_gross_emis_no_driver_biomass_soil,
                                                      cn.pattern_gross_emis_co2_only_all_drivers_biomass_soil,
                                                      cn.pattern_gross_emis_non_co2_all_drivers_biomass_soil,
                                                      cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil]

        else:
            output_pattern_list = output_pattern_list + [cn.pattern_gross_emis_commod_soil_only,
                                       cn.pattern_gross_emis_shifting_ag_soil_only,
                                       cn.pattern_gross_emis_forestry_soil_only,
                                       cn.pattern_gross_emis_wildfire_soil_only,
                                       cn.pattern_gross_emis_urban_soil_only,
                                       cn.pattern_gross_emis_no_driver_soil_only,
                                       cn.pattern_gross_emis_all_gases_all_drivers_soil_only,
                                       cn.pattern_gross_emis_co2_only_all_drivers_soil_only,
                                       cn.pattern_gross_emis_non_co2_all_drivers_soil_only,
                                       cn.pattern_gross_emis_nodes_soil_only]

    output_pattern_list = output_pattern_list + [cn.pattern_net_flux,
                                                cn.pattern_cumul_gain_AGCO2_BGCO2_all_types_per_pixel,
                                                cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil_per_pixel,
                                                cn.pattern_net_flux_per_pixel]


    # Creates tiles of annual AGB and BGB gain rate and AGB stdev for mangroves using the standard model
    # removal function
    if 'annual_removals_mangrove' in actual_stages:

        uu.print_log(":::::Creating tiles of annual removals for mangrove")
        start = datetime.datetime.now()

        mp_annual_gain_rate_mangrove(sensit_type, tile_id_list, run_date = run_date)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.check_storage()
        uu.print_log(":::::Processing time for annual_gain_rate_mangrove:", elapsed_time, "\n")


    # Creates tiles of annual AGC+BGC gain rate and AGC stdev for US-specific removals using the standard model
    # removal function
    if 'annual_removals_us' in actual_stages:

        uu.print_log(":::::Creating tiles of annual removals for US")
        start = datetime.datetime.now()

        mp_US_removal_rates(sensit_type, tile_id_list, run_date = run_date)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.check_storage()
        uu.print_log(":::::Processing time for annual_gain_rate_us:", elapsed_time, "\n")


    # Creates model extent tiles
    if 'model_extent' in actual_stages:

        uu.print_log(":::::Creating tiles of model extent")
        start = datetime.datetime.now()

        mp_model_extent(sensit_type, tile_id_list, run_date = run_date)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.check_storage()
        uu.print_log(":::::Processing time for model_extent:", elapsed_time, "\n", "\n")


    # Creates age category tiles for natural forests
    if 'forest_age_category_IPCC' in actual_stages:

        uu.print_log(":::::Creating tiles of forest age categories for IPCC removal rates")
        start = datetime.datetime.now()

        mp_forest_age_category_IPCC(sensit_type, tile_id_list, run_date = run_date)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.check_storage()
        uu.print_log(":::::Processing time for forest_age_category_IPCC:", elapsed_time, "\n", "\n")


    # Creates tiles of annual AGB and BGB gain rates using IPCC Table 4.9 defaults
    if 'annual_removals_IPCC' in actual_stages:

        uu.print_log(":::::Creating tiles of annual aboveground and belowground removal rates using IPCC defaults")
        start = datetime.datetime.now()

        mp_annual_gain_rate_IPCC_defaults(sensit_type, tile_id_list, run_date = run_date)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.check_storage()
        uu.print_log(":::::Processing time for annual_gain_rate_IPCC:", elapsed_time, "\n", "\n")


    # Creates tiles of annual AGC and BGC removal factors for the entire model, combining removal factors from all forest types
    if 'annual_removals_all_forest_types' in actual_stages:
        uu.print_log(":::::Creating tiles of annual aboveground and belowground removal rates for all forest types")
        start = datetime.datetime.now()

        mp_annual_gain_rate_AGC_BGC_all_forest_types(sensit_type, tile_id_list, run_date=run_date)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.check_storage()
        uu.print_log(":::::Processing time for annual_gain_rate_AGC_BGC_all_forest_types:", elapsed_time, "\n", "\n")


    # Creates tiles of the number of years of removals for all model pixels (across all forest types)
    if 'gain_year_count' in actual_stages:

        uu.print_log(":::::Freeing up memory for gain year count creation by deleting unneeded tiles")
        tiles_to_delete = []
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_mangrove_biomass_2000)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_WHRC_biomass_2000_unmasked)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_annual_gain_AGB_mangrove)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_annual_gain_BGB_mangrove)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_annual_gain_AGC_BGC_natrl_forest_Europe)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_annual_gain_AGC_BGC_planted_forest_unmasked)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_annual_gain_AGC_BGC_natrl_forest_US)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_annual_gain_AGC_natrl_forest_young)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_age_cat_IPCC)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_annual_gain_AGB_IPCC_defaults)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_annual_gain_BGB_IPCC_defaults)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_annual_gain_AGC_BGC_all_types)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_ifl_primary)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_planted_forest_type_unmasked)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_plant_pre_2000)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_stdev_annual_gain_AGB_mangrove)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_stdev_annual_gain_AGC_BGC_natrl_forest_Europe)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_stdev_annual_gain_AGC_BGC_planted_forest_unmasked)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_stdev_annual_gain_AGC_BGC_natrl_forest_US)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_stdev_annual_gain_AGC_natrl_forest_young)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_stdev_annual_gain_AGB_IPCC_defaults)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_stdev_annual_gain_AGC_all_types)))
        uu.print_log("  Deleting", len(tiles_to_delete), "tiles...")

        for tile_to_delete in tiles_to_delete:
            os.remove(tile_to_delete)
        uu.print_log(":::::Deleted unneeded tiles")
        uu.check_storage()

        uu.print_log(":::::Creating tiles of gain year count for all removal pixels")
        start = datetime.datetime.now()

        mp_gain_year_count_all_forest_types(sensit_type, tile_id_list, run_date = run_date)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.check_storage()
        uu.print_log(":::::Processing time for gain_year_count:", elapsed_time, "\n", "\n")


    # Creates tiles of gross removals for all forest types (aboveground, belowground, and above+belowground)
    if 'gross_removals_all_forest_types' in actual_stages:

        uu.print_log(":::::Creating gross removals for all forest types combined (above + belowground) tiles'")
        start = datetime.datetime.now()

        mp_gross_removals_all_forest_types(sensit_type, tile_id_list, run_date = run_date)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.check_storage()
        uu.print_log(":::::Processing time for gross_removals_all_forest_types:", elapsed_time, "\n", "\n")


    # Creates carbon emitted_pools in loss year
    if 'carbon_pools' in actual_stages:

        uu.print_log(":::::Freeing up memory for carbon pool creation by deleting unneeded tiles")
        tiles_to_delete = []
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_model_extent)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_annual_gain_AGB_mangrove)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_annual_gain_BGB_mangrove)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_annual_gain_AGC_BGC_natrl_forest_Europe)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_annual_gain_AGC_BGC_planted_forest_unmasked)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_annual_gain_AGC_BGC_natrl_forest_US)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_annual_gain_AGC_natrl_forest_young)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_age_cat_IPCC)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_annual_gain_AGB_IPCC_defaults)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_annual_gain_BGB_IPCC_defaults)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_annual_gain_BGC_all_types)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_annual_gain_AGC_BGC_all_types)))
        tiles_to_delete.extend(glob.glob('*growth_years*tif'))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_gain_year_count)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_cumul_gain_BGCO2_all_types)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_cumul_gain_AGCO2_BGCO2_all_types)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_ifl_primary)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_planted_forest_type_unmasked)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_plant_pre_2000)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_stdev_annual_gain_AGB_mangrove)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_stdev_annual_gain_AGC_BGC_natrl_forest_Europe)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_stdev_annual_gain_AGC_BGC_planted_forest_unmasked)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_stdev_annual_gain_AGC_BGC_natrl_forest_US)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_stdev_annual_gain_AGC_natrl_forest_young)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_stdev_annual_gain_AGB_IPCC_defaults)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_stdev_annual_gain_AGC_all_types)))
        uu.print_log("  Deleting", len(tiles_to_delete), "tiles...")

        for tile_to_delete in tiles_to_delete:
            os.remove(tile_to_delete)
        uu.print_log(":::::Deleted unneeded tiles")
        uu.check_storage()

        uu.print_log(":::::Creating carbon pool tiles")
        start = datetime.datetime.now()

        mp_create_carbon_pools(sensit_type, tile_id_list, carbon_pool_extent, run_date = run_date)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.check_storage()
        uu.print_log(":::::Processing time for create_carbon_pools:", elapsed_time, "\n", "\n")


    # Creates gross emissions tiles by driver, gas, and all emissions combined
    if 'gross_emissions' in actual_stages:

        uu.print_log(":::::Freeing up memory for gross emissions creation by deleting unneeded tiles")
        tiles_to_delete = []
        # tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_removal_forest_type)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_AGC_2000)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_BGC_2000)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_deadwood_2000)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_litter_2000)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_total_C_2000)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_elevation)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_precip)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_annual_gain_AGC_all_types)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_cumul_gain_AGCO2_all_types)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_cont_eco_processed)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_WHRC_biomass_2000_unmasked)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_mangrove_biomass_2000)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_removal_forest_type)))
        uu.print_log("  Deleting", len(tiles_to_delete), "tiles...")

        uu.print_log(tiles_to_delete)

        for tile_to_delete in tiles_to_delete:
            os.remove(tile_to_delete)
        uu.print_log(":::::Deleted unneeded tiles")
        uu.check_storage()

        uu.print_log(":::::Creating gross emissions tiles")
        start = datetime.datetime.now()

        mp_calculate_gross_emissions(sensit_type, tile_id_list, emitted_pools, run_date = run_date)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.check_storage()
        uu.print_log(":::::Processing time for gross_emissions:", elapsed_time, "\n", "\n")


    # Creates net flux tiles (gross emissions - gross removals)
    if 'net_flux' in actual_stages:

        uu.print_log(":::::Freeing up memory for net flux creation by deleting unneeded tiles")
        tiles_to_delete = []
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_loss)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_gross_emis_non_co2_all_drivers_biomass_soil)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_gross_emis_co2_only_all_drivers_biomass_soil)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_gross_emis_commod_biomass_soil)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_gross_emis_shifting_ag_biomass_soil)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_gross_emis_forestry_biomass_soil)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_gross_emis_wildfire_biomass_soil)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_gross_emis_urban_biomass_soil)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_gross_emis_no_driver_biomass_soil)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_gross_emis_nodes_biomass_soil)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_AGC_emis_year)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_BGC_emis_year)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_deadwood_emis_year_2000)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_litter_emis_year_2000)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_soil_C_emis_year_2000)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_total_C_emis_year)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_peat_mask)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_ifl_primary)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_planted_forest_type_unmasked)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_drivers)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_climate_zone)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_bor_tem_trop_processed)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_burn_year)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_plant_pre_2000)))
        uu.print_log("  Deleting", len(tiles_to_delete), "tiles...")

        for tile_to_delete in tiles_to_delete:
            os.remove(tile_to_delete)
        uu.print_log(":::::Deleted unneeded tiles")
        uu.check_storage()

        uu.print_log(":::::Creating net flux tiles")
        start = datetime.datetime.now()

        mp_net_flux(sensit_type, tile_id_list, run_date = run_date)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.check_storage()
        uu.print_log(":::::Processing time for net_flux:", elapsed_time, "\n", "\n")


    # Aggregates gross emissions, gross removals, and net flux to coarser resolution.
    # For sensitivity analyses, creates percent difference and sign change maps compared to standard model net flux.
    if 'aggregate' in actual_stages:

        uu.print_log(":::::Creating 4x4 km aggregate maps")
        start = datetime.datetime.now()

        mp_aggregate_results_to_4_km(sensit_type, thresh, tile_id_list, std_net_flux = std_net_flux, run_date = run_date)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.check_storage()
        uu.print_log(":::::Processing time for aggregate:", elapsed_time, "\n", "\n")


    # Converts gross emissions, gross removals and net flux from per hectare rasters to per pixel rasters
    if 'per_pixel_results' in actual_stages:

        uu.print_log(":::::Creating per pixel versions of main model outputs")
        start = datetime.datetime.now()

        mp_output_per_pixel(sensit_type, tile_id_list, run_date = run_date)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.check_storage()
        uu.print_log(":::::Processing time for per pixel raster creation:", elapsed_time, "\n", "\n")


    uu.print_log(":::::Counting tiles output to each folder")

    # Modifies output directory names to make them match those used during the model run.
    # The tiles in each of these directories and counted and logged.
    # If the model run isn't the standard one, the output directory and file names are changed
    if sensit_type != 'std':
        uu.print_log("Modifying output directory and file name pattern based on sensitivity analysis")
        output_dir_list = uu.alter_dirs(sensit_type, output_dir_list)

    # Changes the date in the output directories. This date was used during the model run.
    # This replaces the date in constants_and_names.
    if run_date:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, run_date)

    for output in output_dir_list:

        tile_count = uu.count_tiles_s3(output)-1
        uu.print_log("Total tiles in", output, ": ", tile_count)


    script_end = datetime.datetime.now()
    script_elapsed_time = script_end - script_start
    uu.print_log(":::::Processing time for entire run:", script_elapsed_time, "\n")


if __name__ == '__main__':
    main()