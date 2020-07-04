'''
git clone https://github.com/wri/carbon-budget
spotutil new r4.16xlarge dgibbs_wri --disk_size 1024
c++ /usr/local/app/emissions/cpp_util/calc_gross_emissions_generic.cpp -o /usr/local/app/emissions/cpp_util/calc_gross_emissions_generic.exe -lgdal
python run_full_model.py -t std -s forest_age_category_natrl_forest -r false -d 20200327 -l 00N_000E -ce loss -p biomass_soil -tcd 30
python run_full_model.py -t std -s all -r true -d 20200327 -l all -ce loss -p biomass_soil -tcd 30 -ma true -pl true
'''

import argparse
import os
import glob
import datetime
import logging
import constants_and_names as cn
import universal_util as uu
from gain.mp_forest_age_category_natrl_forest import mp_forest_age_category_natrl_forest
from gain.mp_gain_year_count_mangrove import mp_gain_year_count_mangrove
from gain.mp_annual_gain_rate_mangrove import mp_annual_gain_rate_mangrove
from gain.mp_cumulative_gain_mangrove import mp_cumulative_gain_mangrove
from gain.mp_gain_year_count_planted_forest import mp_gain_year_count_planted_forest
from gain.mp_annual_gain_rate_planted_forest import mp_annual_gain_rate_planted_forest
from gain.mp_cumulative_gain_planted_forest import mp_cumulative_gain_planted_forest
from gain.mp_gain_year_count_natrl_forest import mp_gain_year_count_natrl_forest
from gain.mp_annual_gain_rate_natrl_forest import mp_annual_gain_rate_natrl_forest
from gain.mp_cumulative_gain_natrl_forest import mp_cumulative_gain_natrl_forest
from gain.mp_merge_cumulative_annual_gain_all_forest_types import mp_merge_cumulative_annual_gain_all_forest_types
from carbon_pools.mp_create_carbon_pools import mp_create_carbon_pools
from emissions.mp_calculate_gross_emissions import mp_calculate_gross_emissions
from analyses.mp_net_flux import mp_net_flux
from analyses.mp_aggregate_results_to_10_km import mp_aggregate_results_to_10_km

def main ():

    os.chdir(cn.docker_base_dir)

    # List of possible model stages to run (not including mangrove and planted forest stages)
    model_stages = ['all', 'forest_age_category_natrl_forest', 'gain_year_count_natrl_forest',
                    'annual_gain_rate_natrl_forest', 'cumulative_gain_natrl_forest', 'removals_merged',
                    'carbon_pools', 'gross_emissions',
                    'net_flux', 'aggregate']


    # The argument for what kind of model run is being done: standard conditions or a sensitivity analysis run
    parser = argparse.ArgumentParser(description='Create tiles of the number of years of carbon gain for mangrove forests')
    parser.add_argument('--model-type', '-t', required=True, help='{}'.format(cn.model_type_arg_help))
    parser.add_argument('--stages', '-s', required=True,
                        help='Stages of creating Brazil legal Amazon-specific gross cumulative removals. Options are {}'.format(model_stages))
    parser.add_argument('--run-through', '-r', required=True,
                        help='Options: true or false. true: run named stage and following stages. false: run only named stage.')
    parser.add_argument('--run-date', '-d', required=True,
                        help='Date of run. Must be format YYYYMMDD.')
    parser.add_argument('--tile-id-list', '-l', required=True,
                        help='List of tile ids to use in the model. Should be of form 00N_110E or 00N_110E,00N_120E or all.')
    parser.add_argument('--carbon-pool-extent', '-ce', required=False,
                        help='Extent over which carbon pools should be calculated: loss or 2000')
    parser.add_argument('--pools-to-use', '-p', required=False,
                        help='Options are soil_only or biomass_soil. Former only considers emissions from soil. Latter considers emissions from biomass and soil.')
    parser.add_argument('--tcd-threshold', '-tcd', required=False,
                        help='Tree cover density threshold above which pixels will be included in the aggregation.')
    parser.add_argument('--std-net-flux-aggreg', '-sagg', required=False,
                        help='The s3 standard model net flux aggregated tif, for comparison with the sensitivity analysis map')
    parser.add_argument('--mangroves', '-ma', required=False,
                        help='Include mangrove annual gain rate, gain year count, and cumulative gain in stages to run. true or false.')
    parser.add_argument('--plantations', '-pl', required=False,
                        help='Include planted forest annual gain rate, gain year count, and cumulative gain in stages to run. true or false.')
    args = parser.parse_args()

    sensit_type = args.model_type
    stage_input = args.stages
    run_through = args.run_through
    run_date = args.run_date
    tile_id_list = args.tile_id_list
    carbon_pool_extent = args.carbon_pool_extent
    pools = args.pools_to_use
    thresh = args.tcd_threshold
    if thresh is not None:
        thresh = int(thresh)
    std_net_flux = args.std_net_flux_aggreg
    include_mangroves = args.mangroves
    include_plantations = args.plantations

    # Working directory
    working_dir = os.getcwd()

    # Start time for script
    script_start = datetime.datetime.now()

    # Create the output log
    uu.initiate_log(tile_id_list=tile_id_list, sensit_type=sensit_type, run_date=run_date, stage_input=stage_input, run_through=run_through,
                    carbon_pool_extent=carbon_pool_extent, pools=pools, thresh=thresh, std_net_flux=std_net_flux,
                    include_mangroves=include_mangroves, include_plantations=include_plantations)


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
                                       include_mangroves = include_mangroves, include_plantations = include_plantations)
    uu.print_log("Analysis stages to run:", actual_stages)


    # Checks whether the sensitivity analysis argument is valid
    uu.check_sensit_type(sensit_type)

    # Checks if the carbon pool type is specified if the stages to run includes carbon pool generation.
    # Does this up front so the user knows before the run begins that information is missing.
    if 'carbon_pools' in actual_stages and carbon_pool_extent not in ['loss', '2000']:
        uu.exception_log('Carbon pool year not specified for carbon pool creation step')

    # Checks if the correct c++ script has been compiled for the pool option selected.
    # Does this up front so that the user is prompted to compile the C++ before the script starts running, if necessary.
    if 'gross_emissions' in actual_stages:

        if pools == 'biomass_soil':
            # Some sensitivity analyses have specific gross emissions scripts.
            # The rest of the sensitivity analyses and the standard model can all use the same, generic gross emissions script.
            if sensit_type in ['no_shifting_ag', 'convert_to_grassland']:
                if os.path.exists('{0}/calc_gross_emissions_{1}.exe'.format(cn.docker_tmp, sensit_type)):
                    uu.print_log("C++ for {} already compiled.".format(sensit_type))
                else:
                    uu.exception_log('Must compile standard {} model C++...'.format(sensit_type))
            else:
                if os.path.exists('{0}/calc_gross_emissions_generic.exe'.format(cn.docker_tmp)):
                    uu.print_log("C++ for generic emissions already compiled.")
                else:
                    uu.exception_log('Must compile generic emissions C++...')

        elif (pools == 'soil_only') & (sensit_type == 'std'):
            if os.path.exists('{0}/calc_gross_emissions_soil_only.exe'.format(cn.docker_tmp)):
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
    # Otherwise, check that the tile list argument is valid
    else:
        tile_id_list = uu.tile_id_list_check(tile_id_list)


    # List of output directories and output file name patterns.
    # Not actually used in the script-- here just for reference.
    raw_output_dir_list = [
                       cn.age_cat_natrl_forest_dir, cn.gain_year_count_natrl_forest_dir,
                       cn.annual_gain_AGB_natrl_forest_dir, cn.annual_gain_BGB_natrl_forest_dir,
                       cn.cumul_gain_AGCO2_natrl_forest_dir, cn.cumul_gain_BGCO2_natrl_forest_dir,
                       cn.annual_gain_AGB_BGB_all_types_dir, cn.cumul_gain_AGCO2_BGCO2_all_types_dir,
                       cn.AGC_emis_year_dir, cn.BGC_emis_year_dir, cn.deadwood_emis_year_2000_dir,
                       cn.litter_emis_year_2000_dir, cn.soil_C_emis_year_2000_dir, cn.total_C_emis_year_dir,
                       cn.net_flux_dir
                       ]

    raw_output_pattern_list = [
                           cn.pattern_age_cat_natrl_forest, cn.pattern_gain_year_count_natrl_forest,
                           cn.pattern_annual_gain_AGB_natrl_forest, cn.pattern_annual_gain_BGB_natrl_forest,
                           cn.pattern_cumul_gain_AGCO2_natrl_forest, cn.pattern_cumul_gain_BGCO2_natrl_forest,
                           cn.pattern_annual_gain_AGB_BGB_all_types, cn.pattern_cumul_gain_AGCO2_BGCO2_all_types,
                           cn.pattern_AGC_emis_year, cn.pattern_BGC_emis_year, cn.pattern_deadwood_emis_year_2000,
                           cn.pattern_litter_emis_year_2000, cn.pattern_soil_C_emis_year_2000, cn.pattern_total_C_emis_year,
                           cn.pattern_net_flux
                           ]


    # Creates tiles of annual AGB and BGB gain rate for mangroves using the standard model
    # removal function
    if 'annual_gain_rate_mangrove' in actual_stages:

        uu.print_log(":::::Creating annual removals for mangrove tiles")
        start = datetime.datetime.now()

        mp_annual_gain_rate_mangrove(sensit_type, tile_id_list, run_date = run_date)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.print_log(":::::Processing time for annual_gain_rate_mangrove:", elapsed_time, "\n")


    # Creates tiles of the number of years of removals for mangroves
    if 'gain_year_count_mangrove' in actual_stages:

        uu.print_log(":::::Creating gain year count for mangrove tiles")
        start = datetime.datetime.now()

        mp_gain_year_count_mangrove(sensit_type, tile_id_list, run_date = run_date)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.print_log(":::::Processing time for gain_year_count_mangrove:", elapsed_time, "\n", "\n")


    # Creates tiles of cumulative AGCO2 and BGCO2 gain rate for mangroves using the standard model
    # removal function
    if 'cumulative_gain_mangrove' in actual_stages:

        uu.print_log(":::::Creating cumulative removals for mangrove tiles")
        start = datetime.datetime.now()

        mp_cumulative_gain_mangrove(sensit_type, tile_id_list, run_date = run_date)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.print_log(":::::Processing time for cumulative_gain_mangrove:", elapsed_time, "\n", "\n")


    # Creates tiles of annual AGB and BGB gain rate for non-mangrove planted forests using the standard model
    # removal function
    if 'annual_gain_rate_planted_forest' in actual_stages:

        uu.print_log(":::::Creating annual removals for planted forest tiles")
        start = datetime.datetime.now()

        mp_annual_gain_rate_planted_forest(sensit_type, tile_id_list, run_date = run_date)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.print_log(":::::Processing time for annual_gain_rate_planted_forest:", elapsed_time, "\n", "\n")


    # Creates tiles of the number of years of removals for non-mangrove planted forests
    if 'gain_year_count_planted_forest' in actual_stages:

        uu.print_log(":::::Creating gain year count for planted forest tiles")
        start = datetime.datetime.now()

        mp_gain_year_count_planted_forest(sensit_type, tile_id_list, run_date = run_date)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.print_log(":::::Processing time for gain_year_count_planted_forest:", elapsed_time, "\n", "\n")


    # Creates tiles of cumulative AGCO2 and BGCO2 gain rate for non-mangrove planted forests using the standard model
    # removal function
    if 'cumulative_gain_planted_forest' in actual_stages:

        uu.print_log(":::::Creating cumulative removals for planted forest tiles")
        start = datetime.datetime.now()

        mp_cumulative_gain_planted_forest(sensit_type, tile_id_list, run_date = run_date)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.print_log(":::::Processing time for cumulative_gain_planted_forest:", elapsed_time, "\n", "\n")


    # Creates age category tiles for natural forests
    if 'forest_age_category_natrl_forest' in actual_stages:

        uu.print_log(":::::Creating forest age category for natural forest tiles")
        start = datetime.datetime.now()

        mp_forest_age_category_natrl_forest(sensit_type, tile_id_list, run_date = run_date)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.print_log(":::::Processing time for forest_age_category_natrl_forest:", elapsed_time, "\n", "\n")


    # Creates tiles of the number of years of removals for natural forests
    if 'gain_year_count_natrl_forest' in actual_stages:

        uu.print_log(":::::Creating gain year count for natural forest tiles")
        start = datetime.datetime.now()

        mp_gain_year_count_natrl_forest(sensit_type, tile_id_list, run_date = run_date)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.print_log(":::::Processing time for gain_year_count_natrl_forest:", elapsed_time, "\n", "\n")


    # Creates tiles of annual AGB and BGB gain rate for non-mangrove, non-planted forest using the standard model
    # removal function
    if 'annual_gain_rate_natrl_forest' in actual_stages:

        uu.print_log(":::::Creating annual removals for natural forest tiles")
        start = datetime.datetime.now()

        mp_annual_gain_rate_natrl_forest(sensit_type, tile_id_list, run_date = run_date)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.print_log(":::::Processing time for annual_gain_rate_natrl_forest:", elapsed_time, "\n", "\n")


    # Creates tiles of cumulative AGCO2 and BGCO2 gain rate for non-mangrove, non-planted forest using the standard model
    # removal function
    if 'cumulative_gain_natrl_forest' in actual_stages:

        uu.print_log(":::::Creating cumulative removals for natural forest tiles")
        start = datetime.datetime.now()

        mp_cumulative_gain_natrl_forest(sensit_type, tile_id_list, run_date = run_date)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.print_log(":::::Processing time for cumulative_gain_natrl_forest:", elapsed_time, "\n", "\n")


    # Creates tiles of annual gain rate and cumulative removals for all forest types (above + belowground)
    if 'removals_merged' in actual_stages:

        uu.print_log(":::::Creating annual and cumulative removals for all forest types combined (above + belowground) tiles'")
        start = datetime.datetime.now()

        mp_merge_cumulative_annual_gain_all_forest_types(sensit_type, tile_id_list, run_date = run_date)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.print_log(":::::Processing time for removals_merged:", elapsed_time, "\n", "\n")


    # Creates carbon pools in loss year
    if 'carbon_pools' in actual_stages:

        uu.print_log(":::::Freeing up memory for carbon pool creation by deleting unneeded tiles")
        tiles_to_delete = glob.glob('*growth_years*tif')                 # Any forest type
        tiles_to_delete.extend(glob.glob('*gain_year_count*tif'))        # Any forest type
        tiles_to_delete.extend(glob.glob('*annual_gain_rate_BGB*tif'))   # Any forest type
        tiles_to_delete.extend(glob.glob('*annual_gain_rate_BGCO2*tif')) # Any forest type
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_annual_gain_AGB_BGB_all_types)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_cont_eco_processed)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_US_forest_age_cat_processed)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_tcd)))  # This is used again for aggregating results, so it'll have to be downloaded again
        uu.print_log("  Deleting", len(tiles_to_delete), "tiles...")

        for tile_to_delete in tiles_to_delete:
            os.remove(tile_to_delete)
        uu.print_log(":::::Deleted unneeded tiles")

        uu.print_log(":::::Creating emissions year carbon pools tiles")
        start = datetime.datetime.now()

        mp_create_carbon_pools(sensit_type, tile_id_list, carbon_pool_extent, run_date = run_date)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.print_log(":::::Processing time for carbon_pools:", elapsed_time, "\n", "\n")


    # Creates gross emissions tiles by driver, gas, and all emissions combined
    if 'gross_emissions' in actual_stages:

        uu.print_log(":::::Freeing up memory for gross emissions creation by deleting unneeded tiles")
        tiles_to_delete = glob.glob('*{}*tif'.format(cn.pattern_elevation))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_precip)))  # Any forest type
        tiles_to_delete.extend(glob.glob('*annual_gain_rate_AGB*tif'))  # Any forest type
        tiles_to_delete.extend(glob.glob('*annual_gain_rate_AGCO2*tif'))  # Any forest type
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_annual_gain_AGB_BGB_all_types)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_cont_eco_processed)))
        tiles_to_delete.extend(glob.glob('*{}*tif'.format(cn.pattern_WHRC_biomass_2000_unmasked)))
        uu.print_log("  Deleting", len(tiles_to_delete), "tiles...")

        for tile_to_delete in tiles_to_delete:
            os.remove(tile_to_delete)
        uu.print_log(":::::Deleted unneeded tiles")

        uu.print_log(":::::Creating gross emissions tiles")
        start = datetime.datetime.now()

        mp_calculate_gross_emissions(sensit_type, tile_id_list, pools, run_date = run_date, working_dir = working_dir)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.print_log(":::::Processing time for gross_emissions:", elapsed_time, "\n", "\n")


    # Creates net flux tiles (gross emissions - gross removals)
    if 'net_flux' in actual_stages:

        uu.print_log(":::::Creating net flux tiles")
        start = datetime.datetime.now()

        mp_net_flux(sensit_type, tile_id_list, run_date = run_date)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.print_log(":::::Processing time for net_flux:", elapsed_time, "\n", "\n")


    # Aggregates gross emissions, gross removals, and net flux to coarser resolution.
    # For sensitivity analyses, creates percent difference and sign change maps compared to standard model net flux.
    if 'aggregate' in actual_stages:

        uu.print_log(":::::Creating 5km aggregate maps")
        start = datetime.datetime.now()

        mp_aggregate_results_to_10_km(sensit_type, thresh, tile_id_list, std_net_flux = std_net_flux, run_date = run_date)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.print_log(":::::Processing time for aggregate:", elapsed_time, "\n", "\n")


    script_end = datetime.datetime.now()
    script_elapsed_time = script_end - script_start
    uu.print_log(":::::Processing time for entire run:", script_elapsed_time, "\n")


if __name__ == '__main__':
    main()