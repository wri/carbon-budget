"""
Clone repository:
git clone https://github.com/wri/carbon-budget

Create spot machine using spotutil:
spotutil new r5d.24xlarge dgibbs_wri

Build Docker container:
docker build . -t gfw/carbon-budget

Enter Docker container:
docker run --rm -it -e AWS_SECRET_ACCESS_KEY=[] -e AWS_ACCESS_KEY_ID=[] gfw/carbon-budget

Run: standard model; save intermediate outputs; run model from annual_removals_IPCC;
upload to folder with date 20239999; run 00N_000E; get carbon pools at time of loss; add a log note;
do not upload outputs to s3; use multiprocessing (implicit because no -sp flag);
only run listed stage (implicit because no -r flag)
python -m run_full_model -t std -si -s annual_removals_IPCC -nu -l 00N_000E -ce loss -ln "00N_000E test"

Run: standard model; save intermediate outputs; run model from annual_removals_IPCC; run all subsequent model stages;
do not upload outputs to s3; run 00N_000E; get carbon pools at time of loss; add a log note;
upload outputs to s3 (implicit because no -nu flag); use multiprocessing (implicit because no -sp flag)
python -m run_full_model -t std -si -s annual_removals_IPCC -r -nu -l 00N_000E -ce loss -ln "00N_000E test"

Run: standard model; save intermediate outputs; run model from the beginning; run all model stages;
upload to folder with date 20239999; run 00N_000E; get carbon pools at time of loss; add a log note;
upload outputs to s3 (implicit because no -nu flag); use multiprocessing (implicit because no -sp flag)
python -m run_full_model -t std -si -s all -r -d 20239999 -l 00N_000E -ce loss -ln "00N_000E test"

Run: standard model; save intermediate outputs; run model from the beginning; run all model stages;
upload to folder with date 20239999; run 00N_000E; get carbon pools at time of loss; add a log note;
do not upload outputs to s3; use multiprocessing (implicit because no -sp flag)
python -m run_full_model -t std -si -s all -r -d 20239999 -l 00N_000E -ce loss -ln "00N_000E test" -nu

Run: standard model; run model from the beginning; run all model stages;
upload to folder with date 20239999; run 00N_000E; get carbon pools at time of loss; add a log note;
do not upload outputs to s3; use singleprocessing;
do not save intermediate outputs (implicit because no -si flag)
python -m run_full_model -t std -s all -r -nu -d 20239999 -l 00N_000E,00N_010E -ce loss -sp -ln "Two tile test"

FULL STANDARD MODEL RUN: standard model; save intermediate outputs; run model from the beginning; run all model stages;
run all tiles; get carbon pools at time of loss; add a log note;
upload outputs to s3 (implicit because no -nu flag); use multiprocessing (implicit because no -sp flag)
python -m run_full_model -t std -si -s all -r -l all -ce loss -ln "Running all tiles"
"""

import argparse
import datetime
import glob
import os

import constants_and_names as cn
import universal_util as uu
from data_prep.mp_model_extent import mp_model_extent
from removals.mp_annual_gain_rate_mangrove import mp_annual_gain_rate_mangrove
from removals.mp_US_removal_rates import mp_US_removal_rates
from removals.mp_forest_age_category_IPCC import mp_forest_age_category_IPCC
from removals.mp_annual_gain_rate_IPCC_defaults import mp_annual_gain_rate_IPCC_defaults
from removals.mp_annual_gain_rate_AGC_BGC_all_forest_types import mp_annual_gain_rate_AGC_BGC_all_forest_types
from removals.mp_gain_year_count_all_forest_types import mp_gain_year_count_all_forest_types
from removals.mp_gross_removals_all_forest_types import mp_gross_removals_all_forest_types
from carbon_pools.mp_create_carbon_pools import mp_create_carbon_pools
from emissions.mp_calculate_gross_emissions import mp_calculate_gross_emissions
from analyses.mp_net_flux import mp_net_flux
from analyses.mp_derivative_outputs import mp_derivative_outputs

def main ():
    """
    Runs the entire forest GHG flux model or a subset of stages
    :return: Sets of output tiles for the selected stages
    """

    os.chdir(cn.docker_tile_dir)

    # List of possible model stages to run (not including mangrove and planted forest stages)
    model_stages = ['all', 'model_extent', 'forest_age_category_IPCC', 'annual_removals_IPCC',
                    'annual_removals_all_forest_types', 'gain_year_count', 'gross_removals_all_forest_types',
                    'carbon_pools', 'gross_emissions_biomass_soil', 'gross_emissions_soil_only',
                    'net_flux', 'create_derivative_outputs']


    # The argument for what kind of model run is being done: standard conditions or a sensitivity analysis run
    parser = argparse.ArgumentParser(description='Run the full carbon flux model')
    parser.add_argument('--model-type', '-t', required=True, help=f'{cn.model_type_arg_help}')
    parser.add_argument('--stages', '-s', required=True,
                        help=f'Stages for running the flux model. Options are {model_stages}')
    parser.add_argument('--run-through', '-r', action='store_true',
                        help='If activated, run named stage and all following stages. If not activated, run the selected stage only.')
    parser.add_argument('--run-date', '-d', required=False,
                        help='Date of run. Must be format YYYYMMDD.')
    parser.add_argument('--tile-id-list', '-l', required=True,
                        help='List of tile ids to use in the model. Should be of form 00N_110E or 00N_110E,00N_120E or all.')
    parser.add_argument('--carbon-pool-extent', '-ce', required=False,
                        help='Time period for which carbon pools should be calculated: loss, 2000, loss,2000, or 2000,loss')
    parser.add_argument('--std-net-flux-aggreg', '-sagg', required=False,
                        help='The s3 standard model net flux aggregated tif, for comparison with the sensitivity analysis map')
    parser.add_argument('--mangroves', '-ma', action='store_true',
                        help='Include mangrove removal rate and standard deviation tile creation step (before model extent).')
    parser.add_argument('--us-rates', '-us', action='store_true',
                        help='Include US removal rate and standard deviation tile creation step (before model extent).')
    parser.add_argument('--no-upload', '-nu', action='store_true',
                       help='Disables uploading of outputs to s3')
    parser.add_argument('--single-processor', '-sp', action='store_true',
                       help='Uses single processing rather than multiprocessing')
    parser.add_argument('--save-intermediates', '-si', action='store_true',
                        help='Saves intermediate model outputs rather than deleting them to save storage')
    parser.add_argument('--log-note', '-ln', required=False,
                        help='Note to include in log header about model run.')
    args = parser.parse_args()

    # Sets global variables to the command line arguments
    cn.SENSIT_TYPE = args.model_type
    cn.STAGE_INPUT = args.stages
    cn.RUN_THROUGH = args.run_through
    cn.RUN_DATE = args.run_date
    cn.CARBON_POOL_EXTENT = args.carbon_pool_extent
    cn.STD_NET_FLUX = args.std_net_flux_aggreg
    cn.INCLUDE_MANGROVES = args.mangroves
    cn.INCLUDE_US = args.us_rates
    cn.NO_UPLOAD = args.no_upload
    cn.SINGLE_PROCESSOR = args.single_processor
    cn.SAVE_INTERMEDIATES = args.save_intermediates
    cn.LOG_NOTE = args.log_note

    tile_id_list = args.tile_id_list

    # Disables upload to s3 if no AWS credentials are found in environment
    if not uu.check_aws_creds():
        uu.print_log("s3 credentials not found. Uploading to s3 disabled but downloading enabled.")
        cn.NO_UPLOAD = True

    # Forces intermediate files to not be deleted if files can't be uploaded to s3.
    # Rationale is that if uploads to s3 are not occurring, intermediate files can't be downloaded during the model
    # run and therefore must exist locally.
    if cn.NO_UPLOAD:
        cn.SAVE_INTERMEDIATES = True

    # Create the output log
    uu.initiate_log(tile_id_list)

    # Checks whether the sensitivity analysis and tile_id_list arguments are valid
    uu.check_sensit_type(cn.SENSIT_TYPE)

    # Start time for script
    script_start = datetime.datetime.now()

    # Checks the validity of the model stage arguments. If either one is invalid, the script ends.
    if cn.STAGE_INPUT not in model_stages:
        uu.exception_log(f'Invalid stage selection. Please provide a stage from {model_stages}')
    else:
        pass

    # Generates the list of stages to run
    actual_stages = uu.analysis_stages(model_stages, cn.STAGE_INPUT, cn.RUN_THROUGH, cn.SENSIT_TYPE,
                                       include_mangroves = cn.INCLUDE_MANGROVES, include_us=cn.INCLUDE_US)
    uu.print_log(f'Analysis stages to run: {actual_stages}')

    # Reports how much storage is being used with files
    uu.check_storage()

    # Checks whether the sensitivity analysis argument is valid
    uu.check_sensit_type(cn.SENSIT_TYPE)

    # Checks if the carbon pool type is specified if the stages to run includes carbon pool generation.
    # Does this up front so the user knows before the run begins that information is missing.
    if ('carbon_pools' in actual_stages) & (cn.CARBON_POOL_EXTENT not in ['loss', '2000', 'loss,2000', '2000,loss']):
        uu.exception_log('Invalid carbon_pool_extent input. Please choose loss, 2000, loss,2000 or 2000,loss.')

    # If the tile_list argument is an s3 folder, the list of tiles in it is created
    if 's3://' in tile_id_list:
        tile_id_list = uu.tile_list_s3(tile_id_list, 'std')
        uu.print_log(tile_id_list)
        uu.print_log(f'There are {str(len(tile_id_list))} tiles to process', "\n")
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

    # Adds the carbon directories depending on which carbon years are being generated: 2000 and/or emissions year
    if 'carbon_pools' in actual_stages:
        if 'loss' in cn.CARBON_POOL_EXTENT:
            output_dir_list = output_dir_list + [cn.AGC_emis_year_dir, cn.BGC_emis_year_dir,
                                                 cn.deadwood_emis_year_2000_dir, cn.litter_emis_year_2000_dir,
                                                 cn.soil_C_emis_year_2000_dir, cn.total_C_emis_year_dir]

        if '2000' in cn.CARBON_POOL_EXTENT:
            output_dir_list = output_dir_list + [cn.AGC_2000_dir, cn.BGC_2000_dir,
                                                 cn.deadwood_2000_dir, cn.litter_2000_dir,
                                                 cn.soil_C_full_extent_2000_dir, cn.total_C_2000_dir]

    # Adds the biomass_soil output directories and the soil_only output directories
    output_dir_list = output_dir_list + [
                       cn.gross_emis_all_gases_all_drivers_biomass_soil_dir,
                       cn.gross_emis_co2_only_all_drivers_biomass_soil_dir,
                       cn.gross_emis_non_co2_all_drivers_biomass_soil_dir,
                       cn.gross_emis_nodes_biomass_soil_dir]
    # output_dir_list = [cn.gross_emis_all_gases_all_drivers_biomass_soil_dir,
    #                    cn.gross_emis_co2_only_all_drivers_biomass_soil_dir,
    #                    cn.gross_emis_non_co2_all_drivers_biomass_soil_dir,
    #                    cn.gross_emis_ch4_only_all_drivers_biomass_soil_dir,
    #                    cn.gross_emis_n2o_only_all_drivers_biomass_soil_dir,
    #                    cn.gross_emis_nodes_biomass_soil_dir]
    # TODO: Update after splitting non-co2 emissions

    output_dir_list = output_dir_list + [
                       cn.gross_emis_all_gases_all_drivers_soil_only_dir,
                       cn.gross_emis_co2_only_all_drivers_soil_only_dir,
                       cn.gross_emis_non_co2_all_drivers_soil_only_dir,
                       cn.gross_emis_nodes_soil_only_dir]
    # output_dir_list = [cn.gross_emis_all_gases_all_drivers_soil_only_dir,
    #                    cn.gross_emis_co2_only_all_drivers_soil_only_dir,
    #                    cn.gross_emis_non_co2_all_drivers_soil_only_dir,
    #                    cn.gross_emis_ch4_only_all_drivers_soil_only_dir,
    #                    cn.gross_emis_n2o_only_all_drivers_soil_only_dir,
    #                    cn.gross_emis_nodes_soil_only_dir]
    # TODO: Update after splitting non-co2 emissions

    # Adds the net flux output directory
    output_dir_list = output_dir_list + [cn.net_flux_dir]

    # Supplementary outputs
    output_dir_list = output_dir_list + \
                    [cn.cumul_gain_AGCO2_BGCO2_all_types_per_pixel_full_extent_dir,
                    cn.cumul_gain_AGCO2_BGCO2_all_types_forest_extent_dir,
                    cn.cumul_gain_AGCO2_BGCO2_all_types_per_pixel_forest_extent_dir,
                    cn.gross_emis_all_gases_all_drivers_biomass_soil_per_pixel_full_extent_dir,
                    cn.gross_emis_all_gases_all_drivers_biomass_soil_forest_extent_dir,
                    cn.gross_emis_all_gases_all_drivers_biomass_soil_per_pixel_forest_extent_dir,
                    cn.net_flux_per_pixel_full_extent_dir,
                    cn.net_flux_forest_extent_dir,
                    cn.net_flux_per_pixel_forest_extent_dir]


    # Creates tiles of annual AGB and BGB removals rate and AGB stdev for mangroves using the standard model
    # removal function
    if 'annual_removals_mangrove' in actual_stages:

        uu.print_log(':::::Creating tiles of annual removals for mangrove')
        start = datetime.datetime.now()

        mp_annual_gain_rate_mangrove(tile_id_list)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.check_storage()
        uu.print_log(f':::::Processing time for annual_gain_rate_mangrove: {elapsed_time}', "\n", "\n")


    # Creates tiles of annual AGC+BGC removals rate and AGC stdev for US-specific removals using the standard model
    # removal function
    if 'annual_removals_us' in actual_stages:

        uu.print_log(':::::Creating tiles of annual removals for US')
        start = datetime.datetime.now()

        mp_US_removal_rates(tile_id_list)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.check_storage()
        uu.print_log(f':::::Processing time for annual_gain_rate_us: {elapsed_time}', "\n", "\n")


    # Creates model extent tiles
    if 'model_extent' in actual_stages:

        uu.print_log(':::::Creating tiles of model extent')
        start = datetime.datetime.now()

        mp_model_extent(tile_id_list)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.check_storage()
        uu.print_log(f':::::Processing time for model_extent: {elapsed_time}', "\n", "\n")


    # Creates age category tiles for natural forests
    if 'forest_age_category_IPCC' in actual_stages:

        uu.print_log(':::::Creating tiles of forest age categories for IPCC removal rates')
        start = datetime.datetime.now()

        mp_forest_age_category_IPCC(tile_id_list)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.check_storage()
        uu.print_log(f':::::Processing time for forest_age_category_IPCC: {elapsed_time}', "\n", "\n")


    # Creates tiles of annual AGB and BGB removals rates using IPCC Table 4.9 defaults
    if 'annual_removals_IPCC' in actual_stages:

        uu.print_log(':::::Creating tiles of annual aboveground and belowground removal rates using IPCC defaults')
        start = datetime.datetime.now()

        mp_annual_gain_rate_IPCC_defaults(tile_id_list)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.check_storage()
        uu.print_log(f':::::Processing time for annual_gain_rate_IPCC: {elapsed_time}', "\n", "\n")


    # Creates tiles of annual AGC and BGC removal factors for the entire model, combining removal factors from all forest types
    if 'annual_removals_all_forest_types' in actual_stages:
        uu.print_log(':::::Creating tiles of annual aboveground and belowground removal rates for all forest types')
        start = datetime.datetime.now()

        mp_annual_gain_rate_AGC_BGC_all_forest_types(tile_id_list)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.check_storage()
        uu.print_log(f':::::Processing time for annual_gain_rate_AGC_BGC_all_forest_types: {elapsed_time}', "\n", "\n")


    # Creates tiles of the number of years of removals for all model pixels (across all forest types)
    if 'gain_year_count' in actual_stages:

        if not cn.SAVE_INTERMEDIATES:

            uu.print_log(':::::Freeing up memory for gain year count creation by deleting unneeded tiles')
            tiles_to_delete = []
            # tiles_to_delete.extend(glob.glob(f'*{cn.pattern_mangrove_biomass_2000}*tif'))
            # tiles_to_delete.extend(glob.glob(f'*{cn.pattern_WHRC_biomass_2000_unmasked}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_annual_gain_AGB_mangrove}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_annual_gain_BGB_mangrove}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_annual_gain_AGC_BGC_natrl_forest_Europe}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_annual_gain_AGC_BGC_planted_forest}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_annual_gain_AGC_BGC_natrl_forest_US}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_annual_gain_AGC_natrl_forest_young}*tif'))
            # tiles_to_delete.extend(glob.glob(f'*{cn.pattern_age_cat_IPCC}*tif'))
            # tiles_to_delete.extend(glob.glob(f'*{cn.pattern_annual_gain_AGB_IPCC_defaults}*tif'))
            # tiles_to_delete.extend(glob.glob(f'*{cn.pattern_annual_gain_BGB_IPCC_defaults}*tif'))
            # tiles_to_delete.extend(glob.glob(f'*{cn.pattern_annual_gain_AGC_BGC_all_types}*tif'))
            # tiles_to_delete.extend(glob.glob(f'*{cn.pattern_ifl_primary}*tif'))
            # tiles_to_delete.extend(glob.glob(f'*{cn.pattern_planted_forest_type_unmasked}*tif'))
            # tiles_to_delete.extend(glob.glob(f'*{cn.pattern_plant_pre_2000}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_stdev_annual_gain_AGB_mangrove}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_stdev_annual_gain_AGC_BGC_natrl_forest_Europe}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_stdev_annual_gain_AGC_BGC_planted_forest}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_stdev_annual_gain_AGC_BGC_natrl_forest_US}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_stdev_annual_gain_AGC_natrl_forest_young}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_stdev_annual_gain_AGB_IPCC_defaults}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_stdev_annual_gain_AGC_all_types}*tif'))
            uu.print_log(f'  Deleting {len(tiles_to_delete)} tiles...')

            for tile_to_delete in tiles_to_delete:
                os.remove(tile_to_delete)
            uu.print_log(':::::Deleted unneeded tiles')

        uu.check_storage()

        uu.print_log(':::::Creating tiles of gain year count for all removal pixels')
        start = datetime.datetime.now()

        mp_gain_year_count_all_forest_types(tile_id_list)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.check_storage()
        uu.print_log(f':::::Processing time for gain_year_count: {elapsed_time}', "\n", "\n")


    # Creates tiles of gross removals for all forest types (aboveground, belowground, and above+belowground)
    if 'gross_removals_all_forest_types' in actual_stages:

        uu.print_log(':::::Creating gross removals for all forest types combined (above + belowground) tiles')
        start = datetime.datetime.now()

        mp_gross_removals_all_forest_types(tile_id_list)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.check_storage()
        uu.print_log(f':::::Processing time for gross_removals_all_forest_types: {elapsed_time}', "\n", "\n")


    # Creates carbon pools in loss year
    if 'carbon_pools' in actual_stages:

        if not cn.SAVE_INTERMEDIATES:

            uu.print_log(':::::Freeing up memory for carbon pool creation by deleting unneeded tiles')
            tiles_to_delete = []
            # tiles_to_delete.extend(glob.glob(f'*{cn.pattern_model_extent}*tif'))
            # tiles_to_delete.extend(glob.glob(f'*{cn.pattern_age_cat_IPCC}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_annual_gain_AGB_IPCC_defaults}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_annual_gain_BGB_IPCC_defaults}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_annual_gain_BGC_all_types}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_annual_gain_AGC_BGC_all_types}*tif'))
            tiles_to_delete.extend(glob.glob('*growth_years*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_gain_year_count}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_cumul_gain_BGCO2_all_types}*tif'))
            # tiles_to_delete.extend(glob.glob(f'*{cn.pattern_cumul_gain_AGCO2_BGCO2_all_types}*tif'))
            # tiles_to_delete.extend(glob.glob(f'*{cn.pattern_ifl_primary}*tif'))
            # tiles_to_delete.extend(glob.glob(f'*{cn.pattern_planted_forest_type_unmasked}*tif'))
            uu.print_log(f'  Deleting {len(tiles_to_delete)} tiles...')

            for tile_to_delete in tiles_to_delete:
                os.remove(tile_to_delete)
            uu.print_log(':::::Deleted unneeded tiles')

        uu.check_storage()

        uu.print_log(':::::Creating carbon pool tiles')
        start = datetime.datetime.now()

        mp_create_carbon_pools(tile_id_list, cn.CARBON_POOL_EXTENT)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.check_storage()
        uu.print_log(f':::::Processing time for create_carbon_pools: {elapsed_time}', "\n", "\n")


    # Creates gross emissions tiles for biomass+soil by driver, gas, and all emissions combined
    if 'gross_emissions_biomass_soil' in actual_stages:

        if not cn.SAVE_INTERMEDIATES:

            uu.print_log(':::::Freeing up memory for biomass_soil gross emissions creation by deleting unneeded tiles')
            tiles_to_delete = []
            # tiles_to_delete.extend(glob.glob(f'*{cn.pattern_removal_forest_type}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_AGC_2000}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_BGC_2000}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_deadwood_2000}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_litter_2000}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_total_C_2000}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_elevation}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_precip}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_annual_gain_AGC_all_types}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_cumul_gain_AGCO2_all_types}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_cont_eco_processed}*tif'))
            # tiles_to_delete.extend(glob.glob(f'*{cn.pattern_WHRC_biomass_2000_unmasked}*tif'))
            # tiles_to_delete.extend(glob.glob(f'*{cn.pattern_mangrove_biomass_2000}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_removal_forest_type}*tif'))
            uu.print_log(f'  Deleting {len(tiles_to_delete)} tiles...')

            uu.print_log(tiles_to_delete)

            for tile_to_delete in tiles_to_delete:
                os.remove(tile_to_delete)
            uu.print_log(':::::Deleted unneeded tiles')

        uu.check_storage()

        uu.print_log(':::::Creating gross biomass_soil emissions tiles')
        start = datetime.datetime.now()

        mp_calculate_gross_emissions(tile_id_list, 'biomass_soil')

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.check_storage()
        uu.print_log(f':::::Processing time for biomass_soil gross_emissions: {elapsed_time}', "\n", "\n")


    # Creates gross emissions tiles for soil only by driver, gas, and all emissions combined
    if 'gross_emissions_soil_only' in actual_stages:

        if not cn.SAVE_INTERMEDIATES:

            uu.print_log(':::::Freeing up memory for soil_only gross emissions creation by deleting unneeded tiles')
            tiles_to_delete = []
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_gross_emis_non_co2_all_drivers_biomass_soil}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_gross_emis_co2_only_all_drivers_biomass_soil}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_gross_emis_nodes_biomass_soil}*tif'))
            uu.print_log(f'  Deleting {len(tiles_to_delete)} tiles...')

            uu.print_log(tiles_to_delete)

            for tile_to_delete in tiles_to_delete:
                os.remove(tile_to_delete)
            uu.print_log(':::::Deleted unneeded tiles')

        uu.check_storage()

        uu.print_log(':::::Creating soil_only gross emissions tiles')
        start = datetime.datetime.now()

        mp_calculate_gross_emissions(tile_id_list, 'soil_only')

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.check_storage()
        uu.print_log(f':::::Processing time for soil_only gross_emissions: {elapsed_time}', "\n", "\n")


    # Creates net flux tiles (gross emissions - gross removals)
    if 'net_flux' in actual_stages:

        if not cn.SAVE_INTERMEDIATES:

            uu.print_log(':::::Freeing up memory for net flux creation by deleting unneeded tiles')
            tiles_to_delete = []
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_gross_emis_all_gases_all_drivers_soil_only}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_gross_emis_non_co2_all_drivers_soil_only}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_gross_emis_co2_only_all_drivers_soil_only}*tif'))
            tiles_to_delete.extend(glob.glob(f'*{cn.pattern_gross_emis_nodes_soil_only}*tif'))
            uu.print_log(f'  Deleting {len(tiles_to_delete)} tiles...')

            for tile_to_delete in tiles_to_delete:
                os.remove(tile_to_delete)
            uu.print_log(':::::Deleted unneeded tiles')

        uu.check_storage()

        uu.print_log(':::::Creating net flux tiles')
        start = datetime.datetime.now()

        mp_net_flux(tile_id_list)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.check_storage()
        uu.print_log(f':::::Processing time for net_flux: {elapsed_time}', "\n", "\n")


    # Creates derivative outputs for gross emissions, gross removals, and net flux.
    # Creates forest extent and per-pixel tiles at original (0.00025x0.00025 deg) resolution and
    # creates aggregated global maps at 0.04x0.04 deg resolution.
    # For sensitivity analyses, also creates percent difference and sign change maps compared to standard model net flux.
    if 'create_derivative_outputs' in actual_stages:

        # aux.xml files need to be deleted because otherwise they'll be included in the aggregation iteration.
        # They are created by using check_and_delete_if_empty_light()
        uu.print_log(':::::Deleting any aux.xml files')
        tiles_to_delete = []
        tiles_to_delete.extend(glob.glob('*aux.xml'))

        for tile_to_delete in tiles_to_delete:
            os.remove(tile_to_delete)
        uu.print_log(f':::::Deleted {len(tiles_to_delete)} aux.xml files: {tiles_to_delete}', "\n")


        uu.print_log(':::::Creating derivative outputs: forest extent/per-pixel tiles and aggregate maps')
        start = datetime.datetime.now()

        mp_derivative_outputs(tile_id_list)

        end = datetime.datetime.now()
        elapsed_time = end - start
        uu.check_storage()
        uu.print_log(f':::::Processing time for creating derivative outputs: {elapsed_time}', "\n", "\n")


    # If no_upload flag is activated, tiles on s3 aren't counted
    if not cn.NO_UPLOAD:

        uu.print_log(':::::Counting tiles output to each folder')

        # Modifies output directory names to make them match those used during the model run.
        # The tiles in each of these directories and counted and logged.
        # If the model run isn't the standard one, the output directory and file names are changed
        if cn.SENSIT_TYPE != 'std':
            uu.print_log('Modifying output directory and file name pattern based on sensitivity analysis')
            output_dir_list = uu.alter_dirs(cn.SENSIT_TYPE, output_dir_list)

        # A date can optionally be provided by the full model script or a run of this script.
        # This replaces the date in constants_and_names.
        # Only done if output upload is enabled.
        if cn.RUN_DATE is not None and cn.NO_UPLOAD is not None:
            output_dir_list = uu.replace_output_dir_date(output_dir_list, cn.RUN_DATE)

        for output in output_dir_list:

            tile_count = uu.count_tiles_s3(output)
            uu.print_log(f'Total tiles in {output}: {tile_count}')


    script_end = datetime.datetime.now()
    script_elapsed_time = script_end - script_start
    uu.print_log(f':::::Processing time for entire run: {script_elapsed_time}', "\n")

    # If no_upload flag is not activated (by choice or by lack of AWS credentials), output is uploaded
    if not cn.NO_UPLOAD:

        uu.upload_log()


if __name__ == '__main__':
    main()
