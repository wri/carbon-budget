### This script calculates various statistics on all tiles in input folders and saves them to a txt.
### Users can input as many folders as they want for calculating statistics on each tile

import multiprocessing
import tile_statistics
from subprocess import Popen, PIPE, STDOUT, check_call
import datetime
from functools import partial
import argparse
import os
import glob
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def mp_tile_statistics(sensit_type, tile_id_list):

    os.chdir(cn.docker_tile_dir)

    # The column names for the tile summary statistics.
    # If the statistics calculations are changed in tile_statistics.py, the list here needs to be changed, too.
    headers = ['tile_id', 'tile_type', 'tile_name', 'pixel_count', 'mean', 'median', 'percentile10', 'percentile25',
               'percentile75', 'percentile90', 'min', 'max', 'sum']
    header_no_brackets = ', '.join(headers)

    tile_stats_txt = '{0}_v{1}_{2}_{3}.csv'.format(cn.tile_stats_pattern, cn.version, sensit_type, uu.date_time_today)

    # Creates the output text file with the column names
    with open(tile_stats_txt, 'w+') as f:
        f.write(header_no_brackets  +'\r\n')
    f.close()

    uu.print_log(tile_id_list)

    # Pixel area tiles-- necessary for calculating sum of pixels for any set of tiles
    uu.s3_flexible_download(cn.pixel_area_dir, cn.pattern_pixel_area, cn.docker_tile_dir, 'std', tile_id_list)

    # For downloading all tiles in selected folders
    download_dict = {
                    # cn.WHRC_biomass_2000_unmasked_dir: [cn.pattern_WHRC_biomass_2000_unmasked],
                    # cn.JPL_processed_dir: [cn.pattern_JPL_unmasked_processed],
                    # cn.mangrove_biomass_2000_dir: [cn.pattern_mangrove_biomass_2000],
                    # cn.cont_eco_dir: [cn.pattern_cont_eco_processed],

                    # cn.model_extent_dir: [cn.pattern_model_extent], # 15 = 370 GB peak
                    #
                    # # Mangrove removals
                    # cn.annual_gain_AGB_mangrove_dir: [cn.pattern_annual_gain_AGB_mangrove], # 15 = 640 GB peak
                    # cn.annual_gain_BGB_mangrove_dir: [cn.pattern_annual_gain_BGB_mangrove], # 15 = 640 GB peak
                    cn.stdev_annual_gain_AGB_mangrove_dir: [cn.pattern_stdev_annual_gain_AGB_mangrove], # 15 = 640 GB peak
                    #
                    # # European forest removals
                    # cn.annual_gain_AGC_BGC_natrl_forest_Europe_dir: [cn.pattern_annual_gain_AGC_BGC_natrl_forest_Europe], # 15 = 630 GB peak
                    # cn.stdev_annual_gain_AGC_BGC_natrl_forest_Europe_dir: [cn.pattern_stdev_annual_gain_AGC_BGC_natrl_forest_Europe], # 15 = 630 GB peak
                    #
                    # # Planted forest removals
                    # cn.annual_gain_AGC_BGC_planted_forest_unmasked_dir: [cn.pattern_annual_gain_AGC_BGC_planted_forest_unmasked], # 15 = 600 GB peak
                    # cn.planted_forest_type_unmasked_dir: [cn.pattern_planted_forest_type_unmasked], # 15 = 360 GB peak
                    # cn.stdev_annual_gain_AGC_planted_forest_dir: [cn.pattern_stdev_annual_gain_AGC_planted_forest], # 15 = 600 GB peak
                    #
                    # # US forest removals
                    # cn.FIA_regions_processed_dir: [cn.pattern_FIA_regions_processed], # 15 = 350 GB peak
                    # cn.FIA_forest_group_processed_dir: [cn.pattern_FIA_forest_group_processed], # 15 = 340 GB peak
                    # cn.age_cat_natrl_forest_US_dir: [cn.pattern_age_cat_natrl_forest_US], # 15 = 350 GB peak
                    # cn.annual_gain_AGC_BGC_natrl_forest_US_dir: [cn.pattern_annual_gain_AGC_BGC_natrl_forest_US], # 15 = 620 GB peak
                    # # cn.stdev_annual_gain_AGC_BGC_natrl_forest_US_dir: [cn.pattern_stdev_annual_gain_AGC_BGC_natrl_forest_US],
                    #
                    # # Young natural forest removals
                    # cn.annual_gain_AGC_natrl_forest_young_dir: [cn.pattern_annual_gain_AGC_natrl_forest_young], # 15 = 710 GB peak
                    # cn.stdev_annual_gain_AGC_natrl_forest_young_dir: [cn.pattern_stdev_annual_gain_AGC_natrl_forest_young], # 15 = 700 GB peak
                    #
                    # # IPCC defaults forest removals
                    # cn.age_cat_IPCC_dir: [cn.pattern_age_cat_IPCC], # 15 = 330 GB peak
                    # cn.annual_gain_AGB_IPCC_defaults_dir: [cn.pattern_annual_gain_AGB_IPCC_defaults], # 15 = 620 GB peak
                    # cn.annual_gain_BGB_IPCC_defaults_dir: [cn.pattern_annual_gain_BGB_IPCC_defaults], # 15 = 620 GB peak
                    # cn.stdev_annual_gain_AGB_IPCC_defaults_dir: [cn.pattern_stdev_annual_gain_AGB_IPCC_defaults], # 15 = 620 GB peak
                    #
                    # # Annual removals from all forest types
                    # cn.annual_gain_AGC_all_types_dir: [cn.pattern_annual_gain_AGC_all_types], # 15 = XXX GB peak
                    # cn.annual_gain_BGC_all_types_dir: [cn.pattern_annual_gain_BGC_all_types], # 15 > 550 GB peak
                    # cn.annual_gain_AGC_BGC_all_types_dir: [cn.pattern_annual_gain_AGC_BGC_all_types], # 15 = XXX GB peak
                    # cn.removal_forest_type_dir: [cn.pattern_removal_forest_type], # 15 = XXX GB peak
                    cn.stdev_annual_gain_AGC_all_types_dir: [cn.pattern_stdev_annual_gain_AGC_all_types], # 15 = XXX GB peak

                    # # Gain year count
                    # cn.gain_year_count_dir: [cn.pattern_gain_year_count], # 15 = XXX GB peak

                    # # Gross removals from all forest types
                    # cn.cumul_gain_AGCO2_all_types_dir: [cn.pattern_cumul_gain_AGCO2_all_types], # 15 = 630 GB peak
                    # cn.cumul_gain_BGCO2_all_types_dir: [cn.pattern_cumul_gain_BGCO2_all_types], # 15 = XXX GB peak
                    # cn.cumul_gain_AGCO2_BGCO2_all_types_dir: [cn.pattern_cumul_gain_AGCO2_BGCO2_all_types], # 15 = XXX GB peak

                    # # Carbon pool inputs
                    # cn.elevation_processed_dir: [cn.pattern_elevation],
                    # cn.precip_processed_dir: [cn.pattern_precip],
                    # cn.bor_tem_trop_processed_dir: [cn.pattern_bor_tem_trop_processed],
                    # cn.drivers_processed_dir: [cn.pattern_drivers],
                    # cn.climate_zone_processed_dir: [cn.pattern_climate_zone],
                    # cn.soil_C_full_extent_2000_dir: [cn.pattern_soil_C_full_extent_2000], # 15 = 430 GB peak
                    cn.stdev_soil_C_full_extent_2000_dir: [cn.pattern_stdev_soil_C_full_extent],

                    # Carbon pools in emissions year
                    # cn.AGC_emis_year_dir: [cn.pattern_AGC_emis_year], # 14 = 590 GB peak
                    # cn.BGC_emis_year_dir: [cn.pattern_BGC_emis_year], # 14 = > 520 GB peak
                    # cn.deadwood_emis_year_2000_dir: [cn.pattern_deadwood_emis_year_2000], # 14 > 560 GB peak (error memory when using 15, so switched to 14)
                    # cn.litter_emis_year_2000_dir: [cn.pattern_litter_emis_year_2000], # 14 = XXX GB peak
                    # cn.soil_C_emis_year_2000_dir: [cn.pattern_soil_C_emis_year_2000], # 14 = XXX GB peak
                    # cn.total_C_emis_year_dir: [cn.pattern_total_C_emis_year], # 14 = XXX GB peak

                    # # Carbon pools in 2000
                    # cn.AGC_2000_dir: [cn.pattern_AGC_2000],
                    # cn.BGC_2000_dir: [cn.pattern_BGC_2000],
                    # cn.deadwood_2000_dir: [cn.pattern_deadwood_2000],
                    # cn.litter_2000_dir: [cn.pattern_litter_2000],
                    # cn.total_C_2000_dir: [cn.pattern_total_C_2000],

                    # # Net flux
                    # cn.net_flux_dir: [cn.pattern_net_flux],  # 14 = XXX GB peak
                    #
                    # # Gross emissions from biomass and soil
                    # cn.gross_emis_all_gases_all_drivers_biomass_soil_dir: [cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil], # 14 = XXX GB peak
                    # cn.gross_emis_co2_only_all_drivers_biomass_soil_dir: [cn.pattern_gross_emis_co2_only_all_drivers_biomass_soil], # 14 = XXX GB peak
                    # cn.gross_emis_non_co2_all_drivers_biomass_soil_dir: [cn.pattern_gross_emis_non_co2_all_drivers_biomass_soil], # 14 = XXX GB peak
                    # cn.gross_emis_commod_biomass_soil_dir: [cn.pattern_gross_emis_commod_biomass_soil], # 14 = XXX GB peak
                    # cn.gross_emis_shifting_ag_biomass_soil_dir: [cn.pattern_gross_emis_shifting_ag_biomass_soil], # 14 = XXX GB peak
                    # cn.gross_emis_forestry_biomass_soil_dir: [cn.pattern_gross_emis_forestry_biomass_soil], # 14 = XXX GB peak
                    # cn.gross_emis_wildfire_biomass_soil_dir: [cn.pattern_gross_emis_wildfire_biomass_soil], # 14 = XXX GB peak
                    # cn.gross_emis_urban_biomass_soil_dir: [cn.pattern_gross_emis_urban_biomass_soil], # 14 = XXX GB peak
                    # cn.gross_emis_no_driver_biomass_soil_dir: [cn.pattern_gross_emis_no_driver_biomass_soil], # 14 = XXX GB peak
                    # cn.gross_emis_nodes_biomass_soil_dir: [cn.pattern_gross_emis_nodes_biomass_soil], # 14 = XXX GB peak


                    # Gross emissions from soil only
                    cn.gross_emis_all_gases_all_drivers_soil_only_dir: [cn.pattern_gross_emis_all_gases_all_drivers_soil_only],
                    cn.gross_emis_co2_only_all_drivers_soil_only_dir: [cn.pattern_gross_emis_co2_only_all_drivers_soil_only],
                    cn.gross_emis_non_co2_all_drivers_soil_only_dir: [cn.pattern_gross_emis_non_co2_all_drivers_soil_only],
                    cn.gross_emis_commod_soil_only_dir: [cn.pattern_gross_emis_commod_soil_only],
                    cn.gross_emis_shifting_ag_soil_only_dir: [cn.pattern_gross_emis_shifting_ag_soil_only]
                    # cn.gross_emis_forestry_soil_only_dir: [cn.pattern_gross_emis_forestry_soil_only],
                    # cn.gross_emis_wildfire_soil_only_dir: [cn.pattern_gross_emis_wildfire_soil_only],
                    # cn.gross_emis_urban_soil_only_dir: [cn.pattern_gross_emis_urban_soil_only],
                    # cn.gross_emis_no_driver_soil_only_dir: [cn.pattern_gross_emis_no_driver_soil_only],
                    # cn.gross_emis_nodes_soil_only_dir: [cn.pattern_gross_emis_nodes_soil_only]
    }

    # Iterates through each set of tiles and gets statistics of it
    for key, values in download_dict.items():

        # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
        dir = key
        pattern = values[0]
        uu.s3_flexible_download(dir, pattern, cn.docker_tile_dir, sensit_type, tile_id_list)

        # List of all the tiles on the spot machine to be summarized (excludes pixel area tiles and tiles created by gdal_calc
        # (in case this script was already run on this spot machine and created output from gdal_calc)
        tile_list = uu.tile_list_spot_machine(".", ".tif")
        # from https://stackoverflow.com/questions/12666897/removing-an-item-from-list-matching-a-substring
        tile_list = [i for i in tile_list if not ('hanson_2013' in i or 'value_per_pixel' in i)]
        uu.print_log(tile_list)
        uu.print_log("There are {} tiles to process".format(str(len(tile_list))) + "\n")

        # For multiprocessor use.
        processes=14
        uu.print_log('Tile statistics max processors=', processes)
        pool = multiprocessing.Pool(processes)
        pool.map(partial(tile_statistics.create_tile_statistics, sensit_type=sensit_type, tile_stats_txt=tile_stats_txt), tile_list)
        # Added these in response to error12: Cannot allocate memory error.
        # This fix was mentioned here: of https://stackoverflow.com/questions/26717120/python-cannot-allocate-memory-using-multiprocessing-pool
        # Could also try this: https://stackoverflow.com/questions/42584525/python-multiprocessing-debugging-oserror-errno-12-cannot-allocate-memory
        pool.close()
        pool.join()

        # # For single processor use
        # for tile in tile_list:
        #     tile_statistics.create_tile_statistics(tile, sensit_type)

        # Copies the text file to the tile statistics folder on s3
        cmd = ['aws', 's3', 'cp', tile_stats_txt, cn.tile_stats_dir]
        uu.log_subprocess_output_full(cmd)

        # Spot machine can't store all the tiles, so this cleans it up
        uu.print_log("Deleting tiles...")
        for tile in tile_list:
            os.remove(tile)
            tile_short = tile[:-4]
            outname = '{0}_value_per_pixel.tif'.format(tile_short)
            os.remove(outname)
            uu.print_log("  {} deleted".format(tile))

    uu.print_log("Script complete. All tiles analyzed!")



if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Create tiles of the annual AGB and BGB removals rates for mangrove forests')
    parser.add_argument('--model-type', '-t', required=True,
                        help=f'{cn.model_type_arg_help}')
    parser.add_argument('--tile_id_list', '-l', required=True,
                        help='List of tile ids to use in the model. Should be of form 00N_110E or 00N_110E,00N_120E or all.')
    args = parser.parse_args()
    sensit_type = args.model_type
    tile_id_list = args.tile_id_list

    # Create the output log
    uu.initiate_log(tile_id_list)

    # Checks whether the sensitivity analysis and tile_id_list arguments are valid
    uu.check_sensit_type(sensit_type)
    tile_id_list = uu.tile_id_list_check(tile_id_list)

    mp_tile_statistics(sensit_type=sensit_type, tile_id_list=tile_id_list)