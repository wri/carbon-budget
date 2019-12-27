### This script calculates various statistics on all tiles in input folders and saves them to a txt.
### Users can input as many folders as they want for calculating statistics on each tile

import multiprocessing
import tile_statistics
import subprocess
from functools import partial
import argparse
import os
import glob
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def main ():

    # The argument for what kind of model run is being done: standard conditions or a sensitivity analysis run
    parser = argparse.ArgumentParser(description='Create tiles of the number of years of carbon gain for mangrove forests')
    parser.add_argument('--model-type', '-t', required=True,
                        help='{}'.format(cn.model_type_arg_help))
    args = parser.parse_args()
    sensit_type = args.model_type
    # Checks whether the sensitivity analysis argument is valid
    uu.check_sensit_type(sensit_type)


    # The column names for the tile summary statistics.
    # If the statistics calculations are changed in tile_statistics.py, the list here needs to be changed, too.
    headers = ['tile_id', 'tile_type', 'tile_name', 'pixel_count', 'mean', 'median', 'percentile10', 'percentile25',
               'percentile75', 'percentile90', 'min', 'max', 'sum']
    header_no_brackets = ', '.join(headers)

    tile_stats = '{0}_{1}_{2}'.format(uu.date_today, sensit_type, cn.tile_stats_pattern)

    # Creates the output text file with the column names
    with open(tile_stats, 'w+') as f:
        f.write(header_no_brackets  +'\r\n')
    f.close()

    # Creates list of tiles to iterate through, for testing
    tile_id_list = 'all'
    tile_id_list = ['00N_110E'] # test tiles
    print tile_id_list

    # Pixel area tiles-- necessary for calculating sum of pixels for any set of tiles
    uu.s3_flexible_download(cn.pixel_area_dir, cn.pattern_pixel_area, '.', 'std', tile_id_list)

    # For downloading all tiles in selected folders
    download_dict = {
                    # # cn.WHRC_biomass_2000_unmasked_dir,
                    cn.JPL_processed_dir: [cn.pattern_JPL_unmasked_processed]
                    # # cn.mangrove_biomass_2000_dir,
                    # # cn.cont_eco_dir,
                    # # cn.WHRC_biomass_2000_non_mang_non_planted_dir,
                    # # cn.gain_year_count_mangrove_dir,
                    # cn.annual_gain_AGB_mangrove_dir,
                    # cn.annual_gain_BGB_mangrove_dir,
                    # cn.cumul_gain_AGCO2_mangrove_dir,
                    # cn.cumul_gain_BGCO2_mangrove_dir,
                    #
                    # cn.gain_year_count_planted_forest_non_mangrove_dir,
                    # cn.annual_gain_AGC_BGC_planted_forest_unmasked_dir,
                    # cn.planted_forest_type_unmasked_dir,
                    # cn.annual_gain_AGB_planted_forest_non_mangrove_dir,
                    # cn.annual_gain_BGB_planted_forest_non_mangrove_dir,
                    # cn.cumul_gain_AGCO2_planted_forest_non_mangrove_dir,
                    # cn.cumul_gain_BGCO2_planted_forest_non_mangrove_dir,

                    # cn.age_cat_natrl_forest_dir: [cn.pattern_age_cat_natrl_forest],
                    # cn.gain_year_count_natrl_forest_dir: [cn.pattern_gain_year_count_natrl_forest],
                    # cn.annual_gain_AGB_natrl_forest_dir: [cn.pattern_annual_gain_AGB_natrl_forest],
                    # cn.annual_gain_BGB_natrl_forest_dir: [cn.pattern_annual_gain_BGB_natrl_forest],
                    # cn.cumul_gain_AGCO2_natrl_forest_dir: [cn.pattern_cumul_gain_AGCO2_natrl_forest],
                    # cn.cumul_gain_BGCO2_natrl_forest_dir: [cn.pattern_cumul_gain_BGCO2_natrl_forest],
                    # cn.annual_gain_AGB_BGB_all_types_dir: [cn.pattern_annual_gain_AGB_BGB_all_types],
                    # cn.cumul_gain_AGCO2_BGCO2_all_types_dir: [cn.pattern_cumul_gain_AGCO2_BGCO2_all_types],
                    #
                    # # cn.elevation_processed_dir,
                    # # cn.precip_processed_dir,
                    # # cn.planted_forest_type_unmasked_dir,
                    # # cn.bor_tem_trop_processed_dir,
                    # # cn.soil_C_full_extent_2000_dir,
                    # # cn.drivers_processed_dir,
                    # # cn.climate_zone_processed_dir
                    #
                    # cn.AGC_emis_year_dir: [cn.pattern_AGC_emis_year],
                    # cn.BGC_emis_year_dir: [cn.pattern_BGC_emis_year],
                    # cn.deadwood_emis_year_2000_dir: [cn.pattern_deadwood_emis_year_2000],
                    # cn.litter_emis_year_2000_dir: [cn.pattern_litter_emis_year_2000],
                    # cn.soil_C_emis_year_2000_dir: [cn.pattern_soil_C_emis_year_2000],
                    # cn.total_C_emis_year_dir: [cn.pattern_soil_C_emis_year_2000]

                    # cn.net_flux_dir: [cn.pattern_net_flux],
                    # cn.gross_emis_all_gases_all_drivers_biomass_soil_dir: [cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil],
                    # cn.gross_emis_co2_only_all_drivers_biomass_soil_dir: [cn.pattern_gross_emis_co2_only_all_drivers_biomass_soil],
                    # cn.gross_emis_non_co2_all_drivers_biomass_soil_dir: [cn.pattern_gross_emis_non_co2_all_drivers_biomass_soil],
                    # cn.gross_emis_commod_biomass_soil_dir: [cn.pattern_gross_emis_commod_biomass_soil],
                    # cn.gross_emis_shifting_ag_biomass_soil_dir: [cn.pattern_gross_emis_shifting_ag_biomass_soil],
                    # cn.gross_emis_forestry_biomass_soil_dir: [cn.pattern_gross_emis_forestry_biomass_soil],
                    # cn.gross_emis_wildfire_biomass_soil_dir: [cn.pattern_gross_emis_wildfire_biomass_soil],
                    # cn.gross_emis_urban_biomass_soil_dir: [cn.pattern_gross_emis_urban_biomass_soil],
                    # cn.gross_emis_no_driver_biomass_soil_dir: [cn.pattern_gross_emis_no_driver_biomass_soil],
                    # cn.gross_emis_nodes_biomass_soil_dir: [cn.pattern_gross_emis_nodes_biomass_soil]

                    # cn.gross_emis_all_gases_all_drivers_soil_only_dir,
                    # cn.gross_emis_co2_only_all_drivers_soil_only_dir,
                    # cn.gross_emis_non_co2_all_drivers_soil_only_dir,
                    # cn.gross_emis_commod_soil_only_dir,
                    # cn.gross_emis_shifting_ag_soil_only_dir,
                    # cn.gross_emis_forestry_soil_only_dir,
                    # cn.gross_emis_wildfire_soil_only_dir,
                    # cn.gross_emis_urban_soil_only_dir,
                    # cn.gross_emis_no_driver_soil_only_dir,
                    # cn.gross_emis_nodes_soil_only_dir
    }

    # Iterates through each set of tiles and gets statistics of it
    for key, values in download_dict.iteritems():

        dir = key
        pattern = values[0]
        uu.s3_flexible_download(dir, pattern, '.', sensit_type, tile_id_list)

        # List of all the tiles on the spot machine to be summarized (excludes pixel area tiles and tiles created by gdal_calc
        # (in case this script was already run on this spot machine and created output from gdal_calc)
        tile_list = uu.tile_list_spot_machine(".", ".tif")
        # from https://stackoverflow.com/questions/12666897/removing-an-item-from-list-matching-a-substring
        tile_list = [i for i in tile_list if not ('hanson_2013' in i or 'value_per_pixel' in i)]
        # tile_list = ['00N_000E_biomass.tif']
        # tile_list = ['30N_140E_net_flux_t_CO2e_ha_2001_15_biomass_soil_maxgain.tif',
        #              '40N_030W_net_flux_t_CO2e_ha_2001_15_biomass_soil_maxgain.tif']  # test tiles
        # tile_list = tile_id_list
        print tile_list

        # For multiprocessor use.
        count = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(processes=9)
        # processes=9 maxes out at about 340 for gross emissions
        # processes=13 maxes out at above 480 for gross emissions
        # processes=11 maxes out at about 440 for gross emissions
        pool.map(partial(tile_statistics.create_tile_statistics, sensit_type=sensit_type), tile_list)
        # Added these in response to error12: Cannot allocate memory error.
        # This fix was mentioned here: of https://stackoverflow.com/questions/26717120/python-cannot-allocate-memory-using-multiprocessing-pool
        # Could also try this: https://stackoverflow.com/questions/42584525/python-multiprocessing-debugging-oserror-errno-12-cannot-allocate-memory
        pool.close()
        pool.join()

        # # For single processor use
        # for tile in tile_list:
        #     tile_statistics.create_tile_statistics(tile)

        # Even an m4.16xlarge spot machine can't handle all these sets of tiles, so this deletes each set of tiles after it is analyzed
        print "Deleting tiles..."
        for tile in tile_list:
            os.remove(tile)
            tile_short = tile[:-4]
            outname = '{0}_value_per_pixel.tif'.format(tile_short)
            os.remove(outname)
            print "  Tiles deleted"

        # Copies the text file to the tile statistics folder on s3
        cmd = ['aws', 's3', 'cp', tile_stats, cn.tile_stats_dir]
        subprocess.check_call(cmd)


if __name__ == '__main__':
    main()