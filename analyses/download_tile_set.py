'''
This script downloads the listed tiles and creates overviews for them for easy viewing in ArcMap.
It must be run in the Docker container, and so tiles are downloaded to and overviewed in the folder of the Docker container where
all other tiles are downloaded.

python -m analyses.download_tile_set -t std -l 00N_000E
python -m analyses.download_tile_set -t std -l 00N_000E,00N_110E
'''

import multiprocessing
from functools import partial
from osgeo import gdal
import pandas as pd
import datetime
import argparse
import glob
import os
import sys

import constants_and_names as cn
import universal_util as uu

def download_tile_set(tile_id_list):

    uu.print_log("Downloading all tiles for: ", tile_id_list)

    wd = os.path.join(cn.docker_tile_dir, "spot_download")

    os.chdir(wd)

    download_dict = {
        cn.gain_dir: [cn.pattern_data_lake],
        cn.loss_dir: [cn.pattern_loss],
        cn.tcd_dir: [cn.pattern_tcd],
        cn.WHRC_biomass_2000_unmasked_dir: [cn.pattern_WHRC_biomass_2000_unmasked],
        cn.plant_pre_2000_processed_dir: [cn.pattern_plant_pre_2000],

        cn.model_extent_dir: [cn.pattern_model_extent],
        cn.age_cat_IPCC_dir: [cn.pattern_age_cat_IPCC],
        cn.annual_gain_AGB_IPCC_defaults_dir: [cn.pattern_annual_gain_AGB_IPCC_defaults],
        cn.annual_gain_BGB_IPCC_defaults_dir: [cn.pattern_annual_gain_BGB_IPCC_defaults],
        cn.stdev_annual_gain_AGB_IPCC_defaults_dir: [cn.pattern_stdev_annual_gain_AGB_IPCC_defaults],
        cn.removal_forest_type_dir: [cn.pattern_removal_forest_type],
        cn.BGB_AGB_ratio_dir: [cn.pattern_BGB_AGB_ratio],
        cn.annual_gain_AGC_all_types_dir: [cn.pattern_annual_gain_AGC_all_types],
        cn.annual_gain_BGC_all_types_dir: [cn.pattern_annual_gain_BGC_all_types],
        cn.annual_gain_AGC_BGC_all_types_dir: [cn.pattern_annual_gain_AGC_BGC_all_types],
        cn.stdev_annual_gain_AGC_all_types_dir: [cn.pattern_stdev_annual_gain_AGC_all_types],
        cn.gain_year_count_dir: [cn.pattern_gain_year_count],
        cn.cumul_gain_AGCO2_all_types_dir: [cn.pattern_cumul_gain_AGCO2_all_types],
        cn.cumul_gain_BGCO2_all_types_dir: [cn.pattern_cumul_gain_BGCO2_all_types],
        cn.AGC_emis_year_dir: [cn.pattern_AGC_emis_year],
        cn.BGC_emis_year_dir: [cn.pattern_BGC_emis_year],
        cn.deadwood_emis_year_2000_dir: [cn.pattern_deadwood_emis_year_2000],
        cn.litter_emis_year_2000_dir: [cn.pattern_litter_emis_year_2000],
        cn.soil_C_emis_year_2000_dir: [cn.pattern_soil_C_emis_year_2000],
        cn.total_C_emis_year_dir: [cn.pattern_total_C_emis_year],

        # cn.gross_emis_commod_biomass_soil_dir: [cn.pattern_gross_emis_commod_biomass_soil],
        # cn.gross_emis_shifting_ag_biomass_soil_dir: [cn.pattern_gross_emis_shifting_ag_biomass_soil],
        # cn.gross_emis_forestry_biomass_soil_dir: [cn.pattern_gross_emis_forestry_biomass_soil],
        # cn.gross_emis_wildfire_biomass_soil_dir: [cn.pattern_gross_emis_wildfire_biomass_soil],
        # cn.gross_emis_urban_biomass_soil_dir: [cn.pattern_gross_emis_urban_biomass_soil],
        # cn.gross_emis_no_driver_biomass_soil_dir: [cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil],

        cn.gross_emis_all_gases_all_drivers_biomass_soil_dir: [cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil],
        cn.gross_emis_co2_only_all_drivers_biomass_soil_dir: [cn.pattern_gross_emis_co2_only_all_drivers_biomass_soil],
        cn.gross_emis_non_co2_all_drivers_biomass_soil_dir: [cn.pattern_gross_emis_non_co2_all_drivers_biomass_soil],
        cn.gross_emis_nodes_biomass_soil_dir: [cn.pattern_gross_emis_nodes_biomass_soil],
        cn.net_flux_dir: [cn.pattern_net_flux],
        cn.cumul_gain_AGCO2_BGCO2_all_types_dir: [cn.pattern_cumul_gain_AGCO2_BGCO2_all_types],
        cn.cumul_gain_AGCO2_BGCO2_all_types_per_pixel_full_extent_dir: [cn.pattern_cumul_gain_AGCO2_BGCO2_all_types_per_pixel_full_extent],
        cn.cumul_gain_AGCO2_BGCO2_all_types_forest_extent_dir: [cn.pattern_cumul_gain_AGCO2_BGCO2_all_types_forest_extent],
        cn.cumul_gain_AGCO2_BGCO2_all_types_per_pixel_forest_extent_dir: [cn.pattern_cumul_gain_AGCO2_BGCO2_all_types_per_pixel_forest_extent],
        cn.gross_emis_all_gases_all_drivers_biomass_soil_per_pixel_full_extent_dir: [cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil_per_pixel_full_extent],
        cn.gross_emis_all_gases_all_drivers_biomass_soil_forest_extent_dir: [cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil_forest_extent],
        cn.gross_emis_all_gases_all_drivers_biomass_soil_per_pixel_forest_extent_dir: [cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil_per_pixel_forest_extent],
        cn.net_flux_per_pixel_full_extent_dir: [cn.pattern_net_flux_per_pixel_full_extent],
        cn.net_flux_forest_extent_dir: [cn.pattern_net_flux_forest_extent],
        cn.net_flux_per_pixel_forest_extent_dir: [cn.pattern_net_flux_per_pixel_forest_extent]
    }

    # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
    for key, values in download_dict.items():
        dir = key
        pattern = values[0]
        uu.s3_flexible_download(dir, pattern, wd, cn.SENSIT_TYPE, tile_id_list)

    cmd = ['aws', 's3', 'cp', cn.output_aggreg_dir, wd]
    uu.log_subprocess_output_full(cmd)

    tile_list = glob.glob('*tif')
    uu.print_log("Tiles for pyramiding: ", tile_list)

    # https://gis.stackexchange.com/questions/160459/comparing-use-of-gdal-to-build-raster-pyramids-or-overviews-versus-arcmap
    # Example 3 from https://gdal.org/programs/gdaladdo.html
    # https://stackoverflow.com/questions/33158526/how-to-correctly-use-gdaladdo-in-a-python-program
    for tile in tile_list:
        uu.print_log("Pyramiding ", tile)
        Image = gdal.Open(tile, 0)  # 0 = read-only, 1 = read-write.
        gdal.SetConfigOption('COMPRESS_OVERVIEW', 'DEFLATE')
        Image.BuildOverviews('NEAREST', [2, 4, 8, 16, 32], gdal.TermProgress_nocb)
        del Image  # close the dataset (Python object and pointers)

    uu.print_log("Pyramiding done")


if __name__ == '__main__':

    # The arguments for what kind of model run is being run (standard conditions or a sensitivity analysis) and
    # the tiles to include
    parser = argparse.ArgumentParser(
        description='Download model outputs for specific tile')
    parser.add_argument('--model-type', '-t', required=True,
                        help=f'{cn.model_type_arg_help}')
    parser.add_argument('--tile_id_list', '-l', required=True,
                        help='List of tile ids to use in the model. Should be of form 00N_110E or 00N_110E,00N_120E or all.')
    args = parser.parse_args()

    # Sets global variables to the command line arguments
    cn.SENSIT_TYPE = args.model_type

    tile_id_list = args.tile_id_list

    # Create the output log
    uu.initiate_log(tile_id_list)

    # Checks whether the sensitivity analysis and tile_id_list arguments are valid
    uu.check_sensit_type(cn.SENSIT_TYPE)
    tile_id_list = uu.tile_id_list_check(tile_id_list)

    download_tile_set(tile_id_list)