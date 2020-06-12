

import multiprocessing
from functools import partial
import glob
import datetime
import argparse
from osgeo import gdal
import legal_AMZ_loss
import pandas as pd
import subprocess
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu
sys.path.append('../gain')
import annual_gain_rate_natrl_forest
import cumulative_gain_natrl_forest
import merge_cumulative_annual_gain_all_forest_types
sys.path.append('../carbon_pools')
import create_carbon_pools

def main ():

    # Create the output log
    script_start = datetime.datetime.now()
    uu.initiate_log(script_start)

    os.chdir(cn.docker_base_dir)

    Brazil_stages = ['all', 'create_forest_extent', 'create_loss',
                     'forest_age_category', 'gain_year_count', 'annual_removals', 'cumulative_removals', 'removals_merged',
                     'carbon_pools']


    # The argument for what kind of model run is being done: standard conditions or a sensitivity analysis run
    parser = argparse.ArgumentParser(description='Create tiles of the number of years of carbon gain for mangrove forests')
    parser.add_argument('--stages', '-s', required=True,
                        help='Stages of creating Brazil legal Amazon-specific gross cumulative removals. Options are {}'.format(Brazil_stages))
    parser.add_argument('--run_through', '-r', required=True,
                        help='Options: true or false. true: run named stage and following stages. false: run only named stage.')
    args = parser.parse_args()
    stage_input = args.stages
    run_through = args.run_through


    # Checks the validity of the two arguments. If either one is invalid, the script ends.
    if (stage_input not in Brazil_stages):
        raise Exception('Invalid stage selection. Please provide a stage from {}.'.format(Brazil_stages))
    else:
        pass
    if (run_through not in ['true', 'false']):
        raise Exception('Invalid run through option. Please enter true or false.')
    else:
        pass

    actual_stages = uu.analysis_stages(Brazil_stages, stage_input, run_through)
    uu.print_log(actual_stages)


    # By definition, this script is for US-specific removals
    sensit_type = 'legal_Amazon_loss'

    # List of output directories and output file name patterns
    master_output_dir_list = [cn.Brazil_forest_extent_2000_processed_dir, cn.Brazil_annual_loss_processed_dir,
                       cn.age_cat_natrl_forest_dir, cn.gain_year_count_natrl_forest_dir,
                       cn.annual_gain_AGB_natrl_forest_dir, cn.annual_gain_BGB_natrl_forest_dir,
                       cn.cumul_gain_AGCO2_natrl_forest_dir, cn.cumul_gain_BGCO2_natrl_forest_dir,
                       cn.annual_gain_AGB_BGB_all_types_dir, cn.cumul_gain_AGCO2_BGCO2_all_types_dir,
                       cn.AGC_emis_year_dir, cn.BGC_emis_year_dir, cn.deadwood_emis_year_2000_dir,
                       cn.litter_emis_year_2000_dir, cn.soil_C_emis_year_2000_dir, cn.total_C_emis_year_dir
                       ]

    master_output_pattern_list = [cn.pattern_Brazil_forest_extent_2000_processed, cn.pattern_Brazil_annual_loss_processed,
                           cn.pattern_age_cat_natrl_forest, cn.pattern_gain_year_count_natrl_forest,
                           cn.pattern_annual_gain_AGB_natrl_forest, cn.pattern_annual_gain_BGB_natrl_forest,
                           cn.pattern_cumul_gain_AGCO2_natrl_forest, cn.pattern_cumul_gain_BGCO2_natrl_forest,
                           cn.pattern_annual_gain_AGB_BGB_all_types, cn.pattern_cumul_gain_AGCO2_BGCO2_all_types,
                           cn.pattern_AGC_emis_year, cn.pattern_BGC_emis_year, cn.pattern_deadwood_emis_year_2000,
                           cn.pattern_litter_emis_year_2000, cn.pattern_soil_C_emis_year_2000, cn.pattern_total_C_emis_year
                           ]


    # Creates forest extent 2000 raster from multiple PRODES forest extent rasters
    if 'create_forest_extent' in actual_stages:

        uu.print_log('Creating forest extent tiles')

        # List of tiles that could be run. This list is only used to create the FIA region tiles if they don't already exist.
        tile_id_list = uu.tile_list_s3(cn.WHRC_biomass_2000_unmasked_dir)
        # tile_id_list = ["00N_000E", "00N_050W", "00N_060W", "00N_010E", "00N_020E", "00N_030E", "00N_040E", "10N_000E", "10N_010E", "10N_010W", "10N_020E", "10N_020W"] # test tiles
        # tile_id_list = ['50N_130W'] # test tiles
        uu.print_log(tile_id_list)
        uu.print_log("There are {} tiles to process".format(str(len(tile_id_list))) + "\n")

        # Downloads input rasters and lists them
        uu.s3_folder_download(cn.Brazil_forest_extent_2000_raw_dir, cn.docker_base_dir, sensit_type)
        raw_forest_extent_inputs = glob.glob('*_AMZ_warped_*tif')   # The list of tiles to merge

        # Gets the resolution of a more recent PRODES raster, which has a higher resolution. The merged output matches that.
        raw_forest_extent_input_2019 = glob.glob('*2019_AMZ_warped_*tif')
        prodes_2019 = gdal.Open(raw_forest_extent_input_2019[0])
        transform_2019 = prodes_2019.GetGeoTransform()
        pixelSizeX = transform_2019[1]
        pixelSizeY = -transform_2019[5]
        uu.print_log(pixelSizeX)
        uu.print_log(pixelSizeY)

        # This merges all six rasters together, so it takes a lot of memory and time. It seems to repeatedly max out
        # at about 300 GB as it progresses abot 15% each time; then the memory drops back to 0 and slowly increases.
        cmd = ['gdal_merge.py', '-o', '{}.tif'.format(cn.pattern_Brazil_forest_extent_2000_merged),
               '-co', 'COMPRESS=LZW', '-a_nodata', '0', '-n', '0', '-ot', 'Byte', '-ps', '{}'.format(pixelSizeX), '{}'.format(pixelSizeY),
               raw_forest_extent_inputs[0], raw_forest_extent_inputs[1], raw_forest_extent_inputs[2],
               raw_forest_extent_inputs[3], raw_forest_extent_inputs[4], raw_forest_extent_inputs[5]]
        subprocess.check_call(cmd)

        # Uploads the merged forest extent raster to s3 for future reference
        uu.upload_final_set(cn.Brazil_forest_extent_2000_merged_dir, cn.pattern_Brazil_forest_extent_2000_merged)

        # Creates legal Amazon extent 2000 tiles
        source_raster = '{}.tif'.format(cn.pattern_Brazil_forest_extent_2000_merged)
        out_pattern = cn.pattern_Brazil_forest_extent_2000_processed
        dt = 'Byte'
        pool = multiprocessing.Pool(int(cn.count/2))
        pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt), tile_id_list)

        # Checks if each tile has data in it. Only tiles with data are uploaded.
        upload_dir = master_output_dir_list[0]
        pattern = master_output_pattern_list[0]
        pool = multiprocessing.Pool(cn.count - 5)
        pool.map(partial(uu.check_and_upload, upload_dir=upload_dir, pattern=pattern), tile_id_list)


    # Creates annual loss raster for 2001-2015 from multiples PRODES rasters
    if 'create_loss' in actual_stages:

        uu.print_log('Creating annual loss tiles')

        tile_id_list = uu.tile_list_s3(cn.Brazil_forest_extent_2000_processed_dir)
        uu.print_log(tile_id_list)
        uu.print_log("There are {} tiles to process".format(str(len(tile_id_list))) + "\n")

        # Downloads input rasters and lists them
        uu.s3_folder_download(cn.Brazil_annual_loss_raw_dir, cn.docker_base_dir, sensit_type)

        # Gets the resolution of the more recent PRODES raster, which has a higher resolution. The merged output matches that.
        raw_forest_extent_input_2017 = glob.glob('Prodes2017_*tif')
        prodes_2017 = gdal.Open(raw_forest_extent_input_2017[0])
        transform_2017 = prodes_2017.GetGeoTransform()
        pixelSizeX = transform_2017[1]
        pixelSizeY = -transform_2017[5]

        # This merges both loss rasters together, so it takes a lot of memory and time. It seems to max out
        # at about 150 GB. Loss from PRODES2014 needs to go second so that its loss years get priority over PRODES2017,
        # which seems to have a preponderance of 2007 loss that appears to often be earlier loss years.
        cmd = ['gdal_merge.py', '-o', '{}.tif'.format(cn.pattern_Brazil_annual_loss_merged),
               '-co', 'COMPRESS=LZW', '-a_nodata', '0', '-n', '0', '-ot', 'Byte', '-ps', '{}'.format(pixelSizeX), '{}'.format(pixelSizeY),
               'Prodes2017_annual_loss_2008_2015.tif', 'Prodes2014_annual_loss_2001_2007.tif']
        subprocess.check_call(cmd)

        # Uploads the merged loss raster to s3 for future reference
        uu.upload_final_set(cn.Brazil_annual_loss_merged_dir, cn.pattern_Brazil_annual_loss_merged)

        # Creates annual loss 2001-2015 tiles
        source_raster = '{}.tif'.format(cn.pattern_Brazil_annual_loss_merged)
        out_pattern = cn.pattern_Brazil_annual_loss_processed
        dt = 'Byte'
        pool = multiprocessing.Pool(int(cn.count/2))
        pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt), tile_id_list)

        # Checks if each tile has data in it. Only tiles with data are uploaded.
        # In practice, every Amazon tile has loss in it but I figured I'd do this just to be thorough.
        upload_dir = master_output_dir_list[1]
        pattern = master_output_pattern_list[1]
        pool = multiprocessing.Pool(cn.count - 5)
        pool.map(partial(uu.check_and_upload, upload_dir=upload_dir, pattern=pattern), tile_id_list)


    # Creates forest age category tiles
    if 'forest_age_category' in actual_stages:

        uu.print_log('Creating forest age category tiles')

        # Files to download for this script.
        download_dict = {cn.Brazil_annual_loss_processed_dir: [cn.pattern_Brazil_annual_loss_processed],
                         cn.gain_dir: [cn.pattern_gain],
                         cn.WHRC_biomass_2000_non_mang_non_planted_dir: [cn.pattern_WHRC_biomass_2000_non_mang_non_planted],
                         cn.planted_forest_type_unmasked_dir: [cn.pattern_planted_forest_type_unmasked],
                         cn.mangrove_biomass_2000_dir: [cn.pattern_mangrove_biomass_2000],
                         cn.Brazil_forest_extent_2000_processed_dir: [cn.pattern_Brazil_forest_extent_2000_processed]
                         }


        tile_id_list = uu.tile_list_s3(cn.Brazil_forest_extent_2000_processed_dir)
        # tile_id_list = ['00N_050W']
        uu.print_log(tile_id_list)
        uu.print_log("There are {} tiles to process".format(str(len(tile_id_list))) + "\n")


        # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
        for key, values in download_dict.items():
            dir = key
            pattern = values[0]
            uu.s3_flexible_download(dir, pattern, cn.docker_base_dir, sensit_type, tile_id_list)


        # If the model run isn't the standard one, the output directory and file names are changed
        if sensit_type != 'std':
            uu.print_log("Changing output directory and file name pattern based on sensitivity analysis")
            stage_output_dir_list = uu.alter_dirs(sensit_type, master_output_dir_list)
            stage_output_pattern_list = uu.alter_patterns(sensit_type, master_output_pattern_list)


        output_pattern = stage_output_pattern_list[2]

        # This configuration of the multiprocessing call is necessary for passing multiple arguments to the main function
        # It is based on the example here: http://spencerimp.blogspot.com/2015/12/python-multiprocess-with-multiple.html
        # With processes=30, peak usage was about 350 GB using WHRC AGB.
        # processes=26 maxes out above 480 GB for biomass_swap, so better to use fewer than that.
        pool = multiprocessing.Pool(int(cn.count/2))
        pool.map(partial(legal_AMZ_loss.legal_Amazon_forest_age_category,
                         sensit_type=sensit_type, output_pattern=output_pattern), tile_id_list)
        pool.close()
        pool.join()

        # # For single processor use
        # for tile_id in tile_id_list:
        #
        #     legal_AMZ_loss.legal_Amazon_forest_age_category(tile_id, sensit_type, output_pattern)

        # Uploads output from this stage
        uu.upload_final_set(stage_output_dir_list[2], stage_output_pattern_list[2])


    # Creates tiles of the number of years of removals
    if 'gain_year_count' in actual_stages:

        uu.print_log('Creating gain year count tiles for natural forest')

        # Files to download for this script.
        download_dict = {
            cn.Brazil_annual_loss_processed_dir: [cn.pattern_Brazil_annual_loss_processed],
            cn.gain_dir: [cn.pattern_gain],
            cn.WHRC_biomass_2000_non_mang_non_planted_dir: [cn.pattern_WHRC_biomass_2000_non_mang_non_planted],
            cn.planted_forest_type_unmasked_dir: [cn.pattern_planted_forest_type_unmasked],
            cn.mangrove_biomass_2000_dir: [cn.pattern_mangrove_biomass_2000],
            cn.Brazil_forest_extent_2000_processed_dir: [cn.pattern_Brazil_forest_extent_2000_processed]
        }


        tile_id_list = uu.tile_list_s3(cn.Brazil_forest_extent_2000_processed_dir)
        # tile_id_list = ['00N_050W']
        uu.print_log(tile_id_list)
        uu.print_log("There are {} tiles to process".format(str(len(tile_id_list))) + "\n")


        # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
        for key, values in download_dict.items():
            dir = key
            pattern = values[0]
            uu.s3_flexible_download(dir, pattern, cn.docker_base_dir, sensit_type, tile_id_list)


        # If the model run isn't the standard one, the output directory and file names are changed
        if sensit_type != 'std':
            uu.print_log("Changing output directory and file name pattern based on sensitivity analysis")
            stage_output_dir_list = uu.alter_dirs(sensit_type, master_output_dir_list)
            stage_output_pattern_list = uu.alter_patterns(sensit_type, master_output_pattern_list)


        output_pattern = stage_output_pattern_list[3]

        pool = multiprocessing.Pool(int(cn.count/3))
        pool.map(partial(legal_AMZ_loss.legal_Amazon_create_gain_year_count_loss_only, sensit_type=sensit_type),
                 tile_id_list)

        pool.map(partial(legal_AMZ_loss.legal_Amazon_create_gain_year_count_no_change, sensit_type=sensit_type),
                 tile_id_list)

        pool.map(partial(legal_AMZ_loss.legal_Amazon_create_gain_year_count_loss_and_gain_standard, sensit_type=sensit_type),
                 tile_id_list)

        pool = multiprocessing.Pool(int(cn.count/8))  # count/5 uses more than 160GB of memory. count/8 uses about 120GB of memory.
        pool.map(partial(legal_AMZ_loss.legal_Amazon_create_gain_year_count_merge, output_pattern=output_pattern), tile_id_list)

        # # For single processor use
        # for tile_id in tile_id_list:
        #     legal_AMZ_loss.legal_Amazon_create_gain_year_count_loss_only(tile_id, sensit_type)
        #
        # for tile_id in tile_id_list:
        #     legal_AMZ_loss.legal_Amazon_create_gain_year_count_no_change(tile_id, sensit_type)
        #
        # for tile_id in tile_id_list:
        #     legal_AMZ_loss.legal_Amazon_create_gain_year_count_loss_and_gain_standard(tile_id, sensit_type)
        #
        # for tile_id in tile_id_list:
            # legal_AMZ_loss.legal_Amazon_create_gain_year_count_merge(tile_id, output_pattern)

        # Intermediate output tiles for checking outputs
        uu.upload_final_set(stage_output_dir_list[3], "growth_years_loss_only")
        uu.upload_final_set(stage_output_dir_list[3], "growth_years_gain_only")
        uu.upload_final_set(stage_output_dir_list[3], "growth_years_no_change")
        uu.upload_final_set(stage_output_dir_list[3], "growth_years_loss_and_gain")

        # Uploads output from this stage
        uu.upload_final_set(stage_output_dir_list[3], stage_output_pattern_list[3])


    # Creates tiles of annual AGB and BGB gain rate for non-mangrove, non-planted forest using the standard model
    # removal function
    if 'annual_removals' in actual_stages:

        uu.print_log('Creating annual removals for natural forest')

        # Files to download for this script.
        download_dict = {
            cn.age_cat_natrl_forest_dir: [cn.pattern_age_cat_natrl_forest],
            cn.cont_eco_dir: [cn.pattern_cont_eco_processed],
            cn.plant_pre_2000_processed_dir: [cn.pattern_plant_pre_2000]
        }


        tile_id_list = uu.tile_list_s3(cn.Brazil_forest_extent_2000_processed_dir)
        # tile_id_list = ['00N_050W']
        uu.print_log(tile_id_list)
        uu.print_log("There are {} tiles to process".format(str(len(tile_id_list))) + "\n")


        # If the model run isn't the standard one, the output directory and file names are changed.
        # This adapts just the relevant items in the output directory and pattern lists (annual removals).
        if sensit_type != 'std':
            uu.print_log("Changing output directory and file name pattern based on sensitivity analysis")
            stage_output_dir_list = uu.alter_dirs(sensit_type, master_output_dir_list[4:6])
            stage_output_pattern_list = uu.alter_patterns(sensit_type, master_output_pattern_list[4:6])


        # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
        for key, values in download_dict.items():
            dir = key
            pattern = values[0]
            uu.s3_flexible_download(dir, pattern, cn.docker_base_dir, sensit_type, tile_id_list)


        # Table with IPCC Table 4.9 default gain rates
        cmd = ['aws', 's3', 'cp', os.path.join(cn.gain_spreadsheet_dir, cn.gain_spreadsheet), cn.docker_base_dir]
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
        for key, value in age_dict.items():
            gain_table_dict[value] = 0

        # Converts all the keys (continent-ecozone-age codes) to float type
        gain_table_dict = {float(key): value for key, value in gain_table_dict.items()}

        uu.print_log(gain_table_dict)


        # This configuration of the multiprocessing call is necessary for passing multiple arguments to the main function
        # It is based on the example here: http://spencerimp.blogspot.com/2015/12/python-multiprocess-with-multiple.html
        # processes=24 peaks at about 440 GB of memory on an r4.16xlarge machine
        output_pattern_list = stage_output_pattern_list
        pool = multiprocessing.Pool(int(cn.count/2))
        pool.map(partial(annual_gain_rate_natrl_forest.annual_gain_rate, sensit_type=sensit_type,
                         gain_table_dict=gain_table_dict,
                         output_pattern_list=output_pattern_list), tile_id_list)
        pool.close()
        pool.join()

        # # For single processor use
        # for tile in tile_id_list:
        #
        #     annual_gain_rate_natrl_forest.annual_gain_rate(tile, sensit_type, gain_table_dict, stage_output_pattern_list)


        # Uploads outputs from this stage
        for i in range(0, len(stage_output_dir_list)):
            uu.upload_final_set(stage_output_dir_list[i], stage_output_pattern_list[i])


    # Creates tiles of cumulative AGCO2 and BGCO2 gain rate for non-mangrove, non-planted forest using the standard model
    # removal function
    if 'cumulative_removals' in actual_stages:

        uu.print_log('Creating cumulative removals for natural forest')

        # Files to download for this script.
        download_dict = {
            cn.annual_gain_AGB_natrl_forest_dir: [cn.pattern_annual_gain_AGB_natrl_forest],
            cn.annual_gain_BGB_natrl_forest_dir: [cn.pattern_annual_gain_BGB_natrl_forest],
            cn.gain_year_count_natrl_forest_dir: [cn.pattern_gain_year_count_natrl_forest]
        }


        tile_id_list = uu.tile_list_s3(cn.Brazil_forest_extent_2000_processed_dir)
        # tile_id_list = ['00N_050W']
        uu.print_log(tile_id_list)
        uu.print_log("There are {} tiles to process".format(str(len(tile_id_list))) + "\n")


        # If the model run isn't the standard one, the output directory and file names are changed.
        # This adapts just the relevant items in the output directory and pattern lists (cumulative removals).
        if sensit_type != 'std':
            uu.print_log("Changing output directory and file name pattern based on sensitivity analysis")
            stage_output_dir_list = uu.alter_dirs(sensit_type, master_output_dir_list[6:8])
            stage_output_pattern_list = uu.alter_patterns(sensit_type, master_output_pattern_list[6:8])


        # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
        for key, values in download_dict.items():
            dir = key
            pattern = values[0]
            uu.s3_flexible_download(dir, pattern, cn.docker_base_dir, sensit_type, tile_id_list)


        # Calculates cumulative aboveground carbon gain in non-mangrove planted forests
        output_pattern_list = stage_output_pattern_list
        pool = multiprocessing.Pool(int(cn.count/3))
        pool.map(partial(cumulative_gain_natrl_forest.cumulative_gain_AGCO2, output_pattern_list=output_pattern_list,
                         sensit_type=sensit_type), tile_id_list)

        # Calculates cumulative belowground carbon gain in non-mangrove planted forests
        pool = multiprocessing.Pool(int(cn.count/3))
        pool.map(partial(cumulative_gain_natrl_forest.cumulative_gain_BGCO2, output_pattern_list=output_pattern_list,
                         sensit_type=sensit_type), tile_id_list)
        pool.close()
        pool.join()

        # # For single processor use
        # for tile_id in tile_id_list:
        #     cumulative_gain_natrl_forest.cumulative_gain_AGCO2(tile_id, stage_output_pattern_list[0], sensit_type)
        #
        # for tile_id in tile_id_list:
        #     cumulative_gain_natrl_forest.cumulative_gain_BGCO2(tile_id, stage_output_pattern_list[1], sensit_type)


        # Uploads outputs from this stage
        for i in range(0, len(stage_output_dir_list)):
            uu.upload_final_set(stage_output_dir_list[i], stage_output_pattern_list[i])


    # Creates tiles of annual gain rate and cumulative removals for all forest types (above + belowground)
    if 'removals_merged' in actual_stages:

        uu.print_log('Creating annual and cumulative removals for all forest types combined (above + belowground)')

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


        tile_id_list = uu.tile_list_s3(cn.Brazil_forest_extent_2000_processed_dir)
        # tile_id_list = ['00N_050W']
        uu.print_log(tile_id_list)
        uu.print_log("There are {} tiles to process".format(str(len(tile_id_list))) + "\n")


        # If the model run isn't the standard one, the output directory and file names are changed.
        # This adapts just the relevant items in the output directory and pattern lists (cumulative removals).
        if sensit_type != 'std':
            uu.print_log("Changing output directory and file name pattern based on sensitivity analysis")
            stage_output_dir_list = uu.alter_dirs(sensit_type, master_output_dir_list[8:10])
            stage_output_pattern_list = uu.alter_patterns(sensit_type, master_output_pattern_list[8:10])


        # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
        for key, values in download_dict.items():
            dir = key
            pattern = values[0]
            uu.s3_flexible_download(dir, pattern, cn.docker_base_dir, sensit_type, tile_id_list)


        # For multiprocessing
        output_pattern_list = stage_output_pattern_list
        pool = multiprocessing.Pool(int(cn.count/3))
        pool.map(
            partial(merge_cumulative_annual_gain_all_forest_types.gain_merge, output_pattern_list=output_pattern_list,
                    sensit_type=sensit_type), tile_id_list)
        pool.close()
        pool.join()

        # # For single processor use
        # for tile_id in tile_id_list:
        #     merge_cumulative_annual_gain_all_forest_types.gain_merge(tile_id, output_pattern_list, sensit_type)

        # Uploads output tiles to s3
        for i in range(0, len(stage_output_dir_list)):
            uu.upload_final_set(stage_output_dir_list[i], stage_output_pattern_list[i])


    # Creates carbon pools in loss year
    if 'carbon_pools' in actual_stages:

        uu.print_log('Creating emissions year carbon pools')

        # Specifies that carbon pools are created for loss year rather than in 2000
        extent = 'loss'


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


        tile_id_list = uu.tile_list_s3(cn.Brazil_forest_extent_2000_processed_dir)
        # tile_id_list = ['00N_050W']
        uu.print_log(tile_id_list)
        uu.print_log("There are {} tiles to process".format(str(len(tile_id_list))) + "\n")

        for key, values in download_dict.items():
            dir = key
            pattern = values[0]
            uu.s3_flexible_download(dir, pattern, cn.docker_base_dir, sensit_type, tile_id_list)

        # If the model run isn't the standard one, the output directory and file names are changed
        if sensit_type != 'std':
            uu.print_log("Changing output directory and file name pattern based on sensitivity analysis")
            stage_output_dir_list = uu.alter_dirs(sensit_type, master_output_dir_list[10:16])
            stage_output_pattern_list = uu.alter_patterns(sensit_type, master_output_pattern_list[10:16])


        # Table with IPCC Wetland Supplement Table 4.4 default mangrove gain rates
        cmd = ['aws', 's3', 'cp', os.path.join(cn.gain_spreadsheet_dir, cn.gain_spreadsheet), cn.docker_base_dir]
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


        if extent == 'loss':

            uu.print_log("Creating tiles of emitted aboveground carbon (carbon 2000 + carbon accumulation until loss year)")
            # 16 processors seems to use more than 460 GB-- I don't know exactly how much it uses because I stopped it at 460
            # 14 processors maxes out at 410-415 GB
            # Creates a single filename pattern to pass to the multiprocessor call
            pattern = stage_output_pattern_list[0]
            pool = multiprocessing.Pool(int(cn.count/4))
            pool.map(partial(create_carbon_pools.create_emitted_AGC,
                             pattern=pattern, sensit_type=sensit_type), tile_id_list)
            pool.close()
            pool.join()

            # # For single processor use
            # for tile_id in tile_id_list:
            #     create_carbon_pools.create_emitted_AGC(tile_id, stage_output_pattern_list[0], sensit_type)

            uu.upload_final_set(stage_output_dir_list[0], stage_output_pattern_list[0])

        elif extent == '2000':

            uu.print_log("Creating tiles of aboveground carbon in 2000")
            # 16 processors seems to use more than 460 GB-- I don't know exactly how much it uses because I stopped it at 460
            # 14 processors maxes out at 415 GB
            # Creates a single filename pattern to pass to the multiprocessor call
            pattern = stage_output_pattern_list[0]
            pool = multiprocessing.Pool(processes=14)
            pool.map(partial(create_carbon_pools.create_2000_AGC,
                             pattern=pattern, sensit_type=sensit_type), tile_id_list)
            pool.close()
            pool.join()

            # # For single processor use
            # for tile_id in tile_id_list:
            #     create_carbon_pools.create_2000_AGC(tile_id, output_pattern_list[0], sensit_type)

            uu.upload_final_set(stage_output_dir_list[0], stage_output_pattern_list[0])

        else:
            raise Exception("Extent argument not valid")

        uu.print_log("Creating tiles of belowground carbon")
        # 18 processors used between 300 and 400 GB memory, so it was okay on a r4.16xlarge spot machine
        # Creates a single filename pattern to pass to the multiprocessor call
        pattern = stage_output_pattern_list[1]
        pool = multiprocessing.Pool(int(cn.count/2))
        pool.map(partial(create_carbon_pools.create_BGC, mang_BGB_AGB_ratio=mang_BGB_AGB_ratio,
                         extent=extent,
                         pattern=pattern, sensit_type=sensit_type), tile_id_list)
        pool.close()
        pool.join()

        # # For single processor use
        # for tile_id in tile_id_list:
        #     create_carbon_pools.create_BGC(tile_id, mang_BGB_AGB_ratio, extent, stage_output_pattern_list[1], sensit_type)

        uu.upload_final_set(stage_output_dir_list[1], stage_output_pattern_list[1])

        uu.print_log("Creating tiles of deadwood carbon")
        # processes=16 maxes out at about 430 GB
        # Creates a single filename pattern to pass to the multiprocessor call
        pattern = stage_output_pattern_list[2]
        pool = multiprocessing.Pool(int(cn.count/4))
        pool.map(
            partial(create_carbon_pools.create_deadwood, mang_deadwood_AGB_ratio=mang_deadwood_AGB_ratio,
                    extent=extent,
                    pattern=pattern, sensit_type=sensit_type), tile_id_list)
        pool.close()
        pool.join()

        # # For single processor use
        # for tile_id in tile_id_list:
        #     create_carbon_pools.create_deadwood(tile_id, mang_deadwood_AGB_ratio, extent, stage_output_pattern_list[2], sensit_type)

        uu.upload_final_set(stage_output_dir_list[2], stage_output_pattern_list[2])

        uu.print_log("Creating tiles of litter carbon")
        # Creates a single filename pattern to pass to the multiprocessor call
        pattern = stage_output_pattern_list[3]
        pool = multiprocessing.Pool(int(cn.count/4))
        pool.map(partial(create_carbon_pools.create_litter, mang_litter_AGB_ratio=mang_litter_AGB_ratio,
                         extent=extent,
                         pattern=pattern, sensit_type=sensit_type), tile_id_list)
        pool.close()
        pool.join()

        # # For single processor use
        # for tile_id in tile_id_list:
        #     create_carbon_pools.create_litter(tile_id, mang_litter_AGB_ratio, extent, stage_output_pattern_list[3], sensit_type)

        uu.upload_final_set(stage_output_dir_list[3], stage_output_pattern_list[3])

        if extent == 'loss':

            uu.print_log("Creating tiles of soil carbon")
            # Creates a single filename pattern to pass to the multiprocessor call
            pattern = stage_output_pattern_list[4]
            pool = multiprocessing.Pool(int(cn.count/3))
            pool.map(partial(create_carbon_pools.create_soil,
                             pattern=pattern, sensit_type=sensit_type), tile_id_list)
            pool.close()
            pool.join()

            # # For single processor use
            # for tile_id in tile_id_list:
            #     create_carbon_pools.create_soil(tile_id, stage_output_pattern_list[4], sensit_type)

            uu.upload_final_set(stage_output_dir_list[4], stage_output_pattern_list[4])

        elif extent == '2000':
            uu.print_log("Skipping soil for 2000 carbon pool calculation")

        else:
            raise Exception("Extent argument not valid")

        uu.print_log("Creating tiles of total carbon")
        # I tried several different processor numbers for this. Ended up using 14 processors, which used about 380 GB memory
        # at peak. Probably could've handled 16 processors on an r4.16xlarge machine but I didn't feel like taking the time to check.
        # Creates a single filename pattern to pass to the multiprocessor call
        pattern = stage_output_pattern_list[5]
        pool = multiprocessing.Pool(int(cn.count/4))
        pool.map(partial(create_carbon_pools.create_total_C, extent=extent,
                         pattern=pattern, sensit_type=sensit_type), tile_id_list)
        pool.close()
        pool.join()

        # # For single processor use
        # for tile_id in tile_id_list:
        #     create_carbon_pools.create_total_C(tile_id, extent, stage_output_pattern_list[5], sensit_type)

        uu.upload_final_set(stage_output_dir_list[5], stage_output_pattern_list[5])


if __name__ == '__main__':
    main()