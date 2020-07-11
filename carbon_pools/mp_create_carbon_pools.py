'''
This script creates carbon pools.
For the year 2000, it creates aboveground, belowground, deadwood, litter, and total
carbon pools (soil is created in a separate script but is brought in to create total carbon). These are to the extent
of WHRC and mangrove biomass 2000.

It also creates carbon pools for the year of loss/emissions-- only for pixels that had loss. To do this, it adds
CO2 (carbon) accumulated since 2000 to the C (biomass) 2000 stock, so that the CO2 (carbon) emitted is 2000 + gains
until loss. (For Hansen loss+gain pixels, only the portion of C that is accumulated before loss is included in the
lost carbon (lossyr-1), not the entire carbon gain of the pixel.) Because the emissions year carbon pools depend on
carbon removals, any time the removals model changes, the emissions year carbon pools need to be regenerated.

In both cases (carbon pools in 2000 and in the loss year), BGC, deadwood, and litter are calculated from AGC. Thus,
there are two AGC functions (one for AGC2000 and one for AGC in loss year) but only one function for BGC, deadwood,
litter, and total C (since those are purely functions of the AGC supplied to them).

The carbon pools in 2000 are not used for the model at all; they are purely for illustrative purposes. Only the
emissions year pools are used for the model.

Which carbon pools are being generated (2000 or loss pixels) is controlled through the command line argument --extent (-e).
This extent argument determines which AGC function is used and how the outputs of the other pools' scripts are named.

NOTE: Because there are so many input files, this script needs a machine with extra disk space.
Thus, create a spot machine with extra disk space: spotutil new r4.16xlarge dgibbs_wri --disk_size 1024    (this is the maximum value).
'''


import multiprocessing
import pandas as pd
from subprocess import Popen, PIPE, STDOUT, check_call
import datetime
import os
import argparse
from functools import partial
import sys
sys.path.append('/usr/local/app/carbon_pools/')
import create_carbon_pools
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def mp_create_carbon_pools(sensit_type, tile_id_list, carbon_pool_extent, run_date = None):

    os.chdir(cn.docker_base_dir)

    if (sensit_type != 'std') & (carbon_pool_extent != 'loss'):
        uu.exception_log("Sensitivity analysis run must use 'loss' extent")

    # Checks the validity of the carbon_pool_extent argument
    if (carbon_pool_extent not in ['loss', '2000']):
        uu.exception_log("Invalid carbon_pool_extent input. Please choose loss or 2000.")


    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # List of tiles to run in the model
        tile_id_list = uu.create_combined_tile_list(cn.WHRC_biomass_2000_unmasked_dir,
                                                    cn.annual_gain_AGB_mangrove_dir,
                                                    sensit_type=sensit_type
                                                    )
    uu.print_log(tile_id_list)
    uu.print_log("There are {} tiles to process".format(str(len(tile_id_list))) + "\n")


    # Output files and patterns and files to download if carbon pools for loss year are being generated
    if carbon_pool_extent == 'loss':

        # List of output directories and output file name patterns
        output_dir_list = [cn.AGC_emis_year_dir, cn.BGC_emis_year_dir, cn.deadwood_emis_year_2000_dir,
                           cn.litter_emis_year_2000_dir, cn.soil_C_emis_year_2000_dir, cn.total_C_emis_year_dir]
        output_pattern_list = [cn.pattern_AGC_emis_year, cn.pattern_BGC_emis_year, cn.pattern_deadwood_emis_year_2000,
                               cn.pattern_litter_emis_year_2000, cn.pattern_soil_C_emis_year_2000, cn.pattern_total_C_emis_year]

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
        elif sensit_type == 'Mekong_loss':
            download_dict[cn.Mekong_loss_processed_dir] = [cn.pattern_Mekong_loss_processed]
        else:
            download_dict[cn.loss_dir] = ['']


    # Output files and patterns and files to download if carbon pools for 2000 are being generated
    elif carbon_pool_extent == '2000':

        # List of output directories and output file name patterns
        output_dir_list = [cn.AGC_2000_dir, cn.BGC_2000_dir, cn.deadwood_2000_dir,
                           cn.litter_2000_dir, cn.soil_C_full_extent_2000_dir, cn.total_C_2000_dir]
        output_pattern_list = [cn.pattern_AGC_2000, cn.pattern_BGC_2000, cn.pattern_deadwood_2000,
                               cn.pattern_litter_2000, cn.pattern_soil_C_full_extent_2000, cn.pattern_total_C_2000]

        # Files to download for this script
        download_dict = {
            cn.mangrove_biomass_2000_dir: [cn.pattern_mangrove_biomass_2000],
            cn.cont_eco_dir: [cn.pattern_cont_eco_processed],
            cn.bor_tem_trop_processed_dir: [cn.pattern_bor_tem_trop_processed],
            cn.precip_processed_dir: [cn.pattern_precip],
            cn.elevation_processed_dir: [cn.pattern_elevation],
            cn.soil_C_full_extent_2000_dir: [cn.pattern_soil_C_full_extent_2000],
            cn.gain_dir: [cn.pattern_gain],
        }

        # Adds the correct AGB tiles to the download dictionary depending on the model run
        if sensit_type == 'biomass_swap':
            download_dict[cn.JPL_processed_dir] = [cn.pattern_JPL_unmasked_processed]
        else:
            download_dict[cn.WHRC_biomass_2000_unmasked_dir] = [cn.pattern_WHRC_biomass_2000_unmasked]

        # Adds the correct loss tile to the download dictionary depending on the model run
        if sensit_type == 'legal_Amazon_loss':
            download_dict[cn.Brazil_annual_loss_processed_dir] = [cn.pattern_Brazil_annual_loss_processed]
        elif sensit_type == 'Mekong_loss':
            download_dict[cn.Mekong_loss_processed_dir] = [cn.pattern_Mekong_loss_processed]
        else:
            download_dict[cn.loss_dir] = ['']

    else:
        uu.exception_log('Extent not valid.')


    for key, values in download_dict.items():
        dir = key
        pattern = values[0]
        uu.s3_flexible_download(dir, pattern, cn.docker_base_dir, sensit_type, tile_id_list)


    # If the model run isn't the standard one, the output directory and file names are changed
    if sensit_type != 'std':
        uu.print_log("Changing output directory and file name pattern based on sensitivity analysis")
        output_dir_list = uu.alter_dirs(sensit_type, output_dir_list)
        output_pattern_list = uu.alter_patterns(sensit_type, output_pattern_list)

    # A date can optionally be provided by the full model script or a run of this script.
    # This replaces the date in constants_and_names.
    if run_date is not None:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, run_date)


    # Table with IPCC Wetland Supplement Table 4.4 default mangrove gain rates
    cmd = ['aws', 's3', 'cp', os.path.join(cn.gain_spreadsheet_dir, cn.gain_spreadsheet), cn.docker_base_dir]

    # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        uu.log_subprocess_output(process.stdout)

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

        uu.print_log("Creating tiles of emitted aboveground carbon (carbon 2000 + carbon accumulation until loss year)")
        # 16 processors seems to use more than 460 GB-- I don't know exactly how much it uses because I stopped it at 460
        # Creates a single filename pattern to pass to the multiprocessor call
        pattern = output_pattern_list[0]
        if cn.count == 96:
            processes = 12  # 12 processors = 580 GB peak (stays there for a while); 14 = >740 GB peak
        else:
            processes = 8
        uu.print_log('AGC loss year max processors=', processes)
        pool = multiprocessing.Pool(processes)
        pool.map(partial(create_carbon_pools.create_emitted_AGC,
                         pattern=pattern, sensit_type=sensit_type), tile_id_list)
        pool.close()
        pool.join()

        # # For single processor use
        # for tile_id in tile_id_list:
        #     create_carbon_pools.create_emitted_AGC(tile_id, output_pattern_list[0], sensit_type)

        uu.upload_final_set(output_dir_list[0], output_pattern_list[0])
        uu.check_storage()

    elif carbon_pool_extent == '2000':

        uu.print_log("Creating tiles of aboveground carbon in 2000")
        # Creates a single filename pattern to pass to the multiprocessor call
        pattern = output_pattern_list[0]
        if cn.count == 96:
            processes = 12  # 14 processors = XXX GB peak
        else:
            processes = 8
        uu.print_log('AGC 2000 max processors=', processes)
        pool = multiprocessing.Pool(processes)
        pool.map(partial(create_carbon_pools.create_2000_AGC,
                         pattern=pattern, sensit_type=sensit_type), tile_id_list)
        pool.close()
        pool.join()

        # # For single processor use
        # for tile_id in tile_id_list:
        #     create_carbon_pools.create_2000_AGC(tile_id, output_pattern_list[0], sensit_type)

        uu.upload_final_set(output_dir_list[0], output_pattern_list[0])
        uu.check_storage()

    else:
        uu.exception_log("Extent argument not valid")


    uu.print_log("Creating tiles of belowground carbon")
    # Creates a single filename pattern to pass to the multiprocessor call
    pattern = output_pattern_list[1]
    if cn.count == 96:
        processes = 24  # 16 processors = 400 GB peak; 24 = XXX GB peak
    else:
        processes = 8
    uu.print_log('BGC loss year max processors=', processes)
    pool = multiprocessing.Pool(processes)
    pool.map(partial(create_carbon_pools.create_BGC, mang_BGB_AGB_ratio=mang_BGB_AGB_ratio,
                     carbon_pool_extent=carbon_pool_extent,
                     pattern=pattern, sensit_type=sensit_type), tile_id_list)
    pool.close()
    pool.join()

    # # For single processor use
    # for tile_id in tile_id_list:
    #     create_carbon_pools.create_BGC(tile_id, mang_BGB_AGB_ratio, carbon_pool_extent, output_pattern_list[1], sensit_type)

    uu.upload_final_set(output_dir_list[1], output_pattern_list[1])
    uu.check_storage()


    uu.print_log("Creating tiles of deadwood carbon")
    # processes=16 maxes out at about 430 GB
    # Creates a single filename pattern to pass to the multiprocessor call
    pattern = output_pattern_list[2]
    if cn.count == 96:
        processes = 16  # 16 processors = 700 GB peak
    else:
        processes = 8
    uu.print_log('Deadwood loss year max processors=', processes)
    pool = multiprocessing.Pool(processes)
    pool.map(
        partial(create_carbon_pools.create_deadwood, mang_deadwood_AGB_ratio=mang_deadwood_AGB_ratio,
                carbon_pool_extent=carbon_pool_extent,
                pattern=pattern, sensit_type=sensit_type), tile_id_list)
    pool.close()
    pool.join()

    # # For single processor use
    # for tile_id in tile_id_list:
    #     create_carbon_pools.create_deadwood(tile_id, mang_deadwood_AGB_ratio, carbon_pool_extent, output_pattern_list[2], sensit_type)

    uu.upload_final_set(output_dir_list[2], output_pattern_list[2])
    uu.check_storage()


    uu.print_log("Creating tiles of litter carbon")
    # Creates a single filename pattern to pass to the multiprocessor call
    pattern = output_pattern_list[3]
    if cn.count == 96:
        processes = 16  # 16 processors = 700 GB peak
    else:
        processes = 8
    uu.print_log('Litter loss year max processors=', processes)
    pool = multiprocessing.Pool(processes)
    pool.map(partial(create_carbon_pools.create_litter, mang_litter_AGB_ratio=mang_litter_AGB_ratio,
                     carbon_pool_extent=carbon_pool_extent,
                     pattern=pattern, sensit_type=sensit_type), tile_id_list)
    pool.close()
    pool.join()

    # # For single processor use
    # for tile_id in tile_id_list:
    #     create_carbon_pools.create_litter(tile_id, mang_litter_AGB_ratio, carbon_pool_extent, output_pattern_list[3], sensit_type)

    uu.upload_final_set(output_dir_list[3], output_pattern_list[3])
    uu.check_storage()


    if carbon_pool_extent == 'loss':

        uu.print_log("Creating tiles of soil carbon")
        # Creates a single filename pattern to pass to the multiprocessor call
        pattern = output_pattern_list[4]
        if cn.count == 96:
            processes = 16  # 16 processors = XXX GB peak
        else:
            processes = 8
        uu.print_log('Soil carbon loss year max processors=', processes)
        pool = multiprocessing.Pool(processes)
        pool.map(partial(create_carbon_pools.create_soil,
                         pattern=pattern, sensit_type=sensit_type), tile_id_list)
        pool.close()
        pool.join()

        # # For single processor use
        # for tile_id in tile_id_list:
        #     create_carbon_pools.create_soil(tile_id, output_pattern_list[4], sensit_type)

        uu.upload_final_set(output_dir_list[4], output_pattern_list[4])
        uu.check_storage()

    elif carbon_pool_extent == '2000':
        uu.print_log("Skipping soil for 2000 carbon pool calculation")

    else:
        uu.exception_log("Extent argument not valid")


    uu.print_log("Creating tiles of total carbon")
    # I tried several different processor numbers for this. Ended up using 14 processors, which used about 380 GB memory
    # at peak. Probably could've handled 16 processors on an r4.16xlarge machine but I didn't feel like taking the time to check.
    # Creates a single filename pattern to pass to the multiprocessor call
    pattern = output_pattern_list[5]
    if cn.count == 96:
        processes = 14  # 14 processors = XXX GB peak
    else:
        processes = 8
    uu.print_log('Total carbon loss year max processors=', processes)
    pool = multiprocessing.Pool(processes)
    pool.map(partial(create_carbon_pools.create_total_C, carbon_pool_extent=carbon_pool_extent,
                     pattern=pattern, sensit_type=sensit_type), tile_id_list)
    pool.close()
    pool.join()

    # # For single processor use
    # for tile_id in tile_id_list:
    #     create_carbon_pools.create_total_C(tile_id, carbon_pool_extent, output_pattern_list[5], sensit_type)

    uu.upload_final_set(output_dir_list[5], output_pattern_list[5])


if __name__ == '__main__':

    # The argument for what kind of model run is being done: standard conditions or a sensitivity analysis run
    parser = argparse.ArgumentParser(
        description='Create tiles of the number of years of carbon gain for mangrove forests')
    parser.add_argument('--model-type', '-t', required=True,
                        help='{}'.format(cn.model_type_arg_help))
    parser.add_argument('--tile_id_list', '-l', required=True,
                        help='List of tile ids to use in the model. Should be of form 00N_110E or 00N_110E,00N_120E or all.')
    parser.add_argument('--carbon_pool_extent', '-e', required=True,
                        help='Extent over which carbon pools should be calculated: loss or 2000')
    parser.add_argument('--run-date', '-d', required=False,
                        help='Date of run. Must be format YYYYMMDD.')
    args = parser.parse_args()
    sensit_type = args.model_type
    tile_id_list = args.tile_id_list
    carbon_pool_extent = args.carbon_pool_extent  # Tells the pool creation functions to calculate carbon pools as they were at the year of loss in loss pixels only
    run_date = args.run_date

    # Create the output log
    uu.initiate_log(tile_id_list=tile_id_list, sensit_type=sensit_type, run_date=run_date, carbon_pool_extent=carbon_pool_extent)

    # Checks whether the sensitivity analysis and tile_id_list arguments are valid
    uu.check_sensit_type(sensit_type)
    tile_id_list = uu.tile_id_list_check(tile_id_list)

    mp_create_carbon_pools(sensit_type=sensit_type, tile_id_list=tile_id_list,
                           carbon_pool_extent=carbon_pool_extent, run_date=run_date)