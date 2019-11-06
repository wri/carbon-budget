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


import create_carbon_pools
import multiprocessing
import pandas as pd
import subprocess
import os
import argparse
from functools import partial
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def main ():

    # The argument for what kind of model run is being done: standard conditions or a sensitivity analysis run
    parser = argparse.ArgumentParser(description='Create tiles of the number of years of carbon gain for mangrove forests')
    parser.add_argument('--model-type', '-t', required=True,
                        help='{}'.format(cn.model_type_arg_help))
    parser.add_argument('--extent', '-e', required=True,
                        help='Extent over which carbon pools should be calculated: loss or 2000')
    args = parser.parse_args()
    sensit_type = args.model_type
    extent = args.extent     # Tells the pool creation functions to calculate carbon pools as they were at the year of loss in loss pixels only
    # Checks whether the sensitivity analysis argument is valid
    uu.check_sensit_type(sensit_type)

    if (sensit_type != 'std') & (extent != 'loss'):
        raise Exception("Sensitivity analysis run must use 'loss' extent")

    # Checks the validity of the extent argument
    if (extent not in ['loss', '2000']):
        raise Exception("Invalid extent input. Please choose loss or 2000.")


    # List of tiles to run in the model
    tile_id_list = uu.create_combined_tile_list(cn.WHRC_biomass_2000_unmasked_dir,
                                             cn.annual_gain_AGB_mangrove_dir
                                             )
    # tile_id_list = ['30N_080W'] # test tiles
    # tile_id_list = ['00N_110E'] # test tiles
    print tile_id_list
    print "There are {} unique tiles to process".format(str(len(tile_id_list))) + "\n"


    # Output files and patterns and files to download if carbon pools for loss year are being generated
    if extent == 'loss':

        # List of output directories and output file name patterns
        output_dir_list = [cn.AGC_emis_year_dir, cn.BGC_emis_year_dir, cn.deadwood_emis_year_2000_dir,
                           cn.litter_emis_year_2000_dir, cn.soil_C_emis_year_2000_dir, cn.total_C_emis_year_dir]
        output_pattern_list = [cn.pattern_AGC_emis_year, cn.pattern_BGC_emis_year, cn.pattern_deadwood_emis_year_2000,
                               cn.pattern_litter_emis_year_2000, cn.pattern_soil_C_emis_year_2000, cn.pattern_total_C_emis_year]

        # Files to download for this script. 'true'/'false' says whether the input directory and pattern should be
        # changed for a sensitivity analysis. This does not need to change based on what run is being done;
        # this assignment should be true for all sensitivity analyses and the standard model.
        download_dict = {
            cn.WHRC_biomass_2000_unmasked_dir: [cn.pattern_WHRC_biomass_2000_unmasked, 'false'],
            cn.mangrove_biomass_2000_dir: [cn.pattern_mangrove_biomass_2000, 'false'],
            cn.cont_eco_dir: [cn.pattern_cont_eco_processed, 'false'],
            cn.bor_tem_trop_processed_dir: [cn.pattern_bor_tem_trop_processed, 'false'],
            cn.precip_processed_dir: [cn.pattern_precip, 'false'],
            cn.elevation_processed_dir: [cn.pattern_elevation, 'false'],
            cn.soil_C_full_extent_2000_dir: [cn.pattern_soil_C_full_extent_2000, 'false'],
            cn.loss_dir: ['', 'false'],
            cn.gain_dir: [cn.pattern_gain, 'false'],
            cn.AGC_emis_year_dir: [cn.pattern_AGC_emis_year, 'false']
            # cn.cumul_gain_AGCO2_mangrove_dir: [cn.pattern_cumul_gain_AGCO2_mangrove, 'true'],
            # cn.cumul_gain_AGCO2_planted_forest_non_mangrove_dir: [cn.pattern_cumul_gain_AGCO2_planted_forest_non_mangrove, 'true'],
            # cn.cumul_gain_AGCO2_natrl_forest_dir: [cn.pattern_cumul_gain_AGCO2_natrl_forest, 'true'],
            # cn.annual_gain_AGB_mangrove_dir: [cn.pattern_annual_gain_AGB_mangrove, 'false'],
            # cn.annual_gain_AGB_planted_forest_non_mangrove_dir: [cn.pattern_annual_gain_AGB_planted_forest_non_mangrove, 'false'],
            # cn.annual_gain_AGB_natrl_forest_dir: [cn.pattern_annual_gain_AGB_natrl_forest, 'false']
        }

    # Output files and patterns and files to download if carbon pools for 2000 are being generated
    elif extent == '2000':

        # List of output directories and output file name patterns
        output_dir_list = [cn.AGC_2000_dir, cn.BGC_2000_dir, cn.deadwood_2000_dir,
                           cn.litter_2000_dir, cn.soil_C_full_extent_2000_dir, cn.total_C_2000_dir]
        output_pattern_list = [cn.pattern_AGC_2000, cn.pattern_BGC_2000, cn.pattern_deadwood_2000,
                               cn.pattern_litter_2000, cn.pattern_soil_C_full_extent_2000, cn.pattern_total_C_2000]

        # Files to download for this script. 'true'/'false' says whether the input directory and pattern should be
        # changed for a sensitivity analysis. This does not need to change based on what run is being done;
        # this assignment should be true for all sensitivity analyses and the standard model.
        download_dict = {
            cn.WHRC_biomass_2000_unmasked_dir: [cn.pattern_WHRC_biomass_2000_unmasked, 'false'],
            cn.mangrove_biomass_2000_dir: [cn.pattern_mangrove_biomass_2000, 'false'],
            cn.cont_eco_dir: [cn.pattern_cont_eco_processed, 'false'],
            cn.bor_tem_trop_processed_dir: [cn.pattern_bor_tem_trop_processed, 'false'],
            cn.precip_processed_dir: [cn.pattern_precip, 'false'],
            cn.elevation_processed_dir: [cn.pattern_elevation, 'false'],
            cn.soil_C_full_extent_2000_dir: [cn.pattern_soil_C_full_extent_2000, 'false'],
            cn.loss_dir: ['', 'false'],
            cn.gain_dir: [cn.pattern_gain, 'false'],
        }

    else:
        raise Exception('Extent not valid.')


    for key, values in download_dict.iteritems():
        dir = key
        pattern = values[0]
        sensit_use = values[1]
        uu.s3_flexible_download(dir, pattern, '.', sensit_type, sensit_use, tile_id_list)


    # If the model run isn't the standard one, the output directory and file names are changed
    if sensit_type != 'std':
        print "Changing output directory and file name pattern based on sensitivity analysis"
        output_dir_list = uu.alter_dirs(sensit_type, output_dir_list)
        output_pattern_list = uu.alter_patterns(sensit_type, output_pattern_list)



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


    # if extent == 'loss':
    #
    #     print "Creating tiles of emitted aboveground carbon (carbon 2000 + carbon accumulation until loss year)"
    #     # 16 processors seems to use more than 460 GB-- I don't know exactly how much it uses because I stopped it at 460
    #     # 14 processors maxes out at 410-415 GB
    #     # Creates a single filename pattern to pass to the multiprocessor call
    #     pattern = output_pattern_list[0]
    #     count = multiprocessing.cpu_count()
    #     pool = multiprocessing.Pool(processes=14)
    #     pool.map(partial(create_carbon_pools.create_emitted_AGC,
    #                      pattern=pattern, sensit_type=sensit_type), tile_id_list)
    #     pool.close()
    #     pool.join()
    #
    #     # # For single processor use
    #     # for tile_id in tile_id_list:
    #     #     create_carbon_pools.create_emitted_AGC(tile_id, output_pattern_list[0], sensit_type)
    #
    #     uu.upload_final_set(output_dir_list[0], output_pattern_list[0])
    #     # cmd = ['rm *{}*.tif'.format(output_pattern_list[0])]
    #     # subprocess.check_call(cmd)
    #
    # elif extent == '2000':
    #
    #     print "Creating tiles of aboveground carbon in 2000"
    #     # 16 processors seems to use more than 460 GB-- I don't know exactly how much it uses because I stopped it at 460
    #     # 14 processors maxes out at 415 GB
    #     # Creates a single filename pattern to pass to the multiprocessor call
    #     pattern = output_pattern_list[0]
    #     count = multiprocessing.cpu_count()
    #     pool = multiprocessing.Pool(processes=16)
    #     pool.map(partial(create_carbon_pools.create_2000_AGC,
    #                      pattern=pattern, sensit_type=sensit_type), tile_id_list)
    #     pool.close()
    #     pool.join()
    #
    #     # # For single processor use
    #     # for tile_id in tile_id_list:
    #     #     create_carbon_pools.create_2000_AGC(tile_id, output_pattern_list[0], sensit_type)
    #
    #     uu.upload_final_set(output_dir_list[0], output_pattern_list[0])
    #     # cmd = ['rm *{}*.tif'.format(output_pattern_list[0])]
    #     # subprocess.check_call(cmd)
    #
    # else:
    #     raise Exception("Extent argument not valid")
    #
    #
    # print "Creating tiles of belowground carbon"
    # # 18 processors used between 300 and 400 GB memory, so it was okay on a r4.16xlarge spot machine
    # # Creates a single filename pattern to pass to the multiprocessor call
    # pattern = output_pattern_list[1]
    # count = multiprocessing.cpu_count()
    # pool = multiprocessing.Pool(processes=20)
    # pool.map(partial(create_carbon_pools.create_BGC, mang_BGB_AGB_ratio=mang_BGB_AGB_ratio,
    #                  extent=extent,
    #                  pattern=pattern, sensit_type=sensit_type), tile_id_list)
    # pool.close()
    # pool.join()
    #
    # # # For single processor use
    # # for tile_id in tile_id_list:
    # #     create_carbon_pools.create_BGC(tile_id, mang_BGB_AGB_ratio, extent, output_pattern_list[1], sensit_type)
    #
    # uu.upload_final_set(output_dir_list[1], output_pattern_list[1])
    # # cmd = ['rm *{}*.tif'.format(output_pattern_list[1])]
    # # subprocess.check_call(cmd)


    print "Creating tiles of deadwood carbon"
    # processes=16 maxes out at about 430 GB
    # Creates a single filename pattern to pass to the multiprocessor call
    pattern = output_pattern_list[2]
    count = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=16)
    pool.map(
        partial(create_carbon_pools.create_deadwood, mang_deadwood_AGB_ratio=mang_deadwood_AGB_ratio,
                extent=extent,
                pattern=pattern, sensit_type=sensit_type), tile_id_list)
    pool.close()
    pool.join()

    # # For single processor use
    # for tile_id in tile_id_list:
    #     create_carbon_pools.create_deadwood(tile_id, mang_deadwood_AGB_ratio, extent, output_pattern_list[2], sensit_type)

    uu.upload_final_set(output_dir_list[2], output_pattern_list[2])
    # cmd = ['rm *{}*.tif'.format(output_pattern_list[2])]
    # subprocess.check_call(cmd)


    print "Creating tiles of litter carbon"
    # processes=16 maxes out at about 420 GB
    # Creates a single filename pattern to pass to the multiprocessor call
    pattern = output_pattern_list[3]
    count = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=16)
    pool.map(partial(create_carbon_pools.create_litter, mang_litter_AGB_ratio=mang_litter_AGB_ratio,
                     extent=extent,
                     pattern=pattern, sensit_type=sensit_type), tile_id_list)
    pool.close()
    pool.join()

    # # For single processor use
    # for tile_id in tile_id_list:
    #     create_carbon_pools.create_litter(tile_id, mang_litter_AGB_ratio, extent, output_pattern_list[3], sensit_type)

    uu.upload_final_set(output_dir_list[3], output_pattern_list[3])
    # cmd = ['rm *{}*.tif'.format(output_pattern_list[3])]
    # subprocess.check_call(cmd)


    if extent == 'loss':

        print "Creating tiles of soil carbon"
        # Creates a single filename pattern to pass to the multiprocessor call
        pattern = output_pattern_list[4]
        count = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(processes=16)
        pool.map(partial(create_carbon_pools.create_soil,
                         pattern=pattern, sensit_type=sensit_type), tile_id_list)
        pool.close()
        pool.join()

        # # For single processor use
        # for tile_id in tile_id_list:
        #     create_carbon_pools.create_soil(tile_id, output_pattern_list[4], sensit_type)

        uu.upload_final_set(output_dir_list[4], output_pattern_list[4])
        # cmd = ['rm *{}*.tif'.format(output_pattern_list[4])]
        # subprocess.check_call(cmd)

    elif extent == '2000':
        print "Skipping soil for 2000 carbon pool calculation"

    else:
        raise Exception("Extent argument not valid")


    print "Creating tiles of total carbon"
    # I tried several different processor numbers for this. Ended up using 14 processors, which used about 380 GB memory
    # at peak. Probably could've handled 16 processors on an r4.16xlarge machine but I didn't feel like taking the time to check.
    # Creates a single filename pattern to pass to the multiprocessor call
    pattern = output_pattern_list[5]
    count = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=14)
    pool.map(partial(create_carbon_pools.create_total_C, extent=extent,
                     pattern=pattern, sensit_type=sensit_type), tile_id_list)
    pool.close()
    pool.join()

    # # For single processor use
    # for tile_id in tile_id_list:
    #     create_carbon_pools.create_total_C(tile_id, extent, output_pattern_list[5], sensit_type)

    uu.upload_final_set(output_dir_list[5], output_pattern_list[5])
    # cmd = ['rm *{}*.tif'.format(output_pattern_list[5])]
    # subprocess.check_call(cmd)


if __name__ == '__main__':
    main()