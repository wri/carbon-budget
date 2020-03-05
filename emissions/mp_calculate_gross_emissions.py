'''
This script calculates the gross emissions in tonnes CO2e/ha for every loss pixel.
The properties of each pixel determine the appropriate emissions equation, the constants for the equation, and the
carbon pool values that go into the equation.
Unlike all other flux model components, this one uses C++ to quickly iterate through every pixel in each tile.
Before running the model, the C++ script must be compiled.
From carbon-budget/emissions/, do:
c++ ./cpp_util/calc_gross_emissions_generic.cpp -o ./cpp_util/calc_gross_emissions_generic.exe -lgdal
(for the standard model and some sensitivity analysis versions).
calc_gross_emissions_generic.exe should appear in the directory.
For the sensitivity analyses that use a different gross emissions C++ script (currently, soil_only, no_shifting_ag,
and convert_to_grassland), do:
c++ ./cpp_util/calc_gross_emissions_<sensit_type>.cpp -o ./cpp_util/calc_gross_emissions_<sensit_type>.exe -lgdal
Run mp_calculate_gross_emissions.py by typing python mp_calculate_gross_emissions.py -p [POOL_OPTION] -t [MODEL_TYPE].
The Python script will call the compiled C++ code as needed.
The other C++ scripts (equations.cpp and flu_val.cpp) do not need to be compiled.
The --pools-to-use argument specifies whether to calculate gross emissions from biomass+soil or just from soil.
The --model-type argument specifies whether the model run is a sensitivity analysis or standard run.
Emissions from each driver (including loss that had no driver assigned) gets its own tile, as does all emissions combined.
Emissions from all drivers is also output as emissions due to CO2 only and emissions due to other GHG (CH4 and N2O).
The other output shows which branch of the decision tree that determines the emissions equation applies to each pixel.
These codes are summarized in carbon-budget/emissions/node_codes.txt
'''

import multiprocessing
import argparse
import os
import calculate_gross_emissions
from functools import partial
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def mp_calculate_gross_emissions(sensit_type, tile_id_list, pools, run_date = None, working_dir = None):

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    # If the tile_list argument is an s3 folder, the list of tiles in it is created
    if tile_id_list == 'all':
        # List of tiles to run in the model
        tile_id_list = uu.tile_list_s3(cn.AGC_emis_year_dir, sensit_type)

    print tile_id_list
    print "There are {} tiles to process".format(str(len(tile_id_list))) + "\n"


    # Files to download for this script
    download_dict = {
        cn.AGC_emis_year_dir: [cn.pattern_AGC_emis_year],
        cn.BGC_emis_year_dir: [cn.pattern_BGC_emis_year],
        cn.deadwood_emis_year_2000_dir: [cn.pattern_deadwood_emis_year_2000],
        cn.litter_emis_year_2000_dir: [cn.pattern_litter_emis_year_2000],
        cn.soil_C_emis_year_2000_dir: [cn.pattern_soil_C_emis_year_2000],
        cn.peat_mask_dir: [cn.pattern_peat_mask],
        cn.ifl_primary_processed_dir: [cn.pattern_ifl_primary],
        cn.planted_forest_type_unmasked_dir: [cn.pattern_planted_forest_type_unmasked],
        cn.drivers_processed_dir: [cn.pattern_drivers],
        cn.climate_zone_processed_dir: [cn.pattern_climate_zone],
        cn.bor_tem_trop_processed_dir: [cn.pattern_bor_tem_trop_processed],
        cn.burn_year_dir: [cn.pattern_burn_year],
        cn.plant_pre_2000_processed_dir: [cn.pattern_plant_pre_2000]
    }

    # Special loss tiles for the Brazil and Mekong sensitivity analyses
    if sensit_type == 'legal_Amazon_loss':
        download_dict[cn.Brazil_annual_loss_processed_dir] = [cn.pattern_Brazil_annual_loss_processed]
    if sensit_type == 'Mekong_loss':
        download_dict[cn.Mekong_loss_processed_dir] = [cn.pattern_Mekong_loss_processed]
    else:
        download_dict[cn.loss_dir] = ['']


    # Checks the validity of the pools argument
    if (pools not in ['soil_only', 'biomass_soil']):
        raise Exception('Invalid pool input. Please choose soil_only or biomass_soil.')


    # Checks if the correct c++ script has been compiled for the pool option selected
    if pools == 'biomass_soil':

        # Output file directories for biomass+soil. Must be in same order as output pattern directories.
        output_dir_list = [cn.gross_emis_commod_biomass_soil_dir,
                           cn.gross_emis_shifting_ag_biomass_soil_dir,
                           cn.gross_emis_forestry_biomass_soil_dir,
                           cn.gross_emis_wildfire_biomass_soil_dir,
                           cn.gross_emis_urban_biomass_soil_dir,
                           cn.gross_emis_no_driver_biomass_soil_dir,
                           cn.gross_emis_all_gases_all_drivers_biomass_soil_dir,
                           cn.gross_emis_co2_only_all_drivers_biomass_soil_dir,
                           cn.gross_emis_non_co2_all_drivers_biomass_soil_dir,
                           cn.gross_emis_nodes_biomass_soil_dir]

        output_pattern_list = [cn.pattern_gross_emis_commod_biomass_soil,
                               cn.pattern_gross_emis_shifting_ag_biomass_soil,
                               cn.pattern_gross_emis_forestry_biomass_soil,
                               cn.pattern_gross_emis_wildfire_biomass_soil,
                               cn.pattern_gross_emis_urban_biomass_soil,
                               cn.pattern_gross_emis_no_driver_biomass_soil,
                               cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil,
                               cn.pattern_gross_emis_co2_only_all_drivers_biomass_soil,
                               cn.pattern_gross_emis_non_co2_all_drivers_biomass_soil,
                               cn.pattern_gross_emis_nodes_biomass_soil]

        # Some sensitivity analyses have specific gross emissions scripts.
        # The rest of the sensitivity analyses and the standard model can all use the same, generic gross emissions script.
        if sensit_type in ['no_shifting_ag', 'convert_to_grassland']:
            # if os.path.exists('../carbon-budget/emissions/cpp_util/calc_gross_emissions_{}.exe'.format(sensit_type)):
            if os.path.exists('/home/ubuntu/carbon-budget/emissions/cpp_util/calc_gross_emissions_{}.exe'.format(sensit_type)):
                print "C++ for {} already compiled.".format(sensit_type)
            else:
                raise Exception('Must compile standard {} model C++...'.format(sensit_type))
        else:
            if os.path.exists('/home/ubuntu/carbon-budget/emissions/cpp_util/calc_gross_emissions_generic.exe'):
                print "C++ for generic emissions already compiled."
            else:
                print "here"
                raise Exception('Must compile generic emissions C++...')

    elif (pools == 'soil_only') & (sensit_type == 'std'):
        if os.path.exists('/home/ubuntu/carbon-budget/emissions/cpp_util/calc_gross_emissions_soil_only.exe'):
            print "C++ for soil_only already compiled."

            # Output file directories for soil_only. Must be in same order as output pattern directories.
            output_dir_list = [cn.gross_emis_commod_soil_only_dir,
                               cn.gross_emis_shifting_ag_soil_only_dir,
                               cn.gross_emis_forestry_soil_only_dir,
                               cn.gross_emis_wildfire_soil_only_dir,
                               cn.gross_emis_urban_soil_only_dir,
                               cn.gross_emis_no_driver_soil_only_dir,
                               cn.gross_emis_all_gases_all_drivers_soil_only_dir,
                               cn.gross_emis_co2_only_all_drivers_soil_only_dir,
                               cn.gross_emis_non_co2_all_drivers_soil_only_dir,
                               cn.gross_emis_nodes_soil_only_dir]

            output_pattern_list = [cn.pattern_gross_emis_commod_soil_only,
                                   cn.pattern_gross_emis_shifting_ag_soil_only,
                                   cn.pattern_gross_emis_forestry_soil_only,
                                   cn.pattern_gross_emis_wildfire_soil_only,
                                   cn.pattern_gross_emis_urban_soil_only,
                                   cn.pattern_gross_emis_no_driver_soil_only,
                                   cn.pattern_gross_emis_all_gases_all_drivers_soil_only,
                                   cn.pattern_gross_emis_co2_only_all_drivers_soil_only,
                                   cn.pattern_gross_emis_non_co2_all_drivers_soil_only,
                                   cn.pattern_gross_emis_nodes_soil_only]

        else:
            raise Exception('Must compile soil_only C++...')

    else:
        raise Exception('Pool and/or sensitivity analysis option not valid')


    # Assigns the working folder based on whether emissions is being calculated as part of the full model run or not
    if working_dir is not None:
        folder = working_dir        # When emissions are calculated as part of the full model run
    else:
        folder = '/home/ubuntu/carbon-budget/emissions/cpp_util'     # When emissions are calculated on their own


    # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
    for key, values in download_dict.iteritems():
        dir = key
        pattern = values[0]
        uu.s3_flexible_download(dir, pattern, folder, sensit_type, tile_id_list)


    # If the model run isn't the standard one, the output directory and file names are changed
    if sensit_type != 'std':
        print "Changing output directory and file name pattern based on sensitivity analysis"
        output_dir_list = uu.alter_dirs(sensit_type, output_dir_list)
        output_pattern_list = uu.alter_patterns(sensit_type, output_pattern_list)
        print output_dir_list
        print output_pattern_list

    # If the script is called from the full model run script, a date is provided.
    # This replaces the date in constants_and_names.
    if run_date is not None:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, run_date)


    print "Removing loss pixels from plantations that existed in Indonesia and Malaysia before 2000..."
    # Pixels that were in plantations that existed before 2000 should not be included in gross emissions.
    # Pre-2000 plantations have not previously been masked, so that is done here.
    # There are only 8 tiles to process, so count/2 will cover all of them in one go.
    count = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(count/2)
    pool.map(partial(calculate_gross_emissions.mask_pre_2000_plant, sensit_type=sensit_type, folder=folder), tile_id_list)

    # # For single processor use
    # for tile_id in tile_id_list:
    #       calculate_gross_emissions.mask_pre_2000_plant(tile, sensit_type, working_dir)


    # The C++ code expects certain tiles for every input 10x10.
    # However, not all Hansen tiles have all of these inputs.
    # This function creates "dummy" tiles for all Hansen tiles that currently have non-existent tiles.
    # That way, the C++ script gets all the necessary input files.
    print "Making blank tiles for inputs that don't currently exist"
    # All of the inputs that need to have dummy tiles made in order to match the tile list of the carbon pools
    pattern_list = [cn.pattern_planted_forest_type_unmasked, cn.pattern_peat_mask, cn.pattern_ifl_primary,
                    cn.pattern_drivers, cn.pattern_bor_tem_trop_processed]

    for pattern in pattern_list:
        pool = multiprocessing.Pool(processes=50)
        pool.map(partial(uu.make_blank_tile, pattern=pattern, folder=folder, sensit_type=sensit_type), tile_id_list)
        pool.close()
        pool.join()

    # # For single processor use
    # for pattern in pattern_list:
    #     for tile in tile_id_list:
    #         uu.make_blank_tile(tile, pattern, folder, sensit_type)


    # Calculates gross emissions for each tile
    # count/4 uses about 390 GB on a r4.16xlarge spot machine.
    # processes=18 uses about 440 GB on an r4.16xlarge spot machine.
    count = multiprocessing.cpu_count()
    # pool = multiprocessing.Pool(processes=18)
    pool = multiprocessing.Pool(processes=9)
    pool.map(partial(calculate_gross_emissions.calc_emissions, pools=pools, sensit_type=sensit_type, folder=folder), tile_id_list)

    # # For single processor use
    # for tile in tile_id_list:
    #       calculate_gross_emissions.calc_emissions(tile, pools, sensit_type)


    # Uploads emissions to appropriate directory for the carbon pools chosen
    for i in range(0, len(output_dir_list)):
        uu.upload_final_set(output_dir_list[i], output_pattern_list[i])


if __name__ == '__main__':

    # Two arguments for the script: whether only emissions from biomass (soil_only) is being calculated or emissions from biomass and soil (biomass_soil),
    # and which model type is being run (standard or sensitivity analysis)
    parser = argparse.ArgumentParser(description='Calculate gross emissions')
    parser.add_argument('--pools-to-use', '-p', required=True,
                        help='Options are soil_only or biomass_soil. Former only considers emissions from soil. Latter considers emissions from biomass and soil.')
    parser.add_argument('--tile_id_list', '-l', required=True,
                        help='List of tile ids to use in the model. Should be of form 00N_110E or all.')
    parser.add_argument('--model-type', '-t', required=True,
                        help='{}'.format(cn.model_type_arg_help))
    args = parser.parse_args()
    sensit_type = args.model_type
    tile_id_list = args.tile_id_list
    pools = args.pools_to_use
    # Checks whether the sensitivity analysis argument is valid
    uu.check_sensit_type(sensit_type)

    # Checks whether the sensitivity analysis and tile_id_list arguments are valid
    uu.check_sensit_type(sensit_type)

    if 's3://' in tile_id_list:
        tile_id_list = uu.tile_list_s3(tile_id_list, 'std')

    else:
        tile_id_list = uu.tile_id_list_check(tile_id_list)

    mp_calculate_gross_emissions(sensit_type=sensit_type, tile_id_list=tile_id_list, pools=pools)