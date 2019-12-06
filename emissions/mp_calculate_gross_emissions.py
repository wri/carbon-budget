'''
This script calculates the gross emissions in tonnes CO2e/ha for every loss pixel.
The properties of each pixel determine the appropriate emissions equation, the constants for the equation, and the
carbon pool values that go into the equation.
Unlike all other flux model components, this one uses C++ to quickly iterate through every pixel in each tile.
Before running the model, the C++ script must be compiled.
From carbon-budget/emissions/, do:
c++ ./cpp_util/calc_gross_emissions_biomass_soil.cpp -o ./cpp_util/calc_gross_emissions_biomass_soil.exe -lgdal
(for the biomass_soil pools and standard model version).
calc_emissions_v3_biomass_soil.exe should appear in the directory.
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

def main ():

    # Files to download for this script. 'true'/'false' says whether the input directory and pattern should be
    # changed for a sensitivity analysis. This does not need to change based on what run is being done;
    # this assignment should be true for all sensitivity analyses and the standard model.
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
        cn.plant_pre_2000_processed_dir: [cn.pattern_plant_pre_2000],
        cn.loss_dir: ['']
    }


    # List of tiles to run in the model
    tile_id_list = uu.tile_list_s3(cn.AGC_emis_year_dir)
    # tile_id_list = ['30N_140E', '40N_030W']  # test tiles
    tile_id_list = ['00N_110E'] # test tiles
    print tile_id_list
    print "There are {} tiles to process".format(str(len(tile_id_list))) + '\n'


    # Two arguments for the script: whether only emissions from biomass (soil_only) is being calculated or emissions from biomass and soil (biomass_soil),
    # and which model type is being run (standard or sensitivity analysis)
    parser = argparse.ArgumentParser(description='Calculate gross emissions')
    parser.add_argument('--pools-to-use', '-p', required=True,
                        help='Options are soil_only or biomass_soil. Former only considers emissions from soil. Latter considers emissions from biomass and soil.')
    parser.add_argument('--model-type', '-t', required=True,
                        help='{}'.format(cn.model_type_arg_help))
    args = parser.parse_args()
    sensit_type = args.model_type
    pools = args.pools_to_use
    # Checks whether the sensitivity analysis argument is valid
    uu.check_sensit_type(sensit_type)


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

        if sensit_type == 'std':
            if os.path.exists('./cpp_util/calc_gross_emissions_biomass_soil.exe'):
                print "C++ for biomass+soil already compiled."
            else:
                raise Exception('Must compile standard biomass+soil model C++...')
        else:
            if os.path.exists('./cpp_util/calc_gross_emissions_{}.exe'.format(sensit_type)):
                print 'C++ for {} already compiled.'.format(sensit_type)
            else:
                raise Exception('Must compile {} model C++...'.format(sensit_type))


    elif (pools == 'soil_only') & (sensit_type == 'std'):
        if os.path.exists('./cpp_util/calc_gross_emissions_soil_only.exe'):
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


    # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
    for key, values in download_dict.iteritems():
        dir = key
        pattern = values[0]
        uu.s3_flexible_download(dir, pattern, './cpp_util/', sensit_type, tile_id_list)


    # If the model run isn't the standard one, the output directory and file names are changed
    if sensit_type != 'std':
        print "Changing output directory and file name pattern based on sensitivity analysis"
        output_dir_list = uu.alter_dirs(sensit_type, output_dir_list)
        output_pattern_list = uu.alter_patterns(sensit_type, output_pattern_list)


    print "Removing loss pixels from plantations that existed in Indonesia and Malaysia before 2000..."
    # Pixels that were in plantations that existed before 2000 should not be included in gross emissions.
    # Pre-2000 plantations have not previously been masked, so that is done here.
    # There are only 8 tiles to process, so count/2 will cover all of them in one go.
    count = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(count/2)
    pool.map(calculate_gross_emissions.mask_pre_2000_plant, tile_id_list)

    # # For single processor use
    # for tile in tile_id_list:
    #       calculate_gross_emissions.mask_pre_2000_plant(tile)


    # The C++ code expects a plantations tile for every input 10x10.
    # However, not all Hansen tiles have plantations.
    # This function creates "dummy" plantation tiles for all Hansen tiles that do not have plantations.
    # That way, the C++ script gets all the necessary input files
    folder = 'cpp_util/'

    # All of the inputs that need to have dummy tiles made in order to match the tile list of the carbon pools
    pattern_list = [cn.pattern_planted_forest_type_unmasked, cn.pattern_peat_mask, cn.pattern_ifl_primary,
                    cn.pattern_drivers, cn.pattern_bor_tem_trop_processed]

    # for pattern in pattern_list:
    #     count = multiprocessing.cpu_count()
    #     pool = multiprocessing.Pool(count-10)
    #     pool.map(partial(uu.make_blank_tile, pattern=pattern, folder=folder), tile_id_list)
    #     pool.close()
    #     pool.join()

    # For single processor use
    for pattern in pattern_list:
        for tile in tile_id_list:
            uu.make_blank_tile(tile, pattern, folder, sensit_type)


    # # Calculates gross emissions for each tile
    # # count/4 uses about 390 GB on a r4.16xlarge spot machine.
    # # processes=18 uses about 440 GB on an r4.16xlarge spot machine.
    # count = multiprocessing.cpu_count()
    # pool = multiprocessing.Pool(processes=18)
    # pool.map(partial(calculate_gross_emissions.calc_emissions, pools=pools, sensit_type=sensit_type), tile_id_list)

    # For single processor use
    for tile in tile_id_list:
          calculate_gross_emissions.calc_emissions(tile, pools, sensit_type)


    # Uploads emissions to appropriate directory for the carbon pools chosen
    for i in range(0, len(output_dir_list)):
        uu.upload_final_set(output_dir_list[i], output_pattern_list[i])


if __name__ == '__main__':
    main()