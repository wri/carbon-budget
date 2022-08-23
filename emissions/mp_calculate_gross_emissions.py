'''
This script calculates the gross emissions in tonnes CO2e/ha for every loss pixel.
The properties of each pixel determine the appropriate emissions equation, the constants for the equation, and the
carbon pool values that go into the equation.
Unlike all other flux model components, this one uses C++ to quickly iterate through every pixel in each tile.
Before running the model, the C++ script must be compiled.
From carbon-budget/emissions/, do:
c++ /home/dgibbs/carbon-budget/emissions/cpp_util/calc_gross_emissions_generic.cpp -o /home/dgibbs/carbon-budget/emissions/cpp_util/calc_gross_emissions_generic.exe -lgdal
(for the standard model and some sensitivity analysis versions).
calc_gross_emissions_generic.exe should appear in the directory.
For the sensitivity analyses that use a different gross emissions C++ script (currently, soil_only, no_shifting_ag,
and convert_to_grassland), do:
c++ /home/dgibbs/carbon-budget/emissions/cpp_util/calc_gross_emissions_<sensit_type>.cpp -o /home/dgibbs/carbon-budget/emissions/cpp_util/calc_gross_emissions_<sensit_type>.exe -lgdal
Run by typing python mp_calculate_gross_emissions.py -p [POOL_OPTION] -t [MODEL_TYPE] -l [TILE_LIST] -d [RUN_DATE]
The Python script will call the compiled C++ code as needed.
The other C++ scripts (equations.cpp and flu_val.cpp) do not need to be compiled separately.
The --pools-to-use argument specifies whether to calculate gross emissions from biomass+soil or just from soil.
The --model-type argument specifies whether the model run is a sensitivity analysis or standard run.
Emissions from each driver (including loss that had no driver assigned) gets its own tile, as does all emissions combined.
Emissions from all drivers is also output as emissions due to CO2 only and emissions due to other GHG (CH4 and N2O).
The other output shows which branch of the decision tree that determines the emissions equation applies to each pixel.
These codes are summarized in carbon-budget/emissions/node_codes.txt
'''

import multiprocessing
import argparse
import datetime
import os
from functools import partial
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu
sys.path.append(os.path.join(cn.docker_app,'emissions'))
import calculate_gross_emissions

def mp_calculate_gross_emissions(sensit_type, tile_id_list, emitted_pools, run_date = None, no_upload = None):

    os.chdir(cn.docker_base_dir)

    folder = cn.docker_base_dir

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    # If the tile_list argument is an s3 folder, the list of tiles in it is created
    if tile_id_list == 'all':
        # List of tiles to run in the model
        tile_id_list = uu.tile_list_s3(cn.AGC_emis_year_dir, sensit_type)

    uu.print_log(tile_id_list)
    uu.print_log("There are {} tiles to process".format(str(len(tile_id_list))) + "\n")


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
        cn.burn_year_dir: [cn.pattern_burn_year]
    }

    # Special loss tiles for the Brazil and Mekong sensitivity analyses
    if sensit_type == 'legal_Amazon_loss':
        download_dict[cn.Brazil_annual_loss_processed_dir] = [cn.pattern_Brazil_annual_loss_processed]
    elif sensit_type == 'Mekong_loss':
        download_dict[cn.Mekong_loss_processed_dir] = [cn.pattern_Mekong_loss_processed]
    else:
        download_dict[cn.loss_dir] = [cn.pattern_loss]


    # Checks the validity of the emitted_pools argument
    if (emitted_pools not in ['soil_only', 'biomass_soil']):
        uu.exception_log(no_upload, 'Invalid pool input. Please choose soil_only or biomass_soil.')


    # Checks if the correct c++ script has been compiled for the pool option selected
    if emitted_pools == 'biomass_soil':

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
            if os.path.exists('{0}/calc_gross_emissions_{1}.exe'.format(cn.c_emis_compile_dst, sensit_type)):
                uu.print_log("C++ for {} already compiled.".format(sensit_type))
            else:
                uu.exception_log(no_upload, 'Must compile {} model C++...'.format(sensit_type))
        else:
            if os.path.exists('{0}/calc_gross_emissions_generic.exe'.format(cn.c_emis_compile_dst)):
                uu.print_log("C++ for generic emissions already compiled.")
            else:
                uu.exception_log(no_upload, 'Must compile generic emissions C++...')

    elif (emitted_pools == 'soil_only') & (sensit_type == 'std'):
        if os.path.exists('{0}/calc_gross_emissions_soil_only.exe'.format(cn.c_emis_compile_dst)):
            uu.print_log("C++ for soil_only already compiled.")

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
            uu.exception_log(no_upload, 'Must compile soil_only C++...')

    else:
        uu.exception_log(no_upload, 'Pool and/or sensitivity analysis option not valid')


    # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
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
    # Only done if output upload is enabled.
    if run_date is not None and no_upload is not None:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, run_date)

    uu.print_log(output_pattern_list)


    # The C++ code expects certain tiles for every input 10x10.
    # However, not all Hansen tiles have all of these inputs.
    # This function creates "dummy" tiles for all Hansen tiles that currently have non-existent tiles.
    # That way, the C++ script gets all the necessary input files.
    # If it doesn't get the necessary inputs, it skips that tile.
    uu.print_log("Making blank tiles for inputs that don't currently exist")
    # All of the inputs that need to have dummy tiles made in order to match the tile list of the carbon emitted_pools
    pattern_list = [cn.pattern_planted_forest_type_unmasked, cn.pattern_peat_mask, cn.pattern_ifl_primary,
                    cn.pattern_drivers, cn.pattern_bor_tem_trop_processed, cn.pattern_burn_year, cn.pattern_climate_zone,
                    cn.pattern_soil_C_emis_year_2000]


    # textfile that stores the names of the blank tiles that are created for processing.
    # This will be iterated through to delete the tiles at the end of the script.
    uu.create_blank_tile_txt()

    for pattern in pattern_list:
        pool = multiprocessing.Pool(processes=80)  # 60 = 100 GB peak; 80 =  XXX GB peak
        pool.map(partial(uu.make_blank_tile, pattern=pattern, folder=folder,
                                             sensit_type=sensit_type), tile_id_list)
        pool.close()
        pool.join()

    # # For single processor use
    # for pattern in pattern_list:
    #     for tile in tile_id_list:
    #         uu.make_blank_tile(tile, pattern, folder, sensit_type)


    # Calculates gross emissions for each tile
    # count/4 uses about 390 GB on a r4.16xlarge spot machine.
    # processes=18 uses about 440 GB on an r4.16xlarge spot machine.
    if cn.count == 96:
        if sensit_type == 'biomass_swap':
            processes = 15 # 15 processors = XXX GB peak
        else:
            processes = 19   # 17 = 650 GB peak; 18 = 677 GB peak; 19 = 716 GB peak
    else:
        processes = 9
    uu.print_log('Gross emissions max processors=', processes)
    pool = multiprocessing.Pool(processes)
    pool.map(partial(calculate_gross_emissions.calc_emissions, emitted_pools=emitted_pools, sensit_type=sensit_type,
                     folder=folder, no_upload=no_upload), tile_id_list)
    pool.close()
    pool.join()

    # # For single processor use
    # for tile in tile_id_list:
    #       calculate_gross_emissions.calc_emissions(tile, emitted_pools, sensit_type, folder, no_upload)


    # Print the list of blank created tiles, delete the tiles, and delete their text file
    uu.list_and_delete_blank_tiles()


    for i in range(0, len(output_pattern_list)):
        pattern = output_pattern_list[i]

        uu.print_log("Adding metadata tags for pattern {}".format(pattern))

        if cn.count == 96:
            processes = 75  # 45 processors = ~30 GB peak; 55 = XXX GB peak; 75 = XXX GB peak
        else:
            processes = 9
        uu.print_log('Adding metadata tags max processors=', processes)
        pool = multiprocessing.Pool(processes)
        pool.map(partial(calculate_gross_emissions.add_metadata_tags, pattern=pattern, sensit_type=sensit_type),
                 tile_id_list)
        pool.close()
        pool.join()

        # for tile_id in tile_id_list:
        #     calculate_gross_emissions.add_metadata_tags(tile_id, pattern, sensit_type)


    # If no_upload flag is not activated (by choice or by lack of AWS credentials), output is uploaded
    if not no_upload:

        for i in range(0, len(output_dir_list)):
            uu.upload_final_set(output_dir_list[i], output_pattern_list[i])


if __name__ == '__main__':

    # Two arguments for the script: whether only emissions from biomass (soil_only) is being calculated or emissions from biomass and soil (biomass_soil),
    # and which model type is being run (standard or sensitivity analysis)
    parser = argparse.ArgumentParser(description='Calculates gross emissions')
    parser.add_argument('--emitted-pools-to-use', '-p', required=True,
                        help='Options are soil_only or biomass_soil. Former only considers emissions from soil. Latter considers emissions from biomass and soil.')
    parser.add_argument('--tile_id_list', '-l', required=True,
                        help='List of tile ids to use in the model. Should be of form 00N_110E or 00N_110E,00N_120E or all.')
    parser.add_argument('--model-type', '-t', required=True,
                        help='{}'.format(cn.model_type_arg_help))
    parser.add_argument('--run-date', '-d', required=False,
                        help='Date of run. Must be format YYYYMMDD.')
    parser.add_argument('--no-upload', '-nu', action='store_true',
                       help='Disables uploading of outputs to s3')
    args = parser.parse_args()
    sensit_type = args.model_type
    tile_id_list = args.tile_id_list
    emitted_pools = args.emitted_pools_to_use
    run_date = args.run_date
    no_upload = args.NO_UPLOAD

    # Disables upload to s3 if no AWS credentials are found in environment
    if not uu.check_aws_creds():
        no_upload = True

    # Create the output log
    uu.initiate_log(tile_id_list=tile_id_list, sensit_type=sensit_type, run_date=run_date,
                    emitted_pools=emitted_pools, no_upload=no_upload)

    # Checks whether the sensitivity analysis and tile_id_list arguments are valid
    uu.check_sensit_type(sensit_type)

    if 's3://' in tile_id_list:
        tile_id_list = uu.tile_list_s3(tile_id_list, 'std')
    else:
        tile_id_list = uu.tile_id_list_check(tile_id_list)

    mp_calculate_gross_emissions(sensit_type=sensit_type, tile_id_list=tile_id_list, emitted_pools=emitted_pools,
                                 run_date=run_date, no_upload=no_upload)
