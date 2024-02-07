"""
This script calculates the gross emissions in tonnes CO2e/ha for every loss pixel.
The properties of each pixel determine the appropriate emissions equation, the constants for the equation, and the
carbon pool values that go into the equation.
Unlike all other flux model components, this one uses C++ to quickly iterate through every pixel in each tile.
The relevant version of emissions C++ is compiled each time this file is run, so the C++ doesn't need to be compiled
as an extra initial step.

However, if you want to compile the standard emissions model C++ outside of a run,
do the following inside the Docker container:
c++ /usr/local/app/emissions/cpp_util/calc_gross_emissions_generic.cpp -o /usr/local/app/emissions/cpp_util/calc_gross_emissions_generic.exe -lgdal
calc_gross_emissions_generic.exe should appear in the directory if it wasn't already there.
For the sensitivity analyses that use a different gross emissions C++ script (currently, soil_only, no_shifting_ag,
and convert_to_grassland), do:
c++  /usr/local/app/carbon-budget/emissions/cpp_util/calc_gross_emissions_<sensit_type>.cpp -o  /usr/local/app/emissions/cpp_util/calc_gross_emissions_<sensit_type>.exe -lgdal
The other C++ scripts (equations.cpp and flu_val.cpp) do not need to be compiled separately.

Run the emissions model with:
python -m emissions.mp_calculate_gross_emissions -t [MODEL_TYPE] -p [POOL_OPTION] -l [TILE_LIST] [optional_arguments]
The --pools-to-use argument specifies whether to calculate gross emissions from biomass+soil or just from soil.
The --model-type argument specifies whether the model run is a sensitivity analysis or standard run.
Emissions from each driver (including loss that had no driver assigned) gets its own tile, as does all emissions combined.
Emissions from all drivers is also output as emissions due to CO2 only and emissions due to non-CO2 GHGs (CH4 and N2O).
The other output shows which branch of the decision tree that determines the emissions equation applies to each pixel.
These codes are summarized in carbon-budget/emissions/node_codes.txt

python -m emissions.mp_calculate_gross_emissions -t std -l 00N_000E -nu
python -m emissions.mp_calculate_gross_emissions -t std -l all
"""

import argparse
from functools import partial
import multiprocessing
import os
import sys

import constants_and_names as cn
import universal_util as uu

from . import calculate_gross_emissions

def mp_calculate_gross_emissions(tile_id_list, emitted_pools):
    """
    :param tile_id_list: list of tile ids to process
    :param emitted_pools: Whether emissions from soil only is calculated, or emissions from biomass and soil.
        Options are: soil_only or biomass_soil.
    :return: 10 sets of tiles: 6 sets of tiles with emissions for each driver; CO2 emissions from all drivers;
        non-CO2 emissions from all drivers; all gases (CO2 and non-CO2 from all drivers);
        emissions decision tree nodes (used for QC).
        Units: Mg CO2e/ha over entire model period.
    """

    os.chdir(cn.docker_tile_dir)

    folder = cn.docker_tile_dir

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    # If the tile_list argument is an s3 folder, the list of tiles in it is created
    if tile_id_list == 'all':
        # List of tiles to run in the model
        tile_id_list = uu.tile_list_s3(cn.AGC_emis_year_dir, cn.SENSIT_TYPE)

    uu.print_log(tile_id_list)
    uu.print_log(f'There are {str(len(tile_id_list))} tiles to process', "\n")


    # Files to download for this script
    download_dict = {
        cn.AGC_emis_year_dir: [cn.pattern_AGC_emis_year],
        cn.BGC_emis_year_dir: [cn.pattern_BGC_emis_year],
        cn.deadwood_emis_year_2000_dir: [cn.pattern_deadwood_emis_year_2000],
        cn.litter_emis_year_2000_dir: [cn.pattern_litter_emis_year_2000],
        cn.soil_C_emis_year_2000_dir: [cn.pattern_soil_C_emis_year_2000],
        cn.peat_mask_dir: [cn.pattern_peat_mask],
        cn.ifl_primary_processed_dir: [cn.pattern_ifl_primary],
        cn.planted_forest_type_dir: [cn.pattern_planted_forest_type],
        cn.drivers_processed_dir: [cn.pattern_drivers],
        cn.climate_zone_processed_dir: [cn.pattern_climate_zone],
        cn.bor_tem_trop_processed_dir: [cn.pattern_bor_tem_trop_processed],
        cn.TCLF_processed_dir: [cn.pattern_TCLF_processed]
    }

    # Special loss tiles for the Brazil and Mekong sensitivity analyses
    if cn.SENSIT_TYPE == 'legal_Amazon_loss':
        download_dict[cn.Brazil_annual_loss_processed_dir] = [cn.pattern_Brazil_annual_loss_processed]
    elif cn.SENSIT_TYPE == 'Mekong_loss':
        download_dict[cn.Mekong_loss_processed_dir] = [cn.pattern_Mekong_loss_processed]
    else:
        download_dict[cn.loss_dir] = [cn.pattern_loss]


    # Checks the validity of the emitted_pools argument
    if (emitted_pools not in ['soil_only', 'biomass_soil']):
        uu.exception_log('Invalid pool input. Please choose soil_only or biomass_soil.')


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
        if cn.SENSIT_TYPE in ['no_shifting_ag', 'convert_to_grassland']:
            uu.print_log(f'Compiling {cn.SENSIT_TYPE} model C++...')
            cmd = ['c++', f'/usr/local/app/emissions/cpp_util/calc_gross_emissions_{cn.SENSIT_TYPE}.cpp',
                   '-o', f'/usr/local/app/emissions/cpp_util/calc_gross_emissions_{cn.SENSIT_TYPE}.exe', '-lgdal']
            uu.log_subprocess_output_full(cmd)
        else:
            uu.print_log(f'Compiling generic model C++...')
            cmd = ['c++', f'/usr/local/app/emissions/cpp_util/calc_gross_emissions_generic.cpp',
                   '-o', f'/usr/local/app/emissions/cpp_util/calc_gross_emissions_generic.exe', '-lgdal']
            uu.log_subprocess_output_full(cmd)

    elif (emitted_pools == 'soil_only') & (cn.SENSIT_TYPE == 'std'):

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

        uu.print_log(f'Compiling soil_only model C++...')
        cmd = ['c++', f'/usr/local/app/emissions/cpp_util/calc_gross_emissions_soil_only.cpp',
               '-o', f'/usr/local/app/emissions/cpp_util/calc_gross_emissions_soil_only.exe', '-lgdal']
        uu.log_subprocess_output_full(cmd)

    else:
        uu.exception_log('Pool and/or sensitivity analysis option not valid')


    # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
    for key, values in download_dict.items():
        directory = key
        output_pattern = values[0]
        uu.s3_flexible_download(directory, output_pattern, cn.docker_tile_dir, cn.SENSIT_TYPE, tile_id_list)


    # If the model run isn't the standard one, the output directory and file names are changed
    if cn.SENSIT_TYPE != 'std':
        uu.print_log('Changing output directory and file name pattern based on sensitivity analysis')
        output_dir_list = uu.alter_dirs(cn.SENSIT_TYPE, output_dir_list)
        output_pattern_list = uu.alter_patterns(cn.SENSIT_TYPE, output_pattern_list)

    # A date can optionally be provided by the full model script or a run of this script.
    # This replaces the date in constants_and_names.
    # Only done if output upload is enabled.
    if cn.RUN_DATE is not None and cn.NO_UPLOAD is not None:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, cn.RUN_DATE)

    uu.print_log(output_pattern_list)


    # The C++ code expects certain tiles for every input 10x10.
    # However, not all Hansen tiles have all of these inputs.
    # This function creates "dummy" tiles for all Hansen tiles that currently have non-existent tiles.
    # That way, the C++ script gets all the necessary input files.
    # If it doesn't get the necessary inputs, it skips that tile.
    uu.print_log('Making blank tiles for inputs that do not currently exist')
    # All of the inputs that need to have dummy tiles made in order to match the tile list of the carbon emitted_pools
    pattern_list = [cn.pattern_planted_forest_type, cn.pattern_peat_mask, cn.pattern_ifl_primary,
                    cn.pattern_drivers, cn.pattern_bor_tem_trop_processed, cn.pattern_TCLF_processed, cn.pattern_climate_zone,
                    cn.pattern_soil_C_emis_year_2000]


    # textfile that stores the names of the blank tiles that are created for processing.
    # This will be iterated through to delete the tiles at the end of the script.
    uu.create_blank_tile_txt()

    if cn.SINGLE_PROCESSOR:
        for pattern in pattern_list:
            for tile in tile_id_list:
                uu.make_blank_tile(tile, pattern, folder)

    else:
        processes=80 # 60 = 100 GB peak; 80 =  XXX GB peak
        for output_pattern in pattern_list:
            with multiprocessing.Pool(processes) as pool:
                pool.map(partial(uu.make_blank_tile, pattern=output_pattern, folder=folder),
                         tile_id_list)
                pool.close()
                pool.join()


    # Calculates gross emissions for each tile
    if cn.SINGLE_PROCESSOR:
        for tile in tile_id_list:
              calculate_gross_emissions.calc_emissions(tile, emitted_pools, folder)

    else:
        # count/4 uses about 390 GB on a r4.16xlarge spot machine.
        # processes=18 uses about 440 GB on an r4.16xlarge spot machine.
        if cn.count == 96:
            if cn.SENSIT_TYPE == 'biomass_swap':
                processes = 15 # 15 processors = XXX GB peak
            else:
                processes = 19   # 17 = 650 GB peak; 18 = 677 GB peak; 19 = 720 GB peak
        else:
            processes = 9
        uu.print_log(f'Gross emissions max processors={processes}')
        with multiprocessing.Pool(processes) as pool:
            pool.map(partial(calculate_gross_emissions.calc_emissions, emitted_pools=emitted_pools,
                             folder=folder),
                     tile_id_list)
            pool.close()
            pool.join()


    # Print the list of blank created tiles, delete the tiles, and delete their text file
    uu.list_and_delete_blank_tiles()

    for i, output_pattern in enumerate(output_pattern_list):

        uu.print_log(f'Adding metadata tags for pattern {output_pattern}')

        if cn.SINGLE_PROCESSOR:
            for tile_id in tile_id_list:
                uu.add_emissions_metadata(tile_id, output_pattern)

        else:
            if cn.count == 96:
                processes = 75  # 45 processors = ~30 GB peak; 55 = XXX GB peak; 75 = XXX GB peak
            else:
                processes = 9
            uu.print_log(f'Adding metadata tags max processors={processes}')
            with multiprocessing.Pool(processes) as pool:
                pool.map(partial(uu.add_emissions_metadata, output_pattern=output_pattern),
                         tile_id_list)
                pool.close()
                pool.join()



    # If cn.NO_UPLOAD flag is not activated (by choice or by lack of AWS credentials), output is uploaded
    if not cn.NO_UPLOAD:

        for output_dir, output_pattern in zip(output_dir_list, output_pattern_list):
            uu.upload_final_set(output_dir, output_pattern)


if __name__ == '__main__':

    # Two arguments for the script: whether only emissions from biomass (soil_only) is being calculated or emissions from biomass and soil (biomass_soil),
    # and which model type is being run (standard or sensitivity analysis)
    parser = argparse.ArgumentParser(description='Calculates gross emissions')
    parser.add_argument('--tile_id_list', '-l', required=True,
                        help='List of tile ids to use in the model. Should be of form 00N_110E or 00N_110E,00N_120E or all.')
    parser.add_argument('--model-type', '-t', required=True,
                        help=f'{cn.model_type_arg_help}')
    parser.add_argument('--run-date', '-d', required=False,
                        help='Date of run. Must be format YYYYMMDD.')
    parser.add_argument('--no-upload', '-nu', action='store_true',
                       help='Disables uploading of outputs to s3')
    parser.add_argument('--single-processor', '-sp', action='store_true',
                       help='Uses single processing rather than multiprocessing')
    parser.add_argument('--emitted-pools-to-use', '-p', required=True,
                        help='Options are soil_only or biomass_soil. Former only considers emissions from soil. Latter considers emissions from biomass and soil.')
    args = parser.parse_args()

    # Sets global variables to the command line arguments
    cn.SENSIT_TYPE = args.model_type
    cn.RUN_DATE = args.run_date
    cn.NO_UPLOAD = args.no_upload
    cn.SINGLE_PROCESSOR = args.single_processor
    cn.EMITTED_POOLS = args.emitted_pools_to_use

    tile_id_list = args.tile_id_list

    # Disables upload to s3 if no AWS credentials are found in environment
    if not uu.check_aws_creds():
        cn.NO_UPLOAD = True

    # Create the output log
    uu.initiate_log(tile_id_list)

    # Checks whether the sensitivity analysis and tile_id_list arguments are valid
    uu.check_sensit_type(cn.SENSIT_TYPE)

    if 's3://' in tile_id_list:
        tile_id_list = uu.tile_list_s3(tile_id_list, 'std')
    else:
        tile_id_list = uu.tile_id_list_check(tile_id_list)

    mp_calculate_gross_emissions(tile_id_list, cn.EMITTED_POOLS)
