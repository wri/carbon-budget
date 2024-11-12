"""
Function to call C++ executable that calculates gross emissions
"""

import datetime

import constants_and_names as cn
import universal_util as uu

def calc_emissions(tile_id, emitted_pools, folder):
    """
    Calls the c++ script to calculate gross emissions
    :param tile_id: tile to be processed, identified by its tile id
    :param emitted_pools: Whether emissions from soil only is calculated, or emissions from biomass and soil.
        Options are: soil_only or biomass_soil.
    :param folder:
    :return: 4 tiles -
        CO2 emissions from all drivers;
        non-CO2 emissions from all drivers;
        all gases (CO2 and non-CO2 from all drivers);
        emissions decision tree nodes (used for QC).
        Units: Mg CO2e/ha over entire model period.
        #TODO: Include methane and nitrous oxide here once non-CO2 emissions are split
    """

    uu.print_log(f'Calculating gross emissions for {tile_id} using {cn.SENSIT_TYPE} model type...')

    start = datetime.datetime.now()

    uu.check_memory()

    # Runs the correct c++ script given the emitted_pools (biomass+soil or soil_only) and model type selected.
    # soil_only has special gross emissions C++ scripts.
    # The other sensitivity analyses and the standard model all use the same gross emissions C++ script.
    if (emitted_pools == 'soil_only') & (cn.SENSIT_TYPE == 'std'):
        cmd = [f'{cn.c_emis_compile_dst}/calc_gross_emissions_soil_only.exe', tile_id, cn.SENSIT_TYPE, folder]

    # This C++ script has an extra argument that names the input carbon emitted_pools and output emissions correctly
    elif (emitted_pools == 'biomass_soil') & (cn.SENSIT_TYPE not in ['no_shifting_ag', 'convert_to_grassland']):
        cmd = [f'{cn.c_emis_compile_dst}/calc_gross_emissions_generic.exe', tile_id, cn.SENSIT_TYPE, folder]

    else:
        uu.exception_log('Pool and/or sensitivity analysis option not valid')

    uu.log_subprocess_output_full(cmd)


    # Identifies which pattern to use for counting tile completion
    pattern = cn.pattern_gross_emis_co2_only_all_drivers_biomass_soil
    if (emitted_pools == 'biomass_soil') & (cn.SENSIT_TYPE == 'std'):
        pattern = pattern

    elif (emitted_pools == 'biomass_soil') & (cn.SENSIT_TYPE != 'std'):
        pattern = pattern + "_" + cn.SENSIT_TYPE

    elif emitted_pools == 'soil_only':
        pattern = pattern.replace('biomass_soil', 'soil_only')

    else:
        uu.exception_log('Pool option not valid')

    # Prints information about the tile that was just processed
    uu.end_of_fx_summary(start, tile_id, pattern)
