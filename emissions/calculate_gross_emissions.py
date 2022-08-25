from subprocess import Popen, PIPE, STDOUT, check_call
import datetime
import rasterio
from shutil import copyfile
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

# Calls the c++ script to calculate gross emissions
def calc_emissions(tile_id, emitted_pools, folder):

    uu.print_log("Calculating gross emissions for", tile_id, "using", cn.SENSIT_TYPE, "model type...")

    start = datetime.datetime.now()

    uu.check_memory()

    # Runs the correct c++ script given the emitted_pools (biomass+soil or soil_only) and model type selected.
    # soil_only, no_shiftin_ag, and convert_to_grassland have special gross emissions C++ scripts.
    # The other sensitivity analyses and the standard model all use the same gross emissions C++ script.
    if (emitted_pools == 'soil_only') & (cn.SENSIT_TYPE == 'std'):
        cmd = ['{0}/calc_gross_emissions_soil_only.exe'.format(cn.c_emis_compile_dst), tile_id, cn.SENSIT_TYPE, folder]

    elif (emitted_pools == 'biomass_soil') & (cn.SENSIT_TYPE in ['convert_to_grassland', 'no_shifting_ag']):
        cmd = ['{0}/calc_gross_emissions_{1}.exe'.format(cn.c_emis_compile_dst, cn.SENSIT_TYPE), tile_id, cn.SENSIT_TYPE, folder]

    # This C++ script has an extra argument that names the input carbon emitted_pools and output emissions correctly
    elif (emitted_pools == 'biomass_soil') & (cn.SENSIT_TYPE not in ['no_shifting_ag', 'convert_to_grassland']):
        cmd = ['{0}/calc_gross_emissions_generic.exe'.format(cn.c_emis_compile_dst), tile_id, cn.SENSIT_TYPE, folder]

    else:
        uu.exception_log('Pool and/or sensitivity analysis option not valid')

    uu.log_subprocess_output_full(cmd)


    # Identifies which pattern to use for counting tile completion
    pattern = cn.pattern_gross_emis_commod_biomass_soil
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

