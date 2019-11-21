### Calculates the net emissions over the study period, with units of Mg CO2e/ha on a pixel-by-pixel basis.
### This only uses gross emissions from biomass+soil (doesn't run with gross emissions from soil_only).

import multiprocessing
import net_flux
import argparse
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
        cn.cumul_gain_AGCO2_BGCO2_all_types_dir: [cn.pattern_cumul_gain_AGCO2_BGCO2_all_types],
        cn.gross_emis_all_gases_all_drivers_biomass_soil_dir: [cn.pattern_gross_emis_all_gases_all_drivers_biomass_soil]
    }


    # List of tiles to run in the model
    tile_id_list = uu.create_combined_tile_list(cn.gross_emis_all_gases_all_drivers_biomass_soil_dir, cn.cumul_gain_AGCO2_BGCO2_all_types_dir)
    tile_id_list = ['30N_140E', '40N_030W'] # test tiles
    # tile_id_list = ['00N_110E'] # test tiles
    print tile_id_list
    print "There are {} tiles to process".format(str(len(tile_id_list))) + "\n"


    # List of output directories and output file name patterns
    output_dir_list = [cn.net_flux_dir]
    output_pattern_list = [cn.pattern_net_flux]


    # The argument for what kind of model run is being done: standard conditions or a sensitivity analysis run
    parser = argparse.ArgumentParser(description='Create tiles of the number of years of carbon gain for mangrove forests')
    parser.add_argument('--model-type', '-t', required=True,
                        help='{}'.format(cn.model_type_arg_help))
    args = parser.parse_args()
    sensit_type = args.model_type
    # Checks whether the sensitivity analysis argument is valid
    uu.check_sensit_type(sensit_type)


    # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
    for key, values in download_dict.iteritems():
        dir = key
        pattern = values[0]
        uu.s3_flexible_download(dir, pattern, '.', sensit_type, tile_id_list)


    # If the model run isn't the standard one, the output directory and file names are changed
    if sensit_type != 'std':
        print "Changing output directory and file name pattern based on sensitivity analysis"
        output_dir_list = uu.alter_dirs(sensit_type, output_dir_list)
        output_pattern_list = uu.alter_patterns(sensit_type, output_pattern_list)


    # Since the input tile lists have different numbers of tiles, at least one input will need to have some blank tiles made
    # so that it has all the necessary input tiles
    # The inputs that might need to have dummy tiles made in order to match the tile list of the carbon pools
    folder = './'
    for download_dir, download_pattern in download_dict.iteritems():

        # Renames the tiles according to the sensitivity analysis before creating dummy tiles.
        # The renaming function requires a whole tile name, so this passes a dummy time name that is then stripped a few
        # lines later.
        pattern = download_pattern[0]

        count = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(count-10)
        pool.map(partial(uu.make_blank_tile, pattern=pattern, folder=folder, sensit_type=sensit_type), tile_id_list)
        pool.close()
        pool.join()

    # # For single processor use
    # folder = './'
    # for download_dir, download_pattern in download_dict.iteritems():
    #
    #
    #     # download_pattern_name = download_pattern[0]
    #     # sensit_use = download_pattern[1]
    #     # tile_id = 'XXXXXXXX'
    #     # output_pattern = uu.sensit_tile_rename(sensit_type, tile_id, download_pattern_name, sensit_use)
    #     # pattern = output_pattern[9:-4]
    #
    #     for tile_id in tile_id_list:
    #         uu.make_blank_tile(tile_id, download_pattern[0], folder, sensit_type)


    # Creates a single filename pattern to pass to the multiprocessor call
    pattern = output_pattern_list[0]

    # Count/3 uses about 380 GB on a r4.16xlarge spot machine
    # processes/24 maxes out at about 435 GB on an r4.16xlarge spot machine
    count = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=24)
    pool.map(partial(net_flux.net_calc, pattern=pattern, sensit_type=sensit_type), tile_id_list)

    # # For single processor use
    # for tile_id in tile_id_list:
    #     net_flux.net_calc(tile_id, output_pattern_list[0], sensit_type)


    # Uploads output tiles to s3
    for i in range(0, len(output_dir_list)):
        uu.upload_final_set(output_dir_list[i], output_pattern_list[i])


if __name__ == '__main__':
    main()