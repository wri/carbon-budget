

import multiprocessing
from functools import partial
import glob
import argparse
from sensitivity_analysis import legal_AMZ_loss
import pandas as pd
import subprocess
import os
import sys
sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def main ():



    # Files to download for this script.
    download_dict = {cn.loss_dir: [''],
                     cn.WHRC_biomass_2000_unmasked_dir: [cn.pattern_WHRC_biomass_2000_unmasked]
    }

    Brazil_stages = ['all', 'create_forest_extent', 'create_loss',
                     'forest_age_category', 'gain_year_count', 'annual_removals', 'cumulative_removals']

    # List of tiles that could be run. This list is only used to create the FIA region tiles if they don't already exist.
    tile_id_list = uu.tile_list_s3(cn.WHRC_biomass_2000_unmasked_dir)
    # tile_id_list = ["00N_000E", "00N_050W", "00N_060W", "00N_010E", "00N_020E", "00N_030E", "00N_040E", "10N_000E", "10N_010E", "10N_010W", "10N_020E", "10N_020W"] # test tiles
    # tile_id_list = ['50N_130W'] # test tiles

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
    print actual_stages

    # By definition, this script is for US-specific removals
    sensit_type = 'Brazil_loss'

    # List of output directories and output file name patterns
    output_dir_list = [cn.Brazil_forest_extent_2000_processed_dir, cn.Brazil_annual_loss_processed_dir,
                       cn.Brazil_forest_age_category_dir, cn.Brazil_gain_year_count_natrl_forest_dir,
                       cn.Brazil_annual_gain_AGB_natrl_forest_dir, cn.Brazil_annual_gain_BGB_natrl_forest_dir,
                       cn.Brazil_cumul_gain_AGCO2_natrl_forest_dir, cn.Brazil_cumul_gain_BGCO2_natrl_forest_dir]
    output_pattern_list = [cn.pattern_Brazil_forest_extent_2000_processed, cn.pattern_Brazil_annual_loss_processed,
                           cn.pattern_Brazil_forest_age_category, cn.pattern_Again_year_count_natrl_forest,
                           cn.pattern_Brazil_annual_gain_AGB_natrl_forest, cn.pattern_Brazil_annual_gain_BGB_natrl_forest,
                           cn.pattern_Brazil_cumul_gain_AGCO2_natrl_forest, cn.pattern_Brazil_cumul_gain_BGCO2_natrl_forest]


    count = multiprocessing.cpu_count()

    if 'create_forest_extent' in actual_stages:

        print 'Creating forest extent tiles'

        uu.s3_folder_download(cn.Brazil_forest_extent_2000_raw_dir, '.', sensit_type)

        raw_forest_extent_inputs = glob.glob('*AMZ_warped_*tif')
        print raw_forest_extent_inputs

        cmd = ['gdal_merge.py', '-o', cn.Brazil_forest_extent_2000_merged, raw_forest_extent_inputs[5], raw_forest_extent_inputs[4],
               raw_forest_extent_inputs[3], raw_forest_extent_inputs[2], raw_forest_extent_inputs[1], raw_forest_extent_inputs[0],
               '-co', 'COMPRESS=LZW', '-a_nodata', '0', '-n', '0', '-ot', 'Byte']
        subprocess.check_call(cmd)

        # Converts the national forest age category raster to Hansen tiles
        source_raster = cn.Brazil_forest_extent_2000_merged
        out_pattern = cn.pattern_Brazil_forest_extent_2000_processed
        dt = 'Byte'
        pool = multiprocessing.Pool(count/2)
        pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt), tile_id_list)

















    # Counts how many processed FIA region tiles there are on s3 already. 16 tiles cover the continental US.
    FIA_regions_tile_count = uu.count_tiles_s3(cn.FIA_regions_processed_dir)

    # Only creates FIA region tiles if they don't already exist on s3.
    if FIA_regions_tile_count == 16:
        print "FIA region tiles already created. Copying to s3 now..."
        uu.s3_flexible_download(cn.FIA_regions_processed_dir, cn.pattern_FIA_regions_processed, '.', 'std', 'all')

    else:
        print "FIA region tiles do not exist. Creating tiles, then copying to s3 for future use..."
        uu.s3_file_download(os.path.join(cn.FIA_regions_raw_dir, cn.name_FIA_regions_raw), '.', 'std')

        cmd = ['unzip', '-o', '-j', cn.name_FIA_regions_raw]
        subprocess.check_call(cmd)

        # Converts the region shapefile to Hansen tiles
        pool = multiprocessing.Pool(count/2)
        pool.map(US_removal_rates.prep_FIA_regions, tile_id_list)


    # List of FIA region tiles on the spot machine. Only this list is used for the rest of the script.
    US_tile_list = uu.tile_list_spot_machine('.', '{}.tif'.format(cn.pattern_FIA_regions_processed))
    US_tile_id_list = [i[0:8] for i in US_tile_list]
    # US_tile_id_list = ['50N_130W']    # For testing
    print US_tile_id_list
    print "There are {} tiles to process".format(str(len(US_tile_id_list))) + "\n"


    # Counts how many processed forest age category tiles there are on s3 already. 16 tiles cover the continental US.
    US_age_tile_count = uu.count_tiles_s3(cn.US_forest_age_cat_processed_dir)

    # Only creates FIA forest age category tiles if they don't already exist on s3.
    if US_age_tile_count == 16:
        print "Forest age category tiles already created. Copying to spot machine now..."
        uu.s3_flexible_download(cn.US_forest_age_cat_processed_dir, cn.pattern_US_forest_age_cat_processed,
                                '', 'std', US_tile_id_list)

    else:
        print "Southern forest age category tiles do not exist. Creating tiles, then copying to s3 for future use..."
        uu.s3_file_download(os.path.join(cn.US_forest_age_cat_raw_dir, cn.name_US_forest_age_cat_raw), '.', 'std')

        # Converts the national forest age category raster to Hansen tiles
        source_raster = cn.name_US_forest_age_cat_raw
        out_pattern = cn.pattern_US_forest_age_cat_processed
        dt = 'Int16'
        pool = multiprocessing.Pool(count/2)
        pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt), US_tile_id_list)

        uu.upload_final_set(cn.US_forest_age_cat_processed_dir, cn.pattern_US_forest_age_cat_processed)


    # Counts how many processed FIA forest group tiles there are on s3 already. 16 tiles cover the continental US.
    FIA_forest_group_tile_count = uu.count_tiles_s3(cn.FIA_forest_group_processed_dir)

    # Only creates FIA forest group tiles if they don't already exist on s3.
    if FIA_forest_group_tile_count == 16:
        print "FIA forest group tiles already created. Copying to spot machine now..."
        uu.s3_flexible_download(cn.FIA_forest_group_processed_dir, cn.pattern_FIA_forest_group_processed, '', 'std', US_tile_id_list)

    else:
        print "FIA forest group tiles do not exist. Creating tiles, then copying to s3 for future use..."
        uu.s3_file_download(os.path.join(cn.FIA_forest_group_raw_dir, cn.name_FIA_forest_group_raw), '.', 'std')

        # Converts the national forest group raster to Hansen forest group tiles
        source_raster = cn.name_FIA_forest_group_raw
        out_pattern = cn.pattern_FIA_forest_group_processed
        dt = 'Byte'
        pool = multiprocessing.Pool(count/2)
        pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt), US_tile_id_list)

        uu.upload_final_set(cn.FIA_forest_group_processed_dir, cn.pattern_FIA_forest_group_processed)


    # Downloads input files or entire directories, depending on how many tiles are in the tile_id_list
    for key, values in download_dict.iteritems():
        dir = key
        pattern = values[0]
        uu.s3_flexible_download(dir, pattern, '.', sensit_type, US_tile_id_list)



    # Table with US-specific removal rates
    cmd = ['aws', 's3', 'cp', os.path.join(cn.gain_spreadsheet_dir, cn.table_US_removal_rate), '.']
    subprocess.check_call(cmd)

    # Imports the table with the region-group-age AGB removal rates
    gain_table = pd.read_excel("{}".format(cn.table_US_removal_rate),
                               sheet_name="US_rates_for_model")

    # Converts gain table from wide to long, so each region-group-age category has its own row
    gain_table_group_region_by_age = pd.melt(gain_table, id_vars=['FIA_region_code', 'forest_group_code'],
                                      value_vars=['growth_young', 'growth_middle',
                                                  'growth_old'])
    gain_table_group_region_by_age = gain_table_group_region_by_age.dropna()

    # In the forest age category raster, each category has this value
    age_dict = {'growth_young': 1000, 'growth_middle': 2000, 'growth_old': 3000}

    # Creates a unique value for each forest group-region-age category in the table.
    # Although these rates are applied to all standard gain model pixels at first, they are not ultimately used for
    # pixels that have Hansen gain (see below).
    gain_table_group_region_age = gain_table_group_region_by_age.replace({"variable": age_dict})
    gain_table_group_region_age['age_cat'] = gain_table_group_region_age['variable']*10
    gain_table_group_region_age['group_region_age_combined'] = gain_table_group_region_age['age_cat'] + \
                                              gain_table_group_region_age['forest_group_code']*100 + \
                                              gain_table_group_region_age['FIA_region_code']
    # Converts the forest group-region-age codes and corresponding gain rates to a dictionary,
    # where the key is the unique group-region-age code and the value is the AGB removal rate.
    gain_table_group_region_age_dict = pd.Series(gain_table_group_region_age.value.values, index=gain_table_group_region_age.group_region_age_combined).to_dict()
    print gain_table_group_region_age_dict


    # Creates a unique value for each forest group-region category using just young forest rates.
    # These are assigned to Hansen gain pixels, which automatically get the young forest rate, regardless of the
    # forest age category raster.
    gain_table_group_region = gain_table_group_region_age.drop(gain_table_group_region_age[gain_table_group_region_age.age_cat != 10000].index)
    gain_table_group_region['group_region_combined'] = gain_table_group_region['forest_group_code']*100 + \
                                                       gain_table_group_region['FIA_region_code']
    # Converts the forest group-region codes and corresponding gain rates to a dictionary,
    # where the key is the unique group-region code (youngest age category) and the value is the AGB removal rate.
    gain_table_group_region_dict = pd.Series(gain_table_group_region.value.values, index=gain_table_group_region.group_region_combined).to_dict()
    print gain_table_group_region_dict


    # count/2 on a m4.16xlarge maxes out at about 230 GB of memory (processing 16 tiles at once), so it's okay on an m4.16xlarge
    pool = multiprocessing.Pool(count/2)
    pool.map(partial(US_removal_rates.US_removal_rate_calc, gain_table_group_region_age_dict=gain_table_group_region_age_dict,
                     gain_table_group_region_dict=gain_table_group_region_dict,
                     output_pattern_list=output_pattern_list, sensit_type=sensit_type), US_tile_id_list)
    pool.close()
    pool.join()

    # # For single processor use
    # for tile_id in US_tile_id_list:
    #
    #     US_removal_rates.US_removal_rate_calc(tile_id, gain_table_group_region_age_dict, gain_table_group_region_dict,
    #                                           output_pattern_list, sensit_type)


    # Uploads output tiles to s3
    for i in range(0, len(output_dir_list)):
        uu.upload_final_set(output_dir_list[i], output_pattern_list[i])


if __name__ == '__main__':
    main()