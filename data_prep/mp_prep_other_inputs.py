'''
This script processes the inputs for the emissions script that haven't been processed by another script.
At this point, that is: climate zone, Indonesia/Malaysia plantations before 2000, tree cover loss drivers (TSC drivers),
combining IFL2000 (extratropics) and primary forests (tropics) into a single layer,
Hansenizing some removal factor standard deviation inputs, Hansenizing the European removal factors,
and Hansenizing three US-specific removal factor inputs.
'''

from subprocess import Popen, PIPE, STDOUT, check_call
import argparse
import multiprocessing
import datetime
from functools import partial
import sys
import os
import prep_other_inputs

sys.path.append('../')
import constants_and_names as cn
import universal_util as uu

def mp_prep_other_inputs(tile_id_list, run_date, no_upload = None):

    os.chdir(cn.docker_base_dir)
    sensit_type='std'

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # List of tiles to run in the model
        ### BUG: THIS SHOULD ALSO INCLUDE cn.annual_gain_AGC_BGC_planted_forest_unmasked_dir IN ITS LIST
        tile_id_list = uu.create_combined_tile_list(cn.WHRC_biomass_2000_unmasked_dir,
                                             cn.mangrove_biomass_2000_dir,
                                             set3=cn.gain_dir
                                             )

    uu.print_log(tile_id_list)
    uu.print_log("There are {} tiles to process".format(str(len(tile_id_list))) + "\n")

    '''
    Before processing the driver, it needs to be reprojected from Goode Homolosine to WGS84. 
    gdal_warp is producing a weird output, so I did it in ArcMap for the 2020 update, 
    with the output cell size being 0.01 x 0.01 degree and the method being nearest.
    
    arcpy.ProjectRaster_management(in_raster="C:/GIS/Drivers of loss/2020_drivers__tif__from_Forrest_Follett_20210323/FinalClassification_2020_v2__from_Jimmy_MacCarthy_20210323.tif", 
    out_raster="C:/GIS/Drivers of loss/2020_drivers__tif__from_Forrest_Follett_20210323/Final_Classification_2020__reproj_nearest_0-005_0-005_deg__20210323.tif", 
    out_coor_system="GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]]", 
    resampling_type="NEAREST", cell_size="0.005 0.005", geographic_transform="", 
    Registration_Point="", 
    in_coor_system="PROJCS['WGS_1984_Goode_Homolosine',GEOGCS['GCS_unknown',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Goode_Homolosine'],PARAMETER['False_Easting',0.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',0.0],PARAMETER['Option',1.0],UNIT['Meter',1.0]]", 
    vertical="NO_VERTICAL")
    '''

    # List of output directories and output file name patterns
    output_dir_list = [
                       # cn.climate_zone_processed_dir, cn.plant_pre_2000_processed_dir,
                       cn.drivers_processed_dir
                       # cn.ifl_primary_processed_dir,
                       # cn.annual_gain_AGC_natrl_forest_young_dir,
                       # cn.stdev_annual_gain_AGC_natrl_forest_young_dir,
                       # cn.annual_gain_AGC_BGC_natrl_forest_Europe_dir,
                       # cn.stdev_annual_gain_AGC_BGC_natrl_forest_Europe_dir,
                       # cn.FIA_forest_group_processed_dir,
                       # cn.age_cat_natrl_forest_US_dir,
                       # cn.FIA_regions_processed_dir
    ]
    output_pattern_list = [
                           # cn.pattern_climate_zone, cn.pattern_plant_pre_2000,
                           cn.pattern_drivers
                           # cn.pattern_ifl_primary,
                           # cn.pattern_annual_gain_AGC_natrl_forest_young,
                           # cn.pattern_stdev_annual_gain_AGC_natrl_forest_young,
                           # cn.pattern_annual_gain_AGC_BGC_natrl_forest_Europe,
                           # cn.pattern_stdev_annual_gain_AGC_BGC_natrl_forest_Europe,
                           # cn.pattern_FIA_forest_group_processed,
                           # cn.pattern_age_cat_natrl_forest_US,
                           # cn.pattern_FIA_regions_processed
    ]


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


    # # Files to process: climate zone, IDN/MYS plantations before 2000, tree cover loss drivers, combine IFL and primary forest
    # uu.s3_file_download(os.path.join(cn.climate_zone_raw_dir, cn.climate_zone_raw), cn.docker_base_dir, sensit_type)
    # uu.s3_file_download(os.path.join(cn.plant_pre_2000_raw_dir, '{}.zip'.format(cn.pattern_plant_pre_2000_raw)), cn.docker_base_dir, sensit_type)
    uu.s3_file_download(os.path.join(cn.drivers_raw_dir, cn.pattern_drivers_raw), cn.docker_base_dir, sensit_type)
    # uu.s3_file_download(os.path.join(cn.annual_gain_AGC_BGC_natrl_forest_Europe_raw_dir, cn.name_annual_gain_AGC_BGC_natrl_forest_Europe_raw), cn.docker_base_dir, sensit_type)
    # uu.s3_file_download(os.path.join(cn.stdev_annual_gain_AGC_BGC_natrl_forest_Europe_raw_dir, cn.name_stdev_annual_gain_AGC_BGC_natrl_forest_Europe_raw), cn.docker_base_dir, sensit_type)
    # uu.s3_file_download(os.path.join(cn.FIA_regions_raw_dir, cn.name_FIA_regions_raw), cn.docker_base_dir, sensit_type)
    # uu.s3_file_download(os.path.join(cn.age_cat_natrl_forest_US_raw_dir, cn.name_age_cat_natrl_forest_US_raw), cn.docker_base_dir, sensit_type)
    # uu.s3_file_download(os.path.join(cn.FIA_forest_group_raw_dir, cn.name_FIA_forest_group_raw), cn.docker_base_dir, sensit_type)
    # # For some reason, using uu.s3_file_download or otherwise using AWSCLI as a subprocess doesn't work for this raster.
    # # Thus, using wget instead.
    # cmd = ['wget', '{}'.format(cn.annual_gain_AGC_natrl_forest_young_raw_URL), '-P', '{}'.format(cn.docker_base_dir)]
    # process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    # with process.stdout:
    #     uu.log_subprocess_output(process.stdout)
    # uu.s3_file_download(cn.stdev_annual_gain_AGC_natrl_forest_young_raw_URL, cn.docker_base_dir, sensit_type)
    # cmd = ['aws', 's3', 'cp', cn.primary_raw_dir, cn.docker_base_dir, '--recursive']
    # uu.log_subprocess_output_full(cmd)
    #
    # uu.s3_flexible_download(cn.ifl_dir, cn.pattern_ifl, cn.docker_base_dir, sensit_type, tile_id_list)
    #
    # uu.print_log("Unzipping pre-2000 plantations...")
    # cmd = ['unzip', '-j', '{}.zip'.format(cn.pattern_plant_pre_2000_raw)]
    # uu.log_subprocess_output_full(cmd)

    # Creates tree cover loss driver tiles.
    # The raw driver tile should have NoData for unassigned drivers as opposed to 0 for unassigned drivers.
    # For the 2020 driver update, I reclassified the 0 values as NoData in ArcMap. I also unprojected the global drivers
    # map to WGS84 because running the homolosine projection that Jimmy provided was giving incorrect processed results.
    source_raster = cn.pattern_drivers_raw
    out_pattern = cn.pattern_drivers
    dt = 'Byte'
    if cn.count == 96:
        processes = 87  # 45 processors = 70 GB peak; 70 = 90 GB peak; 80 = 100 GB peak; 87 = 125 GB peak
    else:
        processes = int(cn.count/2)
    uu.print_log("Creating tree cover loss driver tiles with {} processors...".format(processes))
    pool = multiprocessing.Pool(processes)
    pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt,
                     no_upload=no_upload), tile_id_list)
    pool.close()
    pool.join()


    # # Creates young natural forest removal rate tiles
    # source_raster = cn.name_annual_gain_AGC_natrl_forest_young_raw
    # out_pattern = cn.pattern_annual_gain_AGC_natrl_forest_young
    # dt = 'float32'
    # if cn.count == 96:
    #     processes = 80  # 32 processors = 210 GB peak; 60 = 370 GB peak; 80 = XXX GB peak
    # else:
    #     processes = int(cn.count/2)
    # uu.print_log("Creating young natural forest removals rate tiles with {} processors...".format(processes))
    # pool = multiprocessing.Pool(processes)
    # pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt, no_upload=no_upload), tile_id_list)
    # pool.close()
    # pool.join()
    #
    # # Creates young natural forest removal rate standard deviation tiles
    # source_raster = cn.name_stdev_annual_gain_AGC_natrl_forest_young_raw
    # out_pattern = cn.pattern_stdev_annual_gain_AGC_natrl_forest_young
    # dt = 'float32'
    # if cn.count == 96:
    #     processes = 80  # 32 processors = 210 GB peak; 60 = 370 GB peak; 80 = XXX GB peak
    # else:
    #     processes = int(cn.count/2)
    # uu.print_log("Creating standard deviation for young natural forest removal rate tiles with {} processors...".format(processes))
    # pool = multiprocessing.Pool(processes)
    # pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt, no_upload=no_upload), tile_id_list)
    # pool.close()
    # pool.join()
    #
    #
    # # Creates pre-2000 oil palm plantation tiles
    # if cn.count == 96:
    #     processes = 80  # 45 processors = 100 GB peak; 80 = XXX GB peak
    # else:
    #     processes = int(cn.count/2)
    # uu.print_log("Creating pre-2000 oil palm plantation tiles with {} processors...".format(processes))
    # pool = multiprocessing.Pool(processes)
    # pool.map(prep_other_inputs.rasterize_pre_2000_plantations, tile_id_list)
    # pool.close()
    # pool.join()
    #
    #
    # # Creates climate zone tiles
    # if cn.count == 96:
    #     processes = 80  # 45 processors = 230 GB peak (on second step); 80 = XXX GB peak
    # else:
    #     processes = int(cn.count/2)
    # uu.print_log("Creating climate zone tiles with {} processors...".format(processes))
    # pool = multiprocessing.Pool(processes)
    # pool.map(prep_other_inputs.create_climate_zone_tiles, tile_id_list)
    # pool.close()
    # pool.join()
    #
    # # Creates European natural forest removal rate tiles
    # source_raster = cn.name_annual_gain_AGC_BGC_natrl_forest_Europe_raw
    # out_pattern = cn.pattern_annual_gain_AGC_BGC_natrl_forest_Europe
    # dt = 'float32'
    # if cn.count == 96:
    #     processes = 60  # 32 processors = 60 GB peak; 60 = XXX GB peak
    # else:
    #     processes = int(cn.count/2)
    # uu.print_log("Creating European natural forest removals rate tiles with {} processors...".format(processes))
    # pool = multiprocessing.Pool(processes)
    # pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt, no_upload=no_upload), tile_id_list)
    # pool.close()
    # pool.join()
    #
    # # Creates European natural forest standard deviation of removal rate tiles
    # source_raster = cn.name_stdev_annual_gain_AGC_BGC_natrl_forest_Europe_raw
    # out_pattern = cn.pattern_stdev_annual_gain_AGC_BGC_natrl_forest_Europe
    # dt = 'float32'
    # if cn.count == 96:
    #     processes = 32  # 32 processors = 60 GB peak; 60 = XXX GB peak
    # else:
    #     processes = int(cn.count/2)
    # uu.print_log("Creating standard deviation for European natural forest removals rate tiles with {} processors...".format(processes))
    # pool = multiprocessing.Pool(processes)
    # pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt, no_upload=no_upload), tile_id_list)
    # pool.close()
    # pool.join()
    #
    #
    # # Creates a vrt of the primary forests with nodata=0 from the continental primary forest rasters
    # uu.print_log("Creating vrt of humid tropial primary forest...")
    # primary_vrt = 'primary_2001.vrt'
    # os.system('gdalbuildvrt -srcnodata 0 {} *2001_primary.tif'.format(primary_vrt))
    # uu.print_log("  Humid tropical primary forest vrt created")
    #
    # # Creates primary forest tiles
    # source_raster = primary_vrt
    # out_pattern = 'primary_2001'
    # dt = 'Byte'
    # if cn.count == 96:
    #     processes = 45  # 45 processors = 650 GB peak
    # else:
    #     processes = int(cn.count/2)
    # uu.print_log("Creating primary forest tiles with {} processors...".format(processes))
    # pool = multiprocessing.Pool(processes)
    # pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt, no_upload=no_upload), tile_id_list)
    # pool.close()
    # pool.join()
    #
    #
    # # Creates a combined IFL/primary forest raster
    # # Uses very little memory since it's just file renaming
    # if cn.count == 96:
    #     processes = 60  # 60 processors = 10 GB peak
    # else:
    #     processes = int(cn.count/2)
    # uu.print_log("Assigning each tile to ifl2000 or primary forest with {} processors...".format(processes))
    # pool = multiprocessing.Pool(processes)
    # pool.map(prep_other_inputs.create_combined_ifl_primary, tile_id_list)
    # pool.close()
    # pool.join()
    #
    #
    # # Creates forest age category tiles for US forests
    # source_raster = cn.name_age_cat_natrl_forest_US_raw
    # out_pattern = cn.pattern_age_cat_natrl_forest_US
    # dt = 'Byte'
    # if cn.count == 96:
    #     processes = 70  # 32 processors = 35 GB peak; 70 = XXX GB peak
    # else:
    #     processes = int(cn.count/2)
    # uu.print_log("Creating US forest age category tiles with {} processors...".format(processes))
    # pool = multiprocessing.Pool(processes)
    # pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt, no_upload=no_upload), tile_id_list)
    # pool.close()
    # pool.join()
    #
    # # Creates forest groups for US forests
    # source_raster = cn.name_FIA_forest_group_raw
    # out_pattern = cn.pattern_FIA_forest_group_processed
    # dt = 'Byte'
    # if cn.count == 96:
    #     processes = 80  # 32 processors = 25 GB peak; 80 = XXX GB peak
    # else:
    #     processes = int(cn.count/2)
    # uu.print_log("Creating US forest group tiles with {} processors...".format(processes))
    # pool = multiprocessing.Pool(processes)
    # pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt, no_upload=no_upload), tile_id_list)
    # pool.close()
    # pool.join()
    #
    # # Creates FIA regions for US forests
    # source_raster = cn.name_FIA_regions_raw
    # out_pattern = cn.pattern_FIA_regions_processed
    # dt = 'Byte'
    # if cn.count == 96:
    #     processes = 70  # 32 processors = 35 GB peak; 70 = XXX GB peak
    # else:
    #     processes = int(cn.count/2)
    # uu.print_log("Creating US forest region tiles with {} processors...".format(processes))
    # pool = multiprocessing.Pool(processes)
    # pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt, no_upload=no_upload), tile_id_list)
    # pool.close()
    # pool.join()
    #
    #
    for output_pattern in [cn.pattern_drivers
        # ,cn.pattern_annual_gain_AGC_natrl_forest_young, cn.pattern_stdev_annual_gain_AGC_natrl_forest_young
    ]:

        # For some reason I can't figure out, the young forest rasters (rate and stdev) have NaN values in some places where 0 (NoData)
        # should be. These NaN values show up as values when the check_and_delete_if_empty function runs, making the tiles not
        # deleted even if they have no data. However, the light version (which uses gdalinfo rather than rasterio masks) doesn't
        # have this problem. So I'm forcing the young forest rates to and stdev to have their emptiness checked by the gdalinfo version.
        if output_pattern in [cn.pattern_annual_gain_AGC_natrl_forest_young, cn.pattern_stdev_annual_gain_AGC_natrl_forest_young]:
            processes = int(cn.count / 2)
            uu.print_log("Checking for empty tiles of {0} pattern with {1} processors using light function...".format(output_pattern, processes))
            pool = multiprocessing.Pool(processes)
            pool.map(partial(uu.check_and_delete_if_empty_light, output_pattern=output_pattern), tile_id_list)
            pool.close()
            pool.join()

        if cn.count == 96:
            processes = 50  # 60 processors = >730 GB peak (for European natural forest forest removal rates); 50 = XXX GB peak
            uu.print_log("Checking for empty tiles of {0} pattern with {1} processors...".format(output_pattern, processes))
            pool = multiprocessing.Pool(processes)
            pool.map(partial(uu.check_and_delete_if_empty, output_pattern=output_pattern), tile_id_list)
            pool.close()
            pool.join()
        elif cn.count <= 2: # For local tests
            processes = 1
            uu.print_log("Checking for empty tiles of {0} pattern with {1} processors using light function...".format(output_pattern, processes))
            pool = multiprocessing.Pool(processes)
            pool.map(partial(uu.check_and_delete_if_empty_light, output_pattern=output_pattern), tile_id_list)
            pool.close()
            pool.join()
        else:
            processes = int(cn.count / 2)
            uu.print_log("Checking for empty tiles of {0} pattern with {1} processors...".format(output_pattern, processes))
            pool = multiprocessing.Pool(processes)
            pool.map(partial(uu.check_and_delete_if_empty, output_pattern=output_pattern), tile_id_list)
            pool.close()
            pool.join()
        uu.print_log('\n')


    # Uploads output tiles to s3
    for i in range(0, len(output_dir_list)):
        uu.upload_final_set(output_dir_list[i], output_pattern_list[i])


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Create tiles of the annual AGB and BGB removals rates for mangrove forests')
    parser.add_argument('--tile_id_list', '-l', required=True,
                        help='List of tile ids to use in the model. Should be of form 00N_110E or 00N_110E,00N_120E or all.')
    parser.add_argument('--run-date', '-d', required=False,
                        help='Date of run. Must be format YYYYMMDD.')
    parser.add_argument('--no-upload', '-nu', action='store_true',
                       help='Disables uploading of outputs to s3')
    args = parser.parse_args()
    tile_id_list = args.tile_id_list
    run_date = args.run_date
    no_upload = args.NO_UPLOAD

    # Disables upload to s3 if no AWS credentials are found in environment
    if not uu.check_aws_creds():
        no_upload = True

    # Create the output log
    uu.initiate_log(tile_id_list=tile_id_list, run_date=run_date, no_upload=no_upload)

    # Checks whether the tile_id_list argument is valid
    tile_id_list = uu.tile_id_list_check(tile_id_list)

    mp_prep_other_inputs(tile_id_list=tile_id_list, run_date=run_date, no_upload=no_upload)