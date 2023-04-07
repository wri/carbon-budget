'''
This script processes the inputs for the emissions script that haven't been processed by another script.
At this point, that is: climate zone, Indonesia/Malaysia plantations before 2000, tree cover loss drivers (TSC drivers),
combining IFL2000 (extratropics) and primary forests (tropics) into a single layer,
Hansenizing some removal factor standard deviation inputs, Hansenizing the European removal factors,
and Hansenizing three US-specific removal factor inputs.

python -m data_prep.mp_prep_other_inputs_one_off -l 00N_000E -nu
python -m data_prep.mp_prep_other_inputs_one_off -l all
'''

import argparse
import multiprocessing
import datetime
from functools import partial
import rioxarray as rio
import os
import sys
import xarray as xr

import constants_and_names as cn
import universal_util as uu

from . import prep_other_inputs_one_off

def mp_prep_other_inputs(tile_id_list):

    os.chdir(cn.docker_tile_dir)
    sensit_type='std'

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # List of tiles to run in the model
        tile_id_list = uu.create_combined_tile_list(
            [cn.WHRC_biomass_2000_unmasked_dir, cn.mangrove_biomass_2000_dir, cn.gain_dir, cn.tcd_dir,
             cn.annual_gain_AGC_BGC_planted_forest_unmasked_dir]
        )

    uu.print_log(tile_id_list)
    uu.print_log(f'There are {str(len(tile_id_list))} tiles to process', "\n")


    # List of output directories and output file name patterns
    output_dir_list = [
                       cn.climate_zone_processed_dir, cn.plant_pre_2000_processed_dir,
                       cn.ifl_primary_processed_dir,
                       cn.annual_gain_AGC_natrl_forest_young_dir,
                       cn.stdev_annual_gain_AGC_natrl_forest_young_dir,
                       cn.annual_gain_AGC_BGC_natrl_forest_Europe_dir,
                       cn.stdev_annual_gain_AGC_BGC_natrl_forest_Europe_dir,
                       cn.FIA_forest_group_processed_dir,
                       cn.age_cat_natrl_forest_US_dir,
                       cn.FIA_regions_processed_dir,
                       cn.BGB_AGB_ratio_dir
    ]
    output_pattern_list = [
                           cn.pattern_climate_zone, cn.pattern_plant_pre_2000,
                           cn.pattern_ifl_primary,
                           cn.pattern_annual_gain_AGC_natrl_forest_young,
                           cn.pattern_stdev_annual_gain_AGC_natrl_forest_young,
                           cn.pattern_annual_gain_AGC_BGC_natrl_forest_Europe,
                           cn.pattern_stdev_annual_gain_AGC_BGC_natrl_forest_Europe,
                           cn.pattern_FIA_forest_group_processed,
                           cn.pattern_age_cat_natrl_forest_US,
                           cn.pattern_FIA_regions_processed,
                           cn.pattern_BGB_AGB_ratio
    ]


    # If the model run isn't the standard one, the output directory and file names are changed
    if sensit_type != 'std':

        uu.print_log('Changing output directory and file name pattern based on sensitivity analysis')
        output_dir_list = uu.alter_dirs(sensit_type, output_dir_list)
        output_pattern_list = uu.alter_patterns(sensit_type, output_pattern_list)


    # A date can optionally be provided by the full model script or a run of this script.
    # This replaces the date in constants_and_names.
    # Only done if output upload is enabled.
    if cn.RUN_DATE is not None and no_upload is not None:
        output_dir_list = uu.replace_output_dir_date(output_dir_list, cn.RUN_DATE)


    # # Files to process: climate zone, IDN/MYS plantations before 2000, tree cover loss drivers, combine IFL and primary forest
    # uu.s3_file_download(os.path.join(cn.climate_zone_raw_dir, cn.climate_zone_raw), cn.docker_base_dir, sensit_type)
    # uu.s3_file_download(os.path.join(cn.plant_pre_2000_raw_dir, '{}.zip'.format(cn.pattern_plant_pre_2000_raw)), cn.docker_base_dir, sensit_type)
    # uu.s3_file_download(os.path.join(cn.annual_gain_AGC_BGC_natrl_forest_Europe_raw_dir, cn.name_annual_gain_AGC_BGC_natrl_forest_Europe_raw), cn.docker_base_dir, sensit_type)
    # uu.s3_file_download(os.path.join(cn.stdev_annual_gain_AGC_BGC_natrl_forest_Europe_raw_dir, cn.name_stdev_annual_gain_AGC_BGC_natrl_forest_Europe_raw), cn.docker_base_dir, sensit_type)
    # uu.s3_file_download(os.path.join(cn.FIA_regions_raw_dir, cn.name_FIA_regions_raw), cn.docker_base_dir, sensit_type)
    # uu.s3_file_download(os.path.join(cn.age_cat_natrl_forest_US_raw_dir, cn.name_age_cat_natrl_forest_US_raw), cn.docker_base_dir, sensit_type)
    # uu.s3_file_download(os.path.join(cn.FIA_forest_group_raw_dir, cn.name_FIA_forest_group_raw), cn.docker_base_dir, sensit_type)
    # # For some reason, using uu.s3_file_download or otherwise using AWSCLI as a subprocess doesn't work for this raster.
    # # Thus, using wget instead.
    # cmd = ['wget', '{}'.format(cn.annual_gain_AGC_natrl_forest_young_raw_URL), '-P', '{}'.format(cn.docker_base_dir)]
    # uu.log_subprocess_output_full(cmd)
    # uu.s3_file_download(cn.stdev_annual_gain_AGC_natrl_forest_young_raw_URL, cn.docker_base_dir, sensit_type)
    # cmd = ['aws', 's3', 'cp', cn.primary_raw_dir, cn.docker_base_dir, '--recursive']
    # uu.log_subprocess_output_full(cmd)
    #
    # uu.s3_flexible_download(cn.ifl_dir, cn.pattern_ifl, cn.docker_base_dir, sensit_type, tile_id_list)
    #
    # uu.print_log("Unzipping pre-2000 plantations...")
    # cmd = ['unzip', '-j', '{}.zip'.format(cn.pattern_plant_pre_2000_raw)]
    # uu.log_subprocess_output_full(cmd)


    # ### Creates young natural forest removal rate tiles
    # source_raster = cn.name_annual_gain_AGC_natrl_forest_young_raw
    # out_pattern = cn.pattern_annual_gain_AGC_natrl_forest_young
    # dt = 'float32'
    # if cn.count == 96:
    #     processes = 80  # 32 processors = 210 GB peak; 60 = 370 GB peak; 80 = XXX GB peak
    # else:
    #     processes = int(cn.count/2)
    # uu.print_log("Creating young natural forest removals rate tiles with {} processors...".format(processes))
    # pool = multiprocessing.Pool(processes)
    # pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt), tile_id_list)
    # pool.close()
    # pool.join()
    #
    #
    # ### Creates young natural forest removal rate standard deviation tiles
    # source_raster = cn.name_stdev_annual_gain_AGC_natrl_forest_young_raw
    # out_pattern = cn.pattern_stdev_annual_gain_AGC_natrl_forest_young
    # dt = 'float32'
    # if cn.count == 96:
    #     processes = 80  # 32 processors = 210 GB peak; 60 = 370 GB peak; 80 = XXX GB peak
    # else:
    #     processes = int(cn.count/2)
    # uu.print_log("Creating standard deviation for young natural forest removal rate tiles with {} processors...".format(processes))
    # pool = multiprocessing.Pool(processes)
    # pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt), tile_id_list)
    # pool.close()
    # pool.join()
    #
    #
    # ### Creates pre-2000 oil palm plantation tiles
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
    # ### Creates climate zone tiles
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
    #
    # ### Creates European natural forest removal rate tiles
    # source_raster = cn.name_annual_gain_AGC_BGC_natrl_forest_Europe_raw
    # out_pattern = cn.pattern_annual_gain_AGC_BGC_natrl_forest_Europe
    # dt = 'float32'
    # if cn.count == 96:
    #     processes = 60  # 32 processors = 60 GB peak; 60 = XXX GB peak
    # else:
    #     processes = int(cn.count/2)
    # uu.print_log("Creating European natural forest removals rate tiles with {} processors...".format(processes))
    # pool = multiprocessing.Pool(processes)
    # pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt), tile_id_list)
    # pool.close()
    # pool.join()
    #
    #
    # ### Creates European natural forest standard deviation of removal rate tiles
    # source_raster = cn.name_stdev_annual_gain_AGC_BGC_natrl_forest_Europe_raw
    # out_pattern = cn.pattern_stdev_annual_gain_AGC_BGC_natrl_forest_Europe
    # dt = 'float32'
    # if cn.count == 96:
    #     processes = 32  # 32 processors = 60 GB peak; 60 = XXX GB peak
    # else:
    #     processes = int(cn.count/2)
    # uu.print_log("Creating standard deviation for European natural forest removals rate tiles with {} processors...".format(processes))
    # pool = multiprocessing.Pool(processes)
    # pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt), tile_id_list)
    # pool.close()
    # pool.join()
    #
    #
    # ### Creates humid tropical primary forest tiles
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
    # pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt), tile_id_list)
    # pool.close()
    # pool.join()
    #
    #
    # ### Creates a combined IFL/primary forest raster
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
    # ### Creates forest age category tiles for US forests
    # source_raster = cn.name_age_cat_natrl_forest_US_raw
    # out_pattern = cn.pattern_age_cat_natrl_forest_US
    # dt = 'Byte'
    # if cn.count == 96:
    #     processes = 70  # 32 processors = 35 GB peak; 70 = XXX GB peak
    # else:
    #     processes = int(cn.count/2)
    # uu.print_log("Creating US forest age category tiles with {} processors...".format(processes))
    # pool = multiprocessing.Pool(processes)
    # pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt), tile_id_list)
    # pool.close()
    # pool.join()
    #
    # ### Creates forest groups for US forests
    # source_raster = cn.name_FIA_forest_group_raw
    # out_pattern = cn.pattern_FIA_forest_group_processed
    # dt = 'Byte'
    # if cn.count == 96:
    #     processes = 80  # 32 processors = 25 GB peak; 80 = XXX GB peak
    # else:
    #     processes = int(cn.count/2)
    # uu.print_log("Creating US forest group tiles with {} processors...".format(processes))
    # pool = multiprocessing.Pool(processes)
    # pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt), tile_id_list)
    # pool.close()
    # pool.join()
    #
    # ### Creates FIA regions for US forests
    # source_raster = cn.name_FIA_regions_raw
    # out_pattern = cn.pattern_FIA_regions_processed
    # dt = 'Byte'
    # if cn.count == 96:
    #     processes = 70  # 32 processors = 35 GB peak; 70 = XXX GB peak
    # else:
    #     processes = int(cn.count/2)
    # uu.print_log("Creating US forest region tiles with {} processors...".format(processes))
    # pool = multiprocessing.Pool(processes)
    # pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt), tile_id_list)
    # pool.close()
    # pool.join()


    ### Creates Hansen tiles of AGB:BGB based on Huang et al. 2021: https://essd.copernicus.org/articles/13/4263/2021/

    # uu.print_log("Downloading raw NetCDF files...")
    # cmd = ['aws', 's3', 'cp', cn.AGB_BGB_Huang_raw_dir, '.']
    # uu.log_subprocess_output_full(cmd)

    # # Converts the AGB and BGB NetCDF files to global geotifs.
    # # Note that, for some reason, this isn't working in Docker locally; when it gets to the to_raster step, it keeps
    # # saying "Killed", perhaps because it's running out of memory (1.87/1.95 GB used).
    # # So I did this in Python shell locally outside Docker and it worked fine.
    # # Methods for converting NetCDF4 to geotif are from approach 1 at
    # # https://help.marine.copernicus.eu/en/articles/5029956-how-to-convert-netcdf-to-geotiff
    # # Compression argument from: https://github.com/corteva/rioxarray/issues/112
    # agb = xr.open_dataset(cn.name_raw_AGB_Huang_global)
    # # uu.print_log(agb)
    # agb_den = agb['ASHOOT']
    # # uu.print_log(agb_den)
    # agb_den = agb_den.rio.set_spatial_dims(x_dim='LON', y_dim='LAT')
    # uu.print_log(agb_den)
    # agb_den.rio.write_crs("epsg:4326", inplace=True)
    # # Produces:
    # # ERROR 1: PROJ: proj_create_from_database: C:\Program Files\GDAL\projlib\proj.db lacks DATABASE.LAYOUT.VERSION.MAJOR / DATABASE.LAYOUT.VERSION.MINOR metadata. It comes from another PROJ installation.
    # # followed by NetCDF properties. But I think this error isn't a problem; the resulting geotif seems fine.
    # agb_den.rio.to_raster(cn.name_rasterized_AGB_Huang_global, compress='DEFLATE')
    # # Produces:
    # # ERROR 1: PROJ: proj_create_from_name: C:\Program Files\GDAL\projlib\proj.db lacks DATABASE.LAYOUT.VERSION.MAJOR / DATABASE.LAYOUT.VERSION.MINOR metadata. It comes from another PROJ installation.
    # # ERROR 1: PROJ: proj_create_from_database: C:\Program Files\GDAL\projlib\proj.db lacks DATABASE.LAYOUT.VERSION.MAJOR / DATABASE.LAYOUT.VERSION.MINOR metadata. It comes from another PROJ installation.
    # # But I think this error isn't a problem; the resulting geotif seems fine.
    #
    # bgb = xr.open_dataset(cn.name_raw_BGB_Huang_global)
    # # uu.print_log(bgb)
    # bgb_den = bgb['AROOT']
    # # uu.print_log(bgb_den)
    # bgb_den = bgb_den.rio.set_spatial_dims(x_dim='LON', y_dim='LAT')
    # uu.print_log(bgb_den)
    # bgb_den.rio.write_crs("epsg:4326", inplace=True)
    # # Produces:
    # # ERROR 1: PROJ: proj_create_from_database: C:\Program Files\GDAL\projlib\proj.db lacks DATABASE.LAYOUT.VERSION.MAJOR / DATABASE.LAYOUT.VERSION.MINOR metadata. It comes from another PROJ installation.
    # # followed by NetCDF properties. But I think this error isn't a problem; the resulting geotif seems fine.
    # bgb_den.rio.to_raster(cn.name_rasterized_BGB_Huang_global, compress='DEFLATE')
    # # Produces:
    # # ERROR 1: PROJ: proj_create_from_name: C:\Program Files\GDAL\projlib\proj.db lacks DATABASE.LAYOUT.VERSION.MAJOR / DATABASE.LAYOUT.VERSION.MINOR metadata. It comes from another PROJ installation.
    # # ERROR 1: PROJ: proj_create_from_database: C:\Program Files\GDAL\projlib\proj.db lacks DATABASE.LAYOUT.VERSION.MAJOR / DATABASE.LAYOUT.VERSION.MINOR metadata. It comes from another PROJ installation.
    # # But I think this error isn't a problem; the resulting geotif seems fine.

    # uu.print_log("Generating global BGB:AGB map...")
    #
    # out = f'--outfile={cn.name_rasterized_BGB_AGB_Huang_global}'
    # calc = '--calc=A/B'
    # datatype = f'--type=Float32'
    #
    # # Divides BGB by AGB to get BGB:AGB (root:shoot ratio)
    # cmd = ['gdal_calc.py', '-A', cn.name_rasterized_BGB_Huang_global, '-B', cn.name_rasterized_AGB_Huang_global,
    #        calc, out, '--NoDataValue=0', '--co', 'COMPRESS=DEFLATE', '--overwrite', datatype, '--quiet']
    # uu.log_subprocess_output_full(cmd)

    # The resulting global BGB:AGB map has many gaps, as Huang et al. didn't map AGB and BGB on all land.
    # Presumably, most of the places without BGB:AGB don't have much forest, but for completeness it seems good to
    # fill the BGB:AGB map gaps, both internally and make sure that continental margins aren't left without BGB:AGB.
    # I used gdal_fillnodata.py to do this (https://gdal.org/programs/gdal_fillnodata.html). I tried different
    # --max_distance parameters, extending it until the interior of the Sahara was covered. Obviously, there's not much
    # carbon flux in the interior of the Sahara but I wanted to have full land coverage, which meant using
    # --max_distance=1400 (pixels). Times for different --max_distance values are below.
    # I didn't experiment with the --smooth_iterations parameter.
    # I confirmed that gdal_fillnodata wasn't changing the original BGB:AGB raster and was just filling the gaps.
    # The pixels it assigned to the gaps looked plausible.

    # # time gdal_fillnodata.py BGB_AGB_ratio_global_from_Huang_2021__20230201.tif BGB_AGB_ratio_global_from_Huang_2021__20230201_extended_10.tif -co COMPRESS=DEFLATE -md 10
    # # real 5m7.600s; 6m17.684s
    # # user 5m7.600s; 5m38.180s
    # # sys  0m5.560s; 0m6.710s
    # #
    # # time gdal_fillnodata.py BGB_AGB_ratio_global_from_Huang_2021__20230201.tif BGB_AGB_ratio_global_from_Huang_2021__20230201_extended_100.tif -co COMPRESS=DEFLATE -md 100
    # # real 7m44.302s
    # # user 7m24.310s
    # # sys  0m4.160s
    # #
    # # time gdal_fillnodata.py BGB_AGB_ratio_global_from_Huang_2021__20230201.tif BGB_AGB_ratio_global_from_Huang_2021__20230201_extended_1000.tif -co COMPRESS=DEFLATE -md 1000
    # # real 51m55.893s
    # # user 51m25.800s
    # # sys  0m6.510s
    # #
    # # time gdal_fillnodata.py BGB_AGB_ratio_global_from_Huang_2021__20230201.tif BGB_AGB_ratio_global_from_Huang_2021__20230201_extended_1200.tif -co COMPRESS=DEFLATE -md 1200
    # # real 74m41.544s
    # # user 74m5.130s
    # # sys  0m7.070s
    # #
    # # time gdal_fillnodata.py BGB_AGB_ratio_global_from_Huang_2021__20230201.tif BGB_AGB_ratio_global_from_Huang_2021__20230201_extended_1400.tif -co COMPRESS=DEFLATE -md 1400
    # # real
    # # user
    # # sys

    # cmd = ['gdal_fillnodata.py',
    #        cn.name_rasterized_BGB_AGB_Huang_global, 'BGB_AGB_ratio_global_from_Huang_2021__20230201_extended_10.tif',
    #        '-co', 'COMPRESS=DEFLATE', '-md', '10']
    # uu.log_subprocess_output_full(cmd)

    # # upload_final_set isn't uploading the global BGB:AGB map for some reason.
    # # It just doesn't show anything in the console and nothing gets uploaded.
    # # But I'm not going to try to debug it since it's not an important part of the workflow.
    # uu.upload_final_set(cn.AGB_BGB_Huang_rasterized_dir, '_global_from_Huang_2021')

    # Creates BGB:AGB tiles
    source_raster = cn.name_rasterized_BGB_AGB_Huang_global_extended
    out_pattern = cn.pattern_BGB_AGB_ratio
    dt = 'Float32'
    if cn.count == 96:
        processes = 75 # 15=95 GB peak; 45=280 GB peak; 75=460 GB peak; 85=XXX GB peak
    else:
        processes = int(cn.count/2)
    uu.print_log(f'Creating BGB:AGB {processes} processors...')
    pool = multiprocessing.Pool(processes)
    pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt), tile_id_list)
    pool.close()
    pool.join()


    for output_pattern in [
        # cn.pattern_annual_gain_AGC_natrl_forest_young, cn.pattern_stdev_annual_gain_AGC_natrl_forest_young,
        cn.pattern_BGB_AGB_ratio
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
            processes = 50  # 60 processors = >730 GB peak (for European natural forest forest removal rates); 50 = 600 GB peak
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
        uu.print_log("\n")


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
    parser.add_argument('--single-processor', '-sp', action='store_true',
                       help='Uses single processing rather than multiprocessing')
    args = parser.parse_args()

    # Sets global variables to the command line arguments
    cn.RUN_DATE = args.run_date
    cn.NO_UPLOAD = args.no_upload
    cn.SINGLE_PROCESSOR = args.single_processor

    tile_id_list = args.tile_id_list

    # Disables upload to s3 if no AWS credentials are found in environment
    if not uu.check_aws_creds():
        cn.NO_UPLOAD = True

    # Create the output log
    uu.initiate_log(tile_id_list)

    # Checks whether the tile_id_list argument is valid
    tile_id_list = uu.tile_id_list_check(tile_id_list)

    mp_prep_other_inputs(tile_id_list=tile_id_list)