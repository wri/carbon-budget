'''
This script processes the inputs for the emissions script that haven't been processed by another script.
At this point, that is: climate zone, Indonesia/Malaysia plantations before 2000, tree cover loss drivers (TSC drivers),
combining IFL2000 (extratropics) and primary forests (tropics) into a single layer,
Hansenizing some removal factor standard deviation inputs, Hansenizing the European removal factors,
and Hansenizing three US-specific removal factor inputs.

python -m data_prep.mp_prep_other_inputs_annual -l 00N_000E -nu -p tcl
python -m data_prep.mp_prep_other_inputs_annual -l all -p tclf

Options for process argument (-p):
1) tcld: Creates tree cover loss driver tiles
2) tclf: Creates tree cover loss due to fires tiles
'''

import argparse
import multiprocessing
import datetime
import glob
from functools import partial
import sys
import os

import constants_and_names as cn
import universal_util as uu

def mp_prep_other_inputs(tile_id_list, process):

    os.chdir(cn.docker_tile_dir)
    sensit_type='std'

    # If a full model run is specified, the correct set of tiles for the particular script is listed
    if tile_id_list == 'all':
        # List of tiles to run in the model
        tile_id_list = uu.create_combined_tile_list(
            [cn.WHRC_biomass_2000_unmasked_dir, cn.mangrove_biomass_2000_dir, cn.gain_dir, cn.tcd_dir,
             cn.annual_gain_AGC_BGC_planted_forest_dir]
        )

    uu.print_log(tile_id_list)
    uu.print_log(f'There are {str(len(tile_id_list))} tiles to process', "\n")

    '''
    Before processing the driver, it needs to be reprojected from Goode Homolosine to WGS84. 
    gdal_warp is producing a weird output, so I did it in ArcMap for the 2022 update, 
    with the output cell size being 0.005 x 0.005 degree and the method being nearest.
    
    arcpy.management.ProjectRaster("TCL_DD_2022_20230407.tif", r"C:\GIS\raw_data\TCL_DD_2022_20230407_wgs84.tif", 
    'GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],
    UNIT["Degree",0.0174532925199433]]', "NEAREST", "0.005 0.005", None, None, 'PROJCS["WGS_1984_Goode_Homolosine",
    GEOGCS["GCS_unknown",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],
    UNIT["Degree",0.0174532925199433]],PROJECTION["Goode_Homolosine"],PARAMETER["False_Easting",0.0],
    PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",0.0],PARAMETER["Option",1.0],UNIT["Meter",1.0]]', "NO_VERTICAL")

    
    The 2022 drivers had 0 instead of NoData, so I used Copy Raster to turn the 0 into NoData:
    arcpy.management.CopyRaster("TCL_DD_2022_20230407_wgs84.tif", 
    r"C:\GIS\raw_data\TCL_DD_2022_20230407_wgs84_setnodata.tif", '', None, "0", "NONE", "NONE", '', "NONE", "NONE", "TIFF", "NONE", 
    "CURRENT_SLICE", "NO_TRANSPOSE")
    
    '''
    if process == 'tcl' or process == 'all':

        # List of output directories and output file name patterns
        output_dir_list = [
                           cn.drivers_processed_dir
        ]
        output_pattern_list = [
                               cn.pattern_drivers
        ]

        # If the model run isn't the standard one, the output directory and file names are changed
        if sensit_type != 'std':
            uu.print_log('Changing output directory and file name pattern based on sensitivity analysis')
            output_dir_list = uu.alter_dirs(sensit_type, output_dir_list)
            output_pattern_list = uu.alter_patterns(sensit_type, output_pattern_list)

        # A date can optionally be provided by the full model script or a run of this script.
        # This replaces the date in constants_and_names.
        # Only done if output upload is enabled.
        if cn.RUN_DATE is not None and cn.NO_UPLOAD is False:
            output_dir_list = uu.replace_output_dir_date(output_dir_list, cn.RUN_DATE)

        ### Drivers of tree cover loss processing
        uu.print_log("STEP 1: Preprocess drivers of tree cover loss")

        uu.s3_file_download(os.path.join(cn.drivers_raw_dir, cn.pattern_drivers_raw), cn.docker_tile_dir, sensit_type)

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
        pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt),
                 tile_id_list)
        pool.close()
        pool.join()

        for output_pattern in [
            cn.pattern_drivers
        ]:

            if cn.count == 96:
                processes = 50  # 60 processors = >730 GB peak (for European natural forest forest removal rates); 50 = XXX GB peak
                uu.print_log(f'Checking for empty tiles of {output_pattern} pattern with {processes} processors...')
                pool = multiprocessing.Pool(processes)
                pool.map(partial(uu.check_and_delete_if_empty, output_pattern=output_pattern), tile_id_list)
                pool.close()
                pool.join()
            elif cn.count <= 2: # For local tests
                processes = 1
                uu.print_log(f'Checking for empty tiles of {output_pattern} pattern with {processes} processors using light function...')
                pool = multiprocessing.Pool(processes)
                pool.map(partial(uu.check_and_delete_if_empty_light, output_pattern=output_pattern), tile_id_list)
                pool.close()
                pool.join()
            else:
                processes = int(cn.count / 2)
                uu.print_log(f'Checking for empty tiles of {output_pattern} pattern with {processes} processors...')
                pool = multiprocessing.Pool(processes)
                pool.map(partial(uu.check_and_delete_if_empty, output_pattern=output_pattern), tile_id_list)
                pool.close()
                pool.join()
            uu.print_log("\n")

        # Uploads output tiles to s3
        if cn.NO_UPLOAD == False:
            for i in range(0, len(output_dir_list)):
                uu.upload_final_set(output_dir_list[i], output_pattern_list[i])

    if process == 'tclf' or process == 'all':
        # List of output directories and output file name patterns
        output_dir_list = [
                            cn.TCLF_processed_dir
        ]

        output_pattern_list = [
                                cn.pattern_TCLF_processed
        ]

        # If the model run isn't the standard one, the output directory and file names are changed
        if sensit_type != 'std':
            uu.print_log('Changing output directory and file name pattern based on sensitivity analysis')
            output_dir_list = uu.alter_dirs(sensit_type, output_dir_list)
            output_pattern_list = uu.alter_patterns(sensit_type, output_pattern_list)

        # A date can optionally be provided by the full model script or a run of this script.
        # This replaces the date in constants_and_names.
        # Only done if output upload is enabled.
        if cn.RUN_DATE is not None and cn.NO_UPLOAD is False:
            output_dir_list = uu.replace_output_dir_date(output_dir_list, cn.RUN_DATE)

        ### Tree cover loss from fires processing
        uu.print_log("STEP 2: Preprocess tree cover loss from fires")

        # TCLF is downloaded to its own folder because it doesn't have a standardized file name pattern.
        # This way, the entire contents of the TCLF folder can be worked on without mixing with other files.
        TCLF_s3_dir = os.path.join(cn.docker_tile_dir, 'TCLF')
        if os.path.exists(TCLF_s3_dir):
            os.rmdir(TCLF_s3_dir)
        os.mkdir(TCLF_s3_dir)
        cmd = ['aws', 's3', 'cp', cn.TCLF_raw_dir, TCLF_s3_dir, '--request-payer', 'requester',
               '--include', '*', '--exclude', 'tiles*', '--exclude', '*geojason', '--exclude', '*Store', '--recursive']
        uu.log_subprocess_output_full(cmd)

        # Creates global vrt of TCLF
        uu.print_log("Creating vrt of TCLF...")
        tclf_vrt = 'TCLF.vrt'
        os.system(f'gdalbuildvrt -srcnodata 0 {tclf_vrt} {TCLF_s3_dir}/*.tif')
        uu.print_log("  TCLF vrt created")

        # Creates TCLF tiles
        source_raster = tclf_vrt
        out_pattern = cn.pattern_TCLF_processed
        dt = 'Byte'
        if cn.count == 96:
            processes = 34  # 30 = 510 GB initial peak; 34=600 GB peak
        else:
            processes = int(cn.count/2)
        uu.print_log(f'Creating TCLF tiles with {processes} processors...')
        pool = multiprocessing.Pool(processes)
        pool.map(partial(uu.mp_warp_to_Hansen, source_raster=source_raster, out_pattern=out_pattern, dt=dt), tile_id_list)
        pool.close()
        pool.join()

        for output_pattern in [
            cn.pattern_TCLF_processed
        ]:

            if cn.count == 96:
                processes = 50  # 60 processors = >730 GB peak (for European natural forest forest removal rates); 50 = XXX GB peak
                uu.print_log(f'Checking for empty tiles of {output_pattern} pattern with {processes} processors...')
                pool = multiprocessing.Pool(processes)
                pool.map(partial(uu.check_and_delete_if_empty, output_pattern=output_pattern), tile_id_list)
                pool.close()
                pool.join()
            elif cn.count <= 2: # For local tests
                processes = 1
                uu.print_log(f'Checking for empty tiles of {output_pattern} pattern with {processes} processors using light function...')
                pool = multiprocessing.Pool(processes)
                pool.map(partial(uu.check_and_delete_if_empty_light, output_pattern=output_pattern), tile_id_list)
                pool.close()
                pool.join()
            else:
                processes = int(cn.count / 2)
                uu.print_log(f'Checking for empty tiles of {output_pattern} pattern with {processes} processors...')
                pool = multiprocessing.Pool(processes)
                pool.map(partial(uu.check_and_delete_if_empty, output_pattern=output_pattern), tile_id_list)
                pool.close()
                pool.join()
            uu.print_log("\n")

        # Uploads output tiles to s3
        if cn.NO_UPLOAD == False:
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
    parser.add_argument('--process', '-p', required=True,
                        help='Specifies which annual process to run: 1 = Pre-process drivers of tree cover loss')
    args = parser.parse_args()

    # Sets global variables to the command line arguments
    tile_id_list = args.tile_id_list
    run_date = args.run_date
    cn.NO_UPLOAD = args.no_upload
    process = args.process

    # Disables upload to s3 if no AWS credentials are found in environment
    if not uu.check_aws_creds():
        cn.NO_UPLOAD = True

    # Create the output log
    uu.initiate_log(tile_id_list)

    # Checks whether the tile_id_list argument is valid
    tile_id_list = uu.tile_id_list_check(tile_id_list)

    mp_prep_other_inputs(tile_id_list=tile_id_list, process=str(process))