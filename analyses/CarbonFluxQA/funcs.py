import os
import arcpy
import pandas as pd
import logging
import re
from subprocess import Popen, PIPE, STDOUT
import constants_and_names as cn

########################################################################################################################
# Functions to run modules
########################################################################################################################
def download_files():
    # Step 1: Checking to see if the AOIS folder exists and if it contains a shapefile
    print("Step 1.1: Checking to see if AOIS folder exists and contains a shapefile")
    check_aois(cn.aois_folder)

    # Step 2: Create Input folder and subfolder for each tile in tile_list
    print("Step 1.2: Creating Input folder structure")
    create_tile_folders(cn.tile_list, cn.input_folder)

    # Step 3: Create Mask folder with Inputs subfolder (gain, mangrove, pre-200 plantations, and tree cover density) and
    # Mask subfolder (folder for each tile in tile_list)
    print("Step 1.3: Creating Mask folder structure")
    create_subfolders([cn.mask_input_folder, cn.gain_folder, cn.mangrove_folder, cn.plantations_folder, cn.tcd_folder, cn.whrc_folder])
    create_tile_folders(cn.tile_list, cn.mask_output_folder)

    # Step 4: Create Output folder with Annual folder, CSV folder, and subfolder for each tile in tile_list
    print("Step 1.4: Creating Output folder structure")
    create_subfolders([cn.csv_folder, cn.annual_folder])
    create_tile_folders(cn.tile_list, cn.outputs_folder)

    # Step 5: Create TCL folder structure
    print("Step 1.5: Creating TCL folder structure")
    create_subfolders([cn.tcl_input_folder, cn.tcl_clip_folder])

    # Step 6: Download emission/removal/netflux tiles (3 - 6 per tile) to Input folder
    print("Step 1.6: Downloading files for Input folder")
    if cn.extent == 'full' or cn.extent == 'both':
        s3_flexible_download(cn.tile_list, cn.gross_emis_full_extent_s3_path, cn.gross_emis_full_extent_s3_pattern, cn.input_folder)
        s3_flexible_download(cn.tile_list, cn.gross_removals_full_extent_s3_path, cn.gross_removals_full_extent_s3_pattern, cn.input_folder)
        s3_flexible_download(cn.tile_list, cn.netflux_full_extent_s3_path, cn.netflux_full_extent_s3_pattern, cn.input_folder)
    if cn.extent == 'forest' or cn.extent == 'both':
        s3_flexible_download(cn.tile_list, cn.gross_emis_forest_extent_s3_path, cn.gross_emis_forest_extent_s3_pattern, cn.input_folder)
        s3_flexible_download(cn.tile_list, cn.gross_removals_forest_extent_s3_path, cn.gross_removals_forest_extent_s3_pattern, cn.input_folder)
        s3_flexible_download(cn.tile_list, cn.netflux_forest_extent_s3_path, cn.netflux_forest_extent_s3_pattern, cn.input_folder)

    # Step 7: Download Gain, Mangrove, Pre_2000_Plantations, TCD, and WHRC subfolders for each tile to Mask, Inputs subfolders
    print("Step 1.7: Downloading files for Mask/Inputs folder")
    s3_flexible_download(cn.tile_list, cn.gain_s3_path, cn.gain_s3_pattern, cn.gain_folder, cn.gain_local_pattern)
    s3_flexible_download(cn.tile_list, cn.mangrove_s3_path, cn.mangrove_s3_pattern, cn.mangrove_folder)
    s3_flexible_download(cn.tile_list, cn.plantation_s3_path, cn.plantation_s3_pattern, cn.plantations_folder)
    s3_flexible_download(cn.tile_list, cn.tcd_s3_path, cn.tcd_s3_pattern, cn.tcd_folder)
    s3_flexible_download(cn.tile_list, cn.whrc_s3_path, cn.whrc_s3_pattern, cn.whrc_folder)

    # Step 8: Download TCL tiles to TCL, Inputs folder
    print("Step 1.8: Downloading files for TCL/Inputs folder")
    s3_flexible_download(cn.tile_list, cn.loss_s3_path, cn.loss_s3_pattern, cn.tcl_input_folder)

def create_masks(tcd_threshold, gain, save_intermediates):
    # Get a list of tcd tiles in the tcd folder
    tcd_list = pathjoin_files_in_directory(cn.tcd_folder, '.tif')
    for tcd in tcd_list:
        tile_id = get_tile_id(get_raster_name(tcd))
        mask_tiles = os.path.join(cn.mask_output_folder, tile_id)
        process_raster(tile_id, tcd, mask_tiles, tcd_threshold, gain, save_intermediates)

def zonal_stats_masked(aois_folder, input_folder, mask_outputs_folder, outputs_folder):
    aoi_list = pathjoin_files_in_directory(aois_folder, '.shp')
    for aoi in aoi_list:
        aoi_name = get_raster_name(aoi)
        print(f"Now processing {aoi_name}:")
        tile_id = get_tile_id_from_country(get_country_id(aoi_name))
        raster_folder = os.path.join(input_folder, tile_id)
        raster_list = pathjoin_files_in_directory(raster_folder, '.tif')
        mask_tiles = os.path.join(mask_outputs_folder, tile_id)
        mask_list = pathjoin_files_in_directory(mask_tiles, '.tif')
        output_folder = os.path.join(outputs_folder, tile_id)
        process_zonal_statistics(aoi, aoi_name, raster_list, mask_list, output_folder, "GID_0")

def zonal_stats_annualized(tcl_clip_folder, input_folder, mask_outputs_folder, annual_folder):
    tcl_list = pathjoin_files_in_directory(tcl_clip_folder, '.tif')
    if len(tcl_list) < 1:
        print("Clipping TCL tiles to GADM boundaries")
        clip_tcl_to_gadm(cn.tcl_input_folder, cn.tcl_clip_folder)
        tcl_list = pathjoin_files_in_directory(tcl_clip_folder, '.tif')
    else:
        print(f"Found {len(tcl_list)} clipped TCL rasters.")

    for tcl in tcl_list:
        tcl_name = get_raster_name(tcl)
        tcl_raster = arcpy.Raster(tcl)
        tcl_raster = arcpy.sa.SetNull(tcl_raster == 0, tcl_raster)
        print(f"Now processing {tcl_name}.tif:")
        tile_id = get_tile_id(tcl)
        raster_folder = os.path.join(input_folder, tile_id)
        raster_list = [os.path.join(raster_folder, f) for f in os.listdir(raster_folder) if "emis" in f and f.endswith('tif')]
        mask_tiles = os.path.join(mask_outputs_folder, tile_id)
        mask_list = pathjoin_files_in_directory(mask_tiles, '.tif')
        process_zonal_statistics(tcl_raster, tcl_name, raster_list, mask_list, annual_folder, "Value")

def zonal_stats_clean():
    # Initialize an empty data frame to store the data
    df = pd.DataFrame()

    # Create a list of all masked zonal stats output folders (one for each tile_id)
    masked_input_folders = []
    for tile in cn.tile_list:
        masked_input_folders.append(os.path.join(cn.outputs_folder, tile))

    # Combine masked zonal stats output csvs
    masked_output = clean_zonal_stats_csv(masked_input_folders, df)

    # Clean masked output dataframe
    masked_output.drop(['ZONE_CODE'], axis=1, inplace=True)
    masked_output.rename(columns={"GID_0": "Country", "SUM": "Sum"}, inplace = True)

    # Create a list of all annual zonal stats output folders
    annual_input_folders = [cn.annual_folder]

    # Combine annual zonal stats outputs csvs
    annual_output = clean_zonal_stats_csv(annual_input_folders, df)
    annual_output.reset_index(inplace=True)

    # Clean and pivot annual output stats
    annual_output["VALUE"] = annual_output["VALUE"] + 2000
    annual_output = annual_output.pivot(columns="VALUE", values="SUM", index="File")
    annual_output.reset_index(inplace=True)

    # Join annual output zonal stats to masked zonal stats
    annual_output["File"] = annual_output["File"].apply(lambda x: x.removeprefix("TCL_annualized_"))
    final_output = masked_output.set_index('File').join(annual_output.set_index('File'), on = "File")
    final_output.reset_index(inplace=True)

    # Make sure emission sums match
    final_output["Annual_Sum"] =final_output.loc[:,2001:].sum(axis=1, min_count=1)
    final_output["Match"] = final_output["Sum"].round() == final_output["Annual_Sum"].round()

    # Define the output location
    output_path = os.path.join(cn.csv_folder, "final_output.csv")

    # Export the data frame as a CSV file
    final_output.to_csv(output_path, index=False)

#######################################################################################################################
# Utility functions
#######################################################################################################################
def check_aois(aois_folder):
    # Checking to see if the AOIS folder exists
    if os.path.isdir(aois_folder):
        print(f"    Success: {aois_folder} exists.")
        # Checking to see if the AOIS folder has any shapefiles
        aoi_list = pathjoin_files_in_directory(aois_folder, ".shp")
        if len(aoi_list) >= 1:
            print(f"    Success: {aois_folder} contains {len(aoi_list)} shapefiles.")
        else:
            raise Exception(f"  Failure: {aois_folder} does not contain a shapefile.")
    else:
        raise Exception(f"  Failure: {aois_folder} does not exist.")

def folder_check(folder):
    if os.path.isdir(folder):
        print(f"    Option 1 success: {folder} exists.")
    else:
        os.makedirs(folder)
        if os.path.isdir(folder):
            print(f"    Option 2 success: {folder} successfully created.")
        else:
            raise Exception(f"  Option 2 failure: {folder} could not be created.")

def create_tile_folders(tile_list, input_folder):
    for tile in tile_list:
        tile_id_folder = os.path.join(input_folder, tile)
        folder_check(tile_id_folder)

def create_subfolders(folder_list):
    for subfolder in folder_list:
        folder_check(subfolder)

def list_files_in_directory(directory, file_extension):
    return [f for f in os.listdir(directory) if f.endswith(file_extension)]

def pathjoin_files_in_directory(directory, file_extension):
    return [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(file_extension)]

def get_tile_id(tile_name):
    tile_id = re.search("[0-9]{2}[A-Z][_][0-9]{3}[A-Z]", tile_name).group()
    return tile_id

def get_country_id(tile_name):
    country = re.search("[_][A-Z]{3}[_]", tile_name).group()
    country_id = country.split("_")[1]
    return country_id

def get_country_id_from_tile_id(tile_id):
    for key, value in cn.tile_dictionary.items():
        if value == tile_id:
            return key

def get_tile_id_from_country(country):
    for key, value in cn.tile_dictionary.items():
        if key == country:
            return value

def get_raster_name(raster):
    return os.path.splitext(os.path.basename(raster))[0]

def get_gadm_boundary(country):
    for f in pathjoin_files_in_directory(cn.aois_folder, ".shp"):
        if country in f:
            return f

def clip_to_gadm(country, input_raster, output_raster):
    clip_feature = get_gadm_boundary(country)
    no_data_value = arcpy.Raster(input_raster).noDataValue
    print(f'    Saving {output_raster}')
    clipped_raster = arcpy.management.Clip(input_raster, "#", output_raster, clip_feature, no_data_value, "ClippingGeometry", "MAINTAIN_EXTENT")
    print(f'    Successfully finished')

def clip_tcl_to_gadm(input_folder, output_folder):
    print(f'    Option 1: Checking if clipped TCL tiles already exist...')
    tcl_list = list_files_in_directory(input_folder, ".tif")
    if len(tcl_list) >= 1:
        for raster in tcl_list:
            raster_name = get_raster_name(raster)
            tile_id = get_tile_id(raster)
            country = get_country_id_from_tile_id(tile_id)

            input_raster = os.path.join(input_folder, raster)
            output_raster = os.path.join(output_folder, f'{raster_name}_{country}_clip.tif')

            if os.path.exists(output_raster):
                print(f"    Option 1 success: Tile {output_raster} already exists.")
            else:
                print(f'    Option 1 failure: Tile {output_raster} does not already exists."')
                print(f'    Option 2: Clipping TCL tile to GADM boundary')
                clip_to_gadm(country, input_raster, output_raster)

                if os.path.exists(output_raster):
                    print(f'    Option 2 success: Tile {output_raster} successfully created')
                else:
                    print(f'    Option 2 failure: Tile {output_raster} was not successfully created')
    else:
        print(f'    Option 1 failure: {input_folder} does not contain any TCL tiles. Make sure TCL tiles have been downloaded.')


def or_mask_logic(raster1, raster2, raster1_value=None, raster2_value=None):
    if raster1_value:
        raster1_mask = arcpy.sa.Con(arcpy.Raster(raster1) > raster1_value, 1, 0)
    else:
        raster1_mask = raster1
    if raster2_value:
        raster2_mask = arcpy.sa.Con(arcpy.Raster(raster2) > raster2_value, 1, 0)
    else:
        raster2_mask = raster2
    r1_and_r2_mask = arcpy.ia.Merge([raster2_mask, raster1_mask], "SUM")
    output_mask = arcpy.sa.Con(arcpy.Raster(r1_and_r2_mask) > 0, 1, 0)
    return output_mask


def and_mask_logic(raster1, raster2, raster1_value=None, raster2_value=None):
    if raster1_value:
        raster1_mask = arcpy.sa.Con(arcpy.Raster(raster1) > raster1_value, 1, 0)
    else:
        raster1_mask = raster1
    if raster2_value:
        raster2_mask = arcpy.sa.Con(arcpy.Raster(raster2) > raster2_value, 1, 0)
    else:
        raster2_mask = raster2
    r1_and_r2_mask = arcpy.sa.Times(raster1_mask, raster2_mask)
    output_mask = arcpy.sa.Con(arcpy.Raster(r1_and_r2_mask) > 0, 1, 0)
    return output_mask

def process_raster(tile_id, tcd, mask_tiles, tcd_threshold, gain, save_intermediates):
    #Paths to Mask, Input files
    gain_raster_path = os.path.join(cn.gain_folder, f'{tile_id}_{cn.gain_local_pattern}.tif')
    whrc_raster_path = os.path.join(cn.whrc_folder, f'{tile_id}_{cn.whrc_s3_pattern}.tif')
    mangrove_raster_path = os.path.join(cn.mangrove_folder, f'{tile_id}_{cn.mangrove_s3_pattern}.tif')
    plantation_raster_path = os.path.join(cn.plantations_folder, f'{tile_id}_{cn.plantation_s3_pattern}.tif')
    print(f'Creating masks for {tile_id}: ')

    for tcd_val in tcd_threshold:
        #Read in the plantation raster and mask before saving each intermediate
        if os.path.exists(plantation_raster_path):
            plantation_raster = arcpy.sa.IsNull(arcpy.Raster(plantation_raster_path))

        # Conditional logic for where TCD AND biomass
        tcd_whrc_mask = and_mask_logic(tcd, whrc_raster_path, tcd_val, 0)
        mask_path_tcd = os.path.join(mask_tiles, f'{tile_id}_tcd{tcd_val}')

        if save_intermediates == True:
            # Conditional logic for TCD AND biomass NOT Pre-2000 Plantation
            if os.path.exists(plantation_raster_path):
                tcd_noplantation_mask = and_mask_logic(tcd_whrc_mask, plantation_raster)
                mask_path_tcd_noplantation = f'{mask_path_tcd}_notPlantation'

                # Saving the tcd_noplantation mask
                print(f'    Saving {mask_path_tcd_noplantation}.tif')
                tcd_noplantation_mask = arcpy.sa.SetNull(tcd_noplantation_mask == 0, tcd_noplantation_mask)
                tcd_noplantation_mask.save(f'{mask_path_tcd_noplantation}.tif')
                print(f'    Successfully finished')

            else:
                # Saving the tcd mask
                print(f'    Saving {mask_path_tcd}.tif')
                tcd_whrc_mask = arcpy.sa.SetNull(tcd_whrc_mask == 0, tcd_whrc_mask)
                tcd_whrc_mask.save(f'{mask_path_tcd}.tif')
                print(f'    Successfully finished')

        if gain == True:
            # Conditional logic for TCD AND biomass OR gain
            tcd_gain_mask = or_mask_logic(gain_raster_path, tcd_whrc_mask, 0)
            mask_path_tcd_gain = f'{mask_path_tcd}_gain'

            if save_intermediates == True:
                # Conditional logic for TCD AND biomass OR gain NOT Pre-2000 Plantation
                if os.path.exists(plantation_raster_path):
                    tcd_gain_noplantation_mask = and_mask_logic(tcd_gain_mask, plantation_raster)
                    mask_path_tcd_gain_noplantation = f'{mask_path_tcd_gain}_notPlantation'

                    # Saving the tcd_gain_noplantation mask
                    print(f'    Saving {mask_path_tcd_gain_noplantation}.tif')
                    tcd_gain_noplantation_mask = arcpy.sa.SetNull(tcd_gain_noplantation_mask == 0, tcd_gain_noplantation_mask)
                    tcd_gain_noplantation_mask.save(f'{mask_path_tcd_gain_noplantation}.tif')
                    print(f'    Successfully finished')

                else:
                    # Saving the tcd_gain mask
                    print(f'    Saving {mask_path_tcd_gain}.tif')
                    tcd_gain_mask = arcpy.sa.SetNull(tcd_gain_mask == 0, tcd_gain_mask)
                    tcd_gain_mask.save(f'{mask_path_tcd_gain}.tif')
                    print(f'    Successfully finished')

        else:
            mask_path_tcd_gain = mask_path_tcd
            tcd_gain_mask = tcd_whrc_mask

        if os.path.exists(mangrove_raster_path):
            # Conditional logic for TCD AND biomass OR gain OR mangrove
            mangrove_raster = arcpy.sa.Con(arcpy.Raster(mangrove_raster_path) > 0, 1, 0)
            tcd_gain_mangrove_raster = arcpy.ia.Merge([tcd_gain_mask, mangrove_raster], "SUM")
            tcd_gain_mangrove_mask = arcpy.sa.Con(arcpy.Raster(tcd_gain_mangrove_raster) > 0, 1, 0)
            mask_path_tcd_gain_mangrove = f'{mask_path_tcd_gain}_mangrove'

            # Conditional logic for TCD AND biomass OR gain OR mangrove NOT Pre-2000 Plantation
            if os.path.exists(plantation_raster_path):
                tcd_gain_mangrove_noplantation_raster = arcpy.sa.Times(tcd_gain_mangrove_mask, plantation_raster)
                tcd_gain_mangrove_noplantation_mask = arcpy.sa.Con(arcpy.Raster(tcd_gain_mangrove_noplantation_raster) > 0, 1, 0)
                mask_path_tcd_gain_mangrove_noplantation = f'{mask_path_tcd_gain_mangrove}_notPlantation'

                # Saving the tcd_gain_mangrove_noplantation mask
                print(f'    Saving {mask_path_tcd_gain_mangrove_noplantation}.tif')
                tcd_gain_mangrove_noplantation_mask = arcpy.sa.SetNull(tcd_gain_mangrove_noplantation_mask == 0, tcd_gain_mangrove_noplantation_mask)
                tcd_gain_mangrove_noplantation_mask.save(f'{mask_path_tcd_gain_mangrove_noplantation}.tif')
                print(f'    Successfully finished')

            else:
                # Saving tcd_gain_mangrove mask
                print(f'Saving {mask_path_tcd_gain_mangrove}.tif')
                tcd_gain_mangrove_mask = arcpy.sa.SetNull(tcd_gain_mangrove_mask == 0, tcd_gain_mangrove_mask)
                tcd_gain_mangrove_mask.save(f'{mask_path_tcd_gain_mangrove}.tif')
                print(f'    Successfully finished')

def process_zonal_statistics(aoi, aoi_name, raster_list, mask_list, output_folder, field):
    for raster in raster_list:
        raster_name = get_raster_name(raster)
        raster_obj = arcpy.Raster(raster)
        print(f'Calculating zonal statistics for {raster_name}.tif')

        for mask in mask_list:
            mask_path = get_raster_name(mask)
            mask_name = mask_path.split("_", 2)[2]
            mask_obj = arcpy.Raster(mask)

            # Check if spatial references are the same
            if (raster_obj.spatialReference.name == mask_obj.spatialReference.name):
                print(f'    Masking {raster_name}.tif with {mask_name}.tif')
                if field == "GID_0":
                    output_name = "{}_{}.dbf".format(get_country_id(aoi_name), str(raster_name) + "_" + str(mask_name))
                    csv_file = "{}_{}.csv".format(get_country_id(aoi_name), str(raster_name) + "_" + str(mask_name))
                elif field == "Value":
                    output_name = "{}_{}.dbf".format("TCL_annualized" + "_" +  str(get_country_id(aoi_name)), str(raster_name) + "_" + str(mask_name))
                    csv_file = "{}_{}.csv".format("TCL_annualized" + "_" +  str(get_country_id(aoi_name)), str(raster_name) + "_" + str(mask_name))
                output_path = os.path.join(output_folder, output_name)

                masked_raster = arcpy.sa.Times(raster_obj, mask_obj)
                arcpy.gp.ZonalStatisticsAsTable_sa(aoi, field, masked_raster, output_path, "DATA", "SUM")
                arcpy.TableToTable_conversion(output_path, output_folder, csv_file)
                print(f'    Successfully finished')

            else:
                print(f"Spatial references or extents do not match for {raster} and {mask_name}")

def clean_zonal_stats_csv(input_folders, df):
    for folder in input_folders:
        # Loop through the files in each folder
        for file in os.listdir(folder):
            if file.endswith(".csv"):
                # Load the csv file into a pandas data frame
                csv_df = pd.read_csv(os.path.join(folder, file))

                # Add column with name of the file
                csv_df["File"] = file

                # Define type of calc
                if "emis" in file:
                    type = "gross emissions"
                elif "removals" in file:
                    type = "gross removals"
                else:
                    type = "net flux"
                csv_df["Type"] = type

                # Define extent of calc
                if "forest_extent" in file:
                    extent = "forest extent"
                else:
                    extent = "full extent"
                csv_df["Extent"] = extent

                # Define tcd threshold
                tcd = re.match(r'.*tcd([0-9]+).*', file)
                if tcd is not None:
                    csv_df["Density"] = tcd.group(1)
                else:
                    csv_df["Density"] = 'NA'

                # Define mask of calc
                if "mangrove" in file:
                    mask = "tcd, gain, mangrove"
                elif "gain" in file:
                    mask = "tcd, gain"
                elif "tcd" in file:
                    mask = "tcd"
                else:
                    mask = "no mask"
                if "notPlantation" in file:
                    mask = f'{mask}, NOT plantation'
                csv_df["Mask"] = mask

                # Drop all other fields
                assert isinstance(csv_df, object)
                csv_df.drop(['OID_', 'COUNT', 'AREA'], axis=1, inplace=True)

                # Append the data to the main data frame
                df = pd.concat([df, csv_df], axis=0)

    return(df)

#######################################################################################################################
# AWS S3 file download utilities
#######################################################################################################################
def s3_flexible_download(tile_id_list, s3_dir, s3_pattern, local_dir, local_pattern = ''):

    # Creates a full download name (path and file)
    for tile_id in tile_id_list:
        if s3_pattern in [cn.tcd_s3_pattern, cn.loss_s3_pattern]:
            source = f'{s3_dir}{s3_pattern}_{tile_id}.tif'
        elif s3_pattern in [cn.gain_s3_pattern]:
            source = f'{s3_dir}{tile_id}.tif'
        else:  # For every other type of tile
            source = f'{s3_dir}{tile_id}_{s3_pattern}.tif'

        if s3_pattern in [cn.gross_emis_forest_extent_s3_pattern, cn.gross_emis_full_extent_s3_pattern, cn.gross_removals_forest_extent_s3_pattern, cn.gross_removals_full_extent_s3_pattern, cn.netflux_forest_extent_s3_pattern, cn.netflux_full_extent_s3_pattern]:
            dir = os.path.join(local_dir, tile_id)
        else:
            dir = local_dir

        s3_file_download(source, dir, local_pattern)

def s3_file_download(source, dest, pattern=''):
    # Retrieves the s3 directory and name of the tile from the full path name
    dir = get_tile_dir(source)
    file_name = get_tile_name(source)

    try:
        tile_id = get_tile_id(file_name)
    except:
        pass

    # Special download procedures for tree cover gain because the tiles have no pattern, just an ID.
    # Tree cover gain tiles are renamed as their downloaded to get a pattern added to them.
    if dir == cn.gain_s3_path[:-1]: # Delete last character of gain_dir because it has the terminal / while dir does not have terminal /
        local_file_name = f'{tile_id}_{pattern}.tif'
        print(f'    Option 1: Checking if {local_file_name} is already downloaded...')
        if os.path.exists(os.path.join(dest, local_file_name)):
            print(f'    Option 1 success: {os.path.join(dest, local_file_name)} already downloaded', "\n")
            return
        else:
            print(f'    Option 1 failure: {local_file_name} is not already downloaded.')
            print(f'    Option 2: Checking for tile {source} on s3...')

            # If the tile isn't already downloaded, download is attempted
            # source = os.path.join(dir, file_name)
            source = f'{dir}/{file_name}'
            local_folder = os.path.join(dest, local_file_name)

            # cmd = ['aws', 's3', 'cp', source, dest, '--no-sign-request', '--only-show-errors']
            cmd = ['aws', 's3', 'cp', source, local_folder,
                   '--request-payer', 'requester', '--only-show-errors']
            log_subprocess_output_full(cmd)

            if os.path.exists(os.path.join(dest, local_file_name)):
                print_log(f'    Option 2 success: Tile {source} found on s3 and downloaded', "\n")
                return
            else:
                print_log(
                    f'  Option 2 failure: Tile {source} not found on s3. Tile not found but it seems it should be. Check file paths and names.', "\n")

    # All other tiles besides tree cover gain
    else:
        print_log(f'    Option 1: Checking if {file_name} is already downloaded...')
        if os.path.exists(os.path.join(dest, file_name)):
            print_log(f'    Option 1 success: {os.path.join(dest, file_name)} already downloaded', "\n")
            return
        else:
            print_log(f'    Option 1 failure: {file_name} is not already downloaded.')
            print_log(f'    Option 2: Checking for tile {source} on s3...')

            # If the tile isn't already downloaded, download is attempted
            #source = os.path.join(dir, file_name)
            source = f'{dir}/{file_name}'

            # cmd = ['aws', 's3', 'cp', source, dest, '--no-sign-request', '--only-show-errors']
            cmd = ['aws', 's3', 'cp', source, dest, '--only-show-errors']
            log_subprocess_output_full(cmd)
            if os.path.exists(os.path.join(dest, file_name)):
                print_log(f'    Option 2 success: Tile {source} found on s3 and downloaded', "\n")
                return
            else:
                print_log(f'    Option 2 failure: Tile {source} not found on s3. Tile not found but it seems it should be. Check file paths and names.', "\n")

# Gets the directory of the tile
def get_tile_dir(tile):
    tile_dir = os.path.split(tile)[0]
    return tile_dir

def get_tile_name(tile):
    tile_name = os.path.split(tile)[1]
    return tile_name

def log_subprocess_output_full(cmd):
    # Solution for adding subprocess output to log is from https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging
    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    pipe = process.stdout
    with pipe:
        # Reads all the output into a string
        for full_out in iter(pipe.readline, b''):  # b"\n"-separated lines
            # Separates the string into an array, where each entry is one line of output
            line_array = full_out.splitlines()
            # For reasons I don't know, the array is backwards, so this prints it out in reverse (i.e. correct) order
            for line in reversed(line_array):
                logging.info(line.decode(
                    "utf-8"))  # https://stackoverflow.com/questions/37016946/remove-b-character-do-in-front-of-a-string-literal-in-python-3, answer by krock
                print(line.decode(
                    "utf-8"))  # https://stackoverflow.com/questions/37016946/remove-b-character-do-in-front-of-a-string-literal-in-python-3, answer by krock

def print_log(*args):
    # Empty string
    full_statement = str(object='')
    # Concatenates all individuals strings to the complete line to print
    for arg in args:
        full_statement = full_statement + str(arg) + " "
    logging.info(full_statement)
    # Prints to console
    print("LOG: " + full_statement)

