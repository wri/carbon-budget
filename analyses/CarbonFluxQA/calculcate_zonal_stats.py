import constants_and_names as cn
from funcs import download_files, create_masks, zonal_stats_masked, zonal_stats_annualized, zonal_stats_clean

#Execute Download File...
print("Step 1: Downloading Files... \n")
download_files()

#Execute Create Masks...
print("Step 2: Creating Masks... \n")
create_masks(cn.tcd_threshold, cn.gain, cn.save_intermediates)

#Execute Calculate Zonal Stats Masked...
print("Step 3: Calculating Zonal Stats with Masks... \n")
zonal_stats_masked(cn.aois_folder, cn.input_folder, cn.mask_output_folder, cn.outputs_folder)

#Execute Calculcate Zonal Stats Annualized...
print("Step 4: Calculating Zonal Stats Annualized... \n")
zonal_stats_annualized(cn.tcl_clip_folder, cn.input_folder, cn.mask_output_folder, cn.annual_folder)

#Execute Zonal Stats Clean...
print("Step 5: Cleaning Zonal Stats... \n")
zonal_stats_clean()