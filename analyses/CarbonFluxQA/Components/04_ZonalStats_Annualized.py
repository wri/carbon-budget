import constants_and_names as cn
from funcs import zonal_stats_annualized

# Calculate emissions for each year of tree cover loss using TCL rasters
zonal_stats_annualized(cn.tcl_clip_folder, cn.input_folder, cn.mask_output_folder, cn.annual_folder)

