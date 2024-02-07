import constants_and_names as cn
from funcs import zonal_stats_masked

# Calculate zonal stats for all input rasters at each tcd threshold value
zonal_stats_masked(cn.aois_folder, cn.input_folder, cn.mask_output_folder, cn.outputs_folder)
