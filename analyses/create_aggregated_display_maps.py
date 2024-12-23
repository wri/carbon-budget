"""
python -m analyses.create_aggregated_display_maps
"""

import rasterio
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import Normalize, ListedColormap
from matplotlib.colorbar import ColorbarBase
import geopandas as gpd
import os
from fiona import path

import constants_and_names as cn

os.chdir(cn.docker_tile_dir)

# Define file paths
tif_file = "gross_emis_all_gases_all_drivers_Mt_per_year_CO2e_biomass_soil__tcd30_0_04deg_modelv1_3_2_std_20240403.tif"
shapefile_path = "world-administrative-boundaries.shp"
output_jpeg = "output_image_with_shapefile_low_vals.jpeg"

# with rasterio.open(tif_file) as src:
#     data = src.read(1)
#     # data = np.ma.masked_invalid(data)  # Mask invalid (NaN) values
#
# fig, ax = plt.subplots()
# ax.imshow(data)
#
# # Save the output map
# plt.savefig(output_jpeg, dpi=300, bbox_inches='tight', pad_inches=0)
# plt.close()



print("Opening raster and shapefile")

# Read the raster and get its CRS
with rasterio.open(tif_file) as src:
    raster_crs = src.crs
    raster_extent = src.bounds

# Read the shapefile and reproject if needed
shapefile = gpd.read_file(shapefile_path)
if shapefile.crs != raster_crs:
    print(f"Reprojecting shapefile from {shapefile.crs} to {raster_crs}")
    shapefile = shapefile.to_crs(raster_crs)

print("Masking invalid data")

# Read raster data and create a binary mask for values > 0
with rasterio.open(tif_file) as src:
    data = src.read(1)
    binary_mask = np.where(data > 0, 1, 0)  # Values >0 are set to 1, others to 0

print("Plotting map")

# Plot the map with the entire figure as 11x7 inches
fig, ax = plt.subplots(figsize=(11, 7))

# Define a reversed black-and-white colormap
cmap = plt.cm.gray_r  # Use the reversed gray colormap to make 1=black and 0=white

# Plot the binary mask
extent = [raster_extent.left, raster_extent.right, raster_extent.bottom, raster_extent.top]
img = ax.imshow(binary_mask, cmap=cmap, extent=extent, origin='upper')

# Overlay the shapefile boundaries
shapefile.boundary.plot(ax=ax, edgecolor='white', linewidth=0.5)

print("Adding legend (no legend required for binary map)")

# Set the background color to white and remove axis labels
ax.set_facecolor('white')
ax.set_axis_off()

print("Saving map")

# Save the output map
plt.savefig(output_jpeg, dpi=300, bbox_inches='tight', pad_inches=0)
plt.close()

print("Map saved successfully!")
