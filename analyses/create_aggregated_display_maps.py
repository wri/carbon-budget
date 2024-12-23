"""
python -m analyses.create_aggregated_display_maps
"""

import rasterio
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import Normalize, ListedColormap, BoundaryNorm
from matplotlib.colorbar import ColorbarBase
import geopandas as gpd
import os
from fiona import path

import constants_and_names as cn

os.chdir(cn.docker_tile_dir)

# Define file paths
tif_file = "gross_emis_all_gases_all_drivers_Mt_per_year_CO2e_biomass_soil__tcd30_0_04deg_modelv1_3_2_std_20240403.tif"
shapefile_path = "world-administrative-boundaries.shp"
output_jpeg = "output_image_with_shapefile_legend.jpeg"

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

print("Classifying data into custom breaks")

# Read raster data
with rasterio.open(tif_file) as src:
    data = src.read(1)

# Define the class breaks and corresponding values
class_breaks = [0.00000001, 0.0001, 0.01, np.inf]  # Class boundaries
class_values = [1, 2, 3]  # Values to assign to each class
class_labels = ['0.00000001 - 0.0001', '0.0001 - 0.01', '>0.01']  # Labels for the legend

# Initialize classified data array
classified_data = np.zeros_like(data)  # Start with all values set to 0 (background)

# Classify the data using a loop
for i in range(len(class_breaks) - 1):
    classified_data[(data > class_breaks[i]) & (data <= class_breaks[i + 1])] = class_values[i]

print("Plotting map")

# Create a custom colormap (no white included in legend)
blues = plt.cm.Blues(np.linspace(0.3, 1, 3))  # Select shades of blue for three classes
cmap = ListedColormap(blues)  # Only three colors for the legend

# Plot the map with the entire figure as 11x7 inches
fig, ax = plt.subplots(figsize=(11, 7))

# Plot the classified data
extent = [raster_extent.left, raster_extent.right, raster_extent.bottom, raster_extent.top]
masked_data = np.ma.masked_where(classified_data == 0, classified_data)  # Mask the background (0)
img = ax.imshow(masked_data, cmap=cmap, extent=extent, origin='upper', vmin=1, vmax=3)

# Overlay the shapefile boundaries
shapefile.boundary.plot(ax=ax, edgecolor='black', linewidth=0.5)

print("Adding legend")

# Add a legend for the colormap
cbar_ax = fig.add_axes([0.3, 0.03, 0.4, 0.02])  # Adjusted [left, bottom, width, height]
cb = plt.colorbar(img, cax=cbar_ax, orientation='horizontal', ticks=[1, 2, 3])  # Ticks for three classes only
cb.ax.set_xticklabels(class_labels)  # Set the class labels
cb.set_label('Gross emissions from forest loss (Mt CO2e/yr)')

# Set the background color to white and remove axis labels
ax.set_facecolor('white')
ax.set_axis_off()

print("Saving map")

# Save the output map
plt.savefig(output_jpeg, dpi=300, bbox_inches='tight', pad_inches=0)
plt.close()

print("Map saved successfully!")
