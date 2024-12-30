"""
python -m analyses.create_aggregated_display_maps
"""

import rasterio
import numpy as np
import matplotlib.pyplot as plt
from rasterio.warp import calculate_default_transform, reproject, Resampling
from matplotlib.colors import Normalize, ListedColormap, BoundaryNorm
from matplotlib.colorbar import ColorbarBase
import geopandas as gpd
import os
from fiona import path

import constants_and_names as cn

os.chdir(cn.docker_tile_dir)

# Define file paths
tif_file = "gross_emis_all_gases_all_drivers_Mt_per_year_CO2e_biomass_soil__tcd30_0_04deg_modelv1_3_2_std_20240403.tif"
reprojected_tif = "reprojected_raster_robinson.tif"
shapefile_path = "world-administrative-boundaries.shp"
output_jpeg = "output_image_with_shapefile_low_vals_with_legend.jpeg"

# Define the Robinson Equal Area projection (ESRI:54030)
robinson_crs = "ESRI:54030"

print("Opening and reprojecting raster")

# Reproject the raster to Robinson projection
with rasterio.open(tif_file) as src:
    transform, width, height = calculate_default_transform(
        src.crs, robinson_crs, src.width, src.height, *src.bounds
    )
    kwargs = src.meta.copy()
    kwargs.update({
        'crs': robinson_crs,
        'transform': transform,
        'width': width,
        'height': height
    })

    with rasterio.open(reprojected_tif, 'w', **kwargs) as dst:
        reproject(
            source=rasterio.band(src, 1),
            destination=rasterio.band(dst, 1),
            src_transform=src.transform,
            src_crs=src.crs,
            dst_transform=transform,
            dst_crs=robinson_crs,
            resampling=Resampling.nearest
        )

# Read the reprojected raster
with rasterio.open(reprojected_tif) as src:
    raster_extent = src.bounds
    data = src.read(1)

print("Opening and reprojecting shapefile")

# Read the shapefile and reproject it to Robinson projection
shapefile = gpd.read_file(shapefile_path)
if shapefile.crs != robinson_crs:
    print(f"Reprojecting shapefile from {shapefile.crs} to {robinson_crs}")
    shapefile = shapefile.to_crs(robinson_crs)

print("Classifying data into custom breaks")

# Read raster data
with rasterio.open(reprojected_tif) as src:
    data = src.read(1)

# Define the class breaks and corresponding values
class_breaks = [0.000000001, 0.0001, 0.005, 0.01, np.inf]  # Class boundaries
class_values = list(range(1, len(class_breaks)))  # Values to assign to each class
class_labels = ['0.0001', '0.05', '0.01', '>0.01']  # Labels for the legend

# Initialize classified data array
classified_data = np.zeros_like(data)  # Start with all values set to 0 (background)

# Classify the data using a loop
for i in range(len(class_breaks) - 1):
    classified_data[(data > class_breaks[i]) & (data <= class_breaks[i + 1])] = class_values[i]

print("Plotting map")

# Create a custom colormap with white background
blues = plt.cm.Blues(np.linspace(0.3, 1, len(class_values)))  # Select shades of blue for three classes
colors = np.vstack(([1, 1, 1, 1], blues))  # Add white (RGBA = 1, 1, 1, 1) for the background
cmap = ListedColormap(colors)  # Create a ListedColormap

# Plot the map with the entire figure as 11x7 inches
fig, ax = plt.subplots(figsize=(12, 6))

# Plot the classified data
extent = [raster_extent.left, raster_extent.right, raster_extent.bottom, raster_extent.top]

# For the legend specifically
colors_legend = np.vstack((blues))
cmap_legend = ListedColormap(colors_legend)  # Create a ListedColormap
img_legend = ax.imshow(classified_data, cmap=cmap_legend, extent=extent, origin='upper')

# Then prints the actual map
img = ax.imshow(classified_data, cmap=cmap, extent=extent, origin='upper')

# Overlay the shapefile boundaries
shapefile.boundary.plot(ax=ax, edgecolor='black', linewidth=0.5)

print("Adding legend")

# Add a vertical legend on the map panel (adjusted position)
cbar_ax = fig.add_axes([0.1, 0.15, 0.02, 0.4])  # [left, bottom, width, height]
cb = plt.colorbar(img_legend, cax=cbar_ax, orientation='vertical', ticks=class_values)  # Vertical colorbar
cb.ax.set_yticklabels(class_labels, va='center')  # Center-align labels
cb.ax.set_title('Gross emissions\n(Mt CO2e/yr)', fontsize=10, pad=10, loc='center')  # Add horizontal title




# Set the background color to white and remove axis labels
ax.set_facecolor('white')
ax.set_axis_off()

print("Saving map")

# Save the output map
plt.savefig(output_jpeg, dpi=300, bbox_inches='tight', pad_inches=0)
plt.close()

print("Map saved successfully!")

