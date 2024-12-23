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

# Read raster data and classify into custom breaks
with rasterio.open(tif_file) as src:
    data = src.read(1)
    classified_data = np.zeros_like(data)  # Start with all values set to 0 (background)

    # Classify the data
    classified_data[(data > 0.00000001) & (data <= 0.0001)] = 1  # Red range
    classified_data[data > 0.0001] = 2  # Black range

print("Plotting map")

# Create a custom colormap: 0=white, 1=red, 2=black
cmap = ListedColormap(["white", "red", "black"])

# Plot the map with the entire figure as 11x7 inches
fig, ax = plt.subplots(figsize=(11, 7))

# Plot the classified data
extent = [raster_extent.left, raster_extent.right, raster_extent.bottom, raster_extent.top]
img = ax.imshow(classified_data, cmap=cmap, extent=extent, origin='upper')

# Overlay the shapefile boundaries
shapefile.boundary.plot(ax=ax, edgecolor='white', linewidth=0.5)

print("Adding legend (no legend required for classified map)")

# Set the background color to white and remove axis labels
ax.set_facecolor('white')
ax.set_axis_off()

print("Saving map")

# Save the output map
plt.savefig(output_jpeg, dpi=300, bbox_inches='tight', pad_inches=0)
plt.close()

print("Map saved successfully!")
