"""
python -m analyses.create_aggregated_display_maps
"""

import rasterio
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap, BoundaryNorm
from matplotlib.colorbar import ColorbarBase
import geopandas as gpd
import os
from fiona import path

import constants_and_names as cn

os.chdir(cn.docker_tile_dir)

# Define file paths
tif_file = "gross_emis_all_gases_all_drivers_Mt_per_year_CO2e_biomass_soil__tcd30_0_04deg_modelv1_3_2_std_20240403.tif"
shapefile_path = "world-administrative-boundaries.shp"
output_jpeg = "output_image_with_shapefile_fixed.jpeg"

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

print("Calculating raster quintiles")

# Recalculate bounds based on quintiles of the data
with rasterio.open(tif_file) as src:
    data = src.read(1)
    data = np.ma.masked_less_equal(data, 0)  # Mask non-positive values
    quintiles = np.percentile(data.compressed(), [0, 20, 40, 60, 80, 100])  # Compute quintiles

# Update bounds with quintiles
quintile_bounds = list(quintiles)
print(f"Quintile bounds: {quintile_bounds}")

print("Plotting map")

# Plot the map with the entire figure as 11x7 inches
fig, ax = plt.subplots(figsize=(12, 5))
with rasterio.open(tif_file) as src:
    data = src.read(1)
    data = np.ma.masked_less_equal(data, 0)  # Mask non-positive values

# Update colormap and boundaries for quintiles
cmap_quintile = ListedColormap(plt.cm.Blues(np.linspace(0.3, 1, len(quintile_bounds) - 1)))
norm_quintile = BoundaryNorm(quintile_bounds, cmap_quintile.N)

# Plot the raster
extent = [raster_extent.left, raster_extent.right, raster_extent.bottom, raster_extent.top]
img = ax.imshow(data, cmap=cmap_quintile, norm=norm_quintile, extent=extent, origin='upper')

# Overlay the shapefile boundaries
shapefile.boundary.plot(ax=ax, edgecolor='darkgray', linewidth=0.5)

print("Adding legend")

# Add the colorbar at the bottom, integrated into the figure space
cbar_ax = fig.add_axes([0.1, 0.03, 0.8, 0.02])  # [left, bottom, width, height]
cb = ColorbarBase(cbar_ax, cmap=cmap_quintile, norm=norm_quintile, boundaries=quintile_bounds,
                  ticks=quintile_bounds, orientation='horizontal')
cb.set_label('Gross emissions from forest loss (Mt CO2e/yr)')

# Set the background color to white and remove axis labels
ax.set_facecolor('white')
ax.set_axis_off()

print("Saving map")

# Save the output map with no padding
plt.savefig(output_jpeg, dpi=300, bbox_inches='tight', pad_inches=0)
plt.close()

print("Map saved successfully!")
