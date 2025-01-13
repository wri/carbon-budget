"""
python -m analyses.create_aggregated_display_maps

With https://chatgpt.com/g/g-vK4oPfjfp-coding-assistant/c/67634e63-bbcc-800a-8267-004e88ced2e4
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
from shapely.geometry import Polygon, MultiPolygon

import constants_and_names as cn

def rgb_to_mpl(rgb):
    """
    Convert RGB from 0-255 range to matplotlib-compatible 0-1 range.
    :param rgb: Tuple of (R, G, B) in 0-255 range.
    :return: Tuple of (R, G, B) in 0-1 range.
    """
    return tuple(val / 255 for val in rgb)

def reproject_raster(reprojected_tif, tif_unproj):
    print("Checking for reprojected raster")

    # Check if the reprojected raster already exists
    if not os.path.exists(reprojected_tif):
        print("Reprojected raster does not exist. Reprojecting now...")
        with rasterio.open(tif_unproj) as src:
            transform, width, height = calculate_default_transform(
                src.crs, robinson_crs, src.width, src.height, *src.bounds
            )
            kwargs = src.meta.copy()
            compression = src.profile.get('compress', 'none')  # Get compression from the original raster
            kwargs.update({
                'crs': robinson_crs,
                'transform': transform,
                'width': width,
                'height': height,
                'nodata': 0,  # Set 0 as the NoData value
                'compress': compression  # Match the compression of the original raster
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
    else:
        print("Reprojected raster already exists. Skipping reprojection.")

def remove_ticks(ax):
    # Set map aesthetics
    # NOTE: can't use ax.set_axis_off() to remove axis ticks and labels because it also changes the background color back to white
    ax.set_xticks([])  # Remove x-axis ticks
    ax.set_yticks([])  # Remove y-axis ticks
    ax.set_xticklabels([])  # Remove x-axis labels
    ax.set_yticklabels([])  # Remove y-axis labels

def create_legend(fig, class_labels):
    print("Adding legend dynamically within map bounds")
    # Add a horizontal legend within the map bounds
    # Normalize position to fit dynamically within the map's southern section
    cbar_ax = fig.add_axes([0.4, 0.22, 0.36, 0.02])  # [left, bottom, width, height]
    cb = plt.colorbar(img, cax=cbar_ax, orientation='horizontal', ticks=range(1, len(class_labels) + 1))
    cb.ax.set_xticklabels(class_labels, ha='center', fontsize=7)  # Center-align the labels
    cb.set_label('Gross emissions from forest loss (Mt CO$_2$e yr$^{-1}$)', fontsize=8, labelpad=4)

def generate_class_labels(class_breaks):
    """
    Generate class labels for a given list of class breaks.

    Parameters:
    - class_breaks (list): List of class breakpoints (e.g., [lower1, lower2, ..., upper]).

    Returns:
    - list: Class labels as strings.
    """
    class_labels = []
    for i in range(len(class_breaks)):
        if i == 0:
            # First class: "< lowest class break"
            class_labels.append(f"<{class_breaks[i+1]:.2f}")
        elif i == len(class_breaks) - 1:
            # Last class: "> highest class break"
            class_labels.append(f">{class_breaks[i-1]:.2f}")
        else:
            # Intermediate classes
            class_labels.append(f"{class_breaks[i]:.4f}")
    return class_labels


def rgb_to_mpl_palette(rgb_palette):
    """
    Convert a list of RGB colors from 0-255 range to 0-1 range for Matplotlib.

    Parameters:
    - rgb_palette (list of tuples): List of RGB tuples (R, G, B) in 0-255 range.

    Returns:
    - list: List of RGB tuples (R, G, B) in 0-1 range.
    """
    return [tuple(val / 255 for val in rgb) for rgb in rgb_palette]

os.chdir(cn.docker_tile_dir)

# Define the Robinson Equal Area projection (ESRI:54030)
robinson_crs = "ESRI:54030"

shapefile_path = "world-administrative-boundaries_simple__20250102.shp"

data_config = {
    "emissions": {
        "base_name": "gross_emis_all_gases_all_drivers_Mt_per_year_CO2e_biomass_soil__tcd30_0_04deg_modelv1_3_2_std_20240403",
        "tif_unproj": "gross_emis_all_gases_all_drivers_Mt_per_year_CO2e_biomass_soil__tcd30_0_04deg_modelv1_3_2_std_20240403.tif",
        "reprojected_tif": "gross_emis_all_gases_all_drivers_Mt_per_year_CO2e_biomass_soil__tcd30_0_04deg_modelv1_3_2_std_20240403_reproj.tif",
        "jpeg": "gross_emis_4x4km__v1.3.2.jpeg",
        "class_breaks": [0.000000001, 0.0001, 0.005, 0.01, np.inf]
    },
    "removals": {
        "base_name": "gross_removals_AGCO2_BGCO2_Mt_per_year_all_forest_types__tcd30_0_04deg_modelv1_3_2_std_20240402",
        "tif_unproj": "gross_removals_AGCO2_BGCO2_Mt_per_year_all_forest_types__tcd30_0_04deg_modelv1_3_2_std_20240402.tif",
        "reprojected_tif": "gross_removals_AGCO2_BGCO2_Mt_per_year_all_forest_types__tcd30_0_04deg_modelv1_3_2_std_20240402_reproj.tif",
        "jpeg": "gross_removals_4x4km__v1.3.2.jpeg",
        "class_breaks": [0.000000001, 0.0001, 0.005, 0.01, np.inf]
    },
    "net_flux": {
        "base_name": "net_flux_Mt_per_year_CO2e_biomass_soil__tcd30_0_04deg_modelv1_3_2_std_20240403",
        "tif_unproj": "net_flux_Mt_per_year_CO2e_biomass_soil__tcd30_0_04deg_modelv1_3_2_std_20240403.tif",
        "reprojected_tif": "net_flux_Mt_per_year_CO2e_biomass_soil__tcd30_0_04deg_modelv1_3_2_std_20240403_reproj.tif",
        "jpeg": "net_flux_4x4km__v1.3.2.jpeg",
        "class_breaks": [-np.inf, -0.1, -0.01, -0.001, 0.0, 0.0001, 0.005, 0.01, np.inf]
    }
}

# Define file paths
emis_base = "gross_emis_all_gases_all_drivers_Mt_per_year_CO2e_biomass_soil__tcd30_0_04deg_modelv1_3_2_std_20240403"
removals_base = "gross_removals_AGCO2_BGCO2_Mt_per_year_all_forest_types__tcd30_0_04deg_modelv1_3_2_std_20240402"
net_base = "net_flux_Mt_per_year_CO2e_biomass_soil__tcd30_0_04deg_modelv1_3_2_std_20240403"
tif_unproj = f"{net_base}.tif"

reprojected_tif = f"{net_base}_reproj.tif"

emis_jpeg = f"gross_emis_4x4km__v1.3.2.jpeg"
removals_jpeg = f"gross_removals_4x4km__v1.3.2.jpeg"
net_jpeg = f"net_flux_4x4km__v1.3.2.jpeg"

three_panel_jpeg = "three_panel_4x4km__v1.3.2.jpeg"

land_bkgrnd = rgb_to_mpl((230, 230, 230))
ocean_color = rgb_to_mpl((250, 250, 250))
boundary_color = rgb_to_mpl((150, 150, 150))
panel_dims = (12, 6)
boundary_width = 0.4

# Your custom RGB palette
rgb_palette = [
    (84, 48, 5),
    (140, 81, 10),
    (191, 129, 45),
    (223, 194, 125),
    (246, 232, 195),
    (199, 234, 229),
    (128, 205, 193),
    (53, 151, 143),
    (1, 102, 94),
    (0, 60, 48),
]

# class_breaks = [0.000000001, 0.0001, 0.005, 0.01, np.inf]  # Emissions class boundaries
# class_breaks = [0.000000001, 0.0001, 0.005, 0.01, np.inf]  # Removals class boundaries
class_breaks =  [-np.inf, -0.1, -0.01, -0.001, 0.0, 0.0001, 0.005, 0.01, np.inf]  # Net flux class boundaries

### Code starts here

reproject_raster(reprojected_tif, tif_unproj)

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
class_values = list(range(1, len(class_breaks)))  # Values to assign to each class
class_labels = generate_class_labels(class_breaks)

# Initialize classified data array
classified_data = np.zeros_like(data)  # Start with all values set to 0 (background)

# Classify the data using a loop
for i in range(len(class_breaks) - 1):
    classified_data[(data > class_breaks[i]) & (data <= class_breaks[i + 1])] = class_values[i]


print("Plotting map")

# Plot the map with the entire figure as 12x6 inches
fig, ax = plt.subplots(figsize=panel_dims)

# Set the background color of the map
ax.set_facecolor(ocean_color)  # Set the background color

# Plot the shapefile polygons with a light gray fill using Matplotlib directly
for geom in shapefile.geometry:
    if isinstance(geom, Polygon):
        # Single Polygon
        x, y = geom.exterior.xy
        ax.fill(x, y, color=land_bkgrnd, zorder=1)
    elif isinstance(geom, MultiPolygon):
        # MultiPolygon: Iterate through each Polygon in the MultiPolygon
        for part in geom.geoms:
            x, y = part.exterior.xy
            ax.fill(x, y, color=land_bkgrnd, zorder=1)

# Convert the palette to Matplotlib-compatible format
mpl_palette = rgb_to_mpl_palette(rgb_palette)
# Create a custom colormap
cmap = ListedColormap(mpl_palette)

# # Create a custom colormap with white background
# # colors = plt.cm.Greens(np.linspace(0.3, 1, len(class_values)))  # Select shades of color for the classes
# colors = plt.cm.RdYlGn(np.linspace(0, 1, len(class_values)))  # Select shades of color for the classes
# cmap = ListedColormap(colors)  # Create a ListedColormap

boundaries = class_values + [len(class_values) + 1]
norm = BoundaryNorm(boundaries, cmap.N, clip=True)  # Ensure colors align with class boundaries

# Mask the 0 values in the classified_data array
masked_data = np.ma.masked_where(classified_data == 0, classified_data)

# Plot the classified raster data on top
extent = [raster_extent.left, raster_extent.right, raster_extent.bottom, raster_extent.top]

img = ax.imshow(masked_data, cmap=cmap, norm=norm, extent=extent, origin='upper', zorder=2)  # `zorder=2` places it on top

# Overlay the shapefile boundaries
shapefile.boundary.plot(ax=ax, edgecolor=boundary_color, linewidth=boundary_width, zorder=3)  # `zorder=3` ensures boundaries are on top


create_legend(fig, class_labels)

remove_ticks(ax)

print("Saving map")

# Save the output map
plt.savefig(net_jpeg, dpi=300, bbox_inches='tight', pad_inches=0)
plt.close()

print("Map saved successfully!")

