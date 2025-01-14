# """
# python -m analyses.create_aggregated_display_maps
#
# With https://chatgpt.com/g/g-vK4oPfjfp-coding-assistant/c/67634e63-bbcc-800a-8267-004e88ced2e4
# """
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import Normalize, TwoSlopeNorm, LinearSegmentedColormap
import rasterio
import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon
import os
from fiona import path

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

def check_and_reproject_shapefile(shapefile_path, target_crs, reprojected_shapefile_path):
    """
    Check if the shapefile is already projected to the target CRS.
    If not, reproject the shapefile, save it, and return the reprojected shapefile.

    Parameters:
    - shapefile_path (str): Path to the input shapefile.
    - target_crs (str): The target CRS in PROJ format (e.g., "EPSG:4326" or "ESRI:54030").
    - reprojected_shapefile_path (str): Path to save the reprojected shapefile.

    Returns:
    - geopandas.GeoDataFrame: The original or reprojected shapefile.
    """
    # Check if the reprojected shapefile already exists
    if os.path.exists(reprojected_shapefile_path):
        print(f"Reprojected shapefile already exists at {reprojected_shapefile_path}.")
        return gpd.read_file(reprojected_shapefile_path)

    # Load the shapefile
    shapefile = gpd.read_file(shapefile_path)

    # Check if the shapefile is already in the target CRS
    if shapefile.crs == target_crs:
        print(f"Shapefile is already projected to {target_crs}.")
        return shapefile

    # Reproject the shapefile
    print(f"Reprojecting shapefile from {shapefile.crs} to {target_crs}.")
    shapefile = shapefile.to_crs(target_crs)

    # Save the reprojected shapefile for future use
    shapefile.to_file(reprojected_shapefile_path)
    print(f"Reprojected shapefile saved to {reprojected_shapefile_path}.")

    return shapefile

def remove_ticks(ax):
    # Set map aesthetics
    # NOTE: can't use ax.set_axis_off() to remove axis ticks and labels because it also changes the background color back to white
    ax.set_xticks([])  # Remove x-axis ticks
    ax.set_yticks([])  # Remove y-axis ticks
    ax.set_xticklabels([])  # Remove x-axis labels
    ax.set_yticklabels([])  # Remove y-axis labels

# def create_legend(fig, class_labels):
#     print("Adding legend dynamically within map bounds")
#     # Add a horizontal legend within the map bounds
#     # Normalize position to fit dynamically within the map's southern section
#     cbar_ax = fig.add_axes([0.4, 0.22, 0.36, 0.02])  # [left, bottom, width, height]
#     cb = plt.colorbar(img, cax=cbar_ax, orientation='horizontal', ticks=range(1, len(class_labels) + 1))
#     cb.ax.set_xticklabels(class_labels, ha='center', fontsize=7)  # Center-align the labels
#     cb.set_label('Gross emissions from forest loss (Mt CO$_2$e yr$^{-1}$)', fontsize=8, labelpad=4)
#
# def generate_class_labels(class_breaks):
#     """
#     Generate class labels for a given list of class breaks.
#
#     Parameters:
#     - class_breaks (list): List of class breakpoints (e.g., [lower1, lower2, ..., upper]).
#
#     Returns:
#     - list: Class labels as strings.
#     """
#     class_labels = []
#     for i in range(len(class_breaks)):
#         if i == 0:
#             # First class: "< lowest class break"
#             class_labels.append(f"<{class_breaks[i+1]:.2f}")
#         elif i == len(class_breaks) - 1:
#             # Last class: "> highest class break"
#             class_labels.append(f">{class_breaks[i-1]:.2f}")
#         else:
#             # Intermediate classes
#             class_labels.append(f"{class_breaks[i]:.4f}")
#     return class_labels
#
#
# def rgb_to_mpl_palette(rgb_palette):
#     """
#     Convert a list of RGB colors from 0-255 range to 0-1 range for Matplotlib.
#
#     Parameters:
#     - rgb_palette (list of tuples): List of RGB tuples (R, G, B) in 0-255 range.
#
#     Returns:
#     - list: List of RGB tuples (R, G, B) in 0-1 range.
#     """
#     return [tuple(val / 255 for val in rgb) for rgb in rgb_palette]
#
# os.chdir(cn.docker_tile_dir)

# Define file paths
original_shapefile_path = "world-administrative-boundaries_simple__20250102.shp"
reprojected_shapefile_path = "world-administrative-boundaries_simple__20250102_reproj.shp"

# Define the target CRS (Robinson projection in this example)
target_crs = "ESRI:54030"

emis_base = "gross_emis_all_gases_all_drivers_Mt_per_year_CO2e_biomass_soil__tcd30_0_04deg_modelv1_3_2_std_20240403"
removals_base = "gross_removals_AGCO2_BGCO2_Mt_per_year_all_forest_types__tcd30_0_04deg_modelv1_3_2_std_20240402"
net_base = "net_flux_Mt_per_year_CO2e_biomass_soil__tcd30_0_04deg_modelv1_3_2_std_20240403"
tif_unproj = f"{net_base}.tif"

reprojected_tif = f"{net_base}_reproj.tif"
#
# emis_jpeg = f"gross_emis_4x4km__v1.3.2.jpeg"
# removals_jpeg = f"gross_removals_4x4km__v1.3.2.jpeg"
# net_jpeg = f"net_flux_4x4km__v1.3.2.jpeg"
#
# three_panel_jpeg = "three_panel_4x4km__v1.3.2.jpeg"
#
# land_bkgrnd = rgb_to_mpl((245, 245, 245))
land_bkgrnd = rgb_to_mpl((50, 50, 50))
ocean_color = rgb_to_mpl((225, 225, 225))
# ocean_color = rgb_to_mpl((50, 50, 50))
boundary_color = rgb_to_mpl((150, 150, 150))
panel_dims = (12, 6)
boundary_width = 0.2

os.chdir(cn.docker_tile_dir)

def generate_percentile_breaks(data, percentiles):
    """
    Generate breakpoints based on given percentiles for the data.

    Parameters:
    - data: 2D numpy array of data values.
    - percentiles: List of percentiles (e.g., [5, 25, 50, 75, 95]).

    Returns:
    - List of breakpoint values corresponding to the specified percentiles.
    """
    # Flatten the data and filter out NoData values (e.g., 0)
    flattened_data = data.flatten()
    valid_data = flattened_data[flattened_data != 0]  # Remove 0 values

    if len(valid_data) == 0:
        raise ValueError("Data contains only NoData values (e.g., 0). Cannot calculate percentiles.")

    # Calculate the percentiles on valid data
    return np.percentile(valid_data, percentiles)


reproject_raster(reprojected_tif, tif_unproj)

# Check and reproject the shapefile
shapefile = check_and_reproject_shapefile(
    shapefile_path=original_shapefile_path,
    target_crs=target_crs,
    reprojected_shapefile_path=reprojected_shapefile_path
)

# Use the reprojected shapefile in your plotting code
print("Shapefile is ready for use.")


# Example color palette (Divergent: Red -> Yellow -> Green)
cmap = plt.cm.PRGn

# Read raster data
with rasterio.open(reprojected_tif) as src:
    data = src.read(1)  # Read the first band
    raster_extent = src.bounds

# Define desired percentiles for colors
percentiles = [5, 25, 50, 75, 85, 95]  # Specify where colors transition in the data

print("Calculating percentile breaks")
# Calculate the breakpoints based on percentiles
breaks = generate_percentile_breaks(data, percentiles)
print(breaks)

# Ensure that vmin, vcenter, and vmax are in ascending order
vmin, vcenter, vmax = breaks[0], breaks[len(breaks) // 2], breaks[-1]  # Use the median as the center

# Validate that vmin < vcenter < vmax
if not (vmin < vcenter < vmax):
    raise ValueError(f"vmin, vcenter, and vmax must be in ascending order. Got vmin={vmin}, vcenter={vcenter}, vmax={vmax}")

print("Normalizing")
# Normalize the data for the colormap
norm = TwoSlopeNorm(vmin=vmin, vcenter=vcenter, vmax=vmax)
# Read and reproject the shapefile

# Plot the map
fig, ax = plt.subplots(figsize=(12, 6))

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

print("Masking raster")
# Mask invalid values (e.g., NoData)
# masked_data = np.ma.masked_invalid(data)
masked_data = np.ma.masked_where(data == 0, data)

# Plot the classified raster data on top
extent = [raster_extent.left, raster_extent.right, raster_extent.bottom, raster_extent.top]

img = ax.imshow(masked_data, cmap=cmap, norm=norm, extent=extent, origin='upper', zorder=2)

# Overlay shapefile boundaries (e.g., country borders)
shapefile.boundary.plot(ax=ax, edgecolor=boundary_color, linewidth=boundary_width, zorder=3)

# Add a colorbar (legend)
cbar_ax = fig.add_axes([0.44, 0.22, 0.25, 0.02])  # [left, bottom, width, height]
cb = plt.colorbar(img, cax=cbar_ax, orientation="horizontal")

print("Creating legend")

# Calculate the minimum and maximum of the raster data (excluding NoData values)
data_min = masked_data.min()  # Minimum of the valid data
data_max = masked_data.max()  # Maximum of the valid data

# Set custom ticks and labels for the colorbar
cb.set_ticks([vmin, vcenter, vmax])  # Set the ticks at the minimum, zero, and maximum
cb.set_ticklabels([f"{data_min:.3f}", "0", f"{data_max:.3f}"])  # Format the labels

cb.set_label('Gross emissions from forest loss (Mt CO$_2$e yr$^{-1}$)', fontsize=8, labelpad=4)

# Remove axis ticks and labels
remove_ticks(ax)

print("Saving map")

# Save the output map
plt.savefig("net_flux_4x4km.png", dpi=300, bbox_inches="tight", pad_inches=0)
plt.close()
