# """
# python -m analyses.create_aggregated_display_maps
#
# With https://chatgpt.com/g/g-vK4oPfjfp-coding-assistant/c/67634e63-bbcc-800a-8267-004e88ced2e4
# """

import os
import rasterio
import geopandas as gpd
import numpy as np
import matplotlib.pyplot as plt

from shapely.geometry import Polygon, MultiPolygon
from matplotlib.colors import Normalize, TwoSlopeNorm, LinearSegmentedColormap
from fiona import path
from scipy.stats import percentileofscore

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

def create_legend(fig, img, data_min, data_max, vmin, vcenter, vmax):
    """
    Create a vertical colorbar legend with a left-aligned, multi-row title above it.
    """
    print("Creating legend")

    # Add a vertical colorbar (legend) in the bottom-left of the map
    cbar_ax = fig.add_axes([0.1, 0.18, 0.02, 0.25])  # [left, bottom, width, height]
    cb = plt.colorbar(img, cax=cbar_ax, orientation="vertical")

    # Set custom ticks and labels for the colorbar
    cb.set_ticks([vmin, vcenter, vmax])  # Set the ticks at the minimum, zero, and maximum
    cb.set_ticklabels([f"{data_min:.3f}", "0", f"{data_max:.3f}"], fontsize=9)  # Format the labels

    # Add a left-aligned, multi-row title above the colorbar
    title_text = "Net forest greenhouse gas flux\nMt CO$_2$e yr$^{-1}$ (2001-2023)"
    cbar_ax.text(
        0, 1,  # Adjust the x (horizontal) and y (vertical) coordinates for the title position
        title_text,
        fontsize=9,
        ha="left",  # Horizontally align the text to the left
        va="bottom",  # Vertically align the text
        transform=cbar_ax.transAxes  # Use axes coordinates for positioning
    )

def rgb_to_mpl_palette(rgb_palette):
    """
    Convert a list of RGB colors from 0-255 range to 0-1 range for Matplotlib.

    Parameters:
    - rgb_palette (list of tuples): List of RGB tuples (R, G, B) in 0-255 range.

    Returns:
    - list: List of RGB tuples (R, G, B) in 0-1 range.
    """
    return [tuple(val / 255 for val in rgb) for rgb in rgb_palette]

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


def percentile_for_0(data):
    # Assuming `data` is your raster array
    # Mask invalid values (e.g., NoData or zero values)
    valid_data = data[data != 0]  # Exclude zeros (or use np.ma.masked_invalid for general NoData masking)
    # Ensure valid_data is not empty
    if len(valid_data) == 0:
        raise ValueError("No valid data found in the raster.")
    # Calculate the percentile of 0
    percentile_0 = percentileofscore(valid_data, 0, kind="mean")
    print(f"0 is at the {percentile_0:.2f}th percentile of the raster data.")
    return percentile_0

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
land_bkgrnd = rgb_to_mpl((245, 245, 245))
# land_bkgrnd = rgb_to_mpl((245, 245, 220)) # Light yellow
ocean_color = rgb_to_mpl((225, 225, 225))
# ocean_color = rgb_to_mpl((50, 50, 50))
boundary_color = rgb_to_mpl((150, 150, 150))
panel_dims = (12, 6)
boundary_width = 0.2

os.chdir(cn.docker_tile_dir)


reproject_raster(reprojected_tif, tif_unproj)

# Check and reproject the shapefile
shapefile = check_and_reproject_shapefile(
    shapefile_path=original_shapefile_path,
    target_crs=target_crs,
    reprojected_shapefile_path=reprojected_shapefile_path
)

# Use the reprojected shapefile in your plotting code
print("Shapefile is ready for use.")

# Read raster data
with rasterio.open(reprojected_tif) as src:
    data = src.read(1)  # Read the first band
    raster_extent = src.bounds


percentile_0 = percentile_for_0(data)

# # Preset matplotlib colormap
# cmap = plt.cm.PRGn_r

# Define desired percentiles for colors
# percentiles = [5, 25, 50, 75, 85, 88, 90, 92, 93, 99.5]  # Specify where colors transition in the data
percentiles = [5, 25, 50, 75, 85, 88, 90, 92, 93, 99]  # Specify where colors transition in the data
colors = [(0,60,48), (1,102,94), (53,151,143), (128,205,193), (199,234,229), (246,232,195),
          (223,194,125), (191,129,45), (140,81,10), (84,48,5)]
colors_matplotlib = rgb_to_mpl_palette(colors)

# # Uses custom colormap but only the first and last breaks
# cmap = LinearSegmentedColormap.from_list("custom", colors_matplotlib)

# Matches percentile breaks with colors
# Normalizes percentiles to a 0-1 scale
percentiles_normalized = np.linspace(0, 1, len(percentiles))
cmap = LinearSegmentedColormap.from_list("custom_colormap", list(zip(percentiles_normalized, colors_matplotlib)))



print("Calculating percentile breaks")
breaks = np.percentile(data[data != 0], percentiles)  # Ignore NoData values
print(breaks)

# Ensure that vmin, vcenter, and vmax are in ascending order
vmin, vcenter, vmax = breaks[0], breaks[len(breaks) // 2], breaks[-1]  # Use the median as the center
print("vcenter: ", vcenter)

print("Masking raster")
# Mask invalid values (e.g., NoData)
# masked_data = np.ma.masked_invalid(data)
masked_data = np.ma.masked_where(data == 0, data)

data_min = masked_data.min()  # Minimum of the valid data
data_max = masked_data.max()  # Maximum of the valid data

print("Normalizing")
# Normalize the data for the colormap
norm = TwoSlopeNorm(vmin=vmin, vcenter=0, vmax=vmax)


print("Plotting map")
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

# Plot the classified raster data on top
extent = [raster_extent.left, raster_extent.right, raster_extent.bottom, raster_extent.top]

img = ax.imshow(masked_data, cmap=cmap, norm=norm, extent=extent, origin='upper', zorder=2)

# Overlay shapefile boundaries (e.g., country borders)
shapefile.boundary.plot(ax=ax, edgecolor=boundary_color, linewidth=boundary_width, zorder=3)

create_legend(fig, img, data_min, data_max, vmin, vcenter, vmax)

# Remove axis ticks and labels
remove_ticks(ax)

print("Saving map")
plt.savefig("net_flux_4x4km.jpeg", dpi=300, bbox_inches="tight", pad_inches=0)
plt.close()
