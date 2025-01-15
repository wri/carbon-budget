# """
# python -m analyses.create_aggregated_display_maps
#
# With https://chatgpt.com/g/g-vK4oPfjfp-coding-assistant/c/67634e63-bbcc-800a-8267-004e88ced2e4
# """

import math
import os
import rasterio
import geopandas as gpd
import numpy as np
import matplotlib.pyplot as plt

from fiona import path
from matplotlib.colors import Normalize, TwoSlopeNorm, LinearSegmentedColormap
from rasterio.warp import calculate_default_transform, reproject, Resampling
from shapely.geometry import Polygon, MultiPolygon
from scipy.stats import percentileofscore

import constants_and_names as cn

os.chdir(cn.docker_tile_dir)

def rgb_to_mpl(rgb):
    """
    Converts RGB from 0-255 range to matplotlib-compatible 0-1 range.
    :param rgb: Tuple of (R, G, B) in 0-255 range.
    :return: Tuple of (R, G, B) in 0-1 range.
    """
    return tuple(val / 255 for val in rgb)

def reproject_raster(reprojected_tif, tif_unproj):
    """
    Reprojects raster to specified projection if it doesn't already exist
    :param reprojected_tif: Name of projected (output) tif
    :param tif_unproj: Name of unprojected (input) tif
    :return: Nothing
    """

    # Checks if the reprojected raster already exists
    if not os.path.exists(reprojected_tif):
        print("Reprojected raster does not exist. Reprojecting now...")
        with rasterio.open(tif_unproj) as src:
            transform, width, height = calculate_default_transform(
                src.crs, cn.Robinson_crs, src.width, src.height, *src.bounds
            )
            kwargs = src.meta.copy()
            compression = src.profile.get('compress', 'none')  # Gets compression from the original raster
            kwargs.update({
                'crs': cn.Robinson_crs,
                'transform': transform,
                'width': width,
                'height': height,
                'nodata': 0,
                'compress': compression
            })

            with rasterio.open(reprojected_tif, 'w', **kwargs) as dst:
                reproject(
                    source=rasterio.band(src, 1),
                    destination=rasterio.band(dst, 1),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=transform,
                    dst_crs=cn.Robinson_crs,
                    resampling=Resampling.nearest
                )
    else:
        print("Reprojected raster already exists. Skipping reprojection.")

def check_and_reproject_shapefile(shapefile_path, target_crs, reprojected_shapefile_path):
    """
    Checks if the shapefile is already projected to the target CRS.
    If not, reprojects the shapefile, saves it, and returns the reprojected shapefile.

    Parameters:
    - shapefile_path (str): Path to the input shapefile.
    - target_crs (str): The target CRS in PROJ format (e.g., "EPSG:4326" or "ESRI:54030").
    - reprojected_shapefile_path (str): Path to save the reprojected shapefile.

    Returns:
    - geopandas.GeoDataFrame: The original or reprojected shapefile.
    """

    # Checks if the reprojected shapefile already exists
    if os.path.exists(reprojected_shapefile_path):
        print(f"Reprojected shapefile already exists at {reprojected_shapefile_path}.")
        return gpd.read_file(reprojected_shapefile_path)

    # Loads the shapefile
    shapefile = gpd.read_file(shapefile_path)

    # Checks if the shapefile is already in the target CRS
    if shapefile.crs == target_crs:
        print(f"Shapefile is already projected to {target_crs}.")
        return shapefile

    # Reprojects the shapefile
    print(f"Reprojecting shapefile from {shapefile.crs} to {target_crs}.")
    shapefile = shapefile.to_crs(target_crs)

    # Saves the reprojected shapefile for future use
    shapefile.to_file(reprojected_shapefile_path)
    print(f"Reprojected shapefile saved to {reprojected_shapefile_path}.")

    return shapefile

def create_plot():
    """
    Creates matplotlib plot
    :return: ax and fig
    """
    fig, ax = plt.subplots(figsize=cn.panel_dims)
    return ax, fig

def remove_ticks(ax):
    """
    Removes ticks from matplotlib plot
    :param ax: graph
    :return: N/A
    """
    # Set map aesthetics
    # NOTE: can't use ax.set_axis_off() to remove axis ticks and labels because it also changes the background color back to white
    ax.set_xticks([])  # Remove x-axis ticks
    ax.set_yticks([])  # Remove y-axis ticks
    ax.set_xticklabels([])  # Remove x-axis labels
    ax.set_yticklabels([])  # Remove y-axis labels

def create_divergent_legend(fig, img, vmin, vcenter, vmax, title_text, tick_labels):
    """
    Creates a vertical colorbar legend with a left-aligned title above it.
    :param fig: The figure
    :param img: The image
    :param vmin: minimum value to use in scaling legend colors
    :param vcenter: middle value to use in scaling legend colors
    :param vmax: maximum value to use in scaling legend colors
    :param title_text: Title for legend
    :param tick_labels: Tick labels for legend
    :return: N/A
    """

    print("Creating legend")

    # Add a vertical colorbar (legend) in the bottom-left of the map
    cbar_ax = fig.add_axes([0.14, 0.18, 0.02, 0.16])  # [left, bottom, width, height]
    cb = plt.colorbar(img, cax=cbar_ax, orientation="vertical")

    # Set custom ticks and labels for the colorbar
    cb.set_ticks([vmin, vcenter, vmax])  # Set the ticks at the minimum, zero, and maximum
    cb.set_ticklabels(tick_labels, fontsize=9)  # Format the labels

    # Add a left-aligned, multi-row title above the colorbar
    cbar_ax.text(
        0, 1.1,  # Adjust the x (horizontal) and y (vertical) coordinates for the title position
        title_text,
        fontsize=9,
        ha="left",  # Horizontally align the text to the left
        va="bottom",  # Vertically align the text
        transform=cbar_ax.transAxes  # Use axes coordinates for positioning
    )

def create_unidirection_legend(fig, img, vmin, vmax, title_text, tick_labels):
    """
    Creates a vertical colorbar legend with a left-aligned title above it.
    :param fig: The figure
    :param img: The image
    :param vmin: minimum value to use in scaling legend colors
    :param vmax: maximum value to use in scaling legend colors
    :param title_text: Title for legend
    :param tick_labels: Tick labels for legend
    :return: N/A
    """

    print("Creating legend")

    # Add a vertical colorbar (legend) in the bottom-left of the map
    cbar_ax = fig.add_axes([0.14, 0.18, 0.02, 0.16])  # [left, bottom, width, height]
    cb = plt.colorbar(img, cax=cbar_ax, orientation="vertical")

    # Set custom ticks and labels for the colorbar
    cb.set_ticks([vmin, vmax])  # Set the ticks at the minimum, zero, and maximum
    cb.set_ticklabels(tick_labels, fontsize=9)  # Format the labels

    # Add a left-aligned, multi-row title above the colorbar
    cbar_ax.text(
        0, 1.1,  # Adjust the x (horizontal) and y (vertical) coordinates for the title position
        title_text,
        fontsize=9,
        ha="left",  # Horizontally align the text to the left
        va="bottom",  # Vertically align the text
        transform=cbar_ax.transAxes  # Use axes coordinates for positioning
    )

def rgb_to_mpl_palette(rgb_palette):
    """
    Converts a list of RGB colors from 0-255 range to 0-1 range for Matplotlib.

    Parameters:
    - rgb_palette (list of tuples): List of RGB tuples (R, G, B) in 0-255 range.

    Returns:
    - list: List of RGB tuples (R, G, B) in 0-1 range.
    """
    return [tuple(val / 255 for val in rgb) for rgb in rgb_palette]

def percentile_for_0(data):

    # Masks invalid values (e.g., NoData or zero values)
    valid_data = data[data != 0]  # Excludes zeros (or use np.ma.masked_invalid for general NoData masking)

    # Ensures valid_data is not empty
    if len(valid_data) == 0:
        raise ValueError("No valid data found in the raster.")

    # Calculates the percentile of 0
    percentile_0 = percentileofscore(valid_data, 0, kind="mean")

    return percentile_0

def set_ocean_color(ax):
    # Sets the background color of the map
    ax.set_facecolor(rgb_to_mpl(cn.ocean_color))  # Set the background color

def plot_country_polygons(ax, shapefile):
    """
    Plots the shapefile polygons or multipolygons with a specified color. zorder sets the order of drawing.
    :param ax: figure
    :param shapefile: shapefile to draw
    :return: N/A
    """

    for geom in shapefile.geometry:
        if isinstance(geom, Polygon):
            # Single Polygon
            x, y = geom.exterior.xy
            ax.fill(x, y, color=rgb_to_mpl(cn.land_bkgrnd), zorder=1)
        elif isinstance(geom, MultiPolygon):
            # MultiPolygon: Iterate through each Polygon in the MultiPolygon
            for part in geom.geoms:
                x, y = part.exterior.xy
                ax.fill(x, y, color=rgb_to_mpl(cn.land_bkgrnd), zorder=1)

def plot_raster(ax, cmap, extent, masked_data, norm):
    """
    Plots raster
    :param ax: figure
    :param cmap: colormap
    :param extent: raster extent
    :param masked_data: masked data (no NoData/0s) to plot
    :param norm: data normalization
    :return: image
    """

    img = ax.imshow(masked_data, cmap=cmap, norm=norm, extent=extent, origin='upper', zorder=2)
    return img

def plot_country_boundaries(ax, shapefile):

    # Overlaya shapefile boundaries (e.g., country borders)
    # zorder determines the order of appearance in the figure
    shapefile.boundary.plot(ax=ax, edgecolor=rgb_to_mpl(cn.boundary_color), linewidth=cn.boundary_width, zorder=3)

def save_jpeg(out_jpeg):

    print("Saving map")
    plt.savefig(out_jpeg, dpi=cn.dpi_jpeg, bbox_inches="tight", pad_inches=0)
    plt.close()

# Makes jpeg of net fluxes
def map_net_flux(base_tif, colors, percentiles, title_text, out_jpeg):

    print(f"---Mapping {base_tif}")

    # Raster name before and after projection
    tif_unproj = f"{base_tif}.tif"
    reprojected_tif = f"{base_tif}_reproj.tif"

    # Reprojects raster, if needed
    reproject_raster(reprojected_tif, tif_unproj)

    # Reprojects shapefile, if needed
    shapefile = check_and_reproject_shapefile(
        shapefile_path=cn.original_shapefile_path,
        target_crs=cn.Robinson_crs,
        reprojected_shapefile_path=cn.reprojected_shapefile_path
    )

    # Reads raster data
    with rasterio.open(reprojected_tif) as src:
        data = src.read(1)  # Read the first band
        raster_extent = src.bounds

    # Calculates the percentile for 0 (no flux)
    percentile_0 = percentile_for_0(data)
    print(f"0 is at the {percentile_0}th percentile of the raster.")

    # Matches percentile breaks with colors.
    # Normalizes percentiles to a 0-1 scale.
    print("Calculating percentiles and breaks")

    # Converts RGB color palette to matplotlib color palette
    colors_matplotlib = rgb_to_mpl_palette(colors)

    # Makes percentiles for the breakpoints and prepares colormap
    percentiles_normalized = np.linspace(0, 1, len(percentiles))
    cmap = LinearSegmentedColormap.from_list("custom_colormap", list(zip(percentiles_normalized, colors_matplotlib)))

    # Calculates breaks in the data based on the percentiles
    breaks = np.percentile(data[data != 0], percentiles)  # Ignores NoData values
    print("Breaks:", breaks)

    # Min, center and max values for the colormap (not the min and max values for the raster)
    vmin, vcenter, vmax = breaks[0], breaks[len(breaks) // 2], breaks[-1]  # Uses the median as the center
    print("vcenter: ", vcenter)

    print("Masking raster to non-0 values")
    masked_data = np.ma.masked_where(data == 0, data)
    data_min = masked_data.min()  # Minimum of the valid data
    data_max = masked_data.max()  # Maximum of the valid data

    print("Normalizing")
    # Normalizes the data for the colormap
    norm = TwoSlopeNorm(vmin=vmin, vcenter=0, vmax=vmax)

    print("Plotting map")
    ax, fig = create_plot()

    # Sets the ocean color
    set_ocean_color(ax)

    # Plots the country polygons first
    plot_country_polygons(ax, shapefile)

    # Raster extent
    extent = [raster_extent.left, raster_extent.right, raster_extent.bottom, raster_extent.top]

    # Plots the raster next
    img = plot_raster(ax, cmap, extent, masked_data, norm)

    # Plots the country boundaries on top
    plot_country_boundaries(ax, shapefile)

    # Creates the legend
    # Rounds data_min down to the nearest 0.01 and data_max up to the nearest 0.01 for legend
    rounded_min = math.ceil(data_min * 100) / 100  # Rounds up
    rounded_max = math.floor(data_max * 100) / 100  # Rounds down
    tick_labels = [f"< {rounded_min:.3f}  (sink)",   # Spaces are to horizontally align the text explanations
                    "0              (neutral)",
                   f"> {rounded_max:.2f}     (source)"]
    create_divergent_legend(fig, img, vmin, vcenter, vmax, title_text, tick_labels)

    # Removes axis ticks and labels
    remove_ticks(ax)

    # Saves jpeg
    save_jpeg(out_jpeg)

# Makes jpeg of gross fluxes
def map_gross(base_tif, colors, percentiles, title_text, out_jpeg):

    print(f"---Mapping {base_tif}")

    # Raster name before and after projection
    tif_unproj = f"{base_tif}.tif"
    reprojected_tif = f"{base_tif}_reproj.tif"

    # Reprojects raster, if needed
    reproject_raster(reprojected_tif, tif_unproj)

    # Reprojects shapefile, if needed
    shapefile = check_and_reproject_shapefile(
        shapefile_path=cn.original_shapefile_path,
        target_crs=cn.Robinson_crs,
        reprojected_shapefile_path=cn.reprojected_shapefile_path
    )

    # Reads raster data
    with rasterio.open(reprojected_tif) as src:
        data = src.read(1)  # Read the first band
        raster_extent = src.bounds

    # Matches percentile breaks with colors.
    # Normalizes percentiles to a 0-1 scale.
    print("Calculating percentiles and breaks")

    # Converts RGB color palette to matplotlib color palette
    colors_matplotlib = rgb_to_mpl_palette(colors)

    # Makes percentiles for the breakpoints and prepares colormap
    percentiles_normalized = np.linspace(0, 1, len(percentiles))
    cmap = LinearSegmentedColormap.from_list("custom_colormap", list(zip(percentiles_normalized, colors_matplotlib)))

    # Calculates breaks in the data based on the percentiles
    breaks = np.percentile(data[data != 0], percentiles)  # Ignores NoData values
    print("Breaks:", breaks)

    # Min and max values for the colormap (not the min and max values for the raster)
    vmin, vmax = breaks[0], breaks[-1]
    print(f"vmin: {vmin}, vmax: {vmax}")

    print("Masking raster to non-0 values")
    masked_data = np.ma.masked_where(data == 0, data)
    data_min = masked_data.min()  # Minimum of the valid data
    data_max = masked_data.max()  # Maximum of the valid data

    print("Normalizing")
    # Normalizes the data for the colormap
    norm = Normalize(vmin=vmin, vmax=vmax)

    print("Plotting map")
    ax, fig = create_plot()

    # Sets the ocean color
    set_ocean_color(ax)

    # Plots the country polygons first
    plot_country_polygons(ax, shapefile)

    # Raster extent
    extent = [raster_extent.left, raster_extent.right, raster_extent.bottom, raster_extent.top]

    # Plots the raster next
    img = plot_raster(ax, cmap, extent, masked_data, norm)

    # Plots the country boundaries on top
    plot_country_boundaries(ax, shapefile)

    # Creates the legend
    # Rounds data_min down to the nearest 0.01 and data_max up to the nearest 0.01 for legend
    rounded_min = math.floor(data_min * 100) / 100  # Round down
    rounded_max = math.ceil(data_max * 100) / 100  # Round up
    # print(data_min, rounded_min)
    # print(data_max, rounded_max)

    # Legend labels depend on whether emissions or removals are displayed
    if "removals" in base_tif:
        tick_labels = [f"< {rounded_min:.2f}", 0]
    elif "emis" in base_tif:
        tick_labels = [0, f"> {rounded_max:.2f}"]
    else:
        tick_labels = ["N/A", "N/A"]
        print("Can't generate tick labels")
    create_unidirection_legend(fig, img, vmin, vmax, title_text, tick_labels)

    # Removes axis ticks and labels
    remove_ticks(ax)

    # Saves jpeg
    save_jpeg(out_jpeg)


def create_three_panel_map(emissions_jpeg, removals_jpeg, net_jpeg, out_jpeg):
    """
    Creates a three-panel map showing emissions, removals, and net flux.
    """
    print("Creating three-panel map")

    # Loads individual panel images
    emissions_img = plt.imread(emissions_jpeg)
    removals_img = plt.imread(removals_jpeg)
    net_img = plt.imread(net_jpeg)

    # Panel titles and images
    panel_labels = ["a", "b", "c"]
    images = [emissions_img, removals_img, net_img]

    three_panel_dims = (cn.panel_dims[0], cn.panel_dims[1] * len(images))

    # Sets up the figure
    fig, axes = plt.subplots(nrows=len(images), ncols=1, figsize=three_panel_dims)

    # Removes spaces between panels
    fig.subplots_adjust(hspace=0, wspace=0)

    # Adds each panel to the figure
    for ax, img, label in zip(axes, images, panel_labels):
        ax.imshow(img, aspect='auto')
        ax.axis("off")  # Removes axis ticks
        # Adds panel label in the top-left corner
        ax.text(0.02, 0.98, label, transform=ax.transAxes, fontsize=10, fontweight="bold",
                ha="left", va="top", color="black")

    # Saves jpeg
    save_jpeg(out_jpeg)


if __name__ == '__main__':

    # Defines desired percentiles for colors
    net_percentiles = [5, 25, 50, 75, 89, 91, 92, 93, 94, 99]  # Specifies where colors transition in the data
    removals_percentiles = [5, 25, 50, 75, 99]
    emissions_percentiles = [5, 25, 50, 75, 99]

    # Colors in RGB. Gross emissions and removals are subset of net flux palette.
    net_color_palette = [(0, 60, 48), (1, 102, 94), (53, 151, 143), (128, 205, 193), (199, 234, 229),  # Used for removals
                         (246, 232, 195), (223, 194, 125), (191, 129, 45), (140, 81, 10), (84, 48, 5)  # Used for emissions
                         ]
    removals_colors = net_color_palette[0:5]
    emissions_colors = net_color_palette[5:]

    # Legend titles
    emissions_title = "Gross forest greenhouse gas emissions\nMt CO$_2$e yr$^{-1}$ (2001-2023)"
    removals_title = "Gross forest CO$_2$ removals\nMt CO$_2$ yr$^{-1}$ (2001-2023)"
    net_title = "Net forest greenhouse gas flux\nMt CO$_2$e yr$^{-1}$ (2001-2023)"

    # # Generates jpegs for gross emissions, removals and net flux
    # map_gross(cn.emissions_base, emissions_colors, emissions_percentiles, emissions_title, cn.emissions_jpeg)
    # map_gross(cn.removals_base, removals_colors, removals_percentiles, removals_title, cn.removals_jpeg)
    # map_net_flux(cn.net_base, net_color_palette, net_percentiles, net_title, cn.net_jpeg)

    # Generates three-panel map
    create_three_panel_map(
        cn.emissions_jpeg,
        cn.removals_jpeg,
        cn.net_jpeg,
        cn.three_panel_jpeg
    )
