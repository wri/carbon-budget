"""
Compares two raster tiles pixel by pixel, e.g. a tile you just made with the published one. Use it to check that a new
run environment gives the same results; see docs/checking-a-new-run-environment.md.

python -m analyses.compare_tiles <new tile> <reference tile>

For example:
python -m analyses.compare_tiles /usr/local/tiles/00N_110E_tree_cover_loss_fire_processed.tif s3://gfw2-data/climate/carbon_model/other_emissions_inputs/tree_cover_loss_fires/20260317/processed/00N_110E_tree_cover_loss_fire_processed.tif

Either tile can be a local path or an s3:// URL. Tiles on s3 are downloaded to a temporary folder, which is deleted
afterwards; this needs AWS credentials, as for the rest of the framework.

It compares pixel values, not file bytes, because GeoTIFFs record when they were made. Differences in metadata
(CRS, transform, nodata, tags other than date_created) are reported but don't count as a difference.

First it checks that the comparison can fail: it copies the reference tile, changes one pixel in the copy, and confirms
that comparing the reference with the copy finds exactly one differing pixel.

Exit code: 0 if the pixels are identical, 1 if they differ, 2 if the check of the comparison itself fails.
"""

import argparse
import os
import shutil
import sys
import tempfile

import numpy as np
import rasterio
from rasterio.windows import Window

ROWS_PER_READ = 1024
IGNORED_TAGS = {"date_created"}


def local_copy(path, folder):
    """Returns a local path for the tile, downloading it first if it's on s3."""
    if not path.startswith("s3://"):
        return path
    import boto3

    bucket, key = path[len("s3://"):].split("/", 1)
    local = os.path.join(folder, os.path.basename(key))
    if os.path.exists(local):
        local = os.path.join(folder, "2_" + os.path.basename(key))
    print(f"Downloading {path}", flush=True)
    boto3.client("s3").download_file(bucket, key, local)
    return local


def compare(path_a, path_b):
    """Compares two rasters pixel by pixel, reading a block of rows at a time so large tiles fit in memory."""
    with rasterio.open(path_a) as a, rasterio.open(path_b) as b:
        metadata = {}
        for attr in ("count", "width", "height", "dtypes", "crs", "transform", "nodata"):
            va, vb = getattr(a, attr), getattr(b, attr)
            if va != vb:
                metadata[attr] = (va, vb)
        tags_a = {k: v for k, v in a.tags().items() if k not in IGNORED_TAGS}
        tags_b = {k: v for k, v in b.tags().items() if k not in IGNORED_TAGS}
        if tags_a != tags_b:
            metadata["tags"] = (tags_a, tags_b)
        result = {"shape_differs": any(k in metadata for k in ("count", "width", "height")), "metadata": metadata}
        if result["shape_differs"]:
            return result

        pixels, differing, nan_mismatches, max_abs = 0, 0, 0, 0.0
        sum_a, sum_b = 0.0, 0.0
        for band in range(1, a.count + 1):
            for row in range(0, a.height, ROWS_PER_READ):
                window = Window(0, row, a.width, min(ROWS_PER_READ, a.height - row))
                x = a.read(band, window=window).astype("float64")
                y = b.read(band, window=window).astype("float64")
                pixels += x.size
                x_nan, y_nan = np.isnan(x), np.isnan(y)
                nan_mismatches += int((x_nan != y_nan).sum())
                diff = ~x_nan & ~y_nan & (x != y)
                n = int(diff.sum())
                if n:
                    differing += n
                    max_abs = max(max_abs, float(np.abs(x[diff] - y[diff]).max()))
                valid_x = ~x_nan if a.nodata is None else ~x_nan & (x != a.nodata)
                valid_y = ~y_nan if b.nodata is None else ~y_nan & (y != b.nodata)
                sum_a += float(x[valid_x].sum())
                sum_b += float(y[valid_y].sum())

    result.update(pixels=pixels, differing=differing, nan_mismatches=nan_mismatches, max_abs=max_abs,
                  sum_a=sum_a, sum_b=sum_b)
    return result


def change_one_pixel(path):
    """Changes the middle pixel of band 1, in place. Adds 1, or sets 0 if the pixel is NaN."""
    with rasterio.open(path, "r+") as r:
        window = Window(r.width // 2, r.height // 2, 1, 1)
        value = r.read(1, window=window)
        if np.issubdtype(value.dtype, np.floating) and np.isnan(value[0, 0]):
            changed = np.zeros_like(value)
        else:
            changed = value + np.array(1, dtype=value.dtype)  # integers at their maximum wrap to the minimum
        r.write(changed, 1, window=window)


def describe(result):
    if result["shape_differs"]:
        return "the tiles have different shapes or band counts"
    if not result["differing"] and not result["nan_mismatches"]:
        return f"identical ({result['pixels']:,} pixels)"
    return (f"{result['differing']:,} of {result['pixels']:,} pixels differ (largest difference "
            f"{result['max_abs']:.6g}), {result['nan_mismatches']:,} NaN mismatches; "
            f"sum {result['sum_a']:.12g} vs {result['sum_b']:.12g}")


def main():
    parser = argparse.ArgumentParser(description="Compares two raster tiles pixel by pixel.")
    parser.add_argument("new", help="the tile to check (local path or s3:// URL)")
    parser.add_argument("reference", help="the tile it should match, e.g. the published one (local path or s3:// URL)")
    args = parser.parse_args()

    folder = tempfile.mkdtemp(prefix="compare_tiles_")
    try:
        new = local_copy(args.new, folder)
        reference = local_copy(args.reference, folder)

        print("Checking that the comparison catches a one-pixel change...", flush=True)
        changed_copy = os.path.join(folder, "changed_" + os.path.basename(reference))
        shutil.copyfile(reference, changed_copy)
        change_one_pixel(changed_copy)
        check = compare(reference, changed_copy)
        if check["shape_differs"] or check["differing"] + check["nan_mismatches"] != 1:
            print(f"The comparison didn't work: reference vs a copy with one pixel changed: {describe(check)}")
            return 2
        print("It does: reference vs a copy with one pixel changed: 1 pixel differs.")

        print("Comparing the tiles...", flush=True)
        result = compare(new, reference)
        print(f"New vs reference: {describe(result)}")
        for attr, (va, vb) in result["metadata"].items():
            print(f"  metadata differs: {attr}: {va} (new) vs {vb} (reference)")
        identical = not result["shape_differs"] and not result["differing"] and not result["nan_mismatches"]
        return 0 if identical else 1
    finally:
        shutil.rmtree(folder, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
