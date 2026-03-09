import numpy as np
import rasterio
from rasterio.warp import reproject, Resampling
from rasterio.transform import Affine


def apply_forest_mask(scene_bands, worldcover_path, forest_class=10):
    """
    Masks each scene's band arrays to forested pixels only using ESA WorldCover.
    Class 10 = Tree cover. Non-forest pixels are set to NaN.

    Operates on numpy arrays (before Spark ingestion) by reprojecting the
    WorldCover raster onto each scene's pixel grid with rasterio — no Spark
    or geospatial vector join required.

    Parameters
    ----------
    scene_bands     : list of (tile_id, acq_date, red, nir, swir1, bbox_wkt,
                                strided_transform, crs_str)
                      as returned by read_bands_from_s3().
    worldcover_path : local path to the ESA WorldCover GeoTIFF.
    forest_class    : WorldCover class value for tree cover (default 10).

    Returns
    -------
    Same structure as scene_bands but with non-forest pixels set to NaN.
    """
    masked = []
    with rasterio.open(worldcover_path) as wc:
        for scene in scene_bands:
            tile_id, acq_date, red, nir, swir1, bbox_wkt, transform, crs = scene
            h, w = red.shape

            # Allocate destination array matching the (subsampled) band grid
            forest_mask = np.zeros((h, w), dtype=np.uint8)

            reproject(
                source=rasterio.band(wc, 1),
                destination=forest_mask,
                dst_transform=transform,
                dst_crs=crs,
                resampling=Resampling.nearest,
            )

            is_forest = (forest_mask == forest_class)
            for arr in (red, nir, swir1):
                arr[~is_forest] = np.nan

            n_forest = int(is_forest.sum())
            n_total  = h * w
            print(f"  [{tile_id}] Forest pixels: {n_forest:,} / {n_total:,} "
                  f"({100 * n_forest / max(n_total, 1):.1f}%)")

            masked.append((tile_id, acq_date, red, nir, swir1, bbox_wkt, transform, crs))

    return masked