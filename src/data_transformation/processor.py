import numpy as np
import boto3
import rasterio
from rasterio.session import AWSSession
from rasterio.transform import Affine

from sedona.utils import SedonaContext
from pyspark.sql.functions import col, month, year, avg

from .indices import compute_vegetation_indices
from .spatial_filter import apply_forest_mask
from .temporal import harmonize_time_series


def get_sedona_context(aws_key, aws_secret):
    config = (
        SedonaContext.builder()
        .config("spark.hadoop.fs.s3a.access.key", aws_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret)
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .getOrCreate()
    )
    return SedonaContext.create(config)


def read_bands_from_s3(results, aws_key, aws_secret, region="us-west-1",
                       red_band=3, nir_band=4, swir1_band=5, pixel_stride=10):
    """
    Reads Red, NIR, and SWIR1 band arrays directly from S3 COGs for each scene
    in `results`. No local download — rasterio streams the data via AWSSession.

    Parameters
    ----------
    results      : list of (tile_id, acq_date, file_url, ndvi_mean, resolution_m, bbox_wkt)
                   as returned by run_spatiotemporal_query().
    aws_key      : AWS_ACCESS_KEY_ID
    aws_secret   : AWS_SECRET_ACCESS_KEY
    region       : AWS region of your S3 bucket (default 'us-west-1')
    red_band     : rasterio band index for Red   (default 3 for S2_SR_HARMONIZED)
    nir_band     : rasterio band index for NIR   (default 4)
    swir1_band   : rasterio band index for SWIR1 (default 5)
    pixel_stride : subsample every N-th pixel to keep the Spark DataFrame manageable

    Returns
    -------
    list of (tile_id, acq_date, red, nir, swir1, bbox_wkt, strided_transform, crs_str)
        red/nir/swir1 are 2-D float32 numpy arrays (subsampled).
        strided_transform is the affine transform adjusted for pixel_stride.
        crs_str is the CRS as a string (for rasterio.open / reproject).
    """
    boto3_session = boto3.Session(
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
        region_name=region,
    )
    aws_session = AWSSession(boto3_session)

    scene_bands = []
    for tile_id, acq_date, file_url, _, _res, bbox_wkt in results:
        print(f"  Reading {tile_id}  ->  {file_url}")
        with rasterio.Env(aws_session):
            with rasterio.open(file_url) as src:
                n = src.count
                print(f"    bands={n}  shape={src.height}x{src.width}  crs={src.crs}")

                # Clamp band indices in case the file has fewer bands
                rb = min(red_band,   n)
                nb = min(nir_band,   n)
                sb = min(swir1_band, n)

                red   = src.read(rb)[::pixel_stride, ::pixel_stride].astype("float32")
                nir   = src.read(nb)[::pixel_stride, ::pixel_stride].astype("float32")
                swir1 = src.read(sb)[::pixel_stride, ::pixel_stride].astype("float32")

                # Replace nodata / 0 with NaN so index math is clean
                nodata = src.nodata if src.nodata is not None else 0
                for arr in (red, nir, swir1):
                    arr[arr == nodata] = np.nan

                # Adjust affine transform for the pixel stride so that
                # apply_forest_mask can reproject WorldCover onto this grid
                t = src.transform
                strided_transform = Affine(
                    t.a * pixel_stride, t.b, t.c,
                    t.d, t.e * pixel_stride, t.f,
                )
                crs_str = src.crs

        print(f"    red   [{np.nanmin(red):.0f}, {np.nanmax(red):.0f}]")
        print(f"    nir   [{np.nanmin(nir):.0f}, {np.nanmax(nir):.0f}]")
        print(f"    swir1 [{np.nanmin(swir1):.0f}, {np.nanmax(swir1):.0f}]")
        scene_bands.append((tile_id, acq_date, red, nir, swir1, bbox_wkt,
                            strided_transform, crs_str))

    return scene_bands


def build_pixel_dataframe(sedona, scene_bands):
    """
    Flattens per-scene band arrays into a Spark DataFrame with one row per pixel.

    Columns: tile_id, date, red, nir, swir1, pixel_id, bbox_wkt
    NaN pixels (including non-forest pixels zeroed by apply_forest_mask) are dropped.

    Accepts both 6-tuple (tile_id, date, red, nir, swir1, bbox_wkt) and
    8-tuple (... , transform, crs) from read_bands_from_s3 — extras are ignored.
    """
    pixel_rows = []
    for scene in scene_bands:
        tile_id, acq_date, red, nir, swir1, bbox_wkt = scene[:6]
        h, w = red.shape
        for r in range(h):
            for c in range(w):
                rv, nv, sv = float(red[r, c]), float(nir[r, c]), float(swir1[r, c])
                if rv != rv or nv != nv or sv != sv:   # NaN check
                    continue
                pixel_rows.append((
                    tile_id, acq_date,
                    rv, nv, sv,
                    f"{tile_id}_{r}_{c}",
                    bbox_wkt,
                ))

    print(f"  Pixel rows (non-NaN): {len(pixel_rows):,}")
    return sedona.createDataFrame(
        pixel_rows,
        ["tile_id", "date", "red", "nir", "swir1", "pixel_id", "bbox_wkt"],
    )


def run_transformation_pipeline(results, forest_data_path, aws_key, aws_secret,
                                 region="us-west-1", pixel_stride=10):
    """
    End-to-end transformation pipeline.

    1. Read band arrays directly from S3 COGs (no download).
    2. Apply ESA WorldCover forest mask on numpy arrays.
    3. Flatten masked arrays to pixel-level Spark DataFrame.
    4. Compute NDVI / NDMI.
    5. Aggregate to monthly composites.

    Parameters
    ----------
    results          : query results from run_spatiotemporal_query()
    forest_data_path : local path to the WorldCover GeoTIFF
    aws_key / secret : AWS credentials
    region           : AWS region
    pixel_stride     : subsample stride when reading bands (1 = full res)
    """
    sedona = get_sedona_context(aws_key, aws_secret)

    # Step 1 — read bands from S3 (returns 8-tuples with transform + crs)
    print("[1/4] Reading bands from S3...")
    scene_bands = read_bands_from_s3(
        results, aws_key, aws_secret,
        region=region, pixel_stride=pixel_stride,
    )

    # Step 2 — forest mask on numpy arrays (before Spark)
    print("[2/4] Applying forest mask...")
    masked_bands = apply_forest_mask(scene_bands, forest_data_path)

    # Step 3 — build pixel DataFrame from masked arrays
    print("[3/4] Building pixel DataFrame...")
    spark_df = build_pixel_dataframe(sedona, masked_bands)

    # Step 4 — vegetation indices + temporal harmonization
    print("[4/4] Computing indices and harmonizing time series...")
    indices_df = compute_vegetation_indices(spark_df)
    final_df   = harmonize_time_series(indices_df)

    return final_df
