from sedona.utils import SedonaContext
from pyspark.sql import SparkSession
from .indices import compute_vegetation_indices
from .spatial_filter import apply_forest_mask
from .temporal import harmonize_time_series

from sedona.register import SedonaRegistrator
from sedona.utils import SedonaContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year, expr, avg, lit

def get_sedona_context(aws_key, aws_secret):
    config = (
        SedonaContext.builder()
        .config("spark.hadoop.fs.s3a.access.key", aws_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret)
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .getOrCreate()
    )
    return SedonaContext.create(config)

def run_transformation_pipeline(results, forest_data_path, aws_key, aws_secret):
    sedona = get_sedona_context(aws_key, aws_secret)
    
    # Data Handoff
    spark_input = sedona.createDataFrame(results, ["tile_id", "acquisition_date", "file_url", "ndvi_mean", "resolution", "bbox_wkt"])
    
    # 1. Compute Indices
    indices_df = compute_vegetation_indices(spark_input)
    
    # 2. Forest Mask
    masked_df = apply_forest_mask(indices_df, forest_data_path)
    
    # 3. Temporal Harmonization
    final_df = harmonize_time_series(masked_df)
    
    return final_df
