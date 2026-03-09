from sedona.utils import SedonaContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year, expr, avg, lit
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

def run_transformation_pipeline(results, forest_data_path, aws_key, aws_secret):
    sedona = get_sedona_context(aws_key, aws_secret)
    
    # Data Handoff
    spark_input = sedona.createDataFrame(results, ["tile_id", "acquisition_date", "file_url", "ndvi_mean", "resolution", "bbox_wkt"])
    
    masked_df = apply_forest_mask(spark_input, forest_data_path)
    
    # 2. Compute Indices (Section 5.2)
    indices_df = compute_vegetation_indices(masked_df)
    
    final_df = harmonize_time_series(indices_df)
    
    return final_df
