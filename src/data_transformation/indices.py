from pyspark.sql.functions import col

def compute_vegetation_indices(df):
    """
    Computes NDVI and NDMI from Red, NIR, and SWIR1 bands using PySpark logic.
    NDVI = (NIR - Red) / (NIR + Red)
    NDMI = (NIR - SWIR1) / (NIR + SWIR1)
    """
    return df.withColumn("ndvi", (col("nir") - col("red")) / (col("nir") + col("red"))) \
             .withColumn("ndmi", (col("nir") - col("swir1")) / (col("nir") + col("swir1")))