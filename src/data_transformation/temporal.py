from pyspark.sql.functions import year, month, avg

def harmonize_time_series(df):
    """
    Aggregates irregular observations into monthly composites.
    Returns a regular time series for each pixel.
    """
    monthly_df = df.groupBy("pixel_id", year("date").alias("year"), month("date").alias("month")) \
                   .agg(avg("ndvi").alias("ndvi_mean"), avg("ndmi").alias("ndmi_mean")) \
                   .orderBy("year", "month")
    
    return monthly_df