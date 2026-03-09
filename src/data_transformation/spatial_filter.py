from sedona.utils import SedonaContext

def apply_forest_mask(df, forest_data_path):
    """
    Filters spectral data to include only forested pixels using a spatial join 
    with ESA WorldCover (Class 10 = Tree cover).
    """
    # Load WorldCover data using Sedona
    forest_df = sedona.read.format("geoparquet").load(forest_data_path)
    
    # Perform spatial join between pixel centroids and forest polygons
    df.createOrReplaceTempView("pixels")
    forest_df.createOrReplaceTempView("forest")
    
    masked_df = sedona.sql("""
        SELECT p.* 
        FROM pixels p, forest f
        WHERE ST_Intersects(p.geom, f.geom)
    """)
    return masked_df