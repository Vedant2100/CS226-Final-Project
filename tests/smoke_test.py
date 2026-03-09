import os
import sys
import datetime

# Add src to path if running from root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.data_ingestion.pipeline import run_pipeline
from src.data_transformation.processor import run_transformation_pipeline

def run_small_test():
    # 1. Mock minimal config
    # Note: User must replace these with real credentials or env vars
    config = {
        'phase': 1,
        'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID', 'YOUR_KEY'),
        'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY', 'YOUR_SECRET'),
        's3_bucket': os.getenv('S3_BUCKET_NAME', 'YOUR_BUCKET'),
        's3_raw_prefix': 'raw/',
        'db_config': {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': 5432,
            'database': 'postgres',
            'user': 'postgres',
            'password': os.getenv('DB_PASSWORD', 'password')
        },
        'local_work_dir': '/tmp/test_pipeline',
        'max_files': 1,
        'upload_to_s3': False
    }

    print("--- [STARTING SMOKE TEST] ---")
    
    # Run Ingestion
    print("\n[STEP 1] Running Ingestion (Max 1 file)...")
    summary = run_pipeline(config)
    print(f"Ingestion Finished: {summary['processed']} scenes processed.")

    # 2. Simulate Handoff (or actual query)
    # We create a dummy result for transformation if ingestion was dry-run
    sample_results = [
        ('S2_SAMPLE_001', datetime.date(2020, 6, 1), 's3://bucket/cog/sample.tif', 0.45, 10.0, 'POLYGON(...)')
    ]

    # Run Transformation
    print("\n[STEP 2] Running Transformation (Spark/Sedona)...")
    try:
        final_df = run_transformation_pipeline(
            sample_results, 
            forest_data_path="s3a://your-bucket/masks/forest.geoparquet",
            aws_key=config['aws_access_key_id'],
            aws_secret=config['aws_secret_access_key']
        )
        print("Transformation Pipeline Initialized Successfully.")
        # final_df.show(5) # Requires spark session to be active
    except Exception as e:
        print(f"Transformation skipped or failed: {e}")
        print("Note: Transformation requires a valid Spark/Sedona environment and S3 access.")

    print("\n--- [SMOKE TEST COMPLETE] ---")

if __name__ == "__main__":
    run_small_test()
