import os, boto3, pandas as pd
from io import BytesIO

AWS_REGION = os.environ.get("AWS_REGION", "us-west-1")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "vegetation-anomaly-cogs")
s3 = boto3.client("s3", region_name=AWS_REGION)

resp = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="results/", Delimiter="/")
run_folders = sorted([p["Prefix"] for p in resp.get("CommonPrefixes", []) if p["Prefix"] != "results/"], reverse=True)
print("Run folders:", run_folders)

latest = run_folders[0]
print("Latest run:", latest)

anomaly_prefix = f"{latest}anomaly_events/"
all_files = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=anomaly_prefix)
files = [obj["Key"] for obj in all_files.get("Contents", [])]
print("Files in anomaly_events/:", files)

for key in files:
    if key.endswith(".parquet"):
        df = pd.read_parquet(BytesIO(s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)["Body"].read()))
        print("\nFile:", key)
        print("Shape:", df.shape)
        print("Columns:", df.columns.tolist())
        print("First 5 rows:\n", df.head(5).to_string())