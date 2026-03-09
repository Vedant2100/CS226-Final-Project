import os
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

import boto3
from botocore.exceptions import ClientError, NoCredentialsError


def get_s3_client(config):
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id     = config['aws_access_key_id'],
            aws_secret_access_key = config['aws_secret_access_key'],
            region_name           = config['aws_region']
        )
        # Quick connectivity test: list first page of bucket objects
        s3.list_objects_v2(Bucket=config['s3_bucket'], MaxKeys=1)
        print(f"  S3 connected: s3://{config['s3_bucket']} (region: {config['aws_region']})")
        return s3
    except NoCredentialsError:
        print("  [ERROR] Invalid AWS credentials. Check your Colab Secrets.")
        return None
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchBucket':
            print(f"  [ERROR] Bucket '{config['s3_bucket']}' does not exist.")
        else:
            print(f"  [ERROR] S3 connection failed: {e}")
        return None

def upload_cog_to_s3(local_path, config, s3_client, tile_metadata):
    year = tile_metadata.get('year', 2020)
    tile_id = tile_metadata.get('tile_id', 'unknown')
    filename = os.path.basename(local_path)

    # Build S3 key with year-based prefix
    s3_key = f"{config['s3_cog_prefix']}{year}/{tile_id}.cog.tif"
    s3_url = f"s3://{config['s3_bucket']}/{s3_key}"

    # Check if already uploaded (skip if exists)
    if config.get('skip_existing'):
        try:
            s3_client.head_object(Bucket=config['s3_bucket'], Key=s3_key)
            print(f"  [SKIP] Already in S3: {s3_url}")
            return s3_url
        except ClientError as e:
            if e.response['Error']['Code'] != '404':
                pass  # File doesn't exist, proceed with upload

    file_size_mb = os.path.getsize(local_path) / 1e6
    print(f"  Uploading {file_size_mb:.1f} MB -> {s3_url}")

    try:
        # Use upload_file for automatic multipart uploads (>8 MB)
        s3_client.upload_file(
            local_path,
            config['s3_bucket'],
            s3_key,
            ExtraArgs={
                'ContentType': 'image/tiff',
                'StorageClass': 'STANDARD',
                'Metadata': {
                    'tile-id':          tile_id,
                    'acquisition-date': str(tile_metadata.get('acquisition_date', '')),
                    'product':          tile_metadata.get('product', 'HLS'),
                }
            }
        )
        print(f"  Upload complete: {s3_url}")
        return s3_url

    except Exception as e:
        print(f"  [ERROR] S3 upload failed: {e}")
        return None

def download_from_s3(s3_key, local_dest, config, s3_client):
    local_path = os.path.join(local_dest, os.path.basename(s3_key))
    if os.path.exists(local_path):
        print(f"  [CACHE] Already downloaded: {os.path.basename(s3_key)}")
        return local_path

    try:
        obj = s3_client.head_object(Bucket=config['s3_bucket'], Key=s3_key)
        size_mb = obj['ContentLength'] / 1e6
        print(f"  Downloading {size_mb:.1f} MB from s3://{config['s3_bucket']}/{s3_key}")

        s3_client.download_file(config['s3_bucket'], s3_key, local_path)
        print(f"  Downloaded -> {local_path}")
        return local_path
    except Exception as e:
        print(f"  [ERROR] S3 download failed: {e}")
        return None

def list_s3_raw_files(config, s3_client):
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(
        Bucket=config['s3_bucket'],
        Prefix=config['s3_raw_prefix']
    )
    keys = []
    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.tif') or key.endswith('.TIF'):
                keys.append(key)
    print(f"  Found {len(keys)} raw TIF files in s3://{config['s3_bucket']}/{config['s3_raw_prefix']}")
    return keys

