import glob
import traceback
import datetime
import os

from .hls_parser import parse_hls_filename, extract_raster_metadata
from .s3_manager import get_s3_client, upload_cog_to_s3, download_from_s3, list_s3_raw_files
from .db_manager import get_db_connection, setup_database, insert_scene_metadata
from .cog_tools import validate_cog, convert_to_cog, benchmark_cog_vs_original

import glob
import traceback
import datetime

def run_pipeline(config):
    phase_label = 'Google Drive' if config['phase'] == 1 else 'AWS S3'
    print(f"{'='*65}\n  PIPELINE | Phase {config['phase']}: {phase_label} -> COG -> S3 + PostGIS\n{'='*65}")

    summary = {'total_found': 0, 'processed': 0, 'skipped': 0, 'failed': 0,
               'total_size_mb': 0, 'start_time': datetime.datetime.now()}

    # --- Connections ---
    s3_client = None
    if config.get('upload_to_s3') or config['phase'] == 2:
        s3_client = get_s3_client(config)
        if s3_client is None and config['phase'] == 2:
            print("[FATAL] Phase 2 requires S3. Aborting.")
            return summary

    conn = get_db_connection(config)
    if conn is None:
        print("[FATAL] Cannot connect to database. Aborting.")
        return summary

    setup_database(conn)

    # --- Discover files ---
    if config['phase'] == 1:
        source_dir = config['drive_source_dir']
        if not os.path.exists(source_dir):
            print(f"[ERROR] Drive source not found: {source_dir}")
            conn.close()
            return summary
        input_files = sorted(glob.glob(os.path.join(source_dir, '*.tif')) +
                             glob.glob(os.path.join(source_dir, '*.TIF')))
        for f in input_files:
            print(f"  {os.path.basename(f)} ({os.path.getsize(f)/1e6:.1f} MB)")
    else:
        input_files = list_s3_raw_files(config, s3_client)

    summary['total_found'] = len(input_files)
    print(f"  Total files to process: {len(input_files)}")

    if not input_files:
        conn.close()
        return summary

    # --- Process each file ---
    for idx, input_item in enumerate(input_files, 1):
        print(f"\n[{idx}/{len(input_files)}] {os.path.basename(str(input_item))}")
        local_raw_path = local_cog_path = None
        cleanup_raw = cleanup_cog = False

        try:
            # Get local file
            if config['phase'] == 1:
                local_raw_path = input_item
            else:
                local_raw_path = download_from_s3(input_item, config['local_work_dir'], config, s3_client)
                cleanup_raw = True

            if not local_raw_path or not os.path.exists(local_raw_path):
                summary['failed'] += 1
                continue

            # Parse filename
            tile_meta = parse_hls_filename(local_raw_path)
            tile_id   = tile_meta['tile_id']
            print(f"  {tile_id} | {tile_meta['acquisition_date']} | {tile_meta['product']}")

            # Skip if already indexed
            if config.get('skip_existing'):
                cur = conn.cursor()
                cur.execute("SELECT id FROM vegetation_metadata WHERE tile_id = %s", (tile_id,))
                if cur.fetchone():
                    print(f"  [SKIP] Already indexed")
                    summary['skipped'] += 1
                    cur.close()
                    continue
                cur.close()

            # Convert to COG if needed
            cog_info = validate_cog(local_raw_path)
            if cog_info['is_valid']:
                local_cog_path = local_raw_path
            else:
                print(f"  Issues: {'; '.join(cog_info['issues'])}")
                cog_filename   = os.path.basename(local_raw_path).replace('.tif', '.cog.tif')
                local_cog_path = os.path.join(config['local_work_dir'], cog_filename)
                cleanup_cog    = True
                conv_result    = convert_to_cog(local_raw_path, local_cog_path, config)
                if not conv_result['success']:
                    print(f"  [ERROR] {conv_result['error']}")
                    summary['failed'] += 1
                    continue
                benchmark_cog_vs_original(local_raw_path, local_cog_path, n_trials=2)

            # Extract metadata
            raster_meta = extract_raster_metadata(local_cog_path)
            bbox = raster_meta['bbox_wgs84']
            print(f"  BBox: W={bbox['west']:.4f} S={bbox['south']:.4f} "
                  f"E={bbox['east']:.4f} N={bbox['north']:.4f} | "
                  f"{raster_meta['width_px']}x{raster_meta['height_px']}px | "
                  f"{raster_meta['resolution_m']}m")

            # Upload to S3
            file_url = local_cog_path
            if config.get('upload_to_s3') and s3_client:
                file_url = upload_cog_to_s3(local_cog_path, config, s3_client, tile_meta) or local_cog_path

            # Index in PostGIS
            if insert_scene_metadata(conn, tile_meta, raster_meta, file_url,config):
                summary['processed']     += 1
                summary['total_size_mb'] += os.path.getsize(local_cog_path) / 1e6
                print(f"  [OK] {tile_id}")
            else:
                summary['failed'] += 1

        except Exception as e:
            print(f"  [ERROR] {e}")
            traceback.print_exc()
            summary['failed'] += 1
            if conn and not conn.closed:
                conn.rollback()

        finally:
            if cleanup_raw and local_raw_path and os.path.exists(local_raw_path):
                os.remove(local_raw_path)
            if cleanup_cog and local_cog_path and os.path.exists(local_cog_path):
                os.remove(local_cog_path)

    # --- Summary ---
    duration = (datetime.datetime.now() - summary['start_time']).total_seconds()
    print(f"\n{'='*65}\n  DONE in {duration:.1f}s | "
          f"Processed: {summary['processed']} | "
          f"Skipped: {summary['skipped']} | "
          f"Failed: {summary['failed']}\n{'='*65}")

    if conn and not conn.closed:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*), MIN(acquisition_date), MAX(acquisition_date) FROM vegetation_metadata")
        count, min_d, max_d = cur.fetchone()
        print(f"  DB: {count} scenes indexed | {min_d} to {max_d}")
        cur.close()
        conn.close()

    return summary

