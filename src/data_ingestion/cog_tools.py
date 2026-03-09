import subprocess
import time
import os
import rasterio
import numpy as np

import subprocess
import time
import os
import rasterio
from rasterio.enums import Resampling

def validate_cog(filepath):
    result = {
        'is_valid': False,
        'is_tiled': False,
        'has_overviews': False,
        'compression': None,
        'issues': []
    }

    try:
        with rasterio.open(filepath) as src:
            result['is_tiled']     = src.is_tiled
            result['compression']  = str(src.compression) if src.compression else 'None'
            result['overviews']    = src.overviews(1)
            result['has_overviews'] = len(src.overviews(1)) > 0
            result['width']        = src.width
            result['height']       = src.height

            # Check for COG ghost metadata
            tags = src.tags()
            result['has_cog_marker'] = 'OVR_RESAMPLING_ALG' in tags or src.is_tiled

            if not result['is_tiled']:
                result['issues'].append("Not internally tiled (required for efficient S3 reads)")
            if not result['has_overviews']:
                result['issues'].append("No overviews (partial zoom reads will be slow)")
            if result['compression'] in ['None', None]:
                result['issues'].append("No compression (wastes S3 storage and transfer costs)")

        result['is_valid'] = len(result['issues']) == 0
    except Exception as e:
        result['issues'].append(f"Cannot open file: {e}")

    return result

def convert_to_cog(input_path, output_path, config):
    if not os.path.exists(input_path):
        return {'success': False, 'error': f"Input file not found: {input_path}"}

    compress  = config.get('cog_compress', 'LZW')
    blocksize = config.get('cog_blocksize', 512)

    cmd = [
        'gdal_translate', '-of', 'COG',
        '-co', f'COMPRESS={compress}',
        '-co', f'BLOCKSIZE={blocksize}',
        '-co', 'OVERVIEWS=AUTO',
        '-co', 'RESAMPLING=AVERAGE',
        '-co', 'BIGTIFF=IF_SAFER',
        '--config', 'GDAL_TIFF_OVR_BLOCKSIZE', str(blocksize),
        input_path, output_path
    ]

    print(f"  Converting to COG: {os.path.basename(input_path)}")
    start_time = time.time()

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        duration = round(time.time() - start_time, 2)

        if proc.returncode != 0:
            print(f"  [ERROR] {proc.stderr[:200]}")
            return {'success': False, 'error': proc.stderr}

        input_mb  = os.path.getsize(input_path)  / 1e6
        output_mb = os.path.getsize(output_path) / 1e6
        print(f"  Done in {duration}s | {input_mb:.1f} MB -> {output_mb:.1f} MB "
              f"({input_mb/output_mb:.1f}x compression)")

        return {
            'success': True,
            'output_path': output_path,
            'duration_sec': duration,
            'input_size_mb': round(input_mb, 2),
            'file_size_mb': round(output_mb, 2),
        }

    except subprocess.TimeoutExpired:
        return {'success': False, 'error': 'Conversion timed out after 5 minutes'}
    except Exception as e:
        return {'success': False, 'error': str(e)}
def benchmark_cog_vs_original(original_path, cog_path, n_trials=3):
    results = {}

    for label, path in [('Original', original_path), ('COG', cog_path)]:
        if not os.path.exists(path):
            continue
        times = []
        with rasterio.open(path) as src:
            w, h = src.width, src.height
            # Read a 500x500 window from center
            window = rasterio.windows.Window(
                col_off=w // 4, row_off=h // 4,
                width=min(500, w // 2), height=min(500, h // 2)
            )
            for _ in range(n_trials):
                t0 = time.time()
                _ = src.read(1, window=window)
                times.append(time.time() - t0)

        results[label] = {
            'mean_ms': round(np.mean(times) * 1000, 1),
            'min_ms':  round(np.min(times) * 1000, 1),
        }

    if 'Original' in results and 'COG' in results:
        speedup = results['Original']['mean_ms'] / results['COG']['mean_ms']
        print(f"  Benchmark Results ({n_trials} trials):")
        print(f"    Original: {results['Original']['mean_ms']:.1f}ms avg")
        print(f"    COG:      {results['COG']['mean_ms']:.1f}ms avg")
        print(f"    Speedup:  {speedup:.1f}x faster with COG")
        print(f"  (On S3, this speedup is typically 10-50x for partial reads)")

    return results

