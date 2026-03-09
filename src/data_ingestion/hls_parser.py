import os
import re
import datetime
import rasterio
from rasterio.warp import transform_bounds
from rasterio.crs import CRS

import re
import datetime
import numpy as np
import rasterio
from rasterio.transform import array_bounds
from rasterio.warp import transform_bounds
from rasterio.crs import CRS
from shapely.geometry import box as shapely_box

def parse_hls_filename(filepath):
    filename = os.path.basename(filepath)
    name_no_ext = filename.replace('.tif', '').replace('.TIF', '')

    # Try to match HLS naming convention
    # Pattern: HLS.{S30|L30}.{tile}.{YYYY}{DOY}T{HHMMSS}.{version}[.{band}]
    hls_pattern = r'^HLS\\.([SL]30)\\.([A-Z0-9]+)\\.(\\d{4})(\\d{3})T(\\d{6})\\.([\\d\\.v]+)(?:\\.([A-Z0-9_]+))?$'
    match = re.match(hls_pattern, name_no_ext)

    if match:
        product   = match.group(1)
        tile      = match.group(2)
        year      = int(match.group(3))
        doy       = int(match.group(4))
        time_str  = match.group(5)
        version   = match.group(6)
        band      = match.group(7)

        acq_date = datetime.date(year, 1, 1) + datetime.timedelta(days=doy - 1)

        # Build tile_id that includes product and tile for uniqueness
        tile_id = f"{product}_{tile}_{year}{doy:03d}"

        return {
            'tile_id':          tile_id,
            'acquisition_date': acq_date,
            'product':          product,          # S30=Sentinel-2, L30=Landsat
            'tile':             tile,
            'band':             band or 'MULTI',
            'version':          version,
            'year':             year,
            'doy':              doy,
            'filename':         filename,
        }
    else:
        print(f"  [WARN] Non-HLS filename: {filename}. Using fallback metadata.")
        # Try to extract year from any 4-digit year in filename
        year_match = re.search(r'(20[12][0-9])', filename)
        year = int(year_match.group(1)) if year_match else 2020
        return {
            'tile_id':          name_no_ext,
            'acquisition_date': datetime.date(int(datetime.datetime.now().year), 1, 1),
            'product':          'UNKNOWN',
            'tile':             name_no_ext,
            'band':             'MULTI',
            'version':          'unknown',
            'year':             year,
            'doy':              1,
            'filename':         filename,
        }

def extract_raster_metadata(filepath):
    metadata = {}

    with rasterio.open(filepath) as src:
        metadata['crs']        = str(src.crs)
        metadata['width_px']   = src.width
        metadata['height_px']  = src.height
        metadata['band_count'] = src.count
        metadata['resolution_m'] = abs(src.res[0])

        # Reproject bounds to WGS84 for PostGIS
        src_bounds = src.bounds
        if src.crs and str(src.crs) != 'EPSG:4326':
            try:
                wgs84_bounds = transform_bounds(
                    src.crs, CRS.from_epsg(4326),
                    src_bounds.left, src_bounds.bottom,
                    src_bounds.right, src_bounds.top
                )
                metadata['bbox_wgs84'] = {
                    'west':  wgs84_bounds[0],
                    'south': wgs84_bounds[1],
                    'east':  wgs84_bounds[2],
                    'north': wgs84_bounds[3],
                }
            except Exception as e:
                print(f"  [WARN] CRS reprojection failed: {e}. Using native bounds.")
                metadata['bbox_wgs84'] = {
                    'west':  src_bounds.left,  'south': src_bounds.bottom,
                    'east':  src_bounds.right, 'north': src_bounds.top,
                }
        else:
            metadata['bbox_wgs84'] = {
                'west':  src_bounds.left,  'south': src_bounds.bottom,
                'east':  src_bounds.right, 'north': src_bounds.top,
            }

        metadata['is_tiled']      = src.is_tiled
        metadata['compression']   = str(src.compression) if src.compression else 'None'
        metadata['has_overviews'] = len(src.overviews(1)) > 0

    return metadata



