import psycopg2
from psycopg2 import sql
import datetime

import psycopg2
from psycopg2 import sql
import datetime

def get_db_connection(config):
    db_config = config['db_config']
    try:
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password'],
            connect_timeout=db_config.get('connect_timeout', 10)
        )
        conn.autocommit = False   # Use explicit transactions

        # Test PostGIS availability
        cur = conn.cursor()
        cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        cur.execute("SELECT PostGIS_Version();")
        postgis_ver = cur.fetchone()[0]
        cur.execute("SELECT version();")
        pg_ver = cur.fetchone()[0].split(' ')[1]

        print(f"  Database connected successfully!")
        print(f"  PostgreSQL version: {pg_ver}")
        print(f"  PostGIS version: {postgis_ver.split(' ')[0]}")
        cur.close()
        return conn

    except psycopg2.OperationalError as e:
        print(f"  [ERROR] Cannot connect to database: {e}")
        print(f"  Check: 1) RDS endpoint correct? 2) Security group allows port 5432?")
        print(f"         3) RDS instance is 'Available' in AWS console?")
        return None

def setup_database(conn):
    cur = conn.cursor()

    # Step 1: Enable PostGIS extensions
    print("  Setting up PostGIS extensions...")
    cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
    cur.execute("CREATE EXTENSION IF NOT EXISTS postgis_raster;")
    conn.commit()

    # Step 2: Create main metadata table
    # This is the "Spatiotemporal Data Cube Index" from Section 4.4:
    #   - Space:  geom (GEOMETRY Polygon in WGS84)
    #   - Time:   acquisition_date (DATE)
    #   - Link:   file_url (S3 URL or local path to COG)
    print("  Creating vegetation_metadata table...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS vegetation_metadata (
            id              SERIAL PRIMARY KEY,
            tile_id         TEXT NOT NULL,
            acquisition_date DATE NOT NULL,
            cloud_cover     FLOAT,
            ndvi_mean       FLOAT,
            ndvi_std        FLOAT,
            ndmi_mean       FLOAT,
            crs             TEXT,
            width_px        INTEGER,
            height_px       INTEGER,
            resolution_m    FLOAT,
            band_count      INTEGER,
            compression     TEXT,
            has_overviews   BOOLEAN,
            source          TEXT DEFAULT 'HLS',
            file_url        TEXT,           -- S3 URL or Drive path
            geom            GEOMETRY(Polygon, 4326),  -- WGS84 bounding box polygon
            inserted_at     TIMESTAMP DEFAULT NOW(),
            UNIQUE(tile_id)                -- Prevent duplicate scene insertions
        );
    """)
    conn.commit()

    # Step 3: Create GIST spatial index
    # This enables fast ST_Intersects queries (the core of the spatiotemporal filter)
    # Equivalent to the "CREATE INDEX ... USING GIST" in Section 4.3 of project outline
    print("  Creating GIST spatial index (idx_vegetation_geom)...")
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_vegetation_geom
            ON vegetation_metadata
            USING GIST (geom);
    """)

    # Step 4: Create temporal B-tree index
    # Enables fast date range queries: WHERE acquisition_date BETWEEN '2020-01' AND '2020-12'
    print("  Creating temporal index (idx_vegetation_date)...")
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_vegetation_date
            ON vegetation_metadata (acquisition_date);
    """)

    # Step 5: Create composite index for tile+date lookups
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_vegetation_tile_date
            ON vegetation_metadata (tile_id, acquisition_date);
    """)

    conn.commit()
    print("  Database schema initialized!")

    # Verify setup
    cur.execute("""
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE tablename = 'vegetation_metadata'
        ORDER BY indexname;
    """)
    indexes = cur.fetchall()
    print(f"  Indexes created: {len(indexes)}")
    for name, defn in indexes:
        print(f"    - {name}")

    cur.close()
    return True

def insert_scene_metadata(conn, tile_meta, raster_meta, file_url, config):
    cur = conn.cursor()

    bbox = raster_meta.get('bbox_wgs84', {})
    west  = bbox.get('west',  0.0)
    south = bbox.get('south', 0.0)
    east  = bbox.get('east',  0.0)
    north = bbox.get('north', 0.0)

    try:
        cur.execute("""
            INSERT INTO vegetation_metadata (
                tile_id, acquisition_date, cloud_cover,
                ndvi_mean, ndvi_std, ndmi_mean,
                crs, width_px, height_px, resolution_m,
                band_count, compression, has_overviews,
                source, file_url,
                geom
            ) VALUES (
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                ST_MakeEnvelope(%s, %s, %s, %s, 4326)
            )
            ON CONFLICT (tile_id) DO UPDATE SET
                file_url        = EXCLUDED.file_url,
                ndvi_mean       = EXCLUDED.ndvi_mean,
                ndvi_std        = EXCLUDED.ndvi_std,
                has_overviews   = EXCLUDED.has_overviews,
                inserted_at     = NOW();
        """, (
            tile_meta.get('tile_id'),
            tile_meta.get('acquisition_date'),
            None,                                   # cloud_cover (not yet computed)
            raster_meta.get('ndvi_mean'),
            raster_meta.get('ndvi_std'),
            raster_meta.get('ndmi_mean'),
            raster_meta.get('crs'),
            raster_meta.get('width_px'),
            raster_meta.get('height_px'),
            raster_meta.get('resolution_m'),
            raster_meta.get('band_count'),
            raster_meta.get('compression'),
            raster_meta.get('has_overviews'),
            tile_meta.get('product', 'HLS'),
            file_url,
            west, south, east, north
        ))
        conn.commit()
        cur.close()
        return True

    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        print(f"  [SKIP] Duplicate: {tile_meta.get('tile_id')} already in DB")
        cur.close()
        return False
    except Exception as e:
        conn.rollback()
        print(f"  [ERROR] Insert failed: {e}")
        cur.close()
        return False
def run_spatiotemporal_query(conn, west, south, east, north,
                              start_date, end_date):
    cur = conn.cursor()
    cur.execute("""
        SELECT
            tile_id,
            acquisition_date,
            file_url,
            ndvi_mean,
            resolution_m,
            ST_AsText(geom) AS bbox_wkt
        FROM vegetation_metadata
        WHERE
            ST_Intersects(
                geom,
                ST_MakeEnvelope(%s, %s, %s, %s, 4326)
            )
            AND acquisition_date BETWEEN %s AND %s
        ORDER BY acquisition_date;
    """, (west, south, east, north, start_date, end_date))

    results = cur.fetchall()
    cur.close()
    return results

