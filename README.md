# CS226-Final-Project
This project implements a data pipeline for ingesting, transforming, and analyzing vegetation data, with a focus on efficient processing and detection workflows.

## Project Title
Scalable Analysis of Vegetation Anomalies Preceding Plant Disease Outbreaks

## Group Name
CS226 Final Project Group

## Group Number
4

## Members
- **THRISHA AMBAREESHARAJE URS URS** | 862638215 | University of California, Riverside, USA
- **SOHUM DAMANI** | 862621529 | University of California, Riverside, USA
- **YASHASWINI DIGGAVI** | 862620058 | University of California, Riverside, USA
- **VEDANT BORKUTE** | 862552981 | University of California, Riverside, USA
- **SHREYANGSHU BERA** | 862485337 | University of California, Riverside, USA

## Authorship Contribution
This section specifies the responsibilities and work performed by each team member for the project deliverables.

• Thrisha Ambareesharaje Urs Urs: I initiated the data acquisition phase of the project by identifying and defining the study area, including determining the bounding box coordinates for Boulder County. I was responsible for collecting and preparing the satellite datasets using Google Earth Engine, initially experimenting with higher resolutions (10m and 30m). Due to export constraints ( 50 MB per task), I adapted the approach by optimizing to a 52m resolution, enabling efficient data extraction and transfer to AWS S3 while preserving analytical utility. I also contributed to acquiring and processing the ESA WorldCover dataset for the Boulder County region, which was later used for forest masking and anomaly detection. Additionally, I was responsible for implementing the project report in the ACM format and ensured clarity, coherence, and consistency across all sections of the final document.

• Sohum Damani: I established the technical project foundation through my design work on the AWS environment and my development of a data pipeline that processed the 20m Sentinel dataset. My research work involved creating Cloud Optimized GeoTIFF (COG) image format conversion methods which improved data accessibility and developed a PostgreSQL indexing method that enabled faster query execution through its advanced indexing capabilities. I worked together with the team to assess model performance in three different operational modes while I helped the team to design the final project presentation which effectively communicated all technical project milestones.

• Yashaswini Diggavi: I designed and implemented the complete Flask-based web dashboard for real-time vegetation anomaly visualization, including fetching and processing results directly from AWS S3 across three spatial resolutions (20m, 30m, 50m). I built the interactive frontend including monthly trend charts, Z-score histograms, a Leaflet-based geographic anomaly map. In addition, I contributed to the final project report by reviewing and updating multiple sections and adding result figures including scatter plots, monthly trend charts, and Z-score distribution plots.

• Vedant Borkute: I contributed to the initial research on dataset sources and experimented with timelapse generation. I helped establish the project repositories and reports and report outlines, and implementing the data transformation pipeline, the evaluation framework, and the generation of output products such as the timelapses, the analytic tables and the anomaly detection run metadata. I also proposed the study area for the pipeline and coordinated the integration of results and documentation across the team.

• Shreyangshu Bera: I contributed in designing and implementing the Z-score-based anomaly detection logic, including the statistical normalization of pixel-level vegetation indices against historical baselines and the calibration of the anomaly threshold from −2.0 to −1.5 to improve sensitivity toward early-stage vegetation stress. In addition, I assisted with the data acquisition pipeline by exporting satellite imagery from Google Earth Engine to local storage and subsequently uploading the processed scenes to the AWS S3 server.

### Source Code Contribution
- **Data Ingestion Notebooks:** Sohum Damani
- **Data Transformation:** Vedant Borkute
- **Anomaly Detection Section in Data Transformation:** Shreyangshu Bera
- **Vegetation UI:** Yashaswini Diggavi

## Overview

This pipeline ingests Sentinel-2 and Landsat satellite imagery into a cloud-native storage and indexing system built on AWS S3 (Cloud Optimized GeoTIFF) and PostgreSQL/PostGIS. It supports three independent datasets (50m, 30m, 20m resolution) that feed the downstream PySpark transformation and anomaly detection stages.

---

## Prerequisites

Before running, ensure the following are set up:

**Colab Secrets** (click the 🔑 icon in the left sidebar, toggle "Notebook access" ON for each):

| Secret | Description |
|--------|-------------|
| `AWS_ACCESS_KEY_ID` | IAM user access key |
| `AWS_SECRET_ACCESS_KEY` | IAM user secret key |
| `S3_BUCKET_NAME` | e.g. `vegetation-anomaly-cogs` |
| `DB_HOST` | RDS endpoint URL |
| `DB_PASSWORD` | RDS postgres user password |

**AWS Resources** (must exist before running):
- S3 bucket with folders: `50m_resolution/`, `20m_resolution/`, `cog_30m/`
- RDS PostgreSQL instance (db.t3.micro) with public access enabled
- Security group allowing inbound TCP 5432

---

## How to Run (for both Data Ingestion and Data Transformation)


Execute the cells in order. Each cell is self-contained and prints its status on completion.

### Step 1 — Secrets Check (Cell 1)
Verifies all 5 required secrets are present. Fix any missing secrets before proceeding.

### Step 2 — Install Dependencies (Cell 2)
```
!apt-get install -y gdal-bin libgdal-dev
!pip install rasterio==1.3.10 psycopg2-binary boto3 shapely numpy
```
Run once per Colab session. Skip on subsequent runs if the runtime is still active.

### Step 3 — AWS + DB Config (Cell 3)
Loads credentials and defines `LOCAL_WORK_DIR`. No edits needed unless changing region or bucket.

### Step 4 — Load Functions (Cells 4–8)
Run Cells 4, 5, 6, 7, and 8 in order to load all parsing, COG, S3, PostGIS, and pipeline functions. These cells define functions only — nothing executes until Cell 9.

### Step 5 — Run Ingestion (Cells 9, 10, 11)
Each cell runs the pipeline for one dataset. Run them independently or sequentially.

| Cell | Dataset | Resolution | S3 Prefix | PostGIS Tag |
|------|---------|-----------|-----------|-------------|
| Cell 9 | Dataset A | 50m | `50m_resolution/` | `50m_legacy` |
| Cell 10 | Dataset C | 20m | `20m_resolution/` | `20m_sentinel2` |
| Cell 11 | Dataset B | 30m | `cog_30m/` | `30m_sentinel2` |

To re-process existing scenes (e.g. to repair bounding boxes), set `'skip_existing': False` in the relevant CONFIG before running.

### Step 6 — Verify (Cell 12)
Runs three PostGIS verification queries:
- Boulder County spatiotemporal query using `ST_Intersects`
- `EXPLAIN ANALYZE` to confirm GIST index is active
- Scene count grouped by dataset and year

### Step 7 — GEE Export (Cells 13–15, Dataset C only)
Only needed when re-exporting 20m Sentinel-2 scenes from Google Earth Engine.

```
Cell 13 — Install earthengine-api + geemap
Cell 14 — Authenticate GEE (browser popup required)
Cell 15 — Export → tile → merge → upload to S3
```

GEE exports Boulder County in a 4×3 grid of 12 tiles to stay under the 50MB API limit. Tiles are merged using `rasterio.merge` with `nodata=-9999`.

---

## How to Run Data Transformation

Execute the cells in order in the `data_transformation_and_detection.ipynb` notebook. Each cell prints its status on completion.

### Step 1 — Secrets Check
Ensure all required Colab secrets are set: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `S3_BUCKET_NAME`, `DB_HOST`, `DB_PASSWORD`.

### Step 2 — Install Dependencies
Run the installation cell to install all required packages (GDAL, rasterio, pyspark, etc.).

### Step 3 — Configure Parameters
Set evaluation window, S3/DB credentials, and pipeline parameters in the Params cell.

### Step 4 — Initialize Spark & Download WorldCover
Run the Spark initialization and WorldCover download cells.

### Step 5 — Fetch Scenes
Choose scene fetch mode (`serial`, `concurrent`, or `query`). Run the cell to fetch scene metadata from S3 or PostGIS.

### Step 6 — Data Transformation
Run the transformation cell to apply forest masking, compute vegetation indices, and aggregate monthly time series.

### Step 7 — Anomaly Detection
Run the anomaly detection cell to compute Z-scores and flag anomalies.

### Step 8 — Export Results
Run the export cells to save anomaly events, monthly stats, and pixel coordinates to S3.

### Step 9 — (Optional) Generate Timelapse
Run the timelapse cell to create NDVI/NDMI/false-color GIFs and upload to S3.

---

## S3 Folder Structure

```
vegetation-anomaly-cogs/
├── 50m_resolution/
│   ├── raw/new_raw/     ← raw TIFs (input)
│   └── cog/YYYY/        ← converted COGs (output)
├── 20m_resolution/
│   ├── raw/             ← GEE tile exports (input)
│   └── cog/YYYY/        ← converted COGs (output)
├── cog_30m/             ← 30m COGs (output)
├── raw_30m_dataset/     ← 30m raw TIFs (input)
└── worldcover/          ← ESA WorldCover mask
```

---

## PostGIS Schema

Table: `vegetation_metadata`

| Index | Type | Purpose |
|-------|------|---------|
| `idx_vegetation_geom` | GIST | Spatial filtering via `ST_Intersects` |
| `idx_vegetation_date` | B-tree | Temporal range queries |
| `idx_vegetation_tile_date` | B-tree | Tile-specific lookups |
| `idx_vegetation_dataset` | B-tree | Per-resolution dataset filtering |

---

## Troubleshooting

**`[ERROR] Insert failed: not all arguments`** — The `dataset` column `%s` placeholder is missing from the VALUES block in `insert_scene_metadata`. Verify the VALUES section has 16 `%s` entries plus 4 inside `ST_MakeEnvelope`.

**`BBox: W=435740`** — UTM coordinates stored instead of WGS84. Ensure `from pyproj import Transformer` is at the top of Cell 4 and re-run with `skip_existing=False`.

**`[FATAL] Phase 2 requires S3`** — S3 credentials are invalid or the bucket name is wrong. Re-check Colab Secrets.

**GEE `403 PERMISSION_DENIED`** — The GEE project is not registered for Earth Engine. Visit `https://console.cloud.google.com/earth-engine/configuration?project=vaulted-dolphin-474319-i7` to register.

**Merged file is 0.2 MB** — Scene is too cloud-covered. The pipeline discards merges under 10MB automatically. No action needed.

---
