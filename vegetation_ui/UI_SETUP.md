# Vegetation Anomaly Detection Dashboard

## Prerequisites
- Python 3.8+
- AWS credentials configured

## Installation

**Step 1 — Install dependencies:**
```
pip install flask boto3 pandas pyarrow pyproj
```

**Step 2 — Configure AWS credentials:**
```
aws configure
```
Enter your AWS Access Key ID, Secret Access Key and region `us-west-1`

## Running the Dashboard

**Step 1 — Navigate to the UI folder:**
```
cd vegetation_ui
```

**Step 2 — Start the Flask app:**
```
python app.py
```

**Step 3 — Open browser:**
```
http://127.0.0.1:5000
```

## How to Use

1. Enter **Start Year** and **End Year** (e.g. 2026)
2. Select a **Resolution** — 20m, 30m or 50m
3. Optionally check **Generate Timelapse** to view animated GIFs
4. Click **Run Analysis**
5. View results:
   - Monthly NDVI and NDMI trend charts
   - Pixel-level scatter plots and Z-score distribution
   - Interactive anomaly map
   - Pipeline metrics table
   - Year-by-year breakdown

## AWS S3 Bucket
- Bucket: `vegetation-anomaly-cogs`
- Region: `us-west-1`
