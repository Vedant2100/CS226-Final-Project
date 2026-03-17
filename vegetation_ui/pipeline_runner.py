import os
import random
import boto3
import pandas as pd
from io import BytesIO

AWS_REGION = os.environ.get("AWS_REGION", "us-west-1")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "vegetation-anomaly-cogs")

s3 = boto3.client("s3", region_name=AWS_REGION)

MONTH_NAMES = {
    1:"Jan", 2:"Feb", 3:"Mar", 4:"Apr", 5:"May", 6:"Jun",
    7:"Jul", 8:"Aug", 9:"Sep", 10:"Oct", 11:"Nov", 12:"Dec"
}

# ── Resolution to Run ID mapping ──────────────────────────────
RESOLUTION_RUN_MAP = {
    "20m": "results/run_id=20260317_061437/",
    "30m": "results/run_id=20260317_013528/",
    "50m": "results/run_id=20260317_055040/",
}

def get_folder_name(run_prefix, base_name):
    for suffix in ["", "_parquet"]:
        prefix = f"{run_prefix}{base_name}{suffix}/"
        resp = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix, MaxKeys=1)
        if resp.get("Contents") or resp.get("CommonPrefixes"):
            return prefix
    return None

def get_first_parquet_key(prefix):
    if not prefix:
        return None
    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix)
    for obj in response.get("Contents", []):
        if obj["Key"].endswith(".parquet"):
            return obj["Key"]
    return None

def read_parquet_from_s3(key):
    response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
    return pd.read_parquet(BytesIO(response["Body"].read()))

def check_file_exists(key):
    try:
        s3.head_object(Bucket=S3_BUCKET_NAME, Key=key)
        return True
    except:
        return False

def get_presigned_url(key, expires=3600):
    try:
        return s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": S3_BUCKET_NAME, "Key": key},
            ExpiresIn=expires
        )
    except:
        return None

def safe_float(val):
    try:
        v = float(val)
        return round(v, 4) if v == v else 0.0
    except:
        return 0.0

def _empty_result():
    return {
        "anomaly_detected":    False,
        "anomaly_count":       0,
        "ndvi_anomaly_count":  0,
        "ndmi_anomaly_count":  0,
        "total_pixels":        0,
        "resolution":          "",
        "run_id":              "",
        "yearly_breakdown":    [],
        "monthly_chart":       [],
        "scatter_data":        {"normal": [], "anomaly": []},
        "zscore_histogram":    {"ndvi": [], "ndmi": [], "bins": []},
        "top_pixels":          [],
        "monthly_heatmap":     [],
        "map_points":          {"normal": [], "anomaly": []},
        "timelapse_urls":      {},
        "pipeline_metrics":    [],
        "params":              {},
    }

# ── Read helpers ──────────────────────────────────────────────
def read_anomaly_df(run_prefix, start_ym, end_ym):
    folder = get_folder_name(run_prefix, "anomaly_events")
    key = get_first_parquet_key(folder)
    if not key:
        return pd.DataFrame()
    df = read_parquet_from_s3(key)
    df["year"]  = pd.to_numeric(df["year"],  errors="coerce")
    df["month"] = pd.to_numeric(df["month"], errors="coerce")
    df["_ym"]   = df["year"] * 100 + df["month"]
    df = df[(df["_ym"] >= start_ym) & (df["_ym"] <= end_ym)].copy()
    if df.empty:
        return pd.DataFrame()
    df["is_ndvi_anomaly"] = df["is_ndvi_anomaly"].astype(str).str.lower() == "true"
    df["is_ndmi_anomaly"] = df["is_ndmi_anomaly"].astype(str).str.lower() == "true"
    df["is_anomaly"]      = df["is_ndvi_anomaly"] | df["is_ndmi_anomaly"]
    return df

def read_monthly_stats(run_prefix, start_ym, end_ym):
    folder = get_folder_name(run_prefix, "monthly_stats")
    key = get_first_parquet_key(folder)
    if not key:
        return pd.DataFrame()
    df = read_parquet_from_s3(key)
    df["year"]  = pd.to_numeric(df["year"],  errors="coerce")
    df["month"] = pd.to_numeric(df["month"], errors="coerce")
    df["_ym"]   = df["year"] * 100 + df["month"]
    return df[(df["_ym"] >= start_ym) & (df["_ym"] <= end_ym)].sort_values("_ym")

def read_plot_stats(run_prefix, start_ym, end_ym):
    folder = get_folder_name(run_prefix, "plot_stats")
    key = get_first_parquet_key(folder)
    if not key:
        return pd.DataFrame()
    df = read_parquet_from_s3(key)
    df["year"]  = pd.to_numeric(df["year"],  errors="coerce")
    df["month"] = pd.to_numeric(df["month"], errors="coerce")
    df["_ym"]   = df["year"] * 100 + df["month"]
    df = df[(df["_ym"] >= start_ym) & (df["_ym"] <= end_ym)].copy()
    if df.empty:
        return pd.DataFrame()
    df["is_ndvi_anomaly"] = df["is_ndvi_anomaly"].astype(str).str.lower() == "true"
    df["is_ndmi_anomaly"] = df["is_ndmi_anomaly"].astype(str).str.lower() == "true"
    df["is_anomaly"]      = df["is_ndvi_anomaly"] | df["is_ndmi_anomaly"]
    return df

def read_pixel_coords(run_prefix):
    folder = get_folder_name(run_prefix, "pixel_coords")
    key = get_first_parquet_key(folder)
    if not key:
        return pd.DataFrame()
    return read_parquet_from_s3(key)

def read_pipeline_metrics(run_prefix):
    key = f"{run_prefix}pipeline_metrics.csv"
    try:
        resp = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
        df = pd.read_csv(BytesIO(resp["Body"].read()))
        return [{k: str(v) for k, v in row.items()} for _, row in df.iterrows()]
    except:
        return []

def read_params(run_prefix):
    key = f"{run_prefix}params.csv"
    try:
        resp = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
        df = pd.read_csv(BytesIO(resp["Body"].read()))
        return {str(row["Parameter"]): str(row["Value"]) for _, row in df.iterrows()}
    except:
        return {}

def get_timelapse_urls(run_prefix):
    urls = {}
    for name in ["timelapse_ndvi.gif", "timelapse_ndmi.gif", "timelapse_false_color.gif"]:
        key = f"{run_prefix}{name}"
        if check_file_exists(key):
            url = get_presigned_url(key)
            if url:
                label = name.replace("timelapse_", "").replace(".gif", "").upper().replace("_", " ")
                urls[label] = url
    return urls

# ── Chart builders ────────────────────────────────────────────
def build_monthly_chart(ms_df):
    result = []
    for _, row in ms_df.iterrows():
        mo = int(row["month"]); yr = int(row["year"])
        na = int(row.get("ndvi_anomaly_count", 0))
        nm = int(row.get("ndmi_anomaly_count", 0))
        result.append({
            "label":              f"{MONTH_NAMES[mo]} {yr}",
            "avg_ndvi":           safe_float(row["avg_ndvi"]),
            "avg_ndmi":           safe_float(row["avg_ndmi"]),
            "avg_ndvi_baseline":  safe_float(row["avg_ndvi_baseline"]),
            "avg_ndmi_baseline":  safe_float(row["avg_ndmi_baseline"]),
            "ndvi_anomaly_count": na,
            "ndmi_anomaly_count": nm,
            "total_pixels":       int(row["total_pixels"]),
            "is_anomaly":         (na + nm) > 0,
        })
    return result

def build_scatter(df, max_pts=600):
    normal  = df[~df["is_anomaly"]]
    anomaly = df[df["is_anomaly"]]
    normal  = normal.sample(min(max_pts, len(normal)),   random_state=42)
    anomaly = anomaly.sample(min(max_pts, len(anomaly)), random_state=42)
    def pts(d):
        result = []
        for _, r in d.iterrows():
            x = safe_float(r["ndvi_mean"])
            y = safe_float(r["ndmi_mean"])
            result.append({"x": x, "y": y, "pixel": str(r["true_pixel_id"])})
        return result
    return {"normal": pts(normal), "anomaly": pts(anomaly)}

def build_zscore(df):
    ndvi_z = df["ndvi_zscore"].dropna().clip(-10, 10).tolist()
    ndmi_z = df["ndmi_zscore"].dropna().clip(-10, 10).tolist()
    bin_edges = list(range(-10, 11, 1))
    ndvi_c = [0]*(len(bin_edges)-1); ndmi_c = [0]*(len(bin_edges)-1)
    for z in ndvi_z:
        if z != z: continue
        i = min(int(z+10), len(ndvi_c)-1)
        if 0<=i<len(ndvi_c): ndvi_c[i]+=1
    for z in ndmi_z:
        if z != z: continue
        i = min(int(z+10), len(ndmi_c)-1)
        if 0<=i<len(ndmi_c): ndmi_c[i]+=1
    return {"ndvi": ndvi_c, "ndmi": ndmi_c, "bins": [str(b) for b in bin_edges[:-1]]}

def build_top_pixels(df, n=10):
    pc = (df[df["is_anomaly"]].groupby("true_pixel_id").size()
          .reset_index(name="cnt").sort_values("cnt", ascending=False).head(n))
    return [{"pixel": str(r["true_pixel_id"]), "count": int(r["cnt"])} for _, r in pc.iterrows()]

def build_monthly_heatmap(df):
    m = (df[df["is_anomaly"]].groupby("month").size()
         .reset_index(name="cnt").sort_values("month"))
    return [{"month": MONTH_NAMES[int(r["month"])], "count": int(r["cnt"])} for _, r in m.iterrows()]

def build_map_points(coords_df, anom_df):
    if coords_df.empty or anom_df.empty:
        return {"normal": [], "anomaly": []}

    # Convert UTM to lat/lon if needed
    # If coordinates are large numbers they are UTM, not lat/lon
    if coords_df["lon"].abs().max() > 1000:
        try:
            from pyproj import Transformer
            # UTM Zone 13N (EPSG:32613) to WGS84 (EPSG:4326)
            transformer = Transformer.from_crs("EPSG:32613", "EPSG:4326", always_xy=True)
            lons, lats = transformer.transform(
                coords_df["lon"].values,
                coords_df["lat"].values
            )
            coords_df = coords_df.copy()
            coords_df["lon"] = lons
            coords_df["lat"] = lats
        except Exception as e:
            print("UTM conversion failed:", e)
            return {"normal": [], "anomaly": []}

    anomaly_pixels = set(anom_df[anom_df["is_anomaly"]]["true_pixel_id"].unique())
    normal_pts = []; anomaly_pts = []
    for _, row in coords_df.iterrows():
        pid = row["true_pixel_id"]
        lat = safe_float(row["lat"])
        lon = safe_float(row["lon"])
        if lat == 0.0 and lon == 0.0: continue
        pt = {"id": str(pid), "lat": lat, "lon": lon}
        if pid in anomaly_pixels:
            anomaly_pts.append(pt)
        else:
            normal_pts.append(pt)
    random.seed(42)
    if len(normal_pts) > 500:
        normal_pts = random.sample(normal_pts, 500)
    return {"normal": normal_pts, "anomaly": anomaly_pts}

# ── Main fetch by resolution ──────────────────────────────────
def fetch_results(eval_start, eval_end, resolution="30m"):
    start_year, start_month = map(int, eval_start.split("-"))
    end_year,   end_month   = map(int, eval_end.split("-"))
    start_ym = start_year * 100 + start_month
    end_ym   = end_year   * 100 + end_month

    run_prefix = RESOLUTION_RUN_MAP.get(resolution)
    if not run_prefix:
        return _empty_result()

    run_id = run_prefix.rstrip("/").split("run_id=")[-1]

    adf = read_anomaly_df(run_prefix, start_ym, end_ym)
    if adf.empty:
        result = _empty_result()
        result["resolution"] = resolution
        result["run_id"]     = run_id
        return result

    ms_df     = read_monthly_stats(run_prefix, start_ym, end_ym)
    plot_df   = read_plot_stats(run_prefix, start_ym, end_ym)
    coords_df = read_pixel_coords(run_prefix)
    metrics   = read_pipeline_metrics(run_prefix)
    params    = read_params(run_prefix)
    timelapse = get_timelapse_urls(run_prefix)

    overall = adf[adf["is_anomaly"]]
    yearly  = []
    for year in range(start_year, end_year+1):
        yd = adf[adf["year"]==year]
        ad = yd[yd["is_anomaly"]]
        yearly.append({
            "year":             year,
            "anomaly_detected": len(ad) > 0,
            "total_events":     len(ad),
            "ndvi_count":       int(yd["is_ndvi_anomaly"].sum()),
            "ndmi_count":       int(yd["is_ndmi_anomaly"].sum()),
        })

    total_pixels = int(ms_df["total_pixels"].iloc[0]) if not ms_df.empty and "total_pixels" in ms_df.columns else 0

    return {
        "anomaly_detected":    len(overall) > 0,
        "anomaly_count":       len(overall),
        "ndvi_anomaly_count":  int(adf["is_ndvi_anomaly"].sum()),
        "ndmi_anomaly_count":  int(adf["is_ndmi_anomaly"].sum()),
        "total_pixels":        total_pixels,
        "resolution":          resolution,
        "run_id":              run_id,
        "yearly_breakdown":    yearly,
        "monthly_chart":       build_monthly_chart(ms_df)      if not ms_df.empty   else [],
        "scatter_data":        build_scatter(plot_df)           if not plot_df.empty else {"normal":[],"anomaly":[]},
        "zscore_histogram":    build_zscore(plot_df)            if not plot_df.empty else {"ndvi":[],"ndmi":[],"bins":[]},
        "top_pixels":          build_top_pixels(plot_df)        if not plot_df.empty else [],
        "monthly_heatmap":     build_monthly_heatmap(plot_df)   if not plot_df.empty else [],
        "map_points":          build_map_points(coords_df, adf),
        "timelapse_urls":      timelapse,
        "pipeline_metrics":    metrics,
        "params":              params,
    }