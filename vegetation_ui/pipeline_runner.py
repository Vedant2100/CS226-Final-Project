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

def list_run_folders():
    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="results/", Delimiter="/")
    run_folders = [p["Prefix"] for p in response.get("CommonPrefixes", []) if p["Prefix"] != "results/"]
    return sorted(run_folders, reverse=True)

def get_first_parquet_key(prefix):
    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix)
    for obj in response.get("Contents", []):
        if obj["Key"].endswith(".parquet"):
            return obj["Key"]
    return None

def read_parquet_from_s3(key):
    response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
    return pd.read_parquet(BytesIO(response["Body"].read()))

def _empty_result():
    return {
        "anomaly_detected":    False,
        "anomaly_count":       0,
        "ndvi_anomaly_count":  0,
        "ndmi_anomaly_count":  0,
        "total_pixels":        0,
        "yearly_breakdown":    [],
        "monthly_chart":       [],
        "scatter_data":        {"normal": [], "anomaly": []},
        "zscore_histogram":    {"ndvi": [], "ndmi": [], "bins": []},
        "top_pixels":          [],
        "monthly_heatmap":     [],
        "map_points":          {"normal": [], "anomaly": []},
    }

# ── monthly_stats ─────────────────────────────────────────────
def get_monthly_chart_data(run_prefix, start_ym, end_ym):
    key = get_first_parquet_key(f"{run_prefix}monthly_stats/")
    if not key:
        return []
    df = read_parquet_from_s3(key)
    df["year"]  = pd.to_numeric(df["year"],  errors="coerce")
    df["month"] = pd.to_numeric(df["month"], errors="coerce")
    df["_ym"]   = df["year"] * 100 + df["month"]
    filtered = df[(df["_ym"] >= start_ym) & (df["_ym"] <= end_ym)].sort_values("_ym")
    result = []
    for _, row in filtered.iterrows():
        mo = int(row["month"]); yr = int(row["year"])
        na = int(row.get("ndvi_anomaly_count", 0))
        nm = int(row.get("ndmi_anomaly_count", 0))
        result.append({
            "label":              f"{MONTH_NAMES[mo]} {yr}",
            "avg_ndvi":           round(float(row["avg_ndvi"]), 4),
            "avg_ndmi":           round(float(row["avg_ndmi"]), 4),
            "avg_ndvi_baseline":  round(float(row["avg_ndvi_baseline"]), 4),
            "avg_ndmi_baseline":  round(float(row["avg_ndmi_baseline"]), 4),
            "ndvi_anomaly_count": na,
            "ndmi_anomaly_count": nm,
            "total_pixels":       int(row["total_pixels"]),
            "is_anomaly":         (na + nm) > 0,
        })
    return result

# ── plot_stats helpers ────────────────────────────────────────
def get_plot_stats(run_prefix, start_ym, end_ym):
    key = get_first_parquet_key(f"{run_prefix}plot_stats/")
    if not key:
        return pd.DataFrame()
    df = read_parquet_from_s3(key)
    df["year"]  = pd.to_numeric(df["year"],  errors="coerce")
    df["month"] = pd.to_numeric(df["month"], errors="coerce")
    df["_ym"]   = df["year"] * 100 + df["month"]
    filtered = df[(df["_ym"] >= start_ym) & (df["_ym"] <= end_ym)].copy()
    filtered["is_ndvi_anomaly"] = filtered["is_ndvi_anomaly"].astype(str).str.lower() == "true"
    filtered["is_ndmi_anomaly"] = filtered["is_ndmi_anomaly"].astype(str).str.lower() == "true"
    filtered["is_anomaly"]      = filtered["is_ndvi_anomaly"] | filtered["is_ndmi_anomaly"]
    return filtered

def build_scatter(df, max_pts=600):
    normal  = df[~df["is_anomaly"]]
    anomaly = df[df["is_anomaly"]]
    normal  = normal.sample(min(max_pts, len(normal)),   random_state=42)
    anomaly = anomaly.sample(min(max_pts, len(anomaly)), random_state=42)
    def pts(d):
        return [{"x": round(float(r["ndvi_mean"]), 4),
                 "y": round(float(r["ndmi_mean"]), 4),
                 "pixel": r["true_pixel_id"]}
                for _, r in d.iterrows()]
    return {"normal": pts(normal), "anomaly": pts(anomaly)}

def build_zscore(df):
    ndvi_z = df["ndvi_zscore"].dropna().clip(-10, 10).tolist()
    ndmi_z = df["ndmi_zscore"].dropna().clip(-10, 10).tolist()
    bin_edges = list(range(-10, 11, 1))
    ndvi_c = [0] * (len(bin_edges) - 1)
    ndmi_c = [0] * (len(bin_edges) - 1)
    for z in ndvi_z:
        i = min(int(z + 10), len(ndvi_c) - 1)
        if 0 <= i < len(ndvi_c): ndvi_c[i] += 1
    for z in ndmi_z:
        i = min(int(z + 10), len(ndmi_c) - 1)
        if 0 <= i < len(ndmi_c): ndmi_c[i] += 1
    return {"ndvi": ndvi_c, "ndmi": ndmi_c, "bins": [str(b) for b in bin_edges[:-1]]}

def build_top_pixels(df, n=10):
    pc = (df[df["is_anomaly"]]
          .groupby("true_pixel_id").size()
          .reset_index(name="cnt")
          .sort_values("cnt", ascending=False)
          .head(n))
    return [{"pixel": r["true_pixel_id"], "count": int(r["cnt"])} for _, r in pc.iterrows()]

def build_monthly_heatmap(df):
    m = (df[df["is_anomaly"]]
         .groupby("month").size()
         .reset_index(name="cnt")
         .sort_values("month"))
    return [{"month": MONTH_NAMES[int(r["month"])], "count": int(r["cnt"])} for _, r in m.iterrows()]

# ── pixel_coords + anomaly_events → map ──────────────────────
def build_map_points(run_prefix, start_ym, end_ym):
    coords_key = get_first_parquet_key(f"{run_prefix}pixel_coords/")
    anom_key   = get_first_parquet_key(f"{run_prefix}anomaly_events/")
    if not coords_key or not anom_key:
        return {"normal": [], "anomaly": []}

    coords = read_parquet_from_s3(coords_key)
    adf    = read_parquet_from_s3(anom_key)

    adf["year"]  = pd.to_numeric(adf["year"],  errors="coerce")
    adf["month"] = pd.to_numeric(adf["month"], errors="coerce")
    adf["_ym"]   = adf["year"] * 100 + adf["month"]
    adf = adf[(adf["_ym"] >= start_ym) & (adf["_ym"] <= end_ym)].copy()
    adf["is_ndvi_anomaly"] = adf["is_ndvi_anomaly"].astype(str).str.lower() == "true"
    adf["is_ndmi_anomaly"] = adf["is_ndmi_anomaly"].astype(str).str.lower() == "true"
    adf["is_anomaly"]      = adf["is_ndvi_anomaly"] | adf["is_ndmi_anomaly"]

    anomaly_pixels = set(adf[adf["is_anomaly"]]["true_pixel_id"].unique())

    normal_pts  = []
    anomaly_pts = []
    for _, row in coords.iterrows():
        pid = row["true_pixel_id"]
        pt  = {"id": pid, "lat": round(float(row["lat"]), 6), "lon": round(float(row["lon"]), 6)}
        if pid in anomaly_pixels:
            anomaly_pts.append(pt)
        else:
            normal_pts.append(pt)

    random.seed(42)
    if len(normal_pts) > 500:
        normal_pts = random.sample(normal_pts, 500)

    return {"normal": normal_pts, "anomaly": anomaly_pts}

# ── main ──────────────────────────────────────────────────────
def get_anomaly_summary(run_prefix, eval_start, eval_end):
    start_year, start_month = map(int, eval_start.split("-"))
    end_year,   end_month   = map(int, eval_end.split("-"))
    start_ym = start_year * 100 + start_month
    end_ym   = end_year   * 100 + end_month

    anom_key = get_first_parquet_key(f"{run_prefix}anomaly_events/")
    if not anom_key:
        return _empty_result()
    adf = read_parquet_from_s3(anom_key)
    if adf.empty:
        return _empty_result()

    adf["year"]  = pd.to_numeric(adf["year"],  errors="coerce")
    adf["month"] = pd.to_numeric(adf["month"], errors="coerce")
    adf["_ym"]   = adf["year"] * 100 + adf["month"]
    adf = adf[(adf["_ym"] >= start_ym) & (adf["_ym"] <= end_ym)].copy()
    if adf.empty:
        return _empty_result()

    adf["is_ndvi_anomaly"] = adf["is_ndvi_anomaly"].astype(str).str.lower() == "true"
    adf["is_ndmi_anomaly"] = adf["is_ndmi_anomaly"].astype(str).str.lower() == "true"
    adf["is_anomaly"]      = adf["is_ndvi_anomaly"] | adf["is_ndmi_anomaly"]

    yearly = []
    for year in range(start_year, end_year + 1):
        yd = adf[adf["year"] == year]
        ad = yd[yd["is_anomaly"]]
        yearly.append({
            "year":             year,
            "anomaly_detected": len(ad) > 0,
            "total_events":     len(ad),
            "ndvi_count":       int(yd["is_ndvi_anomaly"].sum()),
            "ndmi_count":       int(yd["is_ndmi_anomaly"].sum()),
        })

    overall = adf[adf["is_anomaly"]]

    ms_key = get_first_parquet_key(f"{run_prefix}monthly_stats/")
    total_pixels = 0
    if ms_key:
        ms = read_parquet_from_s3(ms_key)
        if "total_pixels" in ms.columns and not ms.empty:
            total_pixels = int(ms["total_pixels"].iloc[0])

    pdf = get_plot_stats(run_prefix, start_ym, end_ym)

    return {
        "anomaly_detected":    len(overall) > 0,
        "anomaly_count":       len(overall),
        "ndvi_anomaly_count":  int(adf["is_ndvi_anomaly"].sum()),
        "ndmi_anomaly_count":  int(adf["is_ndmi_anomaly"].sum()),
        "total_pixels":        total_pixels,
        "yearly_breakdown":    yearly,
        "monthly_chart":       get_monthly_chart_data(run_prefix, start_ym, end_ym),
        "scatter_data":        build_scatter(pdf)       if not pdf.empty else {"normal": [], "anomaly": []},
        "zscore_histogram":    build_zscore(pdf)        if not pdf.empty else {"ndvi": [], "ndmi": [], "bins": []},
        "top_pixels":          build_top_pixels(pdf)    if not pdf.empty else [],
        "monthly_heatmap":     build_monthly_heatmap(pdf) if not pdf.empty else [],
        "map_points":          build_map_points(run_prefix, start_ym, end_ym),
    }

def fetch_results(eval_start, eval_end):
    run_folders = list_run_folders()
    latest      = run_folders[0] if run_folders else None
    if not latest:
        return _empty_result()
    return get_anomaly_summary(latest, eval_start, eval_end)