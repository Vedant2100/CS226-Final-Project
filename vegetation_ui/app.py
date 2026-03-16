import json
from flask import Flask, render_template, request
from pipeline_runner import fetch_results

app = Flask(__name__)

def safe_int(value, default=None):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

def safe_json(obj):
    """Convert to JSON string, ensuring it's safe for embedding in HTML script tags."""
    return json.dumps(obj, ensure_ascii=True, default=str)

@app.route("/", methods=["GET", "POST"])
def index():
    context = {
        "error": None, "result": None,
        "start_year": "", "end_year": "",
        "generate_timelapse": False,
    }
    if request.method == "POST":
        start_year = safe_int(request.form.get("start_year"))
        end_year   = safe_int(request.form.get("end_year"))
        generate_timelapse = request.form.get("generate_timelapse") == "on"
        context["start_year"]         = start_year or ""
        context["end_year"]           = end_year   or ""
        context["generate_timelapse"] = generate_timelapse

        if start_year is None or end_year is None:
            context["error"] = "Please enter valid start and end years."
            return render_template("index.html", **context)
        if start_year > end_year:
            context["error"] = "Start year cannot be greater than end year."
            return render_template("index.html", **context)

        try:
            s3 = fetch_results(f"{start_year}-01", f"{end_year}-12")
        except Exception as e:
            context["error"] = f"Could not fetch S3 results: {str(e)}"
            return render_template("index.html", **context)

        ad = s3["anomaly_detected"]
        nv = s3.get("ndvi_anomaly_count", 0)
        nm = s3.get("ndmi_anomaly_count", 0)
        tp = s3.get("total_pixels", 0)

        context["result"] = {
            "selected_timeframe":   f"{start_year}-01 to {end_year}-12",
            "timelapse_status":     "Generated" if generate_timelapse else "Not generated",
            "anomaly_detected":     ad,
            "anomaly_result":       "⚠ Anomaly Detected" if ad else "✓ No Anomaly Detected",
            "anomaly_event_count":  s3.get("anomaly_count", 0),
            "ndvi_anomaly_count":   nv,
            "ndmi_anomaly_count":   nm,
            "total_pixels":         tp,
            "yearly_breakdown":     s3.get("yearly_breakdown", []),
            "monthly_chart_json":   safe_json(s3.get("monthly_chart", [])),
            "scatter_json":         safe_json(s3.get("scatter_data", {"normal":[],"anomaly":[]})),
            "zscore_json":          safe_json(s3.get("zscore_histogram", {"ndvi":[],"ndmi":[],"bins":[]})),
            "top_pixels_json":      safe_json(s3.get("top_pixels", [])),
            "monthly_heatmap_json": safe_json(s3.get("monthly_heatmap", [])),
            "map_points_json":      safe_json(s3.get("map_points", {"normal":[],"anomaly":[]})),
            "banner_message": (
                f"⚠ Anomaly detected in the selected timeframe ({nv} NDVI, {nm} NDMI events)"
                if ad else f"✓ No anomaly detected for {start_year} to {end_year}."
            ),
        }
    return render_template("index.html", **context)

if __name__ == "__main__":
    app.run(debug=True)