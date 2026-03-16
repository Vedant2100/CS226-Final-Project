from flask import Flask, render_template, request
from pipeline_runner import fetch_results

app = Flask(__name__)

@app.route("/", methods=["GET", "POST"])
def index():
    results = None

    if request.method == "POST":
        eval_start = request.form.get("eval_start")
        eval_end = request.form.get("eval_end")

        timelapse = "timelapse" in request.form
        anomaly = "anomaly_detection" in request.form

        try:
            if not eval_start or not eval_end:
                results = {
                    "error": "Please enter both evaluation start and end month."
                }
                return render_template("index.html", result=results)

            if eval_start > eval_end:
                results = {
                    "error": "Evaluation start month cannot be later than evaluation end month."
                }
                return render_template("index.html", result=results)

            results = fetch_results(eval_start, eval_end)

            results["eval_start"] = eval_start
            results["eval_end"] = eval_end
            results["timelapse"] = timelapse
            results["anomaly_detection"] = anomaly

            if results.get("anomaly_detected") is True:
                results["message"] = f"Anomaly detected for {eval_start} to {eval_end}."
            elif results.get("anomaly_detected") is False:
                results["message"] = f"No anomaly detected for {eval_start} to {eval_end}."
            else:
                results["message"] = f"Results loaded for {eval_start} to {eval_end}."

        except Exception as e:
            results = {
                "error": f"Pipeline execution failed: {str(e)}"
            }

    return render_template("index.html", result=results)

if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)