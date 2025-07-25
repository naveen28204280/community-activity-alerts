from flask import Flask, render_template, request, jsonify
from datetime import datetime
import requests
import pandas as pd
import pymysql
import configparser
import plotly.graph_objects as go
from plotly.io import to_html
import calendar

app = Flask(__name__)
ALERTS_TABLE = 'community_alerts'

# --- DB connection setup ---
def get_db_connection():
    cfg = configparser.ConfigParser()
    cfg.read("/data/project/community-activity-alerts-system/replica.my.cnf")
    user = cfg["client"]["user"]
    password = cfg["client"]["password"]

    conn = pymysql.connect(
        host="tools.db.svc.wikimedia.cloud",
        user=user,
        password=password,
        database="s56391__community_alerts",
        charset="utf8mb4",
    )
    return conn


# --- Get communities list from SiteMatrix API ---
def get_all_communities():
    url = "https://commons.wikimedia.org/w/api.php?action=sitematrix&smtype=language&format=json"
    headers = {
        "User-Agent": "Community Activity Alerts (https://github.com/indictechcom/community-activity-alerts)",
        "tool": "Community Activity Alerts",
        "url": "https://github.com/indictechcom/community-activity-alerts",
        "email": "tools.community-activity-alerts-system@toolforge.org",
    }

    response = requests.get(url, headers=headers)
    data = response.json()
    sitematrix = data["sitematrix"]

    languages = {}
    for key, value in sitematrix.items():
        if key.isdigit() and "localname" in value:
            communities = [
                {"sitename": site["code"], "url": site["url"]}
                for site in value.get("site", [])
            ]
            languages[value["localname"]] = communities

    return languages


# --- Peak detection function ---
def find_peaks_rolling_3_years(df, threshold_percentage=0.30):
    df = df.sort_values("timestamp").reset_index(drop=True)
    peaks = []

    for i in range(len(df)):
        t_i = df.at[i, "timestamp"]
        edits_i = df.at[i, "edits"]

        window = df[
            (df["timestamp"] >= t_i - pd.DateOffset(years=3)) & (df["timestamp"] <= t_i)
        ]
        if window.empty:
            continue

        rolling_mean = window["edits"].mean()
        threshold = rolling_mean * (1 + threshold_percentage)
        pct_diff = ((edits_i - rolling_mean) / rolling_mean) * 100

        if edits_i >= threshold:
            peaks.append(
                {
                    "timestamp": t_i,
                    "edits": edits_i,
                    "rolling_mean": rolling_mean,
                    "threshold": threshold,
                    "percentage_difference": pct_diff,
                }
            )

    return peaks


# --- Format peaks for display ---
def log_peaks(peaks):
    peaks_list = []
    for peak in peaks:
        peaks_list.append(
            {
                "timestamp": peak["timestamp"].strftime("%Y-%m-%d"),
                "edits": int(peak["edits"]),
                "rolling_mean": round(float(peak["rolling_mean"]), 2),
                "threshold": round(float(peak["threshold"]), 2),
                "percentage_difference": round(float(peak["percentage_difference"]), 2),
            }
        )
    return peaks_list


# --- Main route ---
@app.route("/")
def index():
    language = request.args.get("language")
    project_group = request.args.get("project_group")
    datestart = request.args.get("datestart")
    dateend = request.args.get("dateend")
    filter_edits = request.args.get("filter_edits") == "true"
    filter_users = request.args.get("filter_users") == "true"

    if not (language and project_group and datestart and dateend):
        return render_template("index.html", languages=get_all_communities())

    project = project_group.split(":/")[1][1:]  # e.g. "en.wikipedia.org"
    start = datetime.strptime(datestart, "%b %Y")
    end = datetime.strptime(dateend, "%b %Y")
    # Set start to first day, end to last day of month
    start = start.replace(day=1, hour=0, minute=0, second=0)
    last_day = calendar.monthrange(end.year, end.month)[1]
    end = end.replace(day=last_day, hour=23, minute=59, second=59)

    try:
        conn = get_db_connection()
        query = """
            SELECT timestamp, edit_count AS edits
            FROM edit_counts
            WHERE project = %s
              AND timestamp BETWEEN %s AND %s
            ORDER BY timestamp ASC
        """
        df = pd.read_sql(query, conn, params=(project, start, end))
        conn.close()

        if df.empty:
            return render_template(
                "index.html",
                languages=get_all_communities(),
                data=[],
                chart=None,
                message="No data available.",
            )

        df["timestamp"] = pd.to_datetime(df["timestamp"])
        peaks_raw = find_peaks_rolling_3_years(df, threshold_percentage=0.30)
        peaks = log_peaks(peaks_raw)

        # --- Generate plot ---
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=df["timestamp"],
                y=df["edits"],
                mode="lines+markers",
                name="Edits",
                line=dict(color="blue"),
            )
        )

        peak_timestamps = [peak["timestamp"] for peak in peaks]
        peak_values = [peak["edits"] for peak in peaks]
        fig.add_trace(
            go.Scatter(
                x=peak_timestamps,
                y=peak_values,
                mode="markers",
                name="Peaks Above Threshold",
                marker=dict(color="red", size=10, symbol="circle"),
            )
        )

        fig.update_layout(
            title="Edits count over time with peaks (30% over 3-year rolling mean)",
            xaxis_title="Timestamp",
            yaxis_title="Count (Edits)",
            xaxis=dict(tickformat="%Y-%m-%d", tickangle=45),
            showlegend=True,
        )

        chart_html = to_html(fig, full_html=False, include_plotlyjs="cdn")

        return render_template(
            "index.html", languages=get_all_communities(), data=peaks, chart=chart_html
        )

    except Exception as e:
        return f"Database error: {str(e)}"


# --- Optional community name search endpoint ---
@app.route("/search")
def search():
    query = request.args.get("query", "").lower()
    communities = get_all_communities()
    filtered_communities = [
        value["name"]
        for key, value in communities.items()
        if "name" in value and query in value["name"].lower()
    ]
    return jsonify(filtered_communities)


# --- Route for recieving the label and peak data to store in Community Alerts table ---
@app.route("/add-label", methods=["POST"])
def add_label():
    data = request.json
    if not all(field in data for field in ["project", "timestamp", "label"]):
        return (jsonify({'error': 'Missing required fields'}), 400)
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Ensure label column exits
            cursor.execute(f"SHOW COLUMNS FROM {ALERTS_TABLE} LIKE 'label';")
            if cursor.fetchone() is None:
                cursor.execute(f"ALTER TABLE {ALERTS_TABLE} ADD COLUMN label VARCHAR(255);")
                conn.commit()
            # Insert into the table
            cursor.execute(f"""
                UPDATE {ALERTS_TABLE}
                SET label = %s
                WHERE project = %s AND timestamp = %s;
            """, (data["label"], data["project"], data["timestamp"]))
            conn.commit()
            return (jsonify({'success': 'Successfully added the annotation'}), 200)
    except Exception as e:
        return (jsonify({'error': str(e)}), 500)
    finally:
        conn.close()


if __name__ == "__main__":
    app.run(debug=False)
