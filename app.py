from flask import Flask, render_template, request, jsonify
from datetime import datetime
import sqlite3
import json
import requests
import pandas as pd

app = Flask(__name__)


def get_all_communities():
    url = 'https://commons.wikimedia.org/w/api.php?action=sitematrix&smtype=language&format=json'
    response = requests.get(url)
    data = response.json()
    sitematrix = data['sitematrix']
    
    # Extract languages and their communities
    languages = {}
    for key, value in sitematrix.items():
        if key.isdigit() and 'name' in value:
            communities = [
                {"sitename": site['sitename'], "url": site['url']}
                for site in value.get('site', [])
            ]
            languages[value['name']] = communities

    return languages


def generate_dataframe(edit_counts):
    # Convert data to a pandas DataFrame
    df = pd.DataFrame(edit_counts)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df


# Function to identify peaks based on the rolling mean of the past 3 years
def find_peaks_rolling_3_years(df, threshold_percentage=0.50):
    peaks = []
    
    # Iterate over each timestamp
    for i in range(len(df)):  # Start from the first element
        # Get data within the last 3 years
        past_3_years_data = df[df['timestamp'] <= df['timestamp'][i]]
        past_3_years_data = past_3_years_data[past_3_years_data['timestamp'] >= (df['timestamp'][i] - pd.DateOffset(years=3))]

        # Calculate the rolling mean of the last 3 years (average of 'edits' in the past 3 years)
        rolling_mean = past_3_years_data['edits'].mean()

        # Calculate the threshold (mean + 40%)
        threshold = rolling_mean * (1 + threshold_percentage)
        
        #calculate percenrage difference
        percentage_difference = (df["edits"][i]-rolling_mean)*100/rolling_mean
        
        # Check if the current value is above the threshold
        if df['edits'][i] >= threshold:
            peaks.append((df['timestamp'][i], df['edits'][i], rolling_mean, threshold, percentage_difference))

    return peaks


# Log peaks (timestamp, edits, rolling mean, threshold)
def log_peaks(peaks):
    peaks_list = []
    for peak in peaks:
        peaks_list.append({
            "timestamp": peak[0].strftime('%Y-%m-%d'),
            "edits": float(peak[1]),
            "rolling_mean": round(float(peak[2]), 2),
            "threshold": round(float(peak[3]), 2),
            "percentage_difference": round(float(peak[4]), 2)
        })
    return peaks_list


@app.route('/')
def index():
    language = request.args.get('language')
    project_group = request.args.get('project_group')
    datestart = request.args.get('datestart')
    dateend = request.args.get('dateend')
    filter_edits = request.args.get('filter_edits') == 'true'
    filter_users = request.args.get('filter_users') == 'true'

    print(f"Language: {language}")
    print(f"Project Group: {project_group}")
    print(f"Date Start: {datestart}")
    print(f"Date End: {dateend}")
    print(f"Filter Edits: {filter_edits}")
    print(f"Filter Users: {filter_users}")

    if not (language and project_group and datestart and dateend):
        return render_template('index.html', languages=get_all_communities())
    
    # Fetch the necessary data based on the query parameters
    base_url = "https://wikimedia.org/api/rest_v1/metrics/edits/aggregate"
    project = project_group.split(":/")[1][1:]  # Parse project from URL
    editor_type = "all-editor-types"
    page_type = "content"
    granularity = "monthly"
    
    # Convert date format to YYYYMMDD
    start = datetime.strptime(datestart, "%b %Y").strftime("%Y%m%d")
    end = datetime.strptime(dateend, "%b %Y").strftime("%Y%m%d")

    url = f"{base_url}/{project}/{editor_type}/{page_type}/{granularity}/{start}/{end}"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        edit_counts = data["items"][0]["results"]

        # Generate DataFrame and find peaks
        df = generate_dataframe(edit_counts)
        threshold_percentage = 0.30
        peaks = find_peaks_rolling_3_years(df, threshold_percentage)
        peaks = log_peaks(peaks)

        return render_template('index.html', languages=get_all_communities(), data=peaks)  # Show peaks as HTML
    else:
        return f"Error: {response.status_code} - {response.text}"

@app.route('/search')
def search():
    query = request.args.get('query', '').lower()
    communities = get_all_communities()
    # Filter communities based on query
    filtered_communities = [
        value['name'] for key, value in communities.items()
        if 'name' in value and query in value['name'].lower()
    ]
    return jsonify(filtered_communities)

if __name__ == '__main__':
    app.run(debug=True)
