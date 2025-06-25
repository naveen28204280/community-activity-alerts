#standard deviation
#t-test
#seperate functions to find rolling means and calculating peaks
#significant peaks to be excluded from calculating means but should be shown as peaks in the logs

import pandas as pd
import plotly.graph_objects as go
import pymysql
import configparser
import requests

def fetch_edit_counts(
    project: str,
    start: str = "20200101",
    end: str = "20240101",
    editor_type: str = "all-editor-types",
    page_type: str = "content",
    granularity: str = "monthly"
) -> pd.DataFrame:
    """
    Fetches edit count data for a given Wikimedia project and returns a DataFrame.

    Parameters:
        project (str): The project domain, e.g., 'uz.wikipedia.org'
        start (str): Start date in YYYYMMDD format
        end (str): End date in YYYYMMDD format
        editor_type (str): Type of editor (default: all-editor-types)
        page_type (str): Page content type (default: content)
        granularity (str): Time granularity (default: monthly)

    Returns:
        pd.DataFrame: A dataframe with columns: timestamp, edit_count, project
    """
    base_url = "https://wikimedia.org/api/rest_v1/metrics/edits/aggregate"
    url = f"{base_url}/{project}/{editor_type}/{page_type}/{granularity}/{start}/{end}"

    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"API Error: {response.status_code} - {response.text}")

    data = response.json()
    edit_counts = data["items"][0]["results"]

    df = pd.DataFrame(edit_counts)
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    df.rename(columns={'edits': 'edit_count'}, inplace=True)
    df['project'] = project

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
        rolling_mean = past_3_years_data['edit_count'].mean()

        # Calculate the threshold (mean + 40%)
        threshold = rolling_mean * (1 + threshold_percentage)
        
        #calculate percenrage difference
        percentage_difference = (df["edit_count"][i]-rolling_mean)*100/rolling_mean
        
        # Check if the current value is above the threshold
        if df['edit_count'][i] >= threshold:
            peaks.append((df['timestamp'][i], df['edit_count'][i], rolling_mean, threshold, percentage_difference))

    return peaks


# Log peaks (timestamp, edits, rolling mean, threshold)
threshold_percentage = 0.30
def log_peaks(peaks):
    for peak in peaks:
        print(f"Peak: {peak[0].strftime('%Y-%m-%d')}, Edits: {peak[1]}, Rolling Mean: {peak[2]:.2f}, Threshold: {peak[3]:.2f}, percentage difference : {peak[4]: .2f}")


df = fetch_edit_counts("uz.wikipedia.org")
peaks = find_peaks_rolling_3_years(df, threshold_percentage)
log_peaks(peaks)

fig = go.Figure()
fig.add_trace(go.Scatter(x=df['timestamp'], y=df['edit_count'], mode='lines+markers', name='Edits', line=dict(color='blue')))

peak_timestamps = [peak[0] for peak in peaks]
peak_values = [peak[1] for peak in peaks]
fig.add_trace(go.Scatter(x=peak_timestamps, y=peak_values, mode='markers', name='Peaks Above Threshold', 
                         marker=dict(color='red', size=10, symbol='circle')))

fig.update_layout(
    title="Edits count over time with peaks over a threshold(30%) with rolling mean of 3 years",
    xaxis_title="Timestamp",
    yaxis_title="Count(Edits)",
    xaxis=dict(tickformat="%Y-%m-%d", tickangle=45),
    showlegend=True
)

# Display the plot
fig.show()

# Convert peaks data to DataFrame
peaks_df = pd.DataFrame(peaks, columns=["Timestamp", "Edit_count", "Rolling_mean", "Threshold", "Percentage_Difference"])

# --- Load Toolforge DB credentials from replica.my.cnf ---

cfg = configparser.ConfigParser()
cfg.read('/data/project/community-activity-alerts-system/replica.my.cnf')
user = cfg['client']['user']
password = cfg['client']['password']

# --- Config for database ---
DB_NAME = 's56391__community_alerts'
DB_TABLE = 'community_alert_logs_table'

try:
    # Connect to the Toolforge database
    conn = pymysql.connect(
        host='tools.db.svc.wikimedia.cloud',
        user=user,
        password=password,
        database=DB_NAME,
        charset='utf8mb4',
        autocommit=True
    )

    cursor = conn.cursor()

    # Create table if it doesn't exist
    create_table_sql = f'''
    CREATE TABLE IF NOT EXISTS {DB_TABLE} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        timestamp DATETIME,
        edit_count INT,
        rolling_mean FLOAT,
        threshold FLOAT,
        percentage_difference FLOAT,
        project VARCHAR(255)
    )
    '''
    cursor.execute(create_table_sql)

    # Insert peaks data into the table
    for _, row in peaks_df.iterrows():
        insert_sql = f"""
        INSERT INTO {DB_TABLE} (timestamp, edit_count, rolling_mean, threshold, percentage_difference, project)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_sql, (
            row['Timestamp'].to_pydatetime(), 
            int(row['Edit_count']), 
            float(row['Rolling_mean']), 
            float(row['Threshold']), 
            float(row['Percentage_Difference']),
            df['project'].iloc[0]  # Using the project from the original dataframe
        ))

    # Display the results
    cursor.execute(f"SELECT * FROM {DB_TABLE} LIMIT 10")
    result = cursor.fetchall()
    print("Data saved to Toolforge database.")
    print("Recent entries:")
    for row in result:
        print(row)

    cursor.close()
    conn.close()

except Exception as e:
    print(f"Database connection error: {e}")
    print("Displaying peaks data without database storage:")
    print(peaks_df)
