#!/usr/bin/env python
# coding: utf-8

# In[1]:


#standard deviation
#t-test
#seperate functions to find rolling means and calculating peaks
#significant peaks to be excluded from calculating means but should be shown as peaks in the logs


# In[1]:


import requests
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import duckdb
from fetch_and_store_script import fetch_edit_counts
import pymysql
import configparser


# In[2]:


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


# In[6]:


# Log peaks (timestamp, edits, rolling mean, threshold)
threshold_percentage = 0.30
def log_peaks(peaks):
    for peak in peaks:
        print(f"Peak: {peak[0].strftime('%Y-%m-%d')}, Edits: {peak[1]}, Rolling Mean: {peak[2]:.2f}, Threshold: {peak[3]:.2f}, percentage difference : {peak[4]: .2f}")




# --- Load Toolforge DB credentials from replica.my.cnf ---
cfg = configparser.ConfigParser()
cfg.read('/data/project/community-activity-alerts-system/replica.my.cnf')
user = cfg['client']['user']
password = cfg['client']['password']

# --- Database connection config ---
DB_NAME = 's56391__community_alerts'
DB_TABLE = 'edit_counts'

# --- Connect and read data into DataFrame ---
conn = pymysql.connect(
    host='tools.db.svc.wikimedia.cloud',
    user=user,
    password=password,
    database=DB_NAME,
    charset='utf8mb4'
)

query = f"SELECT * FROM {DB_TABLE}"
df = pd.read_sql(query, conn)

conn.close()

#convert timestamp to datetime if not already ---
df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)


#df = fetch_edit_counts("uz.wikipedia.org")
peaks = find_peaks_rolling_3_years(df, threshold_percentage)
log_peaks(peaks)


# In[8]:


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


# In[9]:


my_df=pd.DataFrame(peaks,columns=[["Timestamp","Edit_count","Rolling_mean","Threshold","Percentage_Difference"]])
my_df


# In[8]:


duckdb.sql("CREATE TABLE community_alert_logs_table AS SELECT * FROM my_df")

duckdb.sql("INSERT INTO community_alert_logs_table SELECT * FROM my_df")


# In[ ]:


duckdb.sql("SELECT * FROM community_alert_logs_table LIMIT 10")


# In[9]:





# In[ ]:





# In[10]:


import duckdb

# Use in-memory DuckDB instance
con = duckdb.connect()

# Create and populate tables as before
con.execute("""
CREATE TABLE language_projects (
    code TEXT,
    local_name TEXT,
    database_name TEXT,
    site_name TEXT,
    url TEXT
);
""")

con.execute("""
INSERT INTO language_projects VALUES 
('aa', 'Afar', 'aawiki', 'Wikipedia', 'https://aa.wikipedia.org'),
('aa', 'Afar', 'aawiktionary', 'Wiktionary', 'https://aa.wiktionary.org'),
('ab', 'Abkhazian', 'abwiki', 'Авикипедиа', 'https://ab.wikipedia.org');
""")

con.execute("""
CREATE TABLE edit_counts (
    editcount_P1key INTEGER,
    timestamp TIMESTAMP,
    project TEXT,
    editor_type TEXT,
    page_type TEXT,
    edit_count INTEGER
);
""")

con.execute("""
INSERT INTO edit_counts VALUES 
(1, '2023-12-01 10:15:45', 'aawiki', NULL, 'content', 100),
(2, '2023-11-01 20:08:34', 'aawiktionary', NULL, 'content', 24),
(3, '2023-01-17 05:03:00', 'enwiki', NULL, 'content', 58);
""")

con.execute("""
CREATE TABLE editcount_surges (
    project TEXT,
    editcount_P1key INTEGER,
    difference INTEGER,
    rolling_mean INTEGER,
    threshold_percentage INTEGER,
    event_cause TEXT
);
""")

con.execute("""
INSERT INTO editcount_surges VALUES 
('aawiki', 1, 150, 50, 20, 'Campaign name'),
('aawiktionary', 2, 20, 23, 20, 'Campaign name'),
('enwiki', 3, 60, 84, 20, 'Campaign name');
""")

con.execute("""
CREATE TABLE user_registration_surges (
    project TEXT,
    timestamp TIMESTAMP,
    new_users INTEGER,
    difference INTEGER,
    rolling_mean INTEGER,
    threshold_percentage INTEGER,
    event_cause TEXT
);
""")

print("✅ In-memory tables created and populated.")


# In[ ]:





# In[11]:


import duckdb
import pandas as pd

# Example DataFrame (replace this with API response DataFrame)
my_df = pd.DataFrame({
    "code": ["aa", "aa", "ab"],
    "local_name": ["Afar", "Afar", "Abkhazian"],
    "database_name": ["aawiki", "aawiktionary", "abwiki"],
    "site_name": ["Wikipedia", "Wiktionary", "Авикипедиа"],
    "url": [
        "https://aa.wikipedia.org",
        "https://aa.wiktionary.org",
        "https://ab.wikipedia.org"
    ]
})

# Connect to DuckDB (in-memory or file)
con = duckdb.connect()

# Create table from DataFrame
con.sql("CREATE TABLE community_alert_logs_table AS SELECT * FROM my_df")


# In[ ]:




