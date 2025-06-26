#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#!/usr/bin/env python3

import requests
import pandas as pd
import pymysql
import configparser
from datetime import datetime, timedelta, date

# --- Load Toolforge DB credentials ---
cfg = configparser.ConfigParser()
cfg.read('/data/project/community-activity-alerts-system/replica.my.cnf')
user = cfg['client']['user']
password = cfg['client']['password']

# --- Define constants ---
projects = [
    "uz.wikipedia.org",
    "en.wikipedia.org",
    "hi.wikipedia.org",
    "bn.wikipedia.org"
    # Add more as needed
]
base_url = "https://wikimedia.org/api/rest_v1/metrics/edits/aggregate"
editor_type = "all-editor-types"
page_type = "content"
granularity = "monthly"

# --- Calculate last month's date range ---
today = datetime.utcnow().date().replace(day=1)
last_month_end = today - timedelta(days=1)
last_month_start = last_month_end.replace(day=1)

from datetime import date
if last_month_end > date.today():
    print("Last month is in the future. Exiting.")
    exit(1)

start = last_month_start.strftime("%Y%m%d")
end = today.strftime("%Y%m%d")  # Use the 1st of the current month as the exclusive end date

# --- Connect to Toolforge DB ---
DB_NAME = 's56391__community_alerts'
DB_TABLE = 'edit_counts'

conn = pymysql.connect(
    host='tools.db.svc.wikimedia.cloud',
    user=user,
    password=password,
    database=DB_NAME,
    charset='utf8mb4',
    autocommit=True
)

cursor = conn.cursor()

# --- Ensure table exists ---
create_table_sql = f'''
CREATE TABLE IF NOT EXISTS {DB_TABLE} (
    timestamp DATETIME,
    edit_count INT,
    project VARCHAR(255),
    PRIMARY KEY (timestamp, project)
)
'''
cursor.execute(create_table_sql)

# --- Loop through each project and process ---
for project in projects:
    print(f"Fetching edits for {project} from {start} to {end}")

    url = f"{base_url}/{project}/{editor_type}/{page_type}/{granularity}/{start}/{end}"
    response = requests.get(url)
    if response.status_code != 200:
        print(f"API Error for {project}: {response.status_code} - {response.text}")
        continue

    try:
        data = response.json()
        edit_counts = data["items"][0]["results"]
        if not edit_counts:
            print(f"No data returned for {project}")
            continue
    except Exception as e:
        print(f"Parsing error for {project}: {e}")
        continue

    df = pd.DataFrame(edit_counts)
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    df['project'] = project
    df.rename(columns={'edits': 'edit_count'}, inplace=True)

    for _, row in df.iterrows():
        insert_sql = f"""
        INSERT INTO {DB_TABLE} (timestamp, edit_count, project)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE edit_count = VALUES(edit_count)
        """
        cursor.execute(
            insert_sql,
            (row['timestamp'].to_pydatetime(), int(row['edit_count']), row['project'])
        )

print("All data saved successfully.")

# --- Cleanup ---
cursor.close()
conn.close()

