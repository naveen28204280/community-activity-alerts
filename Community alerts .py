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


# In[7]:


df = fetch_edit_counts("uz.wikipedia.org")
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


