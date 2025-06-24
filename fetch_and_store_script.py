#!/usr/bin/env python
# coding: utf-8

# In[14]:


# fetch_edit_counts.py

import requests
import pandas as pd

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


# In[15]:


#from fetch_edit_counts import fetch_edit_counts

df = fetch_edit_counts("uz.wikipedia.org")
print(df.head())


# In[ ]:





# In[ ]:




