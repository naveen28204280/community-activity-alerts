# Community Activity Alerts

## Overview

This project tracks and analyzes edit activity across Wikimedia projects. It consists of scripts for fetching, storing, and analyzing edit data, as well as a web interface for visualization.

## Script Descriptions

### fetch_and_store_cron.py

- **Purpose:** Fetches monthly edit counts for all Wikimedia projects from the Wikimedia API and stores them in the `edit_counts` table in the database.
- **How it works:**
  - Downloads edit count data for each project.
  - Ensures the `edit_counts` table exists.
  - Inserts or updates edit counts for each project and month.
- **Intended use:** Run regularly (e.g., as a cron job) to keep the edit counts up to date.

### fetch_and_store_script.py

- **Purpose:** Similar to `fetch_and_store_cron.py`; may be used for manual runs or testing.
- **How it works:**
  - Fetches edit counts from the Wikimedia API.
  - Stores results in the `edit_counts` table.
- **Intended use:** Manual or ad-hoc data fetching.

### Community alerts .py

- **Purpose:** Detects peaks (unusual spikes) in edit activity for each project and stores these as alerts in the `community_alerts` table.
- **How it works:**
  - Reads all edit data from `edit_counts`.
  - Runs a peak detection algorithm for each project.
  - Stores detected peaks in the `community_alerts` table.
- **Intended use:** Run after edit data is up to date, to analyze and record significant activity spikes.

## Database Tables

- `edit_counts`: Stores raw monthly edit counts for each project.
- `community_alerts`: Stores detected peaks/alerts for each project.

## Usage

1. Run `fetch_and_store_cron.py` to fetch and store edit counts.
2. Run `Community alerts .py` to analyze the data and store alerts.
3. Use the web interface (see `app.py`) to visualize and explore the results.

---
For more details, see the code and comments in each script.
